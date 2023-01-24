#![allow(clippy::useless_transmute)]

use std::collections::HashMap;
use std::ffi::{CStr, c_void};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::{ptr, mem, ffi::CString};
use log::debug;
use once_cell::sync::OnceCell;
use freetds_sys::*;
use crate::command::CommandArg;
use crate::null::Null;
use crate::{error, parse_query, generate_query, Statement};
use crate::result_set::{ResultSet, Rows, Column, Row};
use crate::{property::Property, Result, error::Error, command::Command};
use crate::to_sql::ToSql;

type DiagnosticsMap = HashMap<usize,Box<dyn Fn(Error) + Send>>;

/* TODO: Investigate using cs_config(CS_USERDATA) and a Pin<Mutex<T>> */
struct Diagnostics {
    handlers: Mutex<DiagnosticsMap>
}

impl Diagnostics {
    fn new() -> Self {
        Diagnostics {
            handlers: Mutex::new(HashMap::new())
        }
    }

    fn set_handler(ctx: *const CS_CONTEXT, handler: Box<dyn Fn(Error) + Send>) {
        DIAGNOSTICS
            .get_or_init(Diagnostics::new)
            .handlers
            .lock()
            .unwrap()
            .insert(ctx as usize, handler);
    }

    fn remove_handler(ctx: *const CS_CONTEXT) {
        if let Some(diag) = DIAGNOSTICS.get() {
            diag.handlers
                .lock()
                .unwrap()
                .remove(&(ctx as usize));
        }
    }
}

static DIAGNOSTICS: OnceCell<Diagnostics> = OnceCell::new();

extern "C" fn csmsg_callback(ctx: *const CS_CONTEXT, msg: *const CS_CLIENTMSG) -> CS_RETCODE {
    let key = ctx as usize;
    let handlers = DIAGNOSTICS
        .get_or_init(Diagnostics::new)
        .handlers
        .lock()
        .unwrap();

    let handler = handlers.get(&key)
        .expect("context not found in Diagnostics::handlers");

    unsafe {
        handler(Error {
            type_: error::Type::Cs,
            code: Some((*msg).msgnumber),
            desc: CStr::from_ptr((*msg).msgstring.as_ptr()).to_string_lossy().trim_end().to_string(),
            severity: Some((*msg).severity),
        });
    }
    CS_SUCCEED
}

extern "C" fn clientmsg_callback(
    ctx: *const CS_CONTEXT,
    _conn: *const CS_CONNECTION,
    msg: *const CS_CLIENTMSG
) -> CS_RETCODE {
    let key = ctx as usize;
    let handlers = DIAGNOSTICS
        .get_or_init(Diagnostics::new)
        .handlers
        .lock()
        .unwrap();

    let handler = handlers.get(&key)
        .expect("context not found in Diagnostics::handlers");
    unsafe {
        handler(Error {
            type_: error::Type::Client,
            code: Some((*msg).msgnumber),
            desc: CStr::from_ptr((*msg).msgstring.as_ptr()).to_string_lossy().trim_end().to_string(),
            severity: Some((*msg).severity),
        });
    }
    CS_SUCCEED
}

extern "C" fn servermsg_callback(
    ctx: *const CS_CONTEXT,
    _conn: *const CS_CONNECTION,
    msg: *const CS_SERVERMSG
) -> CS_RETCODE {
    let key = ctx as usize;
    let handlers = DIAGNOSTICS
        .get_or_init(Diagnostics::new)
        .handlers
        .lock()
        .unwrap();

    let handler = handlers.get(&key)
        .expect("context not found in Diagnostics::handlers");
    unsafe {
        handler(Error {
            type_: error::Type::Server,
            code: Some((*msg).msgnumber),
            desc: CStr::from_ptr((*msg).text.as_ptr()).to_string_lossy().trim_end().to_string(),
            severity: Some((*msg).severity),
        });
    }
    CS_SUCCEED
}

#[derive(Debug, Clone, Default)]
struct Bind {
    buffer: Vec<u8>,
    data_length: i32,
    indicator: i16
}

type MessageCallback = Box<dyn Fn(&Error) -> bool + Send>;

pub(crate) struct CSConnection {
    pub ctx_handle: *mut CS_CONTEXT,
    pub conn_handle: *mut CS_CONNECTION,
    pub messages: Arc<Mutex<Vec<Error>>>,
    pub msg_callback: Arc<Mutex<Option<MessageCallback>>>,
}

impl CSConnection {
    pub fn new() -> Self {
        unsafe {
            let mut ctx_handle: *mut CS_CONTEXT = ptr::null_mut();
            let ret = cs_ctx_alloc(CS_VERSION_125, &mut ctx_handle);
            assert_eq!(CS_SUCCEED, ret);

            let messages = Arc::new(Mutex::new(Vec::new()));
            let msg_callback = Arc::new(Mutex::new(None::<Box<dyn Fn(&Error) -> bool + Send>>));

            let diag_messages = Arc::clone(&messages);
            let diag_callback = Arc::clone(&msg_callback);
            Diagnostics::set_handler(ctx_handle, Box::new(move |msg| {
                let callback = diag_callback.lock().unwrap();
                if let Some(f) = callback.as_ref() {
                    if !f(&msg) {
                        return;
                    }
                }

                diag_messages
                    .lock()
                    .unwrap()
                    .push(msg);
            }));

            let ret = cs_config(ctx_handle,
                CS_SET,
                CS_MESSAGE_CB,
                csmsg_callback as *mut c_void,
                mem::size_of_val(&csmsg_callback) as i32,
                ptr::null_mut());
            assert_eq!(CS_SUCCEED, ret);

            let ret = ct_init(ctx_handle, CS_VERSION_125);
            assert_eq!(CS_SUCCEED, ret);

            let mut conn_handle: *mut CS_CONNECTION = ptr::null_mut();
            let ret = ct_con_alloc(ctx_handle, &mut conn_handle);
            assert_eq!(CS_SUCCEED, ret);

            let ret = ct_callback(
                ctx_handle, 
                conn_handle, 
                CS_SET, 
                CS_CLIENTMSG_CB, 
                clientmsg_callback as *mut c_void);
            assert_eq!(CS_SUCCEED, ret);

            let ret = ct_callback(
                ctx_handle,
                conn_handle,
                CS_SET,
                CS_SERVERMSG_CB,
                servermsg_callback as *mut c_void);
            assert_eq!(CS_SUCCEED, ret);

            Self {
                ctx_handle,
                conn_handle,
                messages,
                msg_callback,
            }
        }
    }   

}


impl Drop for CSConnection {
    fn drop(&mut self) {
        unsafe {
            Diagnostics::remove_handler(self.ctx_handle);

            let ret = ct_con_drop(self.conn_handle);
            if ret != CS_SUCCEED {
                panic!("ct_con_drop failed");
            }

            let ret = ct_exit(self.ctx_handle, CS_UNUSED);
            if ret != CS_SUCCEED {
                ct_exit(self.ctx_handle, CS_FORCE_EXIT);
            }

            let ret = cs_ctx_drop(self.ctx_handle);
            if ret != CS_SUCCEED {
                panic!("cs_ctx_drop failed");
            }
        }
    }
}

#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Arc<Mutex<CSConnection>>,
    connected: bool
}

impl Connection {
    pub fn new() -> Self {
        let conn = Arc::new(Mutex::new(CSConnection::new()));
        let conn_guard = conn.lock().unwrap();
        drop(conn_guard);
        Self {
            conn,
            connected: false
        }
    }

    pub fn set_client_charset(&mut self, charset: impl AsRef<str>) -> Result<()> {
        self.set_props(CS_CLIENTCHARSET, Property::String(charset.as_ref()))
    }

    pub fn set_username(&mut self, username: impl AsRef<str>) -> Result<()> {
        self.set_props(CS_USERNAME, Property::String(username.as_ref()))
    }

    pub fn set_password(&mut self, password: impl AsRef<str>) -> Result<()> {
        self.set_props(CS_PASSWORD, Property::String(password.as_ref()))
    }

    pub fn set_database(&mut self, database: impl AsRef<str>) -> Result<()> {
        self.set_props(CS_DATABASE, Property::String(database.as_ref()))
    }

    pub fn set_tds_version_auto(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_AUTO))
    }

    pub fn set_tds_version_40(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_40))
    }

    pub fn set_tds_version_42(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_42))
    }

    pub fn set_tds_version_495(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_495))
    }

    pub fn set_tds_version_50(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_50))
    }

    pub fn set_tds_version_70(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_70))
    }

    pub fn set_tds_version_71(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_71))
    }

    pub fn set_tds_version_72(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_72))
    }

    pub fn set_tds_version_73(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_73))
    }

    pub fn set_tds_version_74(&mut self) -> Result<()> {
        self.set_props(CS_TDS_VERSION, Property::U32(CS_TDS_74))
    }

    pub fn set_login_timeout(&mut self, timeout: i32) -> Result<()> {
        self.set_props(CS_LOGIN_TIMEOUT, Property::I32(timeout))
    }

    pub fn set_timeout(&mut self, timeout: i32) -> Result<()> {
        self.set_props(CS_TIMEOUT, Property::I32(timeout))
    }

    fn set_props(&mut self, property: u32, value: Property) -> Result<()> {
        self.diag_clear();
        unsafe {
            let ret = match value {
                Property::I32(mut i) => {
                    ct_con_props(
                        self.conn.lock().unwrap().conn_handle,
                        CS_SET,
                        property as CS_INT,
                        std::mem::transmute(&mut i),
                        mem::size_of::<i32>() as i32,
                        ptr::null_mut())
                },
                Property::U32(mut i) => {
                    ct_con_props(
                        self.conn.lock().unwrap().conn_handle,
                        CS_SET,
                        property as CS_INT,
                        std::mem::transmute(&mut i),
                        mem::size_of::<u32>() as i32,
                        ptr::null_mut())
                },
                Property::String(s) => {
                    let s1 = CString::new(s)?;
                    ct_con_props(
                        self.conn.lock().unwrap().conn_handle,
                        CS_SET,
                        property as CS_INT,
                        std::mem::transmute(s1.as_ptr()),
                        s.len() as i32,
                        ptr::null_mut())
                },
                _ => {
                    return Err(Error::from_message("Invalid argument"));
                }
            };

            if ret == CS_SUCCEED {
                Ok(())
            } else {
                Err(self.get_error().unwrap_or_else(|| Error::from_message("ct_con_props failed")))
            }
        }
    }

    pub fn connect(&mut self, server_name: impl AsRef<str>) -> Result<()> {
        if self.connected {
            return Err(Error::from_message("Invalid connection state"));
        }

        self.diag_clear();
        let server_name = CString::new(server_name.as_ref())?;
        let ret;
        unsafe {
            ret = ct_connect(
                self.conn.lock().unwrap().conn_handle,
                mem::transmute(server_name.as_ptr()),
                CS_NULLTERM);
        }
        if ret == CS_SUCCEED {
            self.connected = true;
            Ok(())
        } else {
            let err = self.get_error();
            Err(err.unwrap_or_else(|| Error::from_message("ct_connect failed")))
        }
    }

    pub fn execute(&mut self, text: impl AsRef<str>, params: &[&dyn ToSql]) -> Result<ResultSet> {
        if !self.connected {
            return Err(Error::from_message("Invalid connection state"));
        }
        let parsed_query = parse_query(text.as_ref());
        if parsed_query.params.len() != params.len() {
            return Err(Error::from_message("Invalid parameter count"))
        }

        let text = generate_query(
            &parsed_query,
            params
                .iter()
                .copied()
        );

        let mut command = Command::new(self.clone());
        command.command(CS_LANG_CMD, CommandArg::String(&text), CS_UNUSED)?;
        command.send()?;

        let mut results: Vec<Rows> = Vec::new();
        let mut status_result: Option<i32> = None;
        let mut failed = false;
        let mut errors: Vec<Error> = Vec::new();
        loop {
            let (ret, res_type) = command.results()?;
            if !ret {
                break;
            }

            /*
                Collect diag messages because command.results() clears them
            */
            errors.extend(self.diag_get().iter().cloned());

            match res_type {
                CS_ROW_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    results.push(row_result);
                },
                CS_STATUS_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    let row: &Vec<u8> = row_result.rows[0].buffers[0].as_ref().unwrap();
                    let status: i32;
                    unsafe {
                        let buf: *const i32 = mem::transmute(row.as_ptr());
                        status = *buf;
                    }

                    match status_result {
                        None => {
                            status_result = Some(status)
                        },
                        Some(s) => {
                            if s == 0 {
                                status_result = Some(status);
                            }
                        }
                    };
                },
                CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT => {
                    command.cancel(CS_CANCEL_CURRENT)?;
                },
                CS_CMD_FAIL => {
                    failed = true;
                },
                _ => {
                    /* Do nothing, most notably, ignore CS_CMD_SUCCEED and CS_CMD_DONE */
                }
            }
        }

        if failed {
            if errors.is_empty() {
                return Err(Error::from_message("Query execution resulted in error"));
            } else {
                return Err(errors.last().unwrap().clone());
            }
        }

        Ok(ResultSet::new(self.clone(), results, status_result, errors))
    }

    pub fn execute_statement(&mut self, st: &Statement) -> Result<ResultSet> {
        if !self.connected {
            return Err(Error::from_message("Invalid connection state"));
        }
        
        let params: Vec<&dyn ToSql> = st.params.iter()
            .map(|param| {
                match param {
                    None => &Null {} as &dyn ToSql,
                    Some(param) => param as &dyn ToSql
                }
            })
            .collect();
        let text = generate_query(
            &st.query,
            params.iter().copied()
        );
        debug!("Generated statement: {}", text);

        let mut command = Command::new(self.clone());
        command.command(CS_LANG_CMD, CommandArg::String(&text), CS_UNUSED)?;
        command.send()?;

        let mut results: Vec<Rows> = Vec::new();
        let mut status_result: Option<i32> = None;
        let mut failed = false;
        let mut errors: Vec<Error> = Vec::new();
        loop {
            let (ret, res_type) = command.results()?;
            if !ret {
                break;
            }

            /*
                Collect diag messages because command.results() clears them
            */
            errors.extend(self.diag_get().iter().cloned());

            match res_type {
                CS_ROW_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    results.push(row_result);
                },
                CS_STATUS_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    let row: &Vec<u8> = row_result.rows[0].buffers[0].as_ref().unwrap();
                    let status: i32;
                    unsafe {
                        let buf: *const i32 = mem::transmute(row.as_ptr());
                        status = *buf;
                    }

                    match status_result {
                        None => {
                            status_result = Some(status)
                        },
                        Some(s) => {
                            if s == 0 {
                                status_result = Some(status);
                            }
                        }
                    };
                },
                CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT => {
                    command.cancel(CS_CANCEL_CURRENT)?;
                },
                CS_CMD_FAIL => {
                    failed = true;
                },
                _ => {
                    /* Do nothing, most notably, ignore CS_CMD_SUCCEED and CS_CMD_DONE */
                }
            }
        }

        if failed {
            if errors.is_empty() {
                return Err(Error::from_message("Query execution resulted in error"));
            } else {
                return Err(errors.last().unwrap().clone());
            }
        }

        Ok(ResultSet::new(self.clone(), results, status_result, errors))
    }

    fn fetch_result(cmd: &mut Command) -> Result<Rows> {
        let ncols: usize = cmd
            .res_info(CS_NUMDATA)
            .unwrap();
        let mut binds: Vec<Bind> = vec![Default::default(); ncols];
        let mut columns: Vec<Column> = vec![Default::default(); ncols];
        for col_idx in 0..ncols {
            let bind = &mut binds[col_idx];
            let column = &mut columns[col_idx];

            column.fmt = cmd.describe((col_idx + 1) as i32)?;
            column.fmt.format = CS_FMT_UNUSED as i32;
            match column.fmt.datatype {
                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                    column.fmt.maxlength += 1;
                    column.fmt.format = CS_FMT_NULLTERM as i32;
                },
                _ => {}
            }
            bind.buffer.resize(column.fmt.maxlength as usize, 0);
            column.fmt.count = 1;
            let name_slice: Vec<u8> = column.fmt.name.iter()
                .take(column.fmt.namelen as usize)
                .map(|c| *c as u8)
                .collect();
            column.name = String::from(String::from_utf8_lossy(&name_slice));

            unsafe {
                cmd.bind_unsafe(
                    (col_idx + 1) as i32,
                    &mut column.fmt,
                    mem::transmute(bind.buffer.as_mut_ptr()),
                    &mut bind.data_length,
                    &mut bind.indicator)?;
            }
        }

        let mut rows: Vec<Row> = Vec::new();
        while cmd.fetch()? {
            let mut row = Row { buffers: Vec::new() };
            for col_idx in 0..ncols {
                let bind = &binds[col_idx];
                match bind.indicator {
                    -1 => {
                        row.buffers.push(None);
                    },
                    0 => {
                        let len = bind.data_length as usize;
                        let buffer: Vec<u8> = match columns[col_idx].fmt.datatype {
                            CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                                Vec::from(&bind.buffer.as_slice()[0..len-1])
                            },
                            _ => {
                                Vec::from(&bind.buffer.as_slice()[0..len])
                            }
                        };

                        row.buffers.push(Some(Rc::new(buffer)));
                    },
                    _ => {
                        return Err(Error::from_message("Data truncation occured"));
                    }
                }
            }
            rows.push(row);
        }
        Ok(Rows::new(columns, rows))
    }

    pub fn diag_clear(&mut self) {
        self.conn
            .lock()
            .unwrap()
            .messages
            .lock()
            .unwrap()
            .clear();
    }

    fn diag_get(&mut self) -> Vec<Error> {
        self.conn
            .lock()
            .unwrap()
            .messages
            .lock()
            .unwrap()
            .clone()
    }

    pub fn get_error(&mut self) -> Option<Error> {
        let errors = self.diag_get();
        errors.iter()
            .find(|e| e.severity.unwrap_or(i32::MAX) > 10)
            .cloned()
    }

    pub fn is_connected(&mut self) -> bool {
        if !self.connected {
            return false;
        }

        let ret;
        let status: i32 = Default::default();
        self.diag_clear();
        unsafe {
            ret = ct_con_props(
                self.conn.lock().unwrap().conn_handle,
                CS_GET,
                CS_CON_STATUS as i32,
                mem::transmute(&status),
                CS_UNUSED,
                ptr::null_mut());
        }

        if ret != CS_SUCCEED {
            false
        } else {
            status == CS_CONSTAT_CONNECTED
        }
    }

    pub fn db_name(&mut self) -> Result<String> {
        let mut rs = self.execute("select db_name()", &[])?;
        assert!(rs.next());
        rs.get_string(0)?.ok_or_else(|| Error::from_message("Cannot get database name"))
    }

    pub(crate) fn convert(&mut self, srcfmt: &CS_DATAFMT, srcdata: &[u8], dstfmt: &CS_DATAFMT, dstdata: &mut [u8]) -> Result<usize> {
        self.diag_clear();
        let mut dstlen: i32 = Default::default();
        let ret;
        unsafe {
            ret = cs_convert(
                self.conn.lock().unwrap().ctx_handle,
                mem::transmute(srcfmt as *const CS_DATAFMT),
                mem::transmute(srcdata.as_ptr()),
                mem::transmute(dstfmt as *const CS_DATAFMT),
                mem::transmute(dstdata.as_mut_ptr()),
                &mut dstlen);
        }
        if ret != CS_SUCCEED {
            Err(self.get_error().unwrap_or_else(|| Error::from_message("cs_convert failed")))
        } else {
            Ok(dstlen as usize)
        }
    }

    unsafe fn dt_crack_unsafe<T>(&mut self, type_: i32, dateval: *const T) -> Result<CS_DATEREC> {
        let mut daterec: CS_DATEREC = Default::default();
        let ret;
        {
            ret = cs_dt_crack(self.conn.lock().unwrap().ctx_handle, type_, mem::transmute(dateval), &mut daterec);
        }
        if ret != CS_SUCCEED {
            return Err(self.get_error().unwrap_or_else(|| Error::from_message("cs_dt_crack failed")));
        }
        Ok(daterec)
    }

    pub fn crack_date(&mut self, val: CS_DATE) -> Result<CS_DATEREC> {
        unsafe {
            self.dt_crack_unsafe(CS_DATE_TYPE, &val)
        }
    }

    pub fn crack_time(&mut self, val: CS_TIME) -> Result<CS_DATEREC> {
        unsafe {
            self.dt_crack_unsafe(CS_TIME_TYPE, &val)
        }
    }

    pub fn crack_datetime(&mut self, val: CS_DATETIME) -> Result<CS_DATEREC> {
        unsafe {
            self.dt_crack_unsafe(CS_DATETIME_TYPE, &val)
        }
    }

    pub fn crack_smalldatetime(&mut self, val: CS_DATETIME4) -> Result<CS_DATEREC> {
        unsafe {
            self.dt_crack_unsafe(CS_DATETIME4_TYPE, &val)
        }
    }

    pub fn set_message_callback(&mut self, callback: Box<dyn Fn(&Error) -> bool + Send>) {
        *self.conn
            .lock().unwrap()
            .msg_callback.lock().unwrap() = Some(callback);
    }

    pub fn clear_message_callback(&mut self) {
        *self.conn
            .lock().unwrap()
            .msg_callback.lock().unwrap() = None;
    }

}

impl Default for Connection {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for Connection {}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;
    use chrono::{NaiveDate, NaiveTime};
    use rust_decimal::Decimal;
    use crate::{parse_query, generate_query, Statement, ParamValue};
    use crate::to_sql::ToSql;
    use super::Connection;

    const SERVER: &str = "***REMOVED***";

    fn connect() -> Connection {
        let mut conn = Connection::new();
        conn.set_client_charset("UTF-8").unwrap();
        conn.set_username("sa").unwrap();
        conn.set_password("").unwrap();
        conn.set_database("master").unwrap();
        conn.set_tds_version_50().unwrap();
        conn.set_login_timeout(5).unwrap();
        conn.set_timeout(5).unwrap();
        conn.connect(&format!("{}:2025", SERVER)).unwrap();
        conn
    }

    #[test]
    fn test_select() {
        let mut conn = connect();
        let text = 
            "select \
                'aaaa', \
                2, 5000000000, \
                3.14, \
                cast('1986/07/05 10:30:31.1' as datetime), \
                cast(3.14 as numeric(18,2)), \
                cast(0xDEADBEEF as image), \
                cast('ccc' as text), \
                null";
        let mut rs = conn.execute(&text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(rs.get_string(0).unwrap().unwrap(), "aaaa");
        assert_eq!(rs.get_i32(1).unwrap().unwrap(), 2);
        assert_eq!(rs.get_i64(2).unwrap().unwrap(), 5000000000);
        assert_eq!(rs.get_f64(3).unwrap().unwrap(), 3.14);
        assert_eq!(rs.get_datetime(4).unwrap().unwrap(), NaiveDate::from_ymd_opt(1986, 7, 5).unwrap().and_hms_milli_opt(10, 30, 31, 100).unwrap());
        assert_eq!(rs.get_f64(5).unwrap().unwrap(), 3.14);
        assert_eq!(rs.get_blob(6).unwrap().unwrap(), vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(rs.get_string(7).unwrap().unwrap(), "ccc".to_string());
        assert!(rs.get_string(8).unwrap().is_none());
        assert!(!rs.next());
    }

    #[test]
    fn test_get_value() {
        let mut conn = connect();
        let text = 
            "
            select
                cast(0xDEADBEEF as binary(4)) as binary,
                cast(0xDEADBEEF as image) as image,
                cast('deadbeef' as char(8)) as char,
                cast('deadbeef' as text) as text,
                cast('deadbeef' as unichar(8)) as unichar,
                cast('1986-07-04' as date) as date,
                cast('10:01:02.3' as time) as time,
                cast('1986-07-04 10:01:02.3' as datetime) as datetime,
                cast(2147483647 as int) as int,
                cast(1 as bit) as bit,
                cast(2 as tinyint) as tinyint,
                cast(3 as smallint) as smallint,
                cast(1.23456789 as numeric(9,8)) as numeric,
                cast(2147483648 as numeric) as long,
                cast(42.0 as real) as real,
                cast(1.23456789 as float) as float            
            ";
        let mut rs = conn.execute(text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(ParamValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])), rs.get_value("binary").unwrap());
        assert_eq!(Some(ParamValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])), rs.get_value("image").unwrap());
        assert_eq!(Some(ParamValue::String(String::from("deadbeef"))), rs.get_value("char").unwrap());
        assert_eq!(Some(ParamValue::String(String::from("deadbeef"))), rs.get_value("text").unwrap());
        assert_eq!(Some(ParamValue::String(String::from("deadbeef"))), rs.get_value("unichar").unwrap());
        assert_eq!(Some(ParamValue::Date(NaiveDate::from_ymd_opt(1986, 7, 4).unwrap())), rs.get_value("date").unwrap());
        assert_eq!(Some(ParamValue::Time(NaiveTime::from_hms_milli_opt(10, 1, 2, 300).unwrap())), rs.get_value("time").unwrap());
        assert_eq!(Some(ParamValue::DateTime(
            NaiveDate::from_ymd_opt(1986, 7, 4)
                .unwrap()
                .and_hms_milli_opt(10, 1, 2, 300)
                .unwrap())),
            rs.get_value("datetime").unwrap());
        assert_eq!(Some(ParamValue::I32(2147483647)), rs.get_value("int").unwrap());
        assert_eq!(Some(ParamValue::I32(1)), rs.get_value("bit").unwrap());
        assert_eq!(Some(ParamValue::I32(2)), rs.get_value("tinyint").unwrap());
        assert_eq!(Some(ParamValue::I32(3)), rs.get_value("smallint").unwrap());
        assert_eq!(Some(ParamValue::Decimal(Decimal::from_str_exact("1.23456789").unwrap())), rs.get_value("numeric").unwrap());
        assert_eq!(Some(ParamValue::I64(2147483648)), rs.get_value("long").unwrap());
        assert_eq!(Some(ParamValue::F64(42.0)), rs.get_value("real").unwrap());
        assert_eq!(Some(ParamValue::F64(1.23456789)), rs.get_value("float").unwrap());
    }

    #[test]
    fn test_execution_failure() {
        let mut conn = connect();
        let text = 
            "selecta \
                'aaaa', \
                2, 5000000000, \
                3.14, \
                cast('1986/07/05 10:30:31.1' as datetime), \
                cast(3.14 as numeric(18,2)), \
                cast(0xDEADBEEF as image), \
                cast('ccc' as text), \
                null";
        let ret = conn.execute(text, &[]);
        assert!(ret.is_err());
        assert_eq!("Incorrect syntax near '('.", ret.err().unwrap().desc());
    }

    #[test]
    fn test_params() {
        let mut conn = connect();
        let text = "\
            create table #test(\
                col1 varchar(10), \
                col2 int, \
                col3 numeric, \
                col4 numeric(18,2), \
                col5 datetime, \
                col6 image, \
                col7 text)";
        conn.execute(text, &[]).unwrap();

        let text = "\
            insert into #test(col1, col2, col3, col4, col5, col6, col7) \
            values(?, ?, ?, ?, ?, ?, ?)";
        conn
            .execute(text, &[
                &"aaa",
                &2i32,
                &5000000000i64,
                &3.14f64,
                &NaiveDate::from_ymd_opt(1986, 7, 5).unwrap().and_hms_milli_opt(10, 30, 31, 100).unwrap(),
                &vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8],
                &"bbb"
            ])
            .unwrap();
        
        let text = "select * from #test";
        let mut rs = conn.execute(text, &[]).unwrap();
        assert!(rs.next());
        let ncols: usize = rs.column_count().unwrap();
        assert_eq!(ncols, 7);
        assert_eq!("aaa", rs.get_string(0).unwrap().unwrap());
        assert_eq!(2, rs.get_i32(1).unwrap().unwrap());
        assert_eq!(5000000000, rs.get_i64(2).unwrap().unwrap());
        assert_eq!(3.14, rs.get_f64(3).unwrap().unwrap());
        assert_eq!(NaiveDate::from_ymd_opt(1986, 7, 5).unwrap().and_hms_milli_opt(10, 30, 31, 100).unwrap(), rs.get_datetime(4).unwrap().unwrap());
        assert_eq!(vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8], rs.get_blob(5).unwrap().unwrap());
        assert_eq!("bbb", rs.get_string(6).unwrap().unwrap());
    }

    #[test]
    fn test_multiple_rows() {
        let mut conn = connect();

        let text = "select 1 union select 2 union select 3";
        let mut rs = conn.execute(text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(1, rs.get_i32(0).unwrap().unwrap());
        assert!(rs.next());
        assert_eq!(2, rs.get_i32(0).unwrap().unwrap());
        assert!(rs.next());
        assert_eq!(3, rs.get_i32(0).unwrap().unwrap());
        assert!(!rs.next());
    }

    #[test]
    fn test_generate_query() {
        let s = "string: ?, i32: ?, i64: ?, f64: ?, date: ?, image: ?";
        let mut params: Vec<&dyn ToSql> = Vec::new();
        params.push(&"aaa");
        params.push(&1i32);
        params.push(&2i64);
        params.push(&3.14f64);

        let param5 = NaiveDate::from_ymd_opt(1986, 7, 5).unwrap().and_hms_opt(10, 30, 31).unwrap();
        params.push(&param5);

        let param6 = vec![0xDE_u8, 0xAD_u8, 0xBE_u8, 0xEF_u8];
        params.push(&param6);

        let parsed_query = parse_query(s);
        let generated = generate_query(&parsed_query, params.iter().map(|param| *param));
        assert_eq!("string: 'aaa', i32: 1, i64: 2, f64: 3.14, date: '1986/07/05 10:30:31', image: 0xDEADBEEF", generated);
    }

    #[test]
    fn test_utf8() {
        let mut conn = connect();
        conn
            .execute("if not exists(select id from sysobjects where type='U' and name='freetds_rs_test') execute('create table freetds_rs_test(c varchar(10))')", &[])
            .unwrap();
        conn
            .execute("insert into freetds_rs_test(c) values(?)", &[ &"éçàèä" ])
            .unwrap();
        
        let text = "select c from freetds_rs_test";
        let mut rs = conn.execute(&text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(rs.get_string(0).unwrap().unwrap(), "éçàèä");

        let text = "select 'éçàèä'";
        let mut rs = conn.execute(&text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(rs.get_string(0).unwrap().unwrap(), "éçàèä");

        conn
            .execute("drop table freetds_rs_test", &[])
            .unwrap();
    }

    #[test]
    fn test_multiple_threads() {
        let mut conn = connect();
        let t0 = thread::spawn(move ||{
            thread::sleep(Duration::from_millis(500));
            let mut rs = conn.execute("select getdate()", &[]).unwrap();
            while rs.next() {
                println!("[0] {}", rs.get_string(0).unwrap().unwrap());
            }
        });

        let mut conn = connect();
        let t1 = thread::spawn(move ||{
            thread::sleep(Duration::from_millis(500));
            let mut rs = conn.execute("select getdate()", &[]).unwrap();
            while rs.next() {
                println!("[1] {}", rs.get_string(0).unwrap().unwrap());
            }
        });

        t0.join().unwrap();
        t1.join().unwrap();
    }

    #[test]
    fn test_status_result() {
        let mut conn = connect();

        let res = conn.execute("sp_locklogin test123, 'lock'", &[]);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.status().is_some());
        assert_eq!(0, res.status().unwrap());

        let res = conn.execute("sp_locklogin all_your_base_are_belong_to_us, 'lock'", &[]);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.status().is_some());
        assert_eq!(1, res.status().unwrap());
        assert_eq!("No such account -- nothing changed.", res.error().unwrap().desc());
    }

    #[test]
    fn test_statement() {
        let mut st = Statement::new("select ?, :param1, :param2, ?");
        st.set_param(0, 1);
        st.set_param("param1", 2);
        st.set_param("param2", "3");
        st.set_param(3, "4");

        let mut conn = connect();
        let mut rs = conn.execute_statement(&st).unwrap();
        assert_eq!(rs.next(), true);
        assert_eq!(rs.get_i32(0).unwrap(), Some(1));
        assert_eq!(rs.get_i32(1).unwrap(), Some(2));
        assert_eq!(rs.get_string(2).unwrap(), Some(String::from("3")));
        assert_eq!(rs.get_string(3).unwrap(), Some(String::from("4")));
    }

    #[test]
    fn test_database() {
        /* Test connecting correctly sets the database */
        let mut conn = Connection::new();
        conn.set_client_charset("UTF-8").unwrap();
        conn.set_username("sa").unwrap();
        conn.set_password("").unwrap();
        conn.set_database("master").unwrap();
        conn.set_tds_version_50().unwrap();
        conn.set_login_timeout(5).unwrap();
        conn.set_timeout(5).unwrap();
        conn.connect(&format!("{}:2025", SERVER)).unwrap();

        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(String::from("master")), rs.get_string(0).unwrap());

        let mut conn = Connection::new();
        conn.set_client_charset("UTF-8").unwrap();
        conn.set_username("sa").unwrap();
        conn.set_password("").unwrap();
        conn.set_database("sybsystemprocs").unwrap();
        conn.set_tds_version_50().unwrap();
        conn.set_login_timeout(5).unwrap();
        conn.set_timeout(5).unwrap();
        conn.connect(&format!("{}:2025", SERVER)).unwrap();

        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(String::from("sybsystemprocs")), rs.get_string(0).unwrap());
    }

    #[test]
    fn test_db_name() {
        let mut conn = Connection::new();
        conn.set_client_charset("UTF-8").unwrap();
        conn.set_username("sa").unwrap();
        conn.set_password("").unwrap();
        conn.set_database("master").unwrap();
        conn.set_tds_version_50().unwrap();
        conn.set_login_timeout(5).unwrap();
        conn.set_timeout(5).unwrap();
        conn.connect(&format!("{}:2025", SERVER)).unwrap();

        assert_eq!(String::from("master"), conn.db_name().unwrap());
        conn.execute("use sybsystemprocs", &[]).unwrap();

        assert_eq!(String::from("sybsystemprocs"), conn.db_name().unwrap());
    }

    #[test]
    fn test_message_callback() {
        let mut conn = connect();

        let msg = Arc::new(Mutex::new(RefCell::new(None::<String>)));
        let msg2 = Arc::clone(&msg);
        conn.set_message_callback(Box::new(move |e| {
            *msg2.lock().unwrap().borrow_mut() = Some(e.desc().to_string());
            true
        }));
        let text = 
            "selecta \
                'aaaa', \
                2, 5000000000, \
                3.14, \
                cast('1986/07/05 10:30:31.1' as datetime), \
                cast(3.14 as numeric(18,2)), \
                cast(0xDEADBEEF as image), \
                cast('ccc' as text), \
                null";
        let ret = conn.execute(text, &[]);
        assert!(ret.is_err());
        assert_eq!("Incorrect syntax near '('.", ret.err().unwrap().desc());

        assert!(msg.lock().unwrap().borrow().is_some());
        assert_eq!("Incorrect syntax near '('.", msg.lock().unwrap().borrow().as_ref().unwrap());
    }

}

