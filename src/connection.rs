#![allow(clippy::useless_transmute)]

use crate::command::CommandArg;
use crate::null::Null;
use crate::result_set::{Column, ResultSet, Row, Rows, SybResult};
use crate::to_sql::ToSql;
use crate::{command::Command, error::Error, property::Property, Result};
use crate::{error, generate_query, parse_query, Statement};
use freetds_sys::*;
use log::debug;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::{ffi::CString, mem, ptr};

type DiagnosticsMap = HashMap<usize, Box<dyn Fn(Error) + Send>>;

/* TODO: Investigate using cs_config(CS_USERDATA) and a Pin<Mutex<T>> */
struct Diagnostics {
    handlers: Mutex<DiagnosticsMap>,
}

impl Diagnostics {
    fn new() -> Self {
        Diagnostics {
            handlers: Mutex::new(HashMap::new()),
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
            diag.handlers.lock().unwrap().remove(&(ctx as usize));
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

    let handler = handlers
        .get(&key)
        .expect("context not found in Diagnostics::handlers");

    unsafe {
        handler(Error {
            type_: error::Type::Cs,
            code: Some((*msg).msgnumber),
            desc: CStr::from_ptr((*msg).msgstring.as_ptr())
                .to_string_lossy()
                .trim_end()
                .to_string(),
            severity: Some((*msg).severity),
        });
    }
    CS_SUCCEED
}

extern "C" fn clientmsg_callback(
    ctx: *const CS_CONTEXT,
    _conn: *const CS_CONNECTION,
    msg: *const CS_CLIENTMSG,
) -> CS_RETCODE {
    let key = ctx as usize;
    let handlers = DIAGNOSTICS
        .get_or_init(Diagnostics::new)
        .handlers
        .lock()
        .unwrap();

    let handler = handlers
        .get(&key)
        .expect("context not found in Diagnostics::handlers");
    unsafe {
        handler(Error {
            type_: error::Type::Client,
            code: Some((*msg).msgnumber),
            desc: CStr::from_ptr((*msg).msgstring.as_ptr())
                .to_string_lossy()
                .trim_end()
                .to_string(),
            severity: Some((*msg).severity),
        });
    }
    CS_SUCCEED
}

extern "C" fn servermsg_callback(
    ctx: *const CS_CONTEXT,
    _conn: *const CS_CONNECTION,
    msg: *const CS_SERVERMSG,
) -> CS_RETCODE {
    let key = ctx as usize;
    let handlers = DIAGNOSTICS
        .get_or_init(Diagnostics::new)
        .handlers
        .lock()
        .unwrap();

    let handler = handlers
        .get(&key)
        .expect("context not found in Diagnostics::handlers");
    unsafe {
        handler(Error {
            type_: error::Type::Server,
            code: Some((*msg).msgnumber),
            desc: CStr::from_ptr((*msg).text.as_ptr())
                .to_string_lossy()
                .trim_end()
                .to_string(),
            severity: Some((*msg).severity),
        });
    }
    CS_SUCCEED
}

#[derive(Debug, Clone, Default)]
struct Bind {
    buffer: Vec<u8>,
    data_length: i32,
    indicator: i16,
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
            Diagnostics::set_handler(
                ctx_handle,
                Box::new(move |msg| {
                    let callback = diag_callback.lock().unwrap();
                    if let Some(f) = callback.as_ref() {
                        if !f(&msg) {
                            return;
                        }
                    }

                    diag_messages.lock().unwrap().push(msg);
                }),
            );

            let ret = cs_config(
                ctx_handle,
                CS_SET,
                CS_MESSAGE_CB,
                csmsg_callback as *mut c_void,
                mem::size_of_val(&csmsg_callback) as i32,
                ptr::null_mut(),
            );
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
                clientmsg_callback as *mut c_void,
            );
            assert_eq!(CS_SUCCEED, ret);

            let ret = ct_callback(
                ctx_handle,
                conn_handle,
                CS_SET,
                CS_SERVERMSG_CB,
                servermsg_callback as *mut c_void,
            );
            assert_eq!(CS_SUCCEED, ret);

            Self {
                ctx_handle,
                conn_handle,
                messages,
                msg_callback,
            }
        }
    }

    fn diag_clear(&mut self) {
        self.messages.lock().unwrap().clear();
    }

    fn diag_get(&mut self) -> MutexGuard<Vec<Error>> {
        self.messages.lock().unwrap()
    }

    fn get_error(&mut self) -> Option<Error> {
        self.diag_get()
            .iter()
            .find(|e| e.severity.unwrap_or(i32::MAX) > 10)
            .cloned()
    }

    fn set_prop(&mut self, property: u32, value: Property) -> Result<()> {
        self.diag_clear();
        unsafe {
            let ret = match value {
                Property::I32(mut i) => ct_con_props(
                    self.conn_handle,
                    CS_SET,
                    property as CS_INT,
                    std::mem::transmute(&mut i),
                    mem::size_of::<i32>() as i32,
                    ptr::null_mut(),
                ),
                Property::U32(mut i) => ct_con_props(
                    self.conn_handle,
                    CS_SET,
                    property as CS_INT,
                    std::mem::transmute(&mut i),
                    mem::size_of::<u32>() as i32,
                    ptr::null_mut(),
                ),
                Property::String(s) => {
                    let s1 = CString::new(s)?;
                    ct_con_props(
                        self.conn_handle,
                        CS_SET,
                        property as CS_INT,
                        std::mem::transmute(s1.as_ptr()),
                        s.len() as i32,
                        ptr::null_mut(),
                    )
                }
                _ => {
                    return Err(Error::from_message("Invalid argument"));
                }
            };

            if ret == CS_SUCCEED {
                Ok(())
            } else {
                Err(self
                    .get_error()
                    .unwrap_or_else(|| Error::from_message("ct_con_props failed")))
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

#[derive(Clone,Copy,Debug)]
pub enum TdsVersion {
    Auto,
    Tds40,
    Tds42,
    Tds495,
    Tds50,
    Tds70,
    Tds72,
    Tds73,
    Tds74,
}

#[derive(Default,Clone,Debug)]
pub struct ConnectionBuilder {
    host: Option<String>,
    port: Option<u16>,
    server_name: Option<String>,
    client_charset: Option<String>,
    username: Option<String>,
    password: Option<String>,
    database: Option<String>,
    tds_version: Option<TdsVersion>,
    login_timeout: Option<i32>,
    timeout: Option<i32>,
}

impl ConnectionBuilder {
    pub fn server_name(mut self, server: &str) -> Self {
        self.server_name = Some(server.to_string());
        self
    }

    pub fn host(mut self, host: &str) -> Self {
        self.host = Some(host.to_string());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn client_charset(mut self, charset: &str) -> Self {
        self.client_charset = Some(charset.to_string());
        self
    }

    pub fn username(mut self, username: &str) -> Self {
        self.username = Some(username.to_string());
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    pub fn database(mut self, database: &str) -> Self {
        self.database = Some(database.to_string());
        self
    }

    pub fn get_database(&self) -> Option<String> {
        self.database.clone()
    }

    pub fn tds_version(mut self, version: TdsVersion) -> Self {
        self.tds_version = Some(version);
        self
    }

    pub fn login_timeout(mut self, timeout: i32) -> Self {
        self.login_timeout = Some(timeout);
        self
    }

    pub fn timeout(mut self, timeout: i32) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn connect(&self) -> Result<Connection> {
        let mut conn = CSConnection::new();
        conn.diag_clear();

        if let Some(charset) = self.client_charset.as_ref() {
            conn.set_prop(CS_CLIENTCHARSET, Property::String(charset))?;
        }

        if let Some(username) = self.username.as_ref() {
            conn.set_prop(CS_USERNAME, Property::String(username))?;
        }

        if let Some(password) = self.password.as_ref() {
            conn.set_prop(CS_PASSWORD, Property::String(password))?;
        }

        if let Some(database) = self.database.as_ref() {
            conn.set_prop(CS_DATABASE, Property::String(database))?;
        }

        if let Some(tds_version) = self.tds_version.as_ref() {
            let tdsver = match tds_version {
                TdsVersion::Auto => CS_TDS_AUTO,
                TdsVersion::Tds40 => CS_TDS_40,
                TdsVersion::Tds42 => CS_TDS_42,
                TdsVersion::Tds495 => CS_TDS_495,
                TdsVersion::Tds50 => CS_TDS_50,
                TdsVersion::Tds70 => CS_TDS_70,
                TdsVersion::Tds72 => CS_TDS_72,
                TdsVersion::Tds73 => CS_TDS_73,
                TdsVersion::Tds74 => CS_TDS_74,
            };
            conn.set_prop(CS_TDS_VERSION, Property::U32(tdsver))?;
        }

        if let Some(login_timeout) = self.login_timeout.as_ref() {
            conn.set_prop(CS_LOGIN_TIMEOUT, Property::I32(*login_timeout))?;
        }

        if let Some(timeout) = self.timeout.as_ref() {
            conn.set_prop(CS_TIMEOUT, Property::I32(*timeout))?;
        }

        let server_name = match self.server_name.as_ref() {
            Some(server_name) => {
                server_name.clone()
            },
            None => {
                if let Some(host) = self.host.as_ref() {
                    format!("{}:{}",
                        host,
                        self.port.as_ref().unwrap_or(&5000))
                } else {
                    return Err(Error::from_message("Server host address not configured"));
                }
            },
        };

        let cserver_name = CString::new(server_name)?;
        let ret = unsafe {
            ct_connect(
                conn.conn_handle,
                mem::transmute(cserver_name.as_ptr()),
                CS_NULLTERM,
            )
        };
        if ret != CS_SUCCEED {
            return Err(conn
                        .get_error()
                        .unwrap_or_else(|| Error::from_failure("ct_connect")))
        }

        Ok(Connection::new(conn))
    }
}

#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Arc<Mutex<CSConnection>>,
}

impl Connection {
    fn new(conn: CSConnection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn))
        }
    }

    pub fn builder() -> ConnectionBuilder {
        ConnectionBuilder::default()
    }

    pub fn execute(&mut self, text: impl AsRef<str>, params: &[&dyn ToSql]) -> Result<ResultSet> {
        let parsed_query = parse_query(text.as_ref());
        if parsed_query.params.len() != params.len() {
            return Err(Error::from_message("Invalid parameter count"));
        }

        let text = generate_query(&parsed_query, params.iter().copied());

        let mut command = Command::new(self.clone());
        command.command(CS_LANG_CMD, CommandArg::String(&text), CS_UNUSED)?;
        command.send()?;

        let mut results: Vec<SybResult> = Vec::new();
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
                    results.push(SybResult::Rows(row_result));
                },
                CS_STATUS_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    let row: &Vec<u8> = row_result.rows[0].buffers[0].as_ref().unwrap();
                    let status = unsafe {
                        let buf: *const i32 = mem::transmute(row.as_ptr());
                        *buf
                    };
                    results.push(SybResult::Status(status));
                    if status != 0 {
                        failed = true;
                    }
                },
                CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT => {
                    command.cancel(CS_CANCEL_CURRENT)?;
                },
                CS_CMD_FAIL => {
                    failed = true;
                },
                CS_CMD_SUCCEED | CS_CMD_DONE => {
                    let update_count = command.res_info::<i32>(CS_ROW_COUNT)?;
                    if update_count != CS_NO_COUNT {
                        results.push(SybResult::UpdateCount(update_count as u64));
                    }
                },
                _ => {},
            }
        }

        if failed {
            if errors.is_empty() {
                return Err(Error::from_message("Query execution resulted in error"));
            } else {
                return Err(errors.last().unwrap().clone());
            }
        }

        Ok(ResultSet::new(self.clone(), results, errors))
    }

    pub fn execute_statement(&mut self, st: &Statement) -> Result<ResultSet> {
        let params: Vec<&dyn ToSql> = st
            .params
            .iter()
            .map(|param| match param {
                None => &Null {} as &dyn ToSql,
                Some(param) => param as &dyn ToSql,
            })
            .collect();
        let text = generate_query(&st.query, params.iter().copied());
        debug!("Generated statement: {}", text);

        let mut command = Command::new(self.clone());
        command.command(CS_LANG_CMD, CommandArg::String(&text), CS_UNUSED)?;
        command.send()?;

        let mut results: Vec<SybResult> = Vec::new();
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
                    results.push(SybResult::Rows(row_result));
                },
                CS_STATUS_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    let row: &Vec<u8> = row_result.rows[0].buffers[0].as_ref().unwrap();
                    let status = unsafe {
                        let buf: *const i32 = mem::transmute(row.as_ptr());
                        *buf
                    };
                    if status != 0 {
                        failed = true;
                    }
                    results.push(SybResult::Status(status));
                },
                CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT => {
                    command.cancel(CS_CANCEL_CURRENT)?;
                },
                CS_CMD_FAIL => {
                    failed = true;
                },
                CS_CMD_SUCCEED | CS_CMD_DONE => {
                    let update_count = command.res_info::<u64>(CS_ROW_COUNT)?;
                    results.push(SybResult::UpdateCount(update_count));
                },
                _ => {},
            }
        }

        if failed {
            if errors.is_empty() {
                return Err(Error::from_message("Query execution resulted in error"));
            } else {
                return Err(errors.last().unwrap().clone());
            }
        }

        Ok(ResultSet::new(self.clone(), results, errors))
    }

    fn fetch_result(cmd: &mut Command) -> Result<Rows> {
        let ncols: usize = cmd.res_info(CS_NUMDATA).unwrap();
        let mut binds: Vec<Bind> = vec![Default::default(); ncols];
        let mut columns: Vec<Column> = vec![Default::default(); ncols];
        for col_idx in 0..ncols {
            let bind = &mut binds[col_idx];
            let column = &mut columns[col_idx];

            column.fmt = cmd.describe((col_idx + 1) as i32)?;
            column.fmt.format = CS_FMT_UNUSED as i32;
            match column.fmt.datatype {
                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE
                | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                    column.fmt.maxlength += 1;
                    column.fmt.format = CS_FMT_NULLTERM as i32;
                }
                _ => {}
            }
            bind.buffer.resize(column.fmt.maxlength as usize, 0);
            column.fmt.count = 1;
            let name_slice: Vec<u8> = column
                .fmt
                .name
                .iter()
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
                    &mut bind.indicator,
                )?;
            }
        }

        let mut rows: Vec<Row> = Vec::new();
        while cmd.fetch()? {
            let mut row = Row {
                buffers: Vec::new(),
            };
            for col_idx in 0..ncols {
                let bind = &binds[col_idx];
                match bind.indicator {
                    -1 => {
                        row.buffers.push(None);
                    }
                    0 => {
                        let len = bind.data_length as usize;
                        let buffer: Vec<u8> = match columns[col_idx].fmt.datatype {
                            CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE
                            | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                                Vec::from(&bind.buffer.as_slice()[0..len - 1])
                            }
                            _ => Vec::from(&bind.buffer.as_slice()[0..len]),
                        };

                        row.buffers.push(Some(Rc::new(buffer)));
                    }
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
        self.conn.lock().unwrap().messages.lock().unwrap().clear();
    }

    fn diag_get(&mut self) -> Vec<Error> {
        self.conn.lock().unwrap().messages.lock().unwrap().clone()
    }

    pub fn get_error(&mut self) -> Option<Error> {
        let errors = self.diag_get();
        errors
            .iter()
            .find(|e| e.severity.unwrap_or(i32::MAX) > 10)
            .cloned()
    }

    pub fn is_connected(&mut self) -> bool {
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
                ptr::null_mut(),
            );
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
        rs.get_string(0)?
            .ok_or_else(|| Error::from_message("Cannot get database name"))
    }

    pub(crate) fn convert(
        &mut self,
        srcfmt: &CS_DATAFMT,
        srcdata: &[u8],
        dstfmt: &CS_DATAFMT,
        dstdata: &mut [u8],
    ) -> Result<usize> {
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
                &mut dstlen,
            );
        }
        if ret != CS_SUCCEED {
            Err(self
                .get_error()
                .unwrap_or_else(|| Error::from_message("cs_convert failed")))
        } else {
            Ok(dstlen as usize)
        }
    }

    unsafe fn dt_crack_unsafe<T>(&mut self, type_: i32, dateval: *const T) -> Result<CS_DATEREC> {
        let mut daterec: CS_DATEREC = Default::default();
        let ret;
        {
            ret = cs_dt_crack(
                self.conn.lock().unwrap().ctx_handle,
                type_,
                mem::transmute(dateval),
                &mut daterec,
            );
        }
        if ret != CS_SUCCEED {
            return Err(self
                .get_error()
                .unwrap_or_else(|| Error::from_message("cs_dt_crack failed")));
        }
        Ok(daterec)
    }

    pub fn crack_date(&mut self, val: CS_DATE) -> Result<CS_DATEREC> {
        unsafe { self.dt_crack_unsafe(CS_DATE_TYPE, &val) }
    }

    pub fn crack_time(&mut self, val: CS_TIME) -> Result<CS_DATEREC> {
        unsafe { self.dt_crack_unsafe(CS_TIME_TYPE, &val) }
    }

    pub fn crack_datetime(&mut self, val: CS_DATETIME) -> Result<CS_DATEREC> {
        unsafe { self.dt_crack_unsafe(CS_DATETIME_TYPE, &val) }
    }

    pub fn crack_smalldatetime(&mut self, val: CS_DATETIME4) -> Result<CS_DATEREC> {
        unsafe { self.dt_crack_unsafe(CS_DATETIME4_TYPE, &val) }
    }

    pub fn set_message_callback(&mut self, callback: Box<dyn Fn(&Error) -> bool + Send>) {
        *self.conn.lock().unwrap().msg_callback.lock().unwrap() = Some(callback);
    }

    pub fn clear_message_callback(&mut self) {
        *self.conn.lock().unwrap().msg_callback.lock().unwrap() = None;
    }
}

unsafe impl Send for Connection {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::to_sql::ToSql;
    use crate::{generate_query, parse_query, ParamValue, Statement};
    use chrono::{NaiveDate, NaiveTime};
    use rust_decimal::Decimal;
    use std::cell::RefCell;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    const SERVER: &str = "***REMOVED***";

    fn connect() -> Connection {
        Connection::builder()
            .host(SERVER)
            .port(2025)
            .client_charset("UTF-8")
            .username("sa")
            .password("")
            .database("master")
            .tds_version(TdsVersion::Tds50)
            .login_timeout(5)
            .timeout(5)
            .connect()
            .unwrap()
    }

    #[test]
    fn test_select() {
        let mut conn = connect();
        let text = "select \
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
        assert_eq!(
            rs.get_datetime(4).unwrap().unwrap(),
            NaiveDate::from_ymd_opt(1986, 7, 5)
                .unwrap()
                .and_hms_milli_opt(10, 30, 31, 100)
                .unwrap()
        );
        assert_eq!(rs.get_f64(5).unwrap().unwrap(), 3.14);
        assert_eq!(
            rs.get_blob(6).unwrap().unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(rs.get_string(7).unwrap().unwrap(), "ccc".to_string());
        assert!(rs.get_string(8).unwrap().is_none());
        assert!(!rs.next());
    }

    #[test]
    fn test_get_value() {
        let mut conn = connect();
        let text = "
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
        assert_eq!(
            Some(ParamValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            rs.get_value("binary").unwrap()
        );
        assert_eq!(
            Some(ParamValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            rs.get_value("image").unwrap()
        );
        assert_eq!(
            Some(ParamValue::String(String::from("deadbeef"))),
            rs.get_value("char").unwrap()
        );
        assert_eq!(
            Some(ParamValue::String(String::from("deadbeef"))),
            rs.get_value("text").unwrap()
        );
        assert_eq!(
            Some(ParamValue::String(String::from("deadbeef"))),
            rs.get_value("unichar").unwrap()
        );
        assert_eq!(
            Some(ParamValue::Date(
                NaiveDate::from_ymd_opt(1986, 7, 4).unwrap()
            )),
            rs.get_value("date").unwrap()
        );
        assert_eq!(
            Some(ParamValue::Time(
                NaiveTime::from_hms_milli_opt(10, 1, 2, 300).unwrap()
            )),
            rs.get_value("time").unwrap()
        );
        assert_eq!(
            Some(ParamValue::DateTime(
                NaiveDate::from_ymd_opt(1986, 7, 4)
                    .unwrap()
                    .and_hms_milli_opt(10, 1, 2, 300)
                    .unwrap()
            )),
            rs.get_value("datetime").unwrap()
        );
        assert_eq!(
            Some(ParamValue::I32(2147483647)),
            rs.get_value("int").unwrap()
        );
        assert_eq!(Some(ParamValue::I32(1)), rs.get_value("bit").unwrap());
        assert_eq!(Some(ParamValue::I32(2)), rs.get_value("tinyint").unwrap());
        assert_eq!(Some(ParamValue::I32(3)), rs.get_value("smallint").unwrap());
        assert_eq!(
            Some(ParamValue::Decimal(
                Decimal::from_str_exact("1.23456789").unwrap()
            )),
            rs.get_value("numeric").unwrap()
        );
        assert_eq!(
            Some(ParamValue::I64(2147483648)),
            rs.get_value("long").unwrap()
        );
        assert_eq!(Some(ParamValue::F64(42.0)), rs.get_value("real").unwrap());
        assert_eq!(
            Some(ParamValue::F64(1.23456789)),
            rs.get_value("float").unwrap()
        );
    }

    #[test]
    fn test_execution_failure() {
        let mut conn = connect();

        /* Simple SQL syntax error */
        let text = "selecta \
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

        /* Procedure call where the server spits out an error */
        let text = "sp_bindcache NonExistent,NonExistent";
        let ret = conn.execute(text, &[]);
        assert!(ret.is_err());
        assert_eq!("Specified named cache does not exist.", ret.err().unwrap().desc());
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
        conn.execute(
            text,
            &[
                &"aaa",
                &2i32,
                &5000000000i64,
                &3.14f64,
                &NaiveDate::from_ymd_opt(1986, 7, 5)
                    .unwrap()
                    .and_hms_milli_opt(10, 30, 31, 100)
                    .unwrap(),
                &vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8],
                &"bbb",
            ],
        )
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
        assert_eq!(
            NaiveDate::from_ymd_opt(1986, 7, 5)
                .unwrap()
                .and_hms_milli_opt(10, 30, 31, 100)
                .unwrap(),
            rs.get_datetime(4).unwrap().unwrap()
        );
        assert_eq!(
            vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8],
            rs.get_blob(5).unwrap().unwrap()
        );
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

        let param5 = NaiveDate::from_ymd_opt(1986, 7, 5)
            .unwrap()
            .and_hms_opt(10, 30, 31)
            .unwrap();
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
        conn.execute("insert into freetds_rs_test(c) values(?)", &[&"éçàèä"])
            .unwrap();

        let text = "select c from freetds_rs_test";
        let mut rs = conn.execute(&text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(rs.get_string(0).unwrap().unwrap(), "éçàèä");

        let text = "select 'éçàèä'";
        let mut rs = conn.execute(&text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(rs.get_string(0).unwrap().unwrap(), "éçàèä");

        conn.execute("drop table freetds_rs_test", &[]).unwrap();
    }

    #[test]
    fn test_multiple_threads() {
        let mut conn = connect();
        let t0 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            let mut rs = conn.execute("select getdate()", &[]).unwrap();
            while rs.next() {
                println!("[0] {}", rs.get_string(0).unwrap().unwrap());
            }
        });

        let mut conn = connect();
        let t1 = thread::spawn(move || {
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

        let mut res = res.unwrap();
        assert!(res.status().is_ok());
        assert_eq!(0, res.status().unwrap());

        let res = conn.execute("sp_locklogin all_your_base_are_belong_to_us, 'lock'", &[]);
        assert!(res.is_err());
        assert_eq!("No such account -- nothing changed.", res.err().unwrap().desc());
    }

    #[test]
    fn test_update_count_result() {
        let mut conn = connect();

        conn.execute("create table #freetds_test(idx int not null, val varchar(64) not null, primary key(idx))", &[])
            .unwrap();
        conn.execute("insert into #freetds_test(idx, val) values(1, 'A')", &[]).unwrap();
        conn.execute("insert into #freetds_test(idx, val) values(2, 'B')", &[]).unwrap();
        conn.execute("insert into #freetds_test(idx, val) values(3, 'C')", &[]).unwrap();
        conn.execute("insert into #freetds_test(idx, val) values(4, 'D')", &[]).unwrap();
        conn.execute("insert into #freetds_test(idx, val) values(5, 'E')", &[]).unwrap();
        conn.execute("insert into #freetds_test(idx, val) values(6, 'F')", &[]).unwrap();

        let mut rs = conn.execute("update #freetds_test set val=val+'_'", &[]).unwrap();
        let update_count = rs.update_count();
        assert!(update_count.is_ok());
        assert_eq!(6, update_count.unwrap());
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
        let mut conn = Connection::builder()
            .host(SERVER)
            .port(2025)
            .client_charset("UTF-8")
            .username("sa")
            .password("")
            .database("master")
            .tds_version(TdsVersion::Tds50)
            .login_timeout(5)
            .timeout(5)
            .connect()
            .unwrap();

        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(String::from("master")), rs.get_string(0).unwrap());

        let mut conn = Connection::builder()
            .host(SERVER)
            .port(2025)
            .client_charset("UTF-8")
            .username("sa")
            .password("")
            .database("sybsystemprocs")
            .tds_version(TdsVersion::Tds50)
            .login_timeout(5)
            .timeout(5)
            .connect()
            .unwrap();

        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(
            Some(String::from("sybsystemprocs")),
            rs.get_string(0).unwrap()
        );
    }

    #[test]
    fn test_db_name() {
        let mut conn = connect();

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
        let text = "selecta \
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
        assert_eq!(
            "Incorrect syntax near '('.",
            msg.lock().unwrap().borrow().as_ref().unwrap()
        );
    }

    #[test]
    fn test_multiple_results() {
        let mut conn = connect();

        conn.execute("use tempdb", &[]).unwrap();
        conn.execute("if exists(select * from tempdb..sysobjects where type='P' and name='freetds_003') drop procedure freetds_003", &[]).unwrap();
        conn.execute("
            create procedure freetds_003 as begin
                create table #freetds_002(val int)
                
                insert into #freetds_002(val) values(1)
                insert into #freetds_002(val) values(2)
                insert into #freetds_002(val) values(3)
                
                update #freetds_002 set val = val + 1
                
                select val from #freetds_002
            end
        ", &[])
        .unwrap();

        let mut rs = conn.execute("freetds_003", &[]).unwrap();
        assert!(rs.is_rows());
        
        assert!(rs.next());
        assert_eq!(Some(2), rs.get_i32(0).unwrap());
        assert!(rs.next());
        assert_eq!(Some(3), rs.get_i32(0).unwrap());
        assert!(rs.next());
        assert_eq!(Some(4), rs.get_i32(0).unwrap());
        assert!(!rs.next());
        
        assert!(rs.next_results());
        assert!(rs.is_update_count());
        assert_eq!(3, rs.update_count().unwrap());

        assert!(rs.next_results());
        assert!(rs.is_status());
        assert_eq!(0, rs.status().unwrap());

        assert!(!rs.next_results());
    }

}
