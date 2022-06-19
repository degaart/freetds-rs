use freetds_sys::*;
use std::ffi::CString;
use std::fmt::Display;
use std::ptr;
use std::mem;
use std::fmt::Debug;

extern "C" {
    #[allow(dead_code)]
    fn debug1(ctx: *mut CS_CONTEXT) -> i32;
}

#[derive(Debug, Clone)]
pub struct Error {
    code: Option<i32>,
    fn_name: Option<String>,
    desc: String,
}

impl Error {
    fn new(code: i32, fn_name: impl AsRef<str>) -> Self {
        Self {
            code: Some(code),
            fn_name: Some(fn_name.as_ref().to_string()),
            desc: format!("{} failed (ret: {})", fn_name.as_ref(), Context::return_name(code).unwrap_or(&format!("{}", code)))
        }
    }

    fn from_message(desc: impl AsRef<str>) -> Self {
        Self {
            code: None,
            fn_name: None,
            desc: desc.as_ref().into()
        }
    }

    pub fn code(&self) -> Option<i32> {
        self.code
    }

    pub fn fn_name(&self) -> Option<&str> {
        if let Some(s) = &self.fn_name {
            Some(s)
        } else {
            None
        }
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.desc)
    }
}

impl std::error::Error for Error {

}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Self::from_message(e.to_string())
    }
}

impl From<std::ffi::NulError> for Error {
    fn from(e: std::ffi::NulError) -> Self {
        Self::from_message(e.to_string())
    }
}

macro_rules! err {
    ($code:ident, $fn_name:ident) => {
        Err(Error::new($code, stringify!($fn_name)))
    };
    ($desc:literal) => {
        Err(Error::from_message($desc))
    };
    ($desc:literal, $($arg:tt)*) => {
        Err(Error::from_message(format!($desc, $($arg)*)))
    };
}

macro_rules! succeeded {
    ($code:ident, $fn_name:ident) => {
        if $code != CS_SUCCEED {
            return Err(Error::new($code, stringify!($fn_name)))
        }
    };
}

pub type Result<T, E = Error> = core::result::Result<T, E>;

type CslibMsgCallbackType = extern "C" fn(*mut CS_CONTEXT, *const CS_CLIENTMSG) -> i32;
type ClientMsgCallbackType = extern "C" fn(*mut CS_CONTEXT, *mut CS_CONNECTION, *const CS_CLIENTMSG) -> i32;
type ServerMsgCallbackType = extern "C" fn(*mut CS_CONTEXT, *mut CS_CONNECTION, *const CS_SERVERMSG) -> i32;

pub enum Property<'a> {
    CslibMsgCallback(CslibMsgCallbackType),
    ClientMsgCallback(ClientMsgCallbackType),
    ServerMsgCallback(ServerMsgCallbackType),
    I32(i32),
    String(&'a str),
}

pub enum CommandArg<'a> {
    String(&'a str)
}

pub struct Context {
    handle: *mut CS_CONTEXT
}

impl Context {
    pub fn new() -> Self {
        unsafe {
            let mut ctx: *mut CS_CONTEXT = ptr::null_mut();
            let ret = cs_ctx_alloc(CS_VERSION_125, &mut ctx);
            if ret != CS_SUCCEED {
                panic!("cs_ctx_alloc failed");
            }

            let ret = ct_init(ctx, CS_VERSION_125);
            if ret != CS_SUCCEED {
                panic!("ct_init failed");
            }

            Self {
                handle: ctx
            }
        }
    }

    pub fn set_config(&mut self, property: CS_INT, value: Property) -> Result<()> {
        unsafe {
            match value {
                Property::CslibMsgCallback(f) => {
                    let mut outlen: i32 = Default::default();
                    let ret = cs_config(self.handle, CS_SET, property, mem::transmute(&f), mem::size_of_val(&f).try_into().unwrap(), &mut outlen);
                    if ret == CS_SUCCEED {
                        return Ok(())
                    } else {
                        return err!(ret, cs_config);
                    }
                },
                _ => {
                    return err!("Invalid argument");
                }
            }
        }
    }

    pub fn set_callback(&mut self, property: CS_INT, value: Property) -> Result<()> {
        unsafe {
            let ret;
            match value {
                Property::ClientMsgCallback(f) => {
                    ret = ct_callback(self.handle, ptr::null_mut(), CS_SET, property, mem::transmute(&f));
                },
                Property::ServerMsgCallback(f) => {
                    ret = ct_callback(self.handle, ptr::null_mut(), CS_SET, property, mem::transmute(&f));
                },
                _ => {
                    return err!("Invalid argument");
                }
            }

            if ret == CS_SUCCEED {
                return Ok(())
            } else {
                return err!(ret, ct_callback);
            }
        }
    }

    #[allow(dead_code)]
    extern "C" fn cslibmsg_callback(_ctx: *mut CS_CONTEXT, message: *const CS_CLIENTMSG) -> i32 {
        unsafe {
            let len = (*message).msgstringlen as usize;
            let msgstring: Vec<u8> = (*message).msgstring.iter().take(len).map(|&c| c as u8).collect();
            let msg = String::from_utf8_lossy(&msgstring);

            println!("CS message: severity {} number {}: {}",
                (*message).severity,
                (*message).msgnumber,
                msg);
        }
        CS_SUCCEED
    }

    #[allow(dead_code)]
    extern "C" fn clientmsg_callback(_ctx: *mut CS_CONTEXT, _conn: *mut CS_CONNECTION, message: *const CS_CLIENTMSG) -> i32 {
        unsafe {
            let len = (*message).msgstringlen as usize;
            let msgstring: Vec<u8> = (*message).msgstring.iter().take(len).map(|&c| c as u8).collect();
            let msg = String::from_utf8_lossy(&msgstring);

            println!("Client message: severity {} number {}: {}",
                (*message).severity,
                (*message).msgnumber,
                msg);
        }
        CS_SUCCEED
    }

    #[allow(dead_code)]
    extern "C" fn servermsg_callback(_ctx: *mut CS_CONTEXT, _conn: *mut CS_CONNECTION, message: *const CS_SERVERMSG) -> i32 {
        unsafe {
            let len = (*message).textlen as usize;
            let text: Vec<u8> = (*message).text.iter().take(len).map(|&c| c as u8).collect();
            let text = String::from_utf8_lossy(&text);
            println!("Server message: severity {} number {}: {}",
                (*message).severity,
                (*message).msgnumber,
                text);
        }
        CS_SUCCEED
    }

    pub fn type_name(type_: i32) -> Option<&'static str> {
        match type_ {
            CS_CLIENTMSG_TYPE => Some("CS_CLIENTMSG_TYPE"),
            CS_SERVERMSG_TYPE => Some("CS_SERVERMSG_TYPE"),
            CS_ALLMSG_TYPE => Some("CS_ALLMSG_TYPE"),
            CS_ILLEGAL_TYPE => Some("CS_ILLEGAL_TYPE"),
            CS_CHAR_TYPE => Some("CS_CHAR_TYPE"),
            CS_BINARY_TYPE => Some("CS_BINARY_TYPE"),
            CS_LONGCHAR_TYPE => Some("CS_LONGCHAR_TYPE"),
            CS_LONGBINARY_TYPE => Some("CS_LONGBINARY_TYPE"),
            CS_TEXT_TYPE => Some("CS_TEXT_TYPE"),
            CS_IMAGE_TYPE => Some("CS_IMAGE_TYPE"),
            CS_TINYINT_TYPE => Some("CS_TINYINT_TYPE"),
            CS_SMALLINT_TYPE => Some("CS_SMALLINT_TYPE"),
            CS_INT_TYPE => Some("CS_INT_TYPE"),
            CS_REAL_TYPE => Some("CS_REAL_TYPE"),
            CS_FLOAT_TYPE => Some("CS_FLOAT_TYPE"),
            CS_BIT_TYPE => Some("CS_BIT_TYPE"),
            CS_DATETIME_TYPE => Some("CS_DATETIME_TYPE"),
            CS_DATETIME4_TYPE => Some("CS_DATETIME4_TYPE"),
            CS_MONEY_TYPE => Some("CS_MONEY_TYPE"),
            CS_MONEY4_TYPE => Some("CS_MONEY4_TYPE"),
            CS_NUMERIC_TYPE => Some("CS_NUMERIC_TYPE"),
            CS_DECIMAL_TYPE => Some("CS_DECIMAL_TYPE"),
            CS_VARCHAR_TYPE => Some("CS_VARCHAR_TYPE"),
            CS_VARBINARY_TYPE => Some("CS_VARBINARY_TYPE"),
            CS_LONG_TYPE => Some("CS_LONG_TYPE"),
            CS_SENSITIVITY_TYPE => Some("CS_SENSITIVITY_TYPE"),
            CS_BOUNDARY_TYPE => Some("CS_BOUNDARY_TYPE"),
            CS_VOID_TYPE => Some("CS_VOID_TYPE"),
            CS_USHORT_TYPE => Some("CS_USHORT_TYPE"),
            CS_UNICHAR_TYPE => Some("CS_UNICHAR_TYPE"),
            CS_BLOB_TYPE => Some("CS_BLOB_TYPE"),
            CS_DATE_TYPE => Some("CS_DATE_TYPE"),
            CS_TIME_TYPE => Some("CS_TIME_TYPE"),
            CS_UNITEXT_TYPE => Some("CS_UNITEXT_TYPE"),
            CS_BIGINT_TYPE => Some("CS_BIGINT_TYPE"),
            CS_USMALLINT_TYPE => Some("CS_USMALLINT_TYPE"),
            CS_UINT_TYPE => Some("CS_UINT_TYPE"),
            CS_UBIGINT_TYPE => Some("CS_UBIGINT_TYPE"),
            CS_XML_TYPE => Some("CS_XML_TYPE"),
            CS_BIGDATETIME_TYPE => Some("CS_BIGDATETIME_TYPE"),
            CS_BIGTIME_TYPE => Some("CS_BIGTIME_TYPE"),
            CS_UNIQUE_TYPE => Some("CS_UNIQUE_TYPE"),
            _ => None        
        }
    }

    pub fn format_name(format: i32) -> Option<&'static str> {
        match format as u32{
            CS_FMT_UNUSED => Some("CS_FMT_UNUSED"),
            CS_FMT_NULLTERM => Some("CS_FMT_NULLTERM"),
            CS_FMT_PADNULL => Some("CS_FMT_PADNULL"),
            CS_FMT_PADBLANK => Some("CS_FMT_PADBLANK"),
            CS_FMT_JUSTIFY_RT => Some("CS_FMT_JUSTIFY_RT"),
            _ => None
        }
    }

    pub fn return_name(ret: i32) -> Option<&'static str> {
        match ret {
            CS_FAIL => Some("CS_FAIL"),
            CS_MEM_ERROR => Some("CS_MEM_ERROR"),
            CS_PENDING => Some("CS_PENDING"),
            CS_QUIET => Some("CS_QUIET"),
            CS_BUSY => Some("CS_BUSY"),
            CS_INTERRUPT => Some("CS_INTERRUPT"),
            CS_BLK_HAS_TEXT => Some("CS_BLK_HAS_TEXT"),
            CS_CONTINUE => Some("CS_CONTINUE"),
            CS_FATAL => Some("CS_FATAL"),
            CS_RET_HAFAILOVER => Some("CS_RET_HAFAILOVER"),
            CS_UNSUPPORTED => Some("CS_UNSUPPORTED"),
            CS_CANCELED => Some("CS_CANCELED"),
            CS_ROW_FAIL => Some("CS_ROW_FAIL"),
            CS_END_DATA => Some("CS_END_DATA"),
            CS_END_RESULTS => Some("CS_END_RESULTS"),
            CS_END_ITEM => Some("CS_END_ITEM"),
            CS_NOMSG => Some("CS_NOMSG"),
            CS_TIMED_OUT => Some("CS_TIMED_OUT"),
            _ => None
        }
    }

    pub fn convert(&mut self, srcfmt: &CS_DATAFMT, srcdata: &[u8], dstfmt: &CS_DATAFMT, dstdata: &mut [u8]) -> Result<usize> {
        unsafe {
            let mut dstlen: i32 = Default::default();
            let ret = cs_convert(
                self.handle,
                mem::transmute(srcfmt as *const CS_DATAFMT),
                mem::transmute(srcdata.as_ptr()),
                mem::transmute(dstfmt as *const CS_DATAFMT),
                mem::transmute(dstdata.as_mut_ptr()),
                &mut dstlen);
            if ret != CS_SUCCEED {
                err!(ret, cs_convert)
            } else {
                Ok(dstlen as usize)
            }
        }
    }

    /*pub unsafe fn convert_unsafe(&mut self, srcfmt: &CS_DATAFMT, dstfmt: &CS_DATAFMT) -> Result<()> {
        todo!();
    }*/

}
 
impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            let ret = ct_exit(self.handle, CS_UNUSED);
            if ret != CS_SUCCEED {
                ct_exit(self.handle, CS_FORCE_EXIT);
            }

            let ret = cs_ctx_drop(self.handle);
            if ret != CS_SUCCEED {
                panic!("cs_ctx_drop failed");
            }
        }
    }
}

pub struct Connection<'a> {
    ctx: &'a mut Context,
    handle: *mut CS_CONNECTION,
}

impl<'a> Connection<'a> {
    pub fn new(ctx: &'a mut Context) -> Self {
        unsafe {
            let mut conn: *mut CS_CONNECTION = ptr::null_mut();
            let ret = ct_con_alloc(ctx.handle, &mut conn);
            if ret != CS_SUCCEED {
                panic!("ct_con_alloc failed");
            }

            Self {
                ctx: ctx,
                handle: conn
            }
        }
    }

    pub fn set_props(&mut self, property: u32, value: Property) -> Result<()> {
        unsafe {
            let ret;
            match value {
                Property::I32(mut i) => {
                    let mut outlen: i32 = Default::default();
                    ret = ct_con_props(
                        self.handle,
                        CS_SET,
                        property as CS_INT,
                        std::mem::transmute(&mut i),
                        mem::size_of::<i32>() as i32,
                        &mut outlen);
                },
                Property::String(s) => {
                    let s1 = CString::new(s)?;
                    let mut outlen: i32 = Default::default();
                    ret = ct_con_props(
                        self.handle,
                        CS_SET,
                        property as CS_INT,
                        std::mem::transmute(s1.as_ptr()),
                        s.len() as i32,
                        &mut outlen);
                },
                _ => {
                    return err!("Invalid argument");
                }
            }

            if ret == CS_SUCCEED {
                Ok(())
            } else {
                err!("ct_con_props failed")
            }
        }
    }

    pub fn connect(&mut self, server_name: impl AsRef<str>) -> Result<()> {
        unsafe {
            let server_name = CString::new(server_name.as_ref())?;
            let ret = ct_connect(
                self.handle,
                mem::transmute(server_name.as_ptr()),
                CS_NULLTERM);
            if ret == CS_SUCCEED {
                Ok(())
            } else {
                err!("ct_connect failed")
            }
        }
    }
}

impl<'a> Drop for Connection<'_> {
    fn drop(&mut self) {
        unsafe {
            let ret = ct_con_drop(self.handle);
            if ret != CS_SUCCEED {
                panic!("ct_con_drop failed");
            }
        }
    }
}

pub struct Command<'a> {
    conn: &'a mut Connection<'a>,
    handle: *mut CS_COMMAND
}

impl<'a> Command<'a> {
    pub fn new(conn: &'a mut Connection<'a>) -> Self {
        unsafe {
            let mut cmd: *mut CS_COMMAND = ptr::null_mut();
            let ret = ct_cmd_alloc(conn.handle, &mut cmd);
            if ret != CS_SUCCEED {
                panic!("ct_cmd_alloc failed");
            }
            Self {
                conn: conn,
                handle: cmd
            }
        }
    }

    pub fn command(&mut self, cmd_type: CS_INT, buffer: CommandArg, option: CS_INT) -> Result<()> {
        unsafe {
            match buffer {
                CommandArg::String(s) => {
                    assert_eq!(cmd_type, CS_LANG_CMD);
                    let buffer = CString::new(s)?;
                    let ret = ct_command(
                        self.handle,
                        cmd_type as i32,
                        mem::transmute(buffer.as_ptr()),
                        CS_NULLTERM,
                        option);
                    succeeded!(ret, ct_command);
                }
            }
        }
        Ok(())
    }

    pub fn send(&mut self) -> Result<()> {
        unsafe {
            let ret = ct_send(self.handle);
            succeeded!(ret, ct_send);
            Ok(())
        }
    }

    pub fn results(&mut self) -> Result<(bool,i32), Error> {
        unsafe {
            let mut result_type: i32 = Default::default();
            let ret = ct_results(self.handle, &mut result_type);
            if ret != CS_SUCCEED && ret != CS_END_RESULTS {
                err!(ret, ct_results)
            } else {
                Ok((ret == CS_SUCCEED, result_type))
            }
        }
    }

    pub unsafe fn bind_unsafe(&mut self, item: i32, datafmt: *mut CS_DATAFMT, buffer: *mut CS_VOID, data_length: *mut i32, indicator: *mut i16) -> Result<()> {
        let ret = ct_bind(self.handle, item, datafmt, buffer, data_length, indicator);
        succeeded!(ret, ct_bind);
        Ok(())
    }

    pub fn fetch(&mut self) -> Result<bool, Error> {
        unsafe {
            let mut rows_read: i32 = Default::default();
            let ret = ct_fetch(self.handle, CS_UNUSED, CS_UNUSED, CS_UNUSED, &mut rows_read);
            if ret == CS_SUCCEED {
                Ok(true)
            } else if ret == CS_END_DATA {
                Ok(false)
            } else {
                err!(ret, ct_fetch)
            }
        }
    }

    pub fn res_info<T: Default>(&mut self, type_: CS_INT) -> Result<T> {
        let mut buf: T = Default::default();
        let mut out_len: CS_INT = Default::default();
        unsafe {
            let ret = ct_res_info(
                self.handle,
                type_,
                mem::transmute(&mut buf),
                mem::size_of::<T>() as i32,
                &mut out_len);
            succeeded!(ret, ct_res_info);
            Ok(buf)
        }
    }

    pub fn describe(&mut self, item: i32) -> Result<CS_DATAFMT> {
        let mut buf: CS_DATAFMT = Default::default();
        unsafe {
            let ret = ct_describe(self.handle, item, &mut buf);
            succeeded!(ret, ct_describe);
            Ok(buf)
        }
    }

    pub fn cancel(&mut self, type_: i32) -> Result<(), Error> {
        unsafe {
            let ret = ct_cancel(ptr::null_mut(), self.handle, type_);
            succeeded!(ret, ct_cancel);
            Ok(())
        }
    }

}

impl<'a> Drop for Command<'_> {
    fn drop(&mut self) {
        unsafe {
            let ret = ct_cmd_drop(self.handle);
            if ret != CS_SUCCEED {
                panic!("ct_cmd_drop failed");
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Bind {
    fmt: CS_DATAFMT,
    buffer: Vec<u8>,
    data_length: i32,
    indicator: i16
}

#[derive(PartialEq)]
enum StatementState {
    Invalid,
    ResultsReady,
    ResultSetDone,
    Done,
}

pub struct Statement<'a> {
    command: Command<'a>,
    state: StatementState,
    binds: Vec<Bind>,
    has_errors: bool,
}

impl<'a> Statement<'a> {
    pub fn new(conn: &'a mut Connection<'a>, text: impl AsRef<str>) -> Result<Statement<'a>, Error> {
        let mut command = Command::new(conn);
        command.command(CS_LANG_CMD, CommandArg::String(text.as_ref()), CS_UNUSED)?;
        command.send()?;

        let mut st = Self {
            command: command,
            state: StatementState::Invalid,
            binds: Vec::new(),
            has_errors: false
        };
        st.process_results()?;
        if st.has_errors {
            err!("An error occured while executing the statement")
        } else {
            Ok(st)
        }
    }

    fn get_results(cmd: &mut Command, binds: &mut Vec<Bind>) -> Result<usize, Error> {
        let cols: i32 = cmd
            .res_info(CS_NUMDATA)
            .unwrap();
        binds.resize(cols as usize, Default::default());
        for col in 0..cols {
            /*
                bind.name for column alias
                bind.status & CS_CANBENULL
            */
            let bind = &mut binds[col as usize];
            bind.fmt = cmd.describe(col + 1).unwrap();
            bind.fmt.format = CS_FMT_UNUSED as i32;
            match bind.fmt.datatype {
                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                    bind.fmt.maxlength += 1;
                    bind.fmt.format = CS_FMT_NULLTERM as i32;
                },
                _ => {}
            }
            bind.buffer.resize(bind.fmt.maxlength as usize, 0);
            bind.fmt.count = 1;

            unsafe {
                cmd.bind_unsafe(
                    (col + 1) as i32,
                    &mut bind.fmt,
                    mem::transmute(bind.buffer.as_mut_ptr()),
                    &mut bind.data_length,
                    &mut bind.indicator)
                .unwrap();
            }
        }
        Ok(cols as usize)
    }

    fn process_results(&mut self) -> Result<(), Error> {
        match self.state {
            StatementState::ResultsReady => {
                self.command.cancel(CS_CANCEL_CURRENT)?;
            },
            StatementState::Done => {
                return Ok(())
            },
            _ => {}
        }
        loop {
            let (ret, res_type) = self.command.results()?;
            if ret {
                match res_type {
                    CS_ROW_RESULT => {
                        self.state = StatementState::ResultsReady;
                        Self::get_results(&mut self.command, &mut self.binds)?;
                    },
                    CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT | CS_STATUS_RESULT => {
                        self.command.cancel(CS_CANCEL_CURRENT)?;
                    },
                    CS_CMD_DONE => {
                        self.state = StatementState::Done;
                    },
                    CS_CMD_FAIL => {
                        self.has_errors = true;
                    },
                    _ => {
                        /* Do nothing, most notably, ignore CS_CMD_SUCCEED */
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    pub fn next(&mut self) -> Result<bool, Error> {
        if self.state == StatementState::Done || self.state == StatementState::ResultSetDone {
            return Ok(false)
        } else if self.state == StatementState::Invalid {
            return err!("Invalid statement state");
        }
        
        let ret = self.command.fetch()?;
        if !ret {
            self.state = StatementState::ResultSetDone;
            return Ok(false);
        }
        self.state = StatementState::ResultsReady;
        return Ok(true);
    }

    pub fn get_string(&mut self, col: i32) -> Result<String> {
        let bind = &self.binds[col as usize];
        match bind.fmt.datatype {
            CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                let len = (bind.data_length as usize) - 1;
                let value = String::from_utf8_lossy(&bind.buffer.as_slice()[0..len]);
                return Ok(value.to_string());
            },
            _ => {
                let mut dstfmt: CS_DATAFMT = Default::default();
                dstfmt.datatype = CS_CHAR_TYPE;
                dstfmt.maxlength = if bind.fmt.maxlength < 200 { 100 } else { bind.fmt.maxlength * 2 };
                dstfmt.format = CS_FMT_NULLTERM as i32;
                dstfmt.count = 1;

                let mut dstdata: Vec<u8> = Vec::new();
                dstdata.resize(dstfmt.maxlength as usize, Default::default());
                let dstlen = self.command.conn.ctx.convert(
                    &bind.fmt, &bind.buffer,
                    &dstfmt,
                    &mut dstdata)?;

                return Ok(String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string());
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn text_context() {
        let mut ctx = Context::new();
        unsafe {
            debug1(ctx.handle);
        }
        /*ctx.set_config(CS_MESSAGE_CB, Property::CslibMsgCallback(Context::cslibmsg_callback)).unwrap();
        ctx.set_callback(CS_CLIENTMSG_CB, Property::ClientMsgCallback(Context::clientmsg_callback)).unwrap();
        ctx.set_callback(CS_SERVERMSG_CB, Property::ServerMsgCallback(Context::servermsg_callback)).unwrap();*/

        let mut conn = Connection::new(&mut ctx);
        conn.set_props(CS_CLIENTCHARSET, Property::String("UTF-8")).unwrap();
        conn.set_props(CS_USERNAME, Property::String("sa")).unwrap();
        conn.set_props(CS_PASSWORD, Property::String("")).unwrap();
        conn.set_props(CS_DATABASE, Property::String("***REMOVED***")).unwrap();
        conn.set_props(CS_TDS_VERSION, Property::I32(CS_TDS_50 as i32)).unwrap();
        conn.set_props(CS_LOGIN_TIMEOUT, Property::I32(5)).unwrap();
        conn.connect("***REMOVED***:2025").unwrap();

        {
        let st = Statement::new(&mut conn, "select 'aaa', cast(2 as int), getdate(), cast(3.14 as numeric(18,2)), 'bbb' as text, cast(0xDEADBEEF as image), cast('ccc' as text)")
            .unwrap();
        }

        println!("Here");
    }

}
