use anyhow::bail;
use anyhow::ensure;
use freetds_sys::*;
use std::ffi::CString;
use std::fmt::Display;
use std::ptr;
use std::mem;
use anyhow::Result;

extern "C" {
    #[allow(dead_code)]
    fn debug1(ctx: *mut CS_CONTEXT) -> i32;
}

#[derive(Debug, Clone)]
pub struct Error {
    code: i32,
    desc: String,
}

impl Error {
    pub fn new(code: i32, desc: impl AsRef<str>) -> Self {
        Self { code: code, desc: desc.as_ref().into() }
    }

    pub fn code(&self) -> i32 {
        self.code
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
                        bail!("cs_config failed");
                    }
                },
                _ => {
                    bail!("Invalid argument");
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
                    bail!("Invalid argument");
                }
            }

            if ret == CS_SUCCEED {
                return Ok(())
            } else {
                bail!("ct_callback failed");
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

pub struct Connection {
    handle: *mut CS_CONNECTION
}

impl Connection {
    pub fn new(ctx: &mut Context) -> Self {
        unsafe {
            let mut conn: *mut CS_CONNECTION = ptr::null_mut();
            let ret = ct_con_alloc(ctx.handle, &mut conn);
            if ret != CS_SUCCEED {
                panic!("ct_con_alloc failed");
            }

            Self { handle: conn }
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
                    bail!("Invalid argument");
                }
            }

            if ret == CS_SUCCEED {
                return Ok(())
            } else {
                bail!("ct_con_props failed");
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
                return Ok(())
            } else {
                bail!("ct_connect failed");
            }
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            let ret = ct_con_drop(self.handle);
            if ret != CS_SUCCEED {
                panic!("ct_con_drop failed");
            }
        }
    }
}
pub struct Command {
    handle: *mut CS_COMMAND
}

impl Command {
    pub fn new(conn: &Connection) -> Self {
        unsafe {
            let mut cmd: *mut CS_COMMAND = ptr::null_mut();
            let ret = ct_cmd_alloc(conn.handle, &mut cmd);
            if ret != CS_SUCCEED {
                panic!("ct_cmd_alloc failed");
            }
            Self { handle: cmd }
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
                    ensure!(ret == CS_SUCCEED, "ct_command failed");
                }/*,
                _ => {
                    bail!("Invalid argument");
                }*/
            }
        }
        Ok(())
    }

    pub fn send(&mut self) -> Result<()> {
        unsafe {
            let ret = ct_send(self.handle);
            ensure!(ret == CS_SUCCEED, "ct_send failed");
            Ok(())
        }
    }

    pub fn results(&mut self) -> Result<(i32,i32)> {
        unsafe {
            let mut result_type: i32 = Default::default();
            let ret = ct_results(self.handle, &mut result_type);
            ensure!(ret != CS_FAIL, "ct_results failed");
            Ok((ret, result_type))
        }
    }

    pub unsafe fn bind_unsafe(&mut self, item: i32, datafmt: *mut CS_DATAFMT, buffer: *mut CS_VOID, data_length: *mut i32, indicator: *mut i16) -> Result<()> {
        let ret = ct_bind(self.handle, item, datafmt, buffer, data_length, indicator);
        ensure!(ret == CS_SUCCEED, "ct_bind failed");
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
                Err(Error::new(ret, &format!("ct_fetch failed ({})", Context::return_name(ret).unwrap_or(&format!("{}", ret)))))
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
            ensure!(ret == CS_SUCCEED, "ct_res_info failed");
        }
        Ok(buf)
    }

    pub fn describe(&mut self, item: i32) -> Result<CS_DATAFMT> {
        let mut buf: CS_DATAFMT = Default::default();
        unsafe {
            let ret = ct_describe(self.handle, item, &mut buf);
            ensure!(ret == CS_SUCCEED, "ct_describe failed");
        }
        Ok(buf)
    }
}

impl Drop for Command {
    fn drop(&mut self) {
        unsafe {
            let ret = ct_cmd_drop(self.handle);
            if ret != CS_SUCCEED {
                panic!("ct_cmd_drop failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[derive(Debug, Clone, Default)]
    struct Bind {
        fmt: CS_DATAFMT,
        buffer: Vec<u8>,
        data_length: i32,
        indicator: i16
    }

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

        let mut cmd = Command::new(&mut conn);
        cmd.command(
            CS_LANG_CMD,
            CommandArg::String(
                "select 'This is a string' as col1, getdate(), 1, cast(3.14 as numeric(18,2)), 0x626162757368 as text"),
            CS_UNUSED)
            .unwrap();
        cmd.send().unwrap();

        let mut binds: Vec<Bind> = Vec::new();
        let mut ret;
        loop {
            let res_type;
            (ret, res_type) = cmd.results().unwrap();
            if ret != CS_SUCCEED {
                break;
            }

            match res_type {
                CS_ROW_RESULT => {
                    let cols: i32 = cmd.res_info(CS_NUMDATA).unwrap();
                    println!("Column count: {}", cols);

                    binds.resize(cols as usize, Default::default());
                    for col in 0..cols {
                        /*
                            bind.name for column alias
                            bind.status & CS_CANBENULL
                        */
                        let bind = &mut binds[col as usize];
                        bind.fmt = cmd.describe(col + 1).unwrap();

                        println!("col{}: {}", col, Context::type_name(bind.fmt.datatype).unwrap_or(""));
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
                                        
                    while cmd.fetch().unwrap() {
                        for col in 0..cols {
                            let bind = &binds[col as usize];
                            match bind.fmt.datatype {
                                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                                    let len = (bind.data_length as usize) - 1;
                                    let value = String::from_utf8_lossy(&bind.buffer.as_slice()[0..len]);
                                    println!("{}: {:?}", col, value);
                                },
                                CS_DATETIME_TYPE => {
                                    unsafe {
                                        let buf: *const CS_DATETIME = mem::transmute(bind.buffer.as_ptr());
                                        println!("{}: {:?}", col, *buf);
                                    }
                                },
                                CS_INT_TYPE => {
                                    unsafe {
                                        let buf: *const CS_INT = mem::transmute(bind.buffer.as_ptr());
                                        println!("{}: {:?}", col, *buf);
                                    }
                                },
                                CS_NUMERIC_TYPE => {
                                    unsafe {
                                        let buf: *const CS_NUMERIC = mem::transmute(bind.buffer.as_ptr());
                                        println!("{}: {:?}", col, *buf);
                                    }
                                },
                                CS_BINARY_TYPE => {
                                    println!("{}: {:?}", col, &bind.buffer.as_slice()[0..bind.data_length as usize]);
                                },
                                _ => {
                                    panic!("{} not implemented", Context::type_name(bind.fmt.datatype).unwrap_or(""));
                                }
                            }
                        }
                    }
                },
                CS_CMD_SUCCEED => {
                    println!("No rows returned");
                },
                CS_CMD_FAIL => {
                    println!("Command execution failed");
                },
                CS_CMD_DONE => {
                    println!("Command execution done");
                },
                _ => {
                    println!("ct_results: unexpected return value");
                }
            }
        }

        if ret != CS_SUCCEED && ret != CS_END_RESULTS {
            panic!("ct_results returned {}", ret);
        }
    }

}
