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
                Err(Error::new(ret, &format!("ct_fetch failed ({})", ret)))
            }
        }
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
        cmd.command(CS_LANG_CMD, CommandArg::String("select 'This is a string',getdate(),1, cast(3.14 as numeric(18,2)), 0x626162757368 as text

        "), CS_UNUSED).unwrap();
        cmd.send().unwrap();

        let mut binds: Vec<Bind> = Vec::new();
        binds.resize(5, Default::default());
        let mut ret;
        loop {
            let res_type;
            (ret, res_type) = cmd.results().unwrap();
            if ret != CS_SUCCEED {
                break;
            }

            match res_type {
                CS_ROW_RESULT => {
                    binds[0].fmt.datatype = CS_CHAR_TYPE;
                    binds[0].fmt.format = CS_FMT_NULLTERM as i32;
                    binds[0].fmt.maxlength = 4096;
                    binds[0].fmt.count = 1;
                    binds[0].buffer.resize(4096, 0);

                    binds[1].fmt.datatype = CS_DATETIME_TYPE;
                    binds[1].fmt.count = 1;
                    binds[1].buffer.resize(mem::size_of::<CS_DATETIME>(), 0);

                    binds[2].fmt.datatype = CS_INT_TYPE;
                    binds[2].fmt.count = 1;
                    binds[2].buffer.resize(mem::size_of::<CS_INT>(), 0);

                    binds[3].fmt.datatype = CS_NUMERIC_TYPE;
                    binds[3].fmt.precision = CS_SRC_VALUE;
                    binds[3].fmt.scale = CS_SRC_VALUE;
                    binds[3].buffer.resize(mem::size_of::<CS_NUMERIC>(), 0);

                    binds[4].fmt.datatype = CS_TEXT_TYPE;
                    binds[4].fmt.format =  CS_FMT_NULLTERM as i32;
                    binds[4].fmt.maxlength = 4096;
                    binds[4].fmt.count = 1;
                    binds[4].buffer.resize(4096, 0);

                    for i in 0..binds.len() {
                        unsafe {
                            cmd.bind_unsafe(
                                (i + 1) as i32,
                                &mut binds[i].fmt,
                                mem::transmute(binds[i].buffer.as_mut_ptr()),
                                &mut binds[i].data_length,
                                &mut binds[i].indicator)
                            .unwrap();
                        }
                    }
                                        
                    while cmd.fetch().unwrap() {
                        let len1 = (binds[0].data_length as usize) - 1;
                        let col1 = String::from_utf8_lossy(&binds[0].buffer.as_slice()[0..len1]);

                        let col2: CS_DATETIME;
                        unsafe {
                            let col2_buf: *const CS_DATETIME = mem::transmute(binds[1].buffer.as_ptr());
                            col2 = *col2_buf;
                        }

                        let col3: i32;
                        unsafe {
                            let col3_buf: *const CS_INT = mem::transmute(binds[2].buffer.as_ptr());
                            col3 = *col3_buf;
                        }

                        let col4: CS_NUMERIC;
                        unsafe {
                            let col4_buf: *const CS_NUMERIC = mem::transmute(binds[3].buffer.as_ptr());
                            col4 = *col4_buf;
                        }

                        println!("{:?}\n{:?}\n{:?}\n{:?}",
                                 col1,
                                 col2,
                                 col3,
                                 col4);
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
