use std::ffi::CString;
use std::mem;
use std::ptr;
use std::sync::Arc;
use std::sync::Mutex;
use freetds_sys::*;
use crate::connection::{CSConnection, Connection};
use crate::Result;
use crate::error::Error;

pub struct CSCommand {
    handle: *mut CS_COMMAND
}

impl CSCommand {
    pub fn new(conn: &CSConnection) -> Self {
        unsafe {
            let mut cmd: *mut CS_COMMAND = ptr::null_mut();
            let ret = ct_cmd_alloc(conn.conn_handle, &mut cmd);
            if ret != CS_SUCCEED {
                panic!("ct_cmd_alloc failed");
            }
            Self {
                handle: cmd
            }
        }
    }
}

impl Drop for CSCommand {
    fn drop(&mut self) {
        unsafe {
            let ret = ct_cmd_drop(self.handle);
            if ret != CS_SUCCEED {
                panic!("ct_cmd_drop failed");
            }
        }
    }
}

pub enum CommandArg<'a> {
    String(&'a str)
}

#[derive(Clone)]
pub struct Command {
    pub conn: Connection,
    pub cmd: Arc<Mutex<CSCommand>>
}

impl Command {
    pub fn new(conn: Connection) -> Self {
        let cmd = Arc::new(Mutex::new(CSCommand::new(&conn.conn.lock().unwrap())));
        Self { conn, cmd }
    }

    pub fn command(&mut self, cmd_type: CS_INT, buffer: CommandArg, option: CS_INT) -> Result<()> {
        unsafe {
            match buffer {
                CommandArg::String(s) => {
                    assert_eq!(cmd_type, CS_LANG_CMD);
                    self.conn.diag_clear();

                    let buffer = CString::new(s)?;
                    let ret;
                    {
                        let cmd = self.cmd.lock().unwrap();
                        ret = ct_command(
                            cmd.handle,
                            cmd_type as i32,
                            mem::transmute(buffer.as_ptr()),
                            CS_NULLTERM,
                            option);
                    }
                    if ret != CS_SUCCEED {
                        return Err(self.conn.get_error().unwrap_or(Error::from_message("ct_command failed")));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn send(&mut self) -> Result<()> {
        self.conn.diag_clear();
        let ret;
        unsafe {
            let cmd = self.cmd.lock().unwrap();
            ret = ct_send(cmd.handle);
        }
        if ret == CS_SUCCEED {
            Ok(())
        } else {
            Err(self.conn.get_error().unwrap_or(Error::from_message("ct_send failed")))
        }
    
    }

    pub fn results(&mut self) -> Result<(bool,i32)> {
        self.conn.diag_clear();
        let mut result_type: i32 = Default::default();
        let ret;
        unsafe {
            let cmd = self.cmd.lock().unwrap();
            ret = ct_results(cmd.handle, &mut result_type);
        }
        if ret != CS_SUCCEED && ret != CS_END_RESULTS {
            Err(self.conn.get_error().unwrap_or(Error::from_message("ct_results failed")))
        } else {
            Ok((ret == CS_SUCCEED, result_type))
        }
    }

    pub unsafe fn bind_unsafe(&mut self, item: i32, datafmt: *mut CS_DATAFMT, buffer: *mut CS_VOID, data_length: *mut i32, indicator: *mut i16) -> Result<()> {
        self.conn.diag_clear();
        let ret;
        {
            let cmd = self.cmd.lock().unwrap();
            ret = ct_bind(cmd.handle, item, datafmt, buffer, data_length, indicator);
        }
        if ret == CS_SUCCEED {
            Ok(())
        } else {
            Err(self.conn.get_error().unwrap_or(Error::from_message("ct_bind failed")))
        }
    }

    pub fn fetch(&mut self) -> Result<bool> {
        self.conn.diag_clear();
        let mut rows_read: i32 = Default::default();
        let ret;
        unsafe {
            let cmd = self.cmd.lock().unwrap();
            ret = ct_fetch(cmd.handle, CS_UNUSED, CS_UNUSED, CS_UNUSED, &mut rows_read);
        }
        if ret == CS_SUCCEED {
            Ok(true)
        } else if ret == CS_END_DATA {
            Ok(false)
        } else {
            Err(self.conn.get_error().unwrap_or(Error::from_message("ct_fetch failed")))
        }
    }

    pub fn res_info<T: Default>(&mut self, type_: CS_INT) -> Result<T> {
        self.conn.diag_clear();

        let mut buf: T = Default::default();
        let mut out_len: CS_INT = Default::default();
        let ret;
        unsafe {
            let cmd = self.cmd.lock().unwrap();
            ret = ct_res_info(
                cmd.handle,
                type_,
                mem::transmute(&mut buf),
                mem::size_of::<T>() as i32,
                &mut out_len);
        }
        if ret == CS_SUCCEED {
            Ok(buf)
        } else {
            Err(self.conn.get_error().unwrap_or(Error::from_message("ct_res_info failed")))
        }
    }

    pub fn describe(&mut self, item: i32) -> Result<CS_DATAFMT> {
        self.conn.diag_clear();

        let mut buf: CS_DATAFMT = Default::default();
        let ret;
        unsafe {
            let cmd = self.cmd.lock().unwrap();
            ret = ct_describe(cmd.handle, item, &mut buf);
        }
        if ret == CS_SUCCEED {
            Ok(buf)
        } else {
            Err(self.conn.get_error().unwrap_or(Error::from_message("ct_describe failed")))
        }
    }

    pub fn cancel(&mut self, type_: i32) -> Result<()> {
        self.conn.diag_clear();
        let ret;
        unsafe {
            ret = ct_cancel(ptr::null_mut(), self.cmd.lock().unwrap().handle, type_);
        }
        if ret == CS_SUCCEED {
            Ok(())
        } else {
            Err(self.conn.get_error().unwrap_or(Error::from_message("ct_cancel failed")))
        }
    }

}

unsafe impl Send for Command {}

impl Drop for Command {
    fn drop(&mut self) {
        self
            .cancel(CS_CANCEL_ALL)
            .expect("cs_cancel failed");
    }
}

