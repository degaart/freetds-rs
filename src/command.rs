use std::ffi::CString;
use std::mem;
use std::{ptr, rc::Rc};
use freetds_sys::*;
use crate::connection::{CSConnection, Connection};
use crate::Result;
use crate::error::{succeeded, Error, err};

pub struct CSCommand {
    handle: *mut CS_COMMAND
}

impl CSCommand {
    pub fn new(conn: &CSConnection) -> Self {
        println!("CSCommand::new");
        unsafe {
            let mut cmd: *mut CS_COMMAND = ptr::null_mut();
            let ret = ct_cmd_alloc(conn.handle, &mut cmd);
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
        println!("CSCommand::drop");
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
    pub cmd: Rc<CSCommand>
}

impl Command {
    pub fn new(conn: &Connection) -> Self {
        Self {
            conn: conn.clone(),
            cmd: Rc::new(CSCommand::new(&conn.conn))
        }
    }

    pub fn command(&mut self, cmd_type: CS_INT, buffer: CommandArg, option: CS_INT) -> Result<()> {
        unsafe {
            match buffer {
                CommandArg::String(s) => {
                    assert_eq!(cmd_type, CS_LANG_CMD);
                    let buffer = CString::new(s)?;
                    let ret = ct_command(
                        self.cmd.handle,
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
            let ret = ct_send(self.cmd.handle);
            succeeded!(ret, ct_send);
            Ok(())
        }
    }

    pub fn results(&mut self) -> Result<(bool,i32)> {
        unsafe {
            let mut result_type: i32 = Default::default();
            let ret = ct_results(self.cmd.handle, &mut result_type);
            if ret != CS_SUCCEED && ret != CS_END_RESULTS {
                err!(ret, ct_results)
            } else {
                Ok((ret == CS_SUCCEED, result_type))
            }
        }
    }

    pub unsafe fn bind_unsafe(&mut self, item: i32, datafmt: *mut CS_DATAFMT, buffer: *mut CS_VOID, data_length: *mut i32, indicator: *mut i16) -> Result<()> {
        let ret = ct_bind(self.cmd.handle, item, datafmt, buffer, data_length, indicator);
        succeeded!(ret, ct_bind);
        Ok(())
    }

    pub fn fetch(&mut self) -> Result<bool> {
        unsafe {
            let mut rows_read: i32 = Default::default();
            let ret = ct_fetch(self.cmd.handle, CS_UNUSED, CS_UNUSED, CS_UNUSED, &mut rows_read);
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
                self.cmd.handle,
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
            let ret = ct_describe(self.cmd.handle, item, &mut buf);
            succeeded!(ret, ct_describe);
            Ok(buf)
        }
    }

    pub fn cancel(&mut self, type_: i32) -> Result<()> {
        unsafe {
            let ret = ct_cancel(ptr::null_mut(), self.cmd.handle, type_);
            succeeded!(ret, ct_cancel);
            Ok(())
        }
    }

}

impl Drop for Command {
    fn drop(&mut self) {
        self
            .cancel(CS_CANCEL_ALL)
            .expect("cs_cancel failed");
    }
}

