#![allow(clippy::useless_transmute)]

use crate::connection::{CSConnection, Connection};
use crate::error::Error;
use crate::Result;
use freetds_sys::*;
use std::ffi::CString;
use std::mem;
use std::ptr;
use std::rc::Rc;

pub(crate) struct CSCommand {
    handle: *mut CS_COMMAND,
}

impl CSCommand {
    pub fn new(conn: &CSConnection) -> Self {
        unsafe {
            let mut cmd: *mut CS_COMMAND = ptr::null_mut();
            let ret = ct_cmd_alloc(conn.conn_handle, &mut cmd);
            if ret != CS_SUCCEED {
                panic!("ct_cmd_alloc failed");
            }
            Self { handle: cmd }
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
    String(&'a str),
}

#[derive(Clone)]
pub(crate) struct Command {
    pub conn: Connection,
    pub cmd: Rc<CSCommand>,
}

impl Command {
    pub fn new(conn: Connection) -> Self {
        let cmd = Rc::new(CSCommand::new(&conn.conn.borrow()));
        Self { conn, cmd }
    }

    pub fn command(&mut self, cmd_type: i32, buffer: CommandArg, option: i32) -> Result<()> {
        unsafe {
            match buffer {
                CommandArg::String(s) => {
                    assert_eq!(cmd_type, CS_LANG_CMD);
                    self.conn.diag_clear();

                    let buffer = CString::new(s)?;
                    let ret;
                    {
                        ret = ct_command(
                            self.cmd.handle,
                            cmd_type,
                            mem::transmute(buffer.as_ptr()),
                            CS_NULLTERM,
                            option,
                        );
                    }
                    if ret != CS_SUCCEED {
                        return Err(self
                            .conn
                            .get_error()
                            .unwrap_or_else(|| Error::from_message("ct_command failed")));
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
            ret = ct_send(self.cmd.handle);
        }
        if ret == CS_SUCCEED {
            Ok(())
        } else {
            Err(self
                .conn
                .get_error()
                .unwrap_or_else(|| Error::from_message("ct_send failed")))
        }
    }

    pub fn results(&mut self) -> Result<(bool, i32)> {
        self.conn.diag_clear();
        let mut result_type: i32 = Default::default();
        let ret;
        unsafe {
            ret = ct_results(self.cmd.handle, &mut result_type);
        }
        if ret != CS_SUCCEED && ret != CS_END_RESULTS {
            Err(self
                .conn
                .get_error()
                .unwrap_or_else(|| Error::from_message("ct_results failed")))
        } else {
            Ok((ret == CS_SUCCEED, result_type))
        }
    }

    pub unsafe fn bind_unsafe(
        &mut self,
        item: i32,
        datafmt: *mut CS_DATAFMT,
        buffer: *mut CS_VOID,
        data_length: *mut i32,
        indicator: *mut i16,
    ) -> Result<()> {
        self.conn.diag_clear();
        let ret;
        {
            ret = ct_bind(self.cmd.handle, item, datafmt, buffer, data_length, indicator);
        }
        if ret == CS_SUCCEED {
            Ok(())
        } else {
            Err(self
                .conn
                .get_error()
                .unwrap_or_else(|| Error::from_message("ct_bind failed")))
        }
    }

    pub fn fetch(&mut self) -> Result<bool> {
        self.conn.diag_clear();
        let mut rows_read: i32 = Default::default();
        let ret;
        unsafe {
            ret = ct_fetch(self.cmd.handle, CS_UNUSED, CS_UNUSED, CS_UNUSED, &mut rows_read);
        }
        if ret == CS_SUCCEED {
            Ok(true)
        } else if ret == CS_END_DATA {
            Ok(false)
        } else {
            Err(self
                .conn
                .get_error()
                .unwrap_or_else(|| Error::from_message("ct_fetch failed")))
        }
    }

    pub fn res_info<T: Default>(&mut self, type_: i32) -> Result<T> {
        self.conn.diag_clear();

        let mut buf: T = Default::default();
        let mut out_len: i32 = Default::default();
        let ret;
        unsafe {
            ret = ct_res_info(
                self.cmd.handle,
                type_,
                mem::transmute(&mut buf),
                mem::size_of::<T>() as i32,
                &mut out_len,
            );
        }
        if ret == CS_SUCCEED {
            Ok(buf)
        } else {
            Err(self
                .conn
                .get_error()
                .unwrap_or_else(|| Error::from_message("ct_res_info failed")))
        }
    }

    pub fn describe(&mut self, item: i32) -> Result<CS_DATAFMT> {
        self.conn.diag_clear();

        let mut buf: CS_DATAFMT = Default::default();
        let ret;
        unsafe {
            ret = ct_describe(self.cmd.handle, item, &mut buf);
        }
        if ret == CS_SUCCEED {
            Ok(buf)
        } else {
            Err(self
                .conn
                .get_error()
                .unwrap_or_else(|| Error::from_message("ct_describe failed")))
        }
    }

    pub fn cancel(&mut self, type_: i32) -> Result<()> {
        self.conn.diag_clear();
        let ret;
        unsafe {
            ret = ct_cancel(ptr::null_mut(), self.cmd.handle, type_);
        }
        if ret == CS_SUCCEED {
            Ok(())
        } else {
            Err(self
                .conn
                .get_error()
                .unwrap_or_else(|| Error::from_message("ct_cancel failed")))
        }
    }
}

unsafe impl Send for Command {}

impl Drop for Command {
    fn drop(&mut self) {
        self.cancel(CS_CANCEL_ALL).expect("cs_cancel failed");
    }
}
