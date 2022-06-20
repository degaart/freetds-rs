use std::{ptr, mem, ffi::CString, rc::Rc};
use freetds_sys::*;
use crate::{context::Context, property::Property, Result, error::err, error::Error};

pub struct CSConnection {
    pub handle: *mut CS_CONNECTION,
}

impl CSConnection {
    pub fn new(ctx: *mut CS_CONTEXT) -> Self {
        println!("CSConnection::new");
        unsafe {
            let mut conn: *mut CS_CONNECTION = ptr::null_mut();
            let ret = ct_con_alloc(ctx, &mut conn);
            if ret != CS_SUCCEED {
                panic!("ct_con_alloc failed");
            }

            Self {
                handle: conn
            }
        }
    }

    
}

impl Drop for CSConnection {
    fn drop(&mut self) {
        println!("CSConnection::drop");
        unsafe {
            let ret = ct_con_drop(self.handle);
            if ret != CS_SUCCEED {
                panic!("ct_con_drop failed");
            }
        }
    }
}

#[derive(Clone)]
pub struct Connection {
    pub ctx: Context,
    pub conn: Rc<CSConnection>
}

impl Connection {
    pub fn new(ctx: &Context) -> Self {
        Self {
            ctx: ctx.clone(),
            conn: Rc::new(CSConnection::new(ctx.ctx.handle))
        }
    }

    pub fn set_props(&mut self, property: u32, value: Property) -> Result<()> {
        unsafe {
            let ret;
            match value {
                Property::I32(mut i) => {
                    let mut outlen: i32 = Default::default();
                    ret = ct_con_props(
                        self.conn.handle,
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
                        self.conn.handle,
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
                self.conn.handle,
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



