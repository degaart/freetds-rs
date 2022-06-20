use std::{ptr, rc::Rc, mem};
use freetds_sys::*;
use crate::{Result, error::err, error::Error};

#[derive(Debug)]
pub struct CSContext {
    pub handle: *mut CS_CONTEXT
}

impl CSContext {
    fn new() -> Self {
        unsafe {
            println!("CSContext::new");
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
}

impl Drop for CSContext {
    fn drop(&mut self) {
        println!("CSContext::drop");
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

#[derive(Clone)]
pub struct Context {
    pub ctx: Rc<CSContext>
}

impl Context {
    pub fn new() -> Self {
        Self {
            ctx: Rc::new(CSContext::new())
        }
    }

    pub fn convert(&mut self, srcfmt: &CS_DATAFMT, srcdata: &[u8], dstfmt: &CS_DATAFMT, dstdata: &mut [u8]) -> Result<usize> {
        unsafe {
            let mut dstlen: i32 = Default::default();
            let ret = cs_convert(
                self.ctx.handle,
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
}


