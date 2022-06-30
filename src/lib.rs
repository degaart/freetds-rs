use freetds_sys::*;
pub mod context;
pub mod connection;
pub mod command;
pub mod property;
pub mod error;
pub mod util;
pub mod to_sql;
pub mod null;

pub type Result<T, E = error::Error> = core::result::Result<T, E>;

extern "C" {
    #[allow(dead_code)]
    fn debug1(ctx: *mut CS_CONTEXT) -> i32;
}

#[cfg(test)]
mod tests {
    
}

