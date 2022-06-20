use freetds_sys::*;

pub mod context;
pub mod connection;
pub mod command;
pub mod property;
pub mod error;
pub mod util;
pub mod statement;

pub type Result<T, E = error::Error> = core::result::Result<T, E>;

extern "C" {
    #[allow(dead_code)]
    fn debug1(ctx: *mut CS_CONTEXT) -> i32;
}

#[cfg(test)]
mod tests {
    use crate::{*, context::Context, connection::Connection, property::Property, statement::Statement};

    #[test]
    fn test_context() {
        let ctx = Context::new();
        unsafe {
            debug1(ctx.ctx.handle);
        }

        let mut conn = Connection::new(&ctx);
        conn.set_props(CS_CLIENTCHARSET, Property::String("UTF-8")).unwrap();
        conn.set_props(CS_USERNAME, Property::String("sa")).unwrap();
        conn.set_props(CS_PASSWORD, Property::String("")).unwrap();
        conn.set_props(CS_DATABASE, Property::String("***REMOVED***")).unwrap();
        conn.set_props(CS_TDS_VERSION, Property::I32(CS_TDS_50 as i32)).unwrap();
        conn.set_props(CS_LOGIN_TIMEOUT, Property::I32(5)).unwrap();
        conn.connect("***REMOVED***:2025").unwrap();

        let mut st = Statement::new(&mut conn);
        let has_results = st.execute("select 'aaa', cast(2 as int), getdate(), cast(3.14 as numeric(18,2)), /*cast(0xDEADBEEF as image), */cast('ccc' as text)")
            .unwrap();
        println!("has_results: {}", has_results);

        let cols = st.column_count().unwrap();
        while st.next().unwrap() {
            for i in 0..cols {
                print!("{};", st.get_string(i).unwrap());
            }
            println!("");
        }
        
    }
}

