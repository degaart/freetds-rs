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
    use crate::{*, context::Context, connection::Connection, property::Property, statement::{Statement, ToSql, NULL}};

    #[test]
    fn test_statement() {
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
        let has_results = st
            .execute(
                "select \
                    'aaa', cast(2 as int), cast('1986/07/05 10:30:31.1' as datetime), \
                    cast(40000000000 as numeric), cast(3.14 as numeric(18,2)), \
                    cast(0xDEADBEEF as image), cast('ccc' as text), \
                    cast(null as varchar)",
                &[])
            .unwrap();
        assert!(has_results);
        assert_eq!(8, st.column_count().unwrap());
        while st.next().unwrap() {
            assert_eq!("aaa", st.get_string(0).unwrap().unwrap());
            assert_eq!(2, st.get_int(1).unwrap().unwrap());
            assert_eq!("Jul  5 1986 10:30:31:100AM", st.get_string(2).unwrap().unwrap());
            assert_eq!(40000000000, st.get_int64(3).unwrap().unwrap());
            assert_eq!(3.14, st.get_float(4).unwrap().unwrap());
            assert_eq!(vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8], st.get_blob(5).unwrap().unwrap());
            assert_eq!("ccc", st.get_string(6).unwrap().unwrap());
            assert!(st.get_string(7).unwrap().is_none());
        }
        
    }

    #[test]
    fn test_params() {
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

        let date_param = CS_DATEREC {
            dateyear: 1986,
            datemonth: 6,
            datedmonth: 5,
            datehour: 10,
            dateminute: 30,
            datesecond: 31,
            datesecfrac: 1,
            ..Default::default()
        };
        let byte_param = vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8];
        let params: Vec<&dyn ToSql> = vec![
            &"aaa",
            &2i32,
            &date_param,
            &40000000000i64,
            &3.14f64,
            &byte_param,
            &"ccc"
        ];
        let has_results = st
            .execute(
                "select \
                    ?, ?, cast(? as datetime), \
                    cast(? as numeric), cast(? as numeric(18,2)), \
                    cast(? as image), \
                    cast(? as text)",
                &params)
            .unwrap();
        assert!(has_results);
        
        while st.next().unwrap() {
            assert_eq!("aaa", st.get_string(0).unwrap().unwrap());
            assert_eq!(2i32, st.get_int(1).unwrap().unwrap());
            assert_eq!("Jul  5 1986 10:30:31:100AM", st.get_string(2).unwrap().unwrap());
            assert_eq!(40000000000i64, st.get_int64(3).unwrap().unwrap());
            assert_eq!(3.14f64, st.get_float(4).unwrap().unwrap());
            assert_eq!(vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8], st.get_blob(5).unwrap().unwrap());
            assert_eq!("ccc", st.get_string(6).unwrap().unwrap());
        }
    }

    #[test]
    fn test_null_params() {
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
        let has_results = st
            .execute(
                "select \
                    ?, \
                    cast(? as datetime), \
                    cast(? as numeric), \
                    cast(? as numeric(18,2)), \
                    cast(? as image)",
                &[&NULL, &NULL, &NULL, &NULL, &NULL])
            .unwrap();
        assert!(has_results);
        
        while st.next().unwrap() {
            assert!(st.get_string(0).unwrap().is_none());
            assert!(st.get_int(0).unwrap().is_none());
            assert!(st.get_int64(0).unwrap().is_none());
            assert!(st.get_float(0).unwrap().is_none());
            assert!(st.get_date(0).unwrap().is_none());
            assert!(st.get_blob(0).unwrap().is_none());

            assert!(st.get_date(1).unwrap().is_none());
            assert!(st.get_int64(2).unwrap().is_none());
            assert!(st.get_float(3).unwrap().is_none());
            assert!(st.get_blob(4).unwrap().is_none());
        }
    }

}

