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
    use crate::{*, context::Context, connection::Connection, property::Property, statement::{Statement, ToSql}};

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
                "select 'aaa', cast(2 as int), getdate(), cast(40000000000 as numeric), cast(3.14 as numeric(18,2)), cast(0xDEADBEEF as image), cast('ccc' as text)",
                &[])
            .unwrap();
        println!("has_results: {}", has_results);

        // let cols = st.column_count().unwrap();
        while st.next().unwrap() {
            let col1 = st.get_string(0).unwrap();
            let col2 = st.get_int(1).unwrap();
            let col3 = st.get_date(2).unwrap();
            let col4 = st.get_int64(3).unwrap();
            let col5 = st.get_float(4).unwrap();
            let col6 = st.get_blob(5).unwrap();

            print!("{}\t{}\t{}\t{}\t{}", col1, col2, col3, col4, col5);
            
            let mut col6_str = String::new();
            col6_str.push_str("0x");
            for c in &col6 {
                col6_str.push_str(&format!("{:02X}", c));
            }
            print!("\t{}", col6_str);

            let col7 = st.get_string(6).unwrap();
            println!("\t{}", col7);
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
        let byte_param = vec![0xDEu8, 0xADu8, 0xDEu8, 0xEFu8];
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
                "select ?, ?, cast(? as datetime), cast(? as numeric), cast(? as numeric(18,2)), cast(? as image), cast(? as text)",
                &params)
            .unwrap();
        assert!(has_results);
        
        while st.next().unwrap() {
            assert_eq!("aaa", st.get_string(0).unwrap());
            assert_eq!(2i32, st.get_int(1).unwrap());
            assert_eq!("Jul  5 1986 10:30:31:100AM", st.get_string(2).unwrap());
            assert_eq!(40000000000i64, st.get_int64(3).unwrap());
            assert_eq!(3.14f64, st.get_float(4).unwrap());
            assert_eq!(vec![0xDEu8, 0xADu8, 0xDEu8, 0xEFu8], st.get_blob(5).unwrap());
            assert_eq!("ccc", st.get_string(6).unwrap());
        }
    }

}

