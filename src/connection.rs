use std::{ptr, mem, ffi::CString, rc::Rc};
use freetds_sys::*;
use crate::command::CommandArg;
use crate::{context::Context, property::Property, Result, error::err, error::Error, command::Command};
use crate::to_sql::ToSql;

#[derive(PartialEq, Debug)]
enum TextPiece {
    Literal(String),
    Placeholder
}

struct ParsedQuery {
    text: String,
    pieces: Vec<TextPiece>,
    param_count: i32
}

#[derive(Debug, Clone, Default)]
struct Bind {
    buffer: Vec<u8>,
    data_length: i32,
    indicator: i16
}

#[derive(Debug, Default, Clone)]
struct Column {
    name: String,
    fmt: CS_DATAFMT
}

#[derive(Debug)]
struct Row {
    buffers: Vec<Option<Vec<u8>>>
}

#[derive(Debug)]
struct Rows {
    columns: Vec<Column>,
    rows: Vec<Row>,
    pos: Option<usize>
}

impl Rows {
    fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Self { columns, rows, pos: Default::default() }
    }
}

#[derive(Debug)]
pub struct ResultSet {
    ctx: Context,
    results: Vec<Rows>,
    pos: Option<usize>
}

impl ResultSet {
    fn new(ctx: &Context, results: Vec<Rows>) -> Self {
        Self {
            ctx: ctx.clone(),
            results,
            pos: Default::default()
        }
    }

    pub fn next_resultset(&mut self) -> bool {
        match self.pos {
            None => {
                if self.results.len() == 0 {
                    return false;
                } else {
                    self.pos = Some(0);
                }
            },
            Some(pos) => {
                self.pos = Some(pos + 1);
            }
        }

        return self.pos.unwrap() < self.results.len();
    }

    pub fn next(&mut self) -> bool {
        if self.pos.is_none() {
            if !self.next_resultset() {
                return false;
            }
        }

        let result = &mut self.results[self.pos.unwrap()];
        match result.pos {
            None => {
                if result.rows.len() == 0 {
                    return false;
                } else {
                    result.pos = Some(0);
                }
            },
            Some(pos) => {
                result.pos = Some(pos + 1);
            }
        }

        return result.pos.unwrap() < result.rows.len();
    }

    pub fn has_rows(&self) -> bool {
        self.results.len() > 0
    }

    pub fn column_count(&self) -> Result<usize> {
        let pos = self.pos.unwrap_or(0);
        if pos >= self.results.len() {
            return err!("Invalid statement state");
        }

        Ok(self.results[pos].columns.len())
    }

    fn convert_buffer<T>(&mut self, col: impl TryInto<usize>, mut sink: impl FnMut(&mut Context, &Vec<u8>, &CS_DATAFMT) -> Result<T>) -> Result<Option<T>> {
        if self.pos.is_none() {
            return err!("Invalid state");
        }
        
        let pos = self.pos.unwrap();
        if pos >= self.results.len() {
            return err!("Invalid state");
        }
        
        let results = &self.results[pos];
        if results.pos.is_none() {
            return err!("Invalid state");
        }

        let pos = results.pos.unwrap();
        if pos >= results.rows.len() {
            return err!("Invalid state");
        }

        let row = &results.rows[pos];
        let col: usize = col
            .try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        if col >= results.columns.len() {
            return err!("Invalid column index");
        }

        let column = &results.columns[col];
        let buffer = &row.buffers[col];
        match buffer {
            None => {
                Ok(None)
            },
            Some(buffer) => {
                let result = sink(&mut self.ctx, buffer, &column.fmt)?;
                Ok(Some(result))
            }
        }
    }

    fn get_i64(&mut self, col: impl TryInto<usize>) -> Result<Option<i64>> {
        self.convert_buffer(col, |ctx, buffer, fmt| {
            match fmt.datatype {
                CS_LONG_TYPE => {
                    unsafe {
                        assert_eq!(buffer.len(), mem::size_of::<i64>());
                        let buf: *const i64 = mem::transmute(buffer.as_ptr());
                        Ok(*buf)
                    }
                },
                _ => {
                    let mut dstfmt: CS_DATAFMT = Default::default();
                    dstfmt.datatype = CS_LONG_TYPE;
                    dstfmt.maxlength = mem::size_of::<i64>() as i32;
                    dstfmt.format = CS_FMT_UNUSED as i32;
                    dstfmt.count = 1;
    
                    let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                    let dstlen = ctx.convert(
                        &fmt, &buffer,
                        &dstfmt,
                        &mut dstdata)?;
                    
                    assert_eq!(dstlen, mem::size_of::<i64>());
                    unsafe {
                        let buf: *const i64 = mem::transmute(dstdata.as_ptr());
                        Ok(*buf)
                    }
                }
            }
        })
    }

    fn get_i32(&mut self, col: impl TryInto<usize>) -> Result<Option<i32>> {
        self.convert_buffer(col, |ctx, buffer, fmt| {
            match fmt.datatype {
                CS_INT_TYPE => {
                    unsafe {
                        assert_eq!(buffer.len(), mem::size_of::<i32>());
                        let buf: *const i32 = mem::transmute(buffer.as_ptr());
                        Ok(*buf)
                    }
                },
                _ => {
                    let mut dstfmt: CS_DATAFMT = Default::default();
                    dstfmt.datatype = CS_INT_TYPE;
                    dstfmt.maxlength = mem::size_of::<i32>() as i32;
                    dstfmt.format = CS_FMT_UNUSED as i32;
                    dstfmt.count = 1;
    
                    let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                    let dstlen = ctx.convert(
                        &fmt, &buffer,
                        &dstfmt,
                        &mut dstdata)?;
                    
                    assert_eq!(dstlen, mem::size_of::<i32>());
                    unsafe {
                        let buf: *const i32 = mem::transmute(dstdata.as_ptr());
                        Ok(*buf)
                    }
                }
            }
        })
    }

    fn get_f64(&mut self, col: impl TryInto<usize>) -> Result<Option<f64>> {
        self.convert_buffer(col, |ctx, buffer, fmt| {
            match fmt.datatype {
                CS_FLOAT_TYPE => {
                    unsafe {
                        assert_eq!(buffer.len(), mem::size_of::<f64>());
                        let buf: *const f64 = mem::transmute(buffer.as_ptr());
                        Ok(*buf)
                    }
                },
                _ => {
                    let mut dstfmt: CS_DATAFMT = Default::default();
                    dstfmt.datatype = CS_FLOAT_TYPE;
                    dstfmt.maxlength = mem::size_of::<f64>() as i32;
                    dstfmt.format = CS_FMT_UNUSED as i32;
                    dstfmt.count = 1;
    
                    let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                    let dstlen = ctx.convert(
                        &fmt, &buffer,
                        &dstfmt,
                        &mut dstdata)?;
                    
                    assert_eq!(dstlen, mem::size_of::<f64>());
                    unsafe {
                        let buf: *const f64 = mem::transmute(dstdata.as_ptr());
                        Ok(*buf)
                    }
                }
            }
        })
    }

    fn get_string(&mut self, col: impl TryInto<usize>) -> Result<Option<String>> {
        self.convert_buffer(col, |ctx, buffer, fmt| {
            match fmt.datatype {
                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                    let value = String::from_utf8_lossy(buffer);
                    Ok(value.to_string())
                },
                _ => {
                    let mut dstfmt: CS_DATAFMT = Default::default();
                    dstfmt.datatype = CS_CHAR_TYPE;
                    dstfmt.maxlength = match fmt.datatype {
                        CS_BINARY_TYPE | CS_LONGBINARY_TYPE | CS_IMAGE_TYPE => {
                            ((buffer.len() * 2) + 16) as i32
                        },
                        _ => {
                            128
                        }
                    };
                    dstfmt.format = CS_FMT_UNUSED as i32;
                    dstfmt.count = 1;
    
                    let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                    let dstlen = ctx.convert(
                        &fmt, &buffer,
                        &dstfmt,
                        &mut dstdata)?;
                    Ok(String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string())
                }
            }
        })
    }

    fn get_date(&mut self, col: impl TryInto<usize>) -> Result<Option<CS_DATEREC>> {
        self.convert_buffer(col, |ctx, buffer, fmt| {
            match fmt.datatype {
                CS_DATE_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_DATE>());
                        let buf: *const CS_DATE = mem::transmute(buffer.as_ptr());
                        Ok(ctx.crack_date(*buf)?)
                    }
                },
                CS_TIME_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_TIME>());
                        let buf: *const CS_TIME = mem::transmute(buffer.as_ptr());
                        Ok(ctx.crack_time(*buf)?)
                    }
                },
                CS_DATETIME_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_DATETIME>());
                        let buf: *const CS_DATETIME = mem::transmute(buffer.as_ptr());
                        Ok(ctx.crack_datetime(*buf)?)
                    }
                },
                CS_DATETIME4_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_DATETIME4>());
                        let buf: *const CS_DATETIME4 = mem::transmute(buffer.as_ptr());
                        Ok(ctx.crack_smalldatetime(*buf)?)
                    }
                },
                _ => {
                    let mut dstfmt: CS_DATAFMT = Default::default();
                    dstfmt.datatype = CS_DATETIME_TYPE;
                    dstfmt.maxlength = mem::size_of::<CS_DATETIME>() as i32;
                    dstfmt.format = CS_FMT_UNUSED as i32;
                    dstfmt.count = 1;

                    let mut dstdata: Vec<u8> = Vec::new();
                    dstdata.resize(dstfmt.maxlength as usize, Default::default());
                    let dstlen = ctx.convert(
                        &fmt, &buffer,
                        &dstfmt,
                        &mut dstdata)?;
                    
                    assert!(dstlen == mem::size_of::<CS_DATETIME>());
                    unsafe {
                        let buf: *const CS_DATETIME = mem::transmute(dstdata.as_ptr());
                        Ok(ctx.crack_datetime(*buf)?)
                    }
                }
            }
        })
    }
    
    fn get_blob(&mut self, col: impl TryInto<usize>) -> Result<Option<Vec<u8>>> {
        self.convert_buffer(col, |ctx, buffer, fmt| {
            match fmt.datatype {
                CS_BINARY_TYPE | CS_LONGBINARY_TYPE | CS_IMAGE_TYPE => {
                    Ok(buffer.clone())
                },
                CS_VARBINARY_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_VARBINARY>());
                        let buf: *const CS_VARBINARY = mem::transmute(buffer.as_ptr());
                        let len = (*buf).len as usize;
                        let res: Vec<u8> = (*buf).array.iter().take(len).map(|c| *c as u8).collect();
                        Ok(res)
                    }
                },
                _ => {
                    let mut dstfmt: CS_DATAFMT = Default::default();
                    dstfmt.datatype = CS_BINARY_TYPE;
                    dstfmt.maxlength = fmt.maxlength;
                    dstfmt.format = CS_FMT_UNUSED as i32;
                    dstfmt.count = 1;

                    let mut dstdata: Vec<u8> = Vec::new();
                    dstdata.resize(dstfmt.maxlength as usize, Default::default());
                    let dstlen = ctx.convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                    dstdata.resize(dstlen, Default::default());
                    Ok(dstdata)
                }
            }
        })
    }

}

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

    pub fn execute(&mut self, text: impl AsRef<str>, params: &[&dyn ToSql]) -> Result<ResultSet> {
        let parsed_query = Self::parse_query(text.as_ref());
        let text = Self::generate_query(&parsed_query, params);

        let mut command = Command::new(&self);
        command.command(CS_LANG_CMD, CommandArg::String(&text), CS_UNUSED)?;
        command.send()?;

        let mut results: Vec<Rows> = Vec::new();
        loop {
            let (ret, res_type) = command.results()?;
            if !ret {
                break;
            }

            match res_type {
                CS_ROW_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    results.push(row_result);
                },
                CS_STATUS_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    let row: &Vec<u8> = row_result.rows[0].buffers[0].as_ref().unwrap();
                    let status: i32;
                    unsafe {
                        let buf: *const i32 = mem::transmute(row.as_ptr());
                        status = *buf;
                    }
                    if status != 0 {
                        command.cancel(CS_CANCEL_ALL).unwrap();
                        return err!("Query returned an error status: {}", status);
                    }
                },
                CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT => {
                    command.cancel(CS_CANCEL_CURRENT)?;
                },
                CS_CMD_FAIL => {
                    return err!("Query execution resulted in error");
                },
                _ => {
                    /* Do nothing, most notably, ignore CS_CMD_SUCCEED and CS_CMD_DONE */
                }
            }
        }

        Ok(ResultSet::new(&self.ctx, results))
    }

    fn fetch_result(cmd: &mut Command) -> Result<Rows> {
        let ncols: usize = cmd
            .res_info(CS_NUMDATA)
            .unwrap();
        let mut binds: Vec<Bind> = vec![Default::default(); ncols];
        let mut columns: Vec<Column> = vec![Default::default(); ncols];
        for col_idx in 0..ncols {
            let bind = &mut binds[col_idx];
            let column = &mut columns[col_idx];

            column.fmt = cmd.describe((col_idx + 1) as i32)?;
            column.fmt.format = CS_FMT_UNUSED as i32;
            match column.fmt.datatype {
                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                    column.fmt.maxlength += 1;
                    column.fmt.format = CS_FMT_NULLTERM as i32;
                },
                _ => {}
            }
            bind.buffer.resize(column.fmt.maxlength as usize, 0);
            column.fmt.count = 1;

            unsafe {
                cmd.bind_unsafe(
                    (col_idx + 1) as i32,
                    &mut column.fmt,
                    mem::transmute(bind.buffer.as_mut_ptr()),
                    &mut bind.data_length,
                    &mut bind.indicator)?;
            }
        }

        let mut rows: Vec<Row> = Vec::new();
        while cmd.fetch()? {
            let mut row = Row { buffers: Vec::new() };
            for col_idx in 0..ncols {
                let bind = &binds[col_idx];
                match bind.indicator {
                    -1 => {
                        row.buffers.push(None);
                    },
                    0 => {
                        let len = bind.data_length as usize;
                        let buffer: Vec<u8> = match columns[col_idx].fmt.datatype {
                            CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                                Vec::from(&bind.buffer.as_slice()[0..len-1])
                            },
                            _ => {
                                Vec::from(&bind.buffer.as_slice()[0..len])
                            }
                        };

                        row.buffers.push(Some(buffer));
                    },
                    _ => {
                        return err!("Data truncation occured");
                    }
                }
            }
            rows.push(row);
        }
        Ok(Rows::new(columns, rows))
    }

    /*
        TODO: Handle string quoting '''' and """"
    */
    fn parse_query(text: impl AsRef<str>) -> ParsedQuery {
        let mut pieces: Vec<TextPiece> = Vec::new();
        let mut param_count: i32 = 0;
        let mut cur = String::new();
        let mut it = text.as_ref().chars().peekable();
        loop {
            let c = it.next();
            match c {
                None => {
                    break;
                },
                Some(c) => {
                    match c {
                        '\'' | '"' => {
                            cur.push(c);
                            while let Some(c1) = it.next() {
                                cur.push(c1);
                                if c1 == c {
                                    break;
                                }
                            }
                        },
                        '/' => {
                            if it.peek().unwrap_or(&'\0') == &'*' {
                                cur.push(c);
                                while let Some(c1) = it.next() {
                                    cur.push(c1);
                                    if c1 == '*' && it.peek().unwrap_or(&'\0') == &'/' {
                                        break;
                                    }
                                }
                            } else {
                                cur.push(c);
                            }
                        },
                        '-' => {
                            if it.peek().unwrap_or(&'\0') == &'-' {
                                cur.push(c);
                                while let Some(c1) = it.next() {
                                    cur.push(c1);
                                }
                            }
                        },
                        '?' => {
                            if cur.len() > 0 {
                                pieces.push(TextPiece::Literal(cur.clone()));
                                cur.clear();
                            }
                            pieces.push(TextPiece::Placeholder);
                            param_count += 1;
                        },
                        _ => {
                            cur.push(c);
                        }
                    }
                }
            }
        }
    
        if cur.len() > 0 {
            pieces.push(TextPiece::Literal(cur.clone()));
        }
        
        ParsedQuery {
            text: text.as_ref().to_string(),
            pieces: pieces,
            param_count: param_count
        }
    }

    fn generate_query(query: &ParsedQuery, params: &[&dyn ToSql]) -> String {
        let mut result = String::new();
        let mut params = params.iter();
        for piece in &query.pieces {
            result.push_str(&match piece {
                TextPiece::Literal(s) => {
                    s.to_string()
                },
                TextPiece::Placeholder => {
                    match params.next() {
                        Some(value) => {
                            value.to_sql()
                        },
                        None => {
                            "null".to_string()
                        }
                    }
                }
            });
        }
        return result;
    }
}

#[cfg(test)]
mod tests {
    use freetds_sys::CS_DATEREC;

    use crate::connection::TextPiece;
    use crate::property::Property;
    use crate::to_sql::ToSql;
    use crate::{context::Context, debug1};
    use crate::{CS_CLIENTCHARSET, CS_USERNAME, CS_PASSWORD, CS_DATABASE, CS_TDS_VERSION, CS_LOGIN_TIMEOUT, CS_TDS_50};
    use super::Connection;

    fn connect() -> (Context, Connection) {
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

        (ctx, conn)
    }

    #[test]
    fn test_select() {
        let (_, mut conn) = connect();

        let text = 
            "select 'aaaa', \
                2, 5000000000, \
                3.14, \
                cast('1986/07/05 10:30:31.1' as datetime), \
                cast(3.14 as numeric(18,2)), \
                cast(0xDEADBEEF as image), \
                cast('ccc' as text), \
                null";
        let mut rs = conn.execute(&text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(rs.get_string(0).unwrap().unwrap(), "aaaa");
        assert_eq!(rs.get_i32(1).unwrap().unwrap(), 2);
        assert_eq!(rs.get_i64(2).unwrap().unwrap(), 5000000000);
        assert_eq!(rs.get_f64(3).unwrap().unwrap(), 3.14);
        assert_eq!(rs.get_date(4).unwrap().unwrap(), CS_DATEREC {
            dateyear: 1986,
            datemonth: 6,
            datedmonth: 5,
            datehour: 10,
            dateminute: 30,
            datesecond: 31,
            datedyear: 186,
            datedweek: 6,
            datemsecond: 100,
            ..Default::default()
        });
        assert_eq!(rs.get_f64(5).unwrap().unwrap(), 3.14);
        assert_eq!(rs.get_blob(6).unwrap().unwrap(), vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(rs.get_string(7).unwrap().unwrap(), "ccc".to_string());
        assert!(rs.get_string(8).unwrap().is_none());
        assert!(!rs.next());
    }

    #[test]
    fn test_params() {
        let (_, mut conn) = connect();
        let text = "\
            create table #test(\
                col1 varchar(10), \
                col2 int, \
                col3 numeric, \
                col4 numeric(18,2), \
                col5 datetime, \
                col6 image, \
                col7 text)";
        conn.execute(text, &[]).unwrap();

        let text = "\
            insert into #test(col1, col2, col3, col4, col5, col6, col7) \
            values(?, ?, ?, ?, ?, ?, ?)";
        conn
            .execute(text, &[
                &"aaa",
                &2i32,
                &5000000000i64,
                &3.14f64,
                &CS_DATEREC {
                    dateyear: 1986,
                    datemonth: 6,
                    datedmonth: 5,
                    datehour: 10,
                    dateminute: 30,
                    datesecond: 31,
                    datedyear: 186,
                    datedweek: 6,
                    datemsecond: 100,
                    ..Default::default()
                },
                &vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8],
                &"bbb"
            ])
            .unwrap();
        
        let text = "select * from #test";
        let mut rs = conn.execute(text, &[]).unwrap();
        assert!(rs.next());
        let ncols: usize = rs.column_count().unwrap();
        assert_eq!(ncols, 7);
        assert_eq!("aaa", rs.get_string(0).unwrap().unwrap());
        assert_eq!(2, rs.get_i32(1).unwrap().unwrap());
        assert_eq!(5000000000, rs.get_i64(2).unwrap().unwrap());
        assert_eq!(3.14, rs.get_f64(3).unwrap().unwrap());
        assert_eq!(CS_DATEREC {
            dateyear: 1986,
            datemonth: 6,
            datedmonth: 5,
            datehour: 10,
            dateminute: 30,
            datesecond: 31,
            datedyear: 186,
            datedweek: 6,
            datemsecond: 0,
            ..Default::default()
        }, rs.get_date(4).unwrap().unwrap());
        assert_eq!(vec![0xDEu8, 0xADu8, 0xBEu8, 0xEFu8], rs.get_blob(5).unwrap().unwrap());
        assert_eq!("bbb", rs.get_string(6).unwrap().unwrap());
    }

    #[test]
    fn test_multiple_rows() {
        let (_, mut conn) = connect();

        let text = "select 1 union select 2 union select 3";
        let mut rs = conn.execute(text, &[]).unwrap();
        assert!(rs.next());
        assert_eq!(1, rs.get_i32(0).unwrap().unwrap());
        assert!(rs.next());
        assert_eq!(2, rs.get_i32(0).unwrap().unwrap());
        assert!(rs.next());
        assert_eq!(3, rs.get_i32(0).unwrap().unwrap());
        assert!(!rs.next());
    }

    #[test]
    fn test_parse_query() {
        let s = "?, '?', ?, \"?\", ? /* que? */, ? -- ?no?";
        let query = Connection::parse_query(s);
        assert_eq!(query.text, s);
        assert_eq!(query.pieces.len(), 8);
        assert_eq!(query.param_count, 4);

        let concated: String = query.pieces.iter().map(
            |p| match p {
                TextPiece::Literal(s) => {
                    &s
                },
                TextPiece::Placeholder => {
                    "?"
                }
            })
            .collect();
        assert_eq!(s, concated);
    }

    #[test]
    fn test_generate_query() {
        let s = "string: ?, i32: ?, i64: ?, f64: ?, date: ?, image: ?";
        let mut params: Vec<&dyn ToSql> = Vec::new();
        params.push(&"aaa");
        params.push(&1i32);
        params.push(&2i64);
        params.push(&3.14f64);

        let param5 = CS_DATEREC {
            dateyear: 1986,
            datemonth: 6,
            datedmonth: 5,
            datehour: 10,
            dateminute: 30,
            datesecond: 31,
            ..Default::default()
        };
        params.push(&param5);

        let param6 = vec![0xDE_u8, 0xAD_u8, 0xBE_u8, 0xEF_u8];
        params.push(&param6);

        let parsed_query = Connection::parse_query(s);
        let generated = Connection::generate_query(&parsed_query, &params);
        assert_eq!("string: 'aaa', i32: 1, i64: 2, f64: 3.14, date: '1986/07/05 10:30:31.0', image: 0xDEADBEEF", generated);
    }

}

