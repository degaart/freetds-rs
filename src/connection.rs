use std::{ptr, mem, ffi::CString, rc::Rc};
use freetds_sys::*;
use crate::command::CommandArg;
use crate::{context::Context, property::Property, Result, error::err, error::Error, command::Command};
use crate::statement::ToSql;

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
struct RowResult {
    columns: Vec<Column>,
    rows: Vec<Option<Vec<u8>>>
}

#[derive(Debug)]
enum SqlResult {
    Row(RowResult),
    Status(i32),
    Error
}

#[derive(Debug)]
pub struct ResultSet {
    results: Vec<SqlResult>
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

        let mut results: Vec<SqlResult> = Vec::new();
        loop {
            let (ret, res_type) = command.results()?;
            if !ret {
                break;
            }

            match res_type {
                CS_ROW_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    results.push(SqlResult::Row(row_result));
                },
                CS_STATUS_RESULT => {
                    let row_result = Self::fetch_result(&mut command)?;
                    let row: &Vec<u8> = row_result.rows[0].as_ref().unwrap();
                    let status: i32;
                    unsafe {
                        let buf: *const i32 = mem::transmute(row.as_ptr());
                        status = *buf;
                    }
                    results.push(SqlResult::Status(status));
                },
                CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT => {
                    command.cancel(CS_CANCEL_CURRENT)?;
                },
                CS_CMD_FAIL => {
                    results.push(SqlResult::Error);
                },
                _ => {
                    /* Do nothing, most notably, ignore CS_CMD_SUCCEED and CS_CMD_DONE */
                }
            }
        }

        Ok(ResultSet {
            results
        })
    }

    fn fetch_result(cmd: &mut Command) -> Result<RowResult> {
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

        let mut rows: Vec<Option<Vec<u8>>> = Vec::new();
        while cmd.fetch()? {
            for col_idx in 0..ncols {
                let bind = &binds[col_idx];
                match bind.indicator {
                    -1 => {
                        rows.push(None);
                    },
                    0 => {
                        let len = bind.data_length as usize;
                        let buffer: Vec<u8> = Vec::from(&bind.buffer.as_slice()[0..len]);
                        rows.push(Some(buffer));
                    },
                    _ => {
                        return err!("Data truncation occured");
                    }
                }
            }
        }
        Ok(RowResult {
            columns,
            rows
        })
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
    use crate::property::Property;
    use crate::{context::Context, debug1};
    use crate::{CS_CLIENTCHARSET, CS_USERNAME, CS_PASSWORD, CS_DATABASE, CS_TDS_VERSION, CS_LOGIN_TIMEOUT, CS_TDS_50};
    use super::Connection;

    #[test]
    fn test_execute() {
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

        let text = "select 1";
        let rs = conn.execute(&text, &[]).unwrap();
        dbg!(rs);
    }

}

