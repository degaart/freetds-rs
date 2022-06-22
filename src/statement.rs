use std::mem;
use std::fmt::Debug;
use freetds_sys::*;
use crate::command::CommandArg;
use crate::error::err;
use crate::{command::Command, connection::Connection, error::Error};
use crate::Result;

pub trait ToSql {
    fn to_sql(&self) -> String;

}
pub type ToSqlValue = Box<dyn ToSql>;

impl ToSql for &str {
    fn to_sql(&self) -> String {
        let mut result = String::new();
        result.push('\'');
        for c in self.chars() {
            result.push(c);
            if c == '\'' {
                result.push(c);
            }
        }
        result.push('\'');
        return result;
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> String {
        return format!("{}", self);
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> String {
        return format!("{}", self);
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> String {
        return format!("{}", self);
    }
}

impl ToSql for CS_DATEREC {
    fn to_sql(&self) -> String {
        format!("'{:04}/{:02}/{:02} {:02}:{:02}:{:02}.{}'",
            self.dateyear,
            self.datemonth + 1,
            self.datedmonth,
            self.datehour,
            self.dateminute,
            self.datesecond,
            self.datesecfrac)
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> String {
        let mut result = String::new();
        result.push_str("0x");
        for c in self.iter() {
            result.push_str(&format!("{:02X}", c));
        }
        return result;
    }
}

impl ToSql for &[u8] {
    fn to_sql(&self) -> String {
        let mut result = String::new();
        result.push_str("0x");
        for c in self.iter() {
            result.push_str(&format!("{:02X}", c));
        }
        return result;
    }
}

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
    fmt: CS_DATAFMT,
    buffer: Vec<u8>,
    data_length: i32,
    indicator: i16
}

#[derive(PartialEq)]
enum StatementState {
    New,
    ResultsReady,
    ResultSetDone,
    Done,
}

pub struct Statement {
    command: Command,
    state: StatementState,
    binds: Vec<Bind>,
    has_errors: bool
}

impl Statement {
    pub fn new(conn: &mut Connection) -> Self {
        let command = Command::new(conn);
        Self {
            command: command,
            state: StatementState::New,
            binds: Vec::new(),
            has_errors: false
        }
    }

    pub fn execute(&mut self, text: impl AsRef<str>, params: &[&dyn ToSql]) -> Result<bool> {
        if self.state != StatementState::New {
            return err!("Invalid statement state");
        }

        let parsed_query = Self::parse_query(text.as_ref());
        let text = Self::generate_query(&parsed_query, params);
        
        self.command.command(CS_LANG_CMD, CommandArg::String(&text), CS_UNUSED)?;
        self.command.send()?;
        self.get_row_result()?;
        if self.has_errors {
            err!("An error occured while executing the statement")
        } else {
            Ok(true)
        }
    }

    fn create_binds(cmd: &mut Command, binds: &mut Vec<Bind>) -> Result<usize> {
        let cols: i32 = cmd
            .res_info(CS_NUMDATA)
            .unwrap();
        binds.resize(cols as usize, Default::default());
        for col in 0..cols {
            /*
                bind.name for column alias
                bind.status & CS_CANBENULL
            */
            let bind = &mut binds[col as usize];
            bind.fmt = cmd.describe(col + 1).unwrap();
            bind.fmt.format = CS_FMT_UNUSED as i32;
            match bind.fmt.datatype {
                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                    bind.fmt.maxlength += 1;
                    bind.fmt.format = CS_FMT_NULLTERM as i32;
                },
                _ => {}
            }
            bind.buffer.resize(bind.fmt.maxlength as usize, 0);
            bind.fmt.count = 1;

            unsafe {
                cmd.bind_unsafe(
                    (col + 1) as i32,
                    &mut bind.fmt,
                    mem::transmute(bind.buffer.as_mut_ptr()),
                    &mut bind.data_length,
                    &mut bind.indicator)
                .unwrap();
            }
        }
        Ok(cols as usize)
    }

    fn get_row_result(&mut self) -> Result<bool> {
        match self.state {
            StatementState::Done => {
                return Ok(false)
            },
            StatementState::New | StatementState::ResultsReady => {
                if self.state == StatementState::ResultsReady {
                    self.command.cancel(CS_CANCEL_CURRENT)?;
                }

                loop {
                    let (ret, res_type) = self.command.results()?;
                    if ret {
                        match res_type {
                            CS_ROW_RESULT => {
                                self.state = StatementState::ResultsReady;
                                Self::create_binds(&mut self.command, &mut self.binds)?;
                                return Ok(true);
                            },
                            CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT | CS_STATUS_RESULT => {
                                self.command.cancel(CS_CANCEL_CURRENT)?;
                            },
                            CS_CMD_DONE => {
                                self.state = StatementState::Done;
                            },
                            CS_CMD_FAIL => {
                                self.has_errors = true;
                            },
                            _ => {
                                /* Do nothing, most notably, ignore CS_CMD_SUCCEED */
                            }
                        }
                    } else {
                        break;
                    }
                }
                return Ok(false);
            },
            _ => {
                return err!("Invalid statement state");
            }
        }
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

    pub fn next(&mut self) -> Result<bool> {
        if self.state == StatementState::Done || self.state == StatementState::ResultSetDone {
            return Ok(false)
        } else if self.state == StatementState::New {
            return err!("Invalid statement state");
        }
        
        let ret = self.command.fetch()?;
        if !ret {
            self.state = StatementState::ResultSetDone;
            return Ok(false);
        }
        self.state = StatementState::ResultsReady;
        return Ok(true);
    }

    pub fn column_count(&self) -> Result<usize> {
        if self.state != StatementState::ResultsReady {
            err!("Invalid statement state")
        } else {
            Ok(self.binds.len())
        }
    }

    pub fn get_string(&mut self, col: impl TryInto<usize>) -> Result<Option<String>> {
        if self.state != StatementState::ResultsReady {
            return err!("Invalid statement state");
        }

        let col_index: usize = col.try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        let bind = &self.binds[col_index];
        match bind.indicator {
            -1 => {
                Ok(None)
            },
            0 => {
                match bind.fmt.datatype {
                    CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                        let len = (bind.data_length as usize) - 1;
                        let value = String::from_utf8_lossy(&bind.buffer.as_slice()[0..len]);
                        Ok(Some(value.to_string()))
                    },
                    _ => {
                        let mut dstfmt: CS_DATAFMT = Default::default();
                        dstfmt.datatype = CS_CHAR_TYPE;
                        dstfmt.maxlength = match bind.fmt.datatype {
                            CS_BINARY_TYPE | CS_LONGBINARY_TYPE | CS_IMAGE_TYPE => {
                                ((bind.data_length * 2) + 16) as i32
                            },
                            _ => {
                                128
                            }
                        };
                        dstfmt.format = CS_FMT_UNUSED as i32;
                        dstfmt.count = 1;
        
                        let mut dstdata: Vec<u8> = Vec::new();
                        dstdata.resize(dstfmt.maxlength as usize, Default::default());
                        let dstlen = self.command.conn.ctx.convert(
                            &bind.fmt, &bind.buffer,
                            &dstfmt,
                            &mut dstdata)?;
        
                        Ok(Some(String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string()))
                    }
                }
            },
            _ => {
                err!("Truncation occured")
            }
        }
    }

    pub fn get_int(&mut self, col: impl TryInto<usize>) -> Result<Option<i32>> {
        if self.state != StatementState::ResultsReady {
            return err!("Invalid statement state");
        }

        let col_index: usize = col.try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        let bind = &self.binds[col_index];
        match bind.indicator {
            -1 => {
                Ok(None)
            },
            0 => {
                match bind.fmt.datatype {
                    CS_INT_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<i32>());
                            let buf: *const i32 = mem::transmute(bind.buffer.as_ptr());
                            Ok(Some(*buf))
                        }
                    },
                    _ => {
                        let mut dstfmt: CS_DATAFMT = Default::default();
                        dstfmt.datatype = CS_INT_TYPE;
                        dstfmt.maxlength = mem::size_of::<i32>() as i32;
                        dstfmt.format = CS_FMT_UNUSED as i32;
                        dstfmt.count = 1;

                        let mut dstdata: Vec<u8> = Vec::new();
                        dstdata.resize(dstfmt.maxlength as usize, Default::default());
                        let dstlen = self.command.conn.ctx.convert(
                            &bind.fmt, &bind.buffer,
                            &dstfmt,
                            &mut dstdata)?;
                        
                        assert!(dstlen == mem::size_of::<i32>());
                        unsafe {
                            let buf: *const i32 = mem::transmute(dstdata.as_ptr());
                            Ok(Some(*buf))
                        }
                    }
                }
            },
            _ => {
                err!("Truncation occured")
            }
        }
    }

    pub fn get_int64(&mut self, col: impl TryInto<usize>) -> Result<Option<i64>> {
        if self.state != StatementState::ResultsReady {
            return err!("Invalid statement state");
        }

        let col_index: usize = col.try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        let bind = &self.binds[col_index];
        match bind.indicator {
            -1 => {
                Ok(None)
            },
            0 => {
                match bind.fmt.datatype {
                    CS_LONG_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<i64>());
                            let buf: *const i64 = mem::transmute(bind.buffer.as_ptr());
                            Ok(Some(*buf))
                        }
                    },
                    _ => {
                        let mut dstfmt: CS_DATAFMT = Default::default();
                        dstfmt.datatype = CS_LONG_TYPE;
                        dstfmt.maxlength = mem::size_of::<i64>() as i32;
                        dstfmt.format = CS_FMT_UNUSED as i32;
                        dstfmt.count = 1;

                        let mut dstdata: Vec<u8> = Vec::new();
                        dstdata.resize(dstfmt.maxlength as usize, Default::default());
                        let dstlen = self.command.conn.ctx.convert(
                            &bind.fmt, &bind.buffer,
                            &dstfmt,
                            &mut dstdata)?;
                        
                        assert!(dstlen == mem::size_of::<i64>());
                        unsafe {
                            let buf: *const i64 = mem::transmute(dstdata.as_ptr());
                            Ok(Some(*buf))
                        }
                    }
                }
            },
            _ => {
                err!("Truncation occured")
            }
        }
    }

    pub fn get_float(&mut self, col: impl TryInto<usize>) -> Result<Option<f64>> {
        if self.state != StatementState::ResultsReady {
            return err!("Invalid statement state");
        }

        let col_index: usize = col.try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        let bind = &self.binds[col_index];
        match bind.indicator {
            -1 => {
                Ok(None)
            },
            0 => {
                match bind.fmt.datatype {
                    CS_FLOAT_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<f64>());
                            let buf: *const f64 = mem::transmute(bind.buffer.as_ptr());
                            Ok(Some(*buf))
                        }
                    },
                    _ => {
                        let mut dstfmt: CS_DATAFMT = Default::default();
                        dstfmt.datatype = CS_FLOAT_TYPE;
                        dstfmt.maxlength = mem::size_of::<f64>() as i32;
                        dstfmt.format = CS_FMT_UNUSED as i32;
                        dstfmt.count = 1;

                        let mut dstdata: Vec<u8> = Vec::new();
                        dstdata.resize(dstfmt.maxlength as usize, Default::default());
                        let dstlen = self.command.conn.ctx.convert(
                            &bind.fmt, &bind.buffer,
                            &dstfmt,
                            &mut dstdata)?;
                        
                        assert!(dstlen == mem::size_of::<f64>());
                        unsafe {
                            let buf: *const f64 = mem::transmute(dstdata.as_ptr());
                            Ok(Some(*buf))
                        }
                    }
                }
            },
            _ => {
                err!("Truncation occured")
            }
        }
    }

    pub fn get_date(&mut self, col: impl TryInto<usize>) -> Result<Option<CS_DATEREC>> {
        if self.state != StatementState::ResultsReady {
            return err!("Invalid statement state");
        }

        let col_index: usize = col.try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        let bind = &self.binds[col_index];
        match bind.indicator {
            -1 => {
                Ok(None)
            },
            0 => {
                match bind.fmt.datatype {
                    CS_DATE_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<CS_DATE>());
                            let buf: *const CS_DATE = mem::transmute(bind.buffer.as_ptr());
                            Ok(Some(self.command.conn.ctx.crack_date(*buf)?))
                        }
                    },
                    CS_TIME_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<CS_TIME>());
                            let buf: *const CS_TIME = mem::transmute(bind.buffer.as_ptr());
                            Ok(Some(self.command.conn.ctx.crack_time(*buf)?))
                        }
                    },
                    CS_DATETIME_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<CS_DATETIME>());
                            let buf: *const CS_DATETIME = mem::transmute(bind.buffer.as_ptr());
                            Ok(Some(self.command.conn.ctx.crack_datetime(*buf)?))
                        }
                    },
                    CS_DATETIME4_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<CS_DATETIME4>());
                            let buf: *const CS_DATETIME4 = mem::transmute(bind.buffer.as_ptr());
                            Ok(Some(self.command.conn.ctx.crack_smalldatetime(*buf)?))
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
                        let dstlen = self.command.conn.ctx.convert(
                            &bind.fmt, &bind.buffer,
                            &dstfmt,
                            &mut dstdata)?;
                        
                        assert!(dstlen == mem::size_of::<CS_DATETIME>());
                        unsafe {
                            let buf: *const CS_DATETIME = mem::transmute(dstdata.as_ptr());
                            Ok(Some(self.command.conn.ctx.crack_datetime(*buf)?))
                        }
                    }
                }
            },
            _ => {
                err!("Truncation occured")
            }
        }
    }

    pub fn get_blob(&mut self, col: impl TryInto<usize>) -> Result<Option<Vec<u8>>> {
        if self.state != StatementState::ResultsReady {
            return err!("Invalid statement state");
        }

        let col_index: usize = col.try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        let bind = &self.binds[col_index];
        match bind.indicator {
            -1 => {
                Ok(None)
            },
            0 => {
                match bind.fmt.datatype {
                    CS_BINARY_TYPE | CS_LONGBINARY_TYPE | CS_IMAGE_TYPE => {
                        let len = bind.data_length as usize;
                        Ok(Some(bind.buffer.as_slice()[0..len].to_vec()))
                    },
                    CS_VARBINARY_TYPE => {
                        unsafe {
                            assert!(bind.buffer.len() == mem::size_of::<CS_VARBINARY>());
                            let buf: *const CS_VARBINARY = mem::transmute(bind.buffer.as_ptr());
                            let len = (*buf).len as usize;
                            let res: Vec<u8> = (*buf).array.iter().take(len).map(|c| *c as u8).collect();
                            Ok(Some(res))
                        }
                    },
                    _ => {
                        let mut dstfmt: CS_DATAFMT = Default::default();
                        dstfmt.datatype = CS_BINARY_TYPE;
                        dstfmt.maxlength = bind.fmt.maxlength;
                        dstfmt.format = CS_FMT_UNUSED as i32;
                        dstfmt.count = 1;

                        let mut dstdata: Vec<u8> = Vec::new();
                        dstdata.resize(dstfmt.maxlength as usize, Default::default());
                        let dstlen = self.command.conn.ctx.convert(&bind.fmt, &bind.buffer, &dstfmt, &mut dstdata)?;
                        dstdata.resize(dstlen, Default::default());
                        Ok(Some(dstdata))
                    }
                }
            },
            _ => {
                err!("Truncation occured")
            }
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
    use super::*;

    #[test]
    fn test_parse_query() {
        let s = "?, '?', ?, \"?\", ? /* que? */, ? -- ?no?";
        let query = Statement::parse_query(s);
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

        let parsed_query = Statement::parse_query(s);
        let generated = Statement::generate_query(&parsed_query, &params);
        assert_eq!("string: 'aaa', i32: 1, i64: 2, f64: 3.14, date: '1986/07/05 10:30:31.0', image: 0xDEADBEEF", generated);
    }

}

