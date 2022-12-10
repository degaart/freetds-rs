use std::mem;

use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
use freetds_sys::*;
use crate::{Connection, Error, Result, column_id::ColumnId};

#[derive(Debug, Default, Clone)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) fmt: CS_DATAFMT
}

#[derive(Debug)]
pub struct Row {
    pub(crate) buffers: Vec<Option<Vec<u8>>>
}

#[derive(Debug)]
pub struct Rows {
    pub(crate) columns: Vec<Column>,
    pub(crate) rows: Vec<Row>,
    pub(crate) pos: Option<usize>
}

impl Rows {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Self { columns, rows, pos: Default::default() }
    }
}

pub struct ResultSet {
    pub(crate) conn: Connection,
    pub(crate) results: Vec<Rows>,
    pub(crate) pos: Option<usize>,
    pub(crate) status: Option<i32>,
    pub(crate) messages: Vec<Error>
}

impl ResultSet {

    pub(crate) fn new(conn: Connection, results: Vec<Rows>, status: Option<i32>, messages: Vec<Error>) -> Self {
        Self { conn, results, pos: Default::default(), status, messages }
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
            return Err(Error::from_message("Invalid statement state"));
        }

        Ok(self.results[pos].columns.len())
    }

    pub fn column_name(&self, index: usize) -> Result<Option<String>> {
        let pos = self.pos.unwrap_or(0);
        if pos >= self.results.len() {
            return Err(Error::from_message("Invalid statement state"));
        }

        if index >= self.results[pos].columns.len() {
            return Err(Error::from_message("Invalid column index"));
        }

        let column_name = &self.results[pos].columns[index].name;
        if column_name.is_empty() {
            Ok(None)
        } else {
            Ok(Some(column_name.clone()))
        }
    }

    fn convert_buffer<T>(&mut self, col: impl Into<ColumnId>, mut sink: impl FnMut(&mut Connection, &Vec<u8>, &CS_DATAFMT) -> Result<T>) -> Result<Option<T>> {
        if self.pos.is_none() {
            return Err(Error::from_message("Invalid state"));
        }
        
        let pos = self.pos.unwrap();
        if pos >= self.results.len() {
            return Err(Error::from_message("Invalid state"));
        }
        
        let results = &self.results[pos];
        if results.pos.is_none() {
            return Err(Error::from_message("Invalid state"));
        }

        let pos = results.pos.unwrap();
        if pos >= results.rows.len() {
            return Err(Error::from_message("Invalid state"));
        }

        let row = &results.rows[pos];
        let col: ColumnId = col.into();
        let col: usize = match col {
            ColumnId::I32(i) => i.try_into().expect("Invalid column index"),
            ColumnId::String(s) => {
                let mut column_index: Option<usize> = None;
                for i in 0..self.column_count()? {
                    if self.column_name(i)?.unwrap_or(String::from("")) == s {
                        column_index = Some(i);
                        break;
                    }
                }
                column_index.expect("Invalid column name")
            }
        };

        if col >= results.columns.len() {
            return Err(Error::from_message("Invalid column index"));
        }

        let column = &results.columns[col];
        let buffer = &row.buffers[col];
        match buffer {
            None => {
                Ok(None)
            },
            Some(buffer) => {
                let result = sink(&mut self.conn, buffer, &column.fmt)?;
                Ok(Some(result))
            }
        }
    }

    pub fn get_i64(&mut self, col: impl Into<ColumnId>) -> Result<Option<i64>> {
        self.convert_buffer(col, |conn, buffer, fmt| {
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
                    let dstlen = conn.convert(
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

    pub fn get_i32(&mut self, col: impl Into<ColumnId>) -> Result<Option<i32>> {
        self.convert_buffer(col, |conn, buffer, fmt| {
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
                    let dstlen = conn.convert(
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

    pub fn get_bool(&mut self, col: impl Into<ColumnId>) -> Result<Option<bool>> {
        let val = self.get_i64(col)?;
        match val {
            None => Ok(None),
            Some(val) => Ok(Some(val != 0))
        }
    }

    pub fn get_f64(&mut self, col: impl Into<ColumnId>) -> Result<Option<f64>> {
        self.convert_buffer(col, |conn, buffer, fmt| {
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
                    let dstlen = conn.convert(
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

    pub fn get_string(&mut self, col: impl Into<ColumnId>) -> Result<Option<String>> {
        self.convert_buffer(col, |conn, buffer, fmt| {
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
                    let dstlen = conn.convert(
                        &fmt, &buffer,
                        &dstfmt,
                        &mut dstdata)?;
                    Ok(String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string())
                }
            }
        })
    }
    
    pub fn get_date(&mut self, col: impl Into<ColumnId>) -> Result<Option<NaiveDate>> {
        match self.get_daterec(col)? {
            None => Ok(None),
            Some(date_rec) => {
                Ok(
                    Some(
                        NaiveDate::from_ymd_opt(
                            date_rec.dateyear,
                            (date_rec.datemonth + 1) as u32,
                            date_rec.datedmonth as u32)
                        .ok_or(Error::new(None, None, "Invalid date"))?
                    )
                )
            }
        }
    }

    pub fn get_time(&mut self, col: impl Into<ColumnId>) -> Result<Option<NaiveTime>> {
        match self.get_daterec(col)? {
            None => Ok(None),
            Some(date_rec) => {
                Ok(Some(NaiveTime::from_hms_milli_opt(
                    date_rec.datehour as u32, 
                    date_rec.dateminute as u32, 
                    date_rec.datesecond as u32, 
                    date_rec.datemsecond as u32)
                    .ok_or(Error::new(None, None, "Invalid time"))?))
            }
        }
    }

    pub fn get_datetime(&mut self, col: impl Into<ColumnId>) -> Result<Option<NaiveDateTime>> {
        match self.get_daterec(col)? {
            None => Ok(None),
            Some(date_rec) => {
                let date = NaiveDate::from_ymd_opt(
                    date_rec.dateyear,
                    (date_rec.datemonth + 1) as u32,
                    date_rec.datedmonth as u32)
                    .ok_or(Error::new(None, None, "Invalid date"))?;
                Ok(Some(date.and_hms_milli_opt(date_rec.datehour as u32, 
                    date_rec.dateminute as u32, 
                    date_rec.datesecond as u32, 
                    date_rec.datemsecond as u32)
                    .ok_or(Error::new(None, None, "Invalid time"))?))
            }
        }
    }

    pub fn get_blob(&mut self, col: impl Into<ColumnId>) -> Result<Option<Vec<u8>>> {
        self.convert_buffer(col, |conn, buffer, fmt| {
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
                    let dstlen = conn.convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                    dstdata.resize(dstlen, Default::default());
                    Ok(dstdata)
                }
            }
        })
    }

    fn get_daterec(&mut self, col: impl Into<ColumnId>) -> Result<Option<CS_DATEREC>> {
        self.convert_buffer(col, |conn, buffer, fmt| {
            match fmt.datatype {
                CS_DATE_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_DATE>());
                        let buf: *const CS_DATE = mem::transmute(buffer.as_ptr());
                        Ok(conn.crack_date(*buf)?)
                    }
                },
                CS_TIME_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_TIME>());
                        let buf: *const CS_TIME = mem::transmute(buffer.as_ptr());
                        Ok(conn.crack_time(*buf)?)
                    }
                },
                CS_DATETIME_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_DATETIME>());
                        let buf: *const CS_DATETIME = mem::transmute(buffer.as_ptr());
                        Ok(conn.crack_datetime(*buf)?)
                    }
                },
                CS_DATETIME4_TYPE => {
                    unsafe {
                        assert!(buffer.len() == mem::size_of::<CS_DATETIME4>());
                        let buf: *const CS_DATETIME4 = mem::transmute(buffer.as_ptr());
                        Ok(conn.crack_smalldatetime(*buf)?)
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
                    let dstlen = conn.convert(
                        &fmt, &buffer,
                        &dstfmt,
                        &mut dstdata)?;
                    
                    assert!(dstlen == mem::size_of::<CS_DATETIME>());
                    unsafe {
                        let buf: *const CS_DATETIME = mem::transmute(dstdata.as_ptr());
                        Ok(conn.crack_datetime(*buf)?)
                    }
                }
            }
        })
    }

    pub fn status(&self) -> Option<i32> {
        self.status
    }

    pub fn messages(&self) -> &Vec<Error> {
        &self.messages
    }

    pub fn error(&self) -> Option<Error> {
        match self.messages.first() {
            None => None,
            Some(error) => Some(error.clone())
        }
    }

}

