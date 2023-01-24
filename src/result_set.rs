#![allow(clippy::expect_fun_call)]

use std::{mem, rc::Rc, ops::Deref};
use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
use freetds_sys::*;
use rust_decimal::Decimal;
use crate::{Connection, Error, Result, column_id::ColumnId, ParamValue};

#[derive(Debug, Default, Clone)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) fmt: CS_DATAFMT
}

#[derive(Debug)]
pub struct Row {
    pub(crate) buffers: Vec<Option<Rc<Vec<u8>>>>
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
                if self.results.is_empty() {
                    return false;
                } else {
                    self.pos = Some(0);
                }
            },
            Some(pos) => {
                self.pos = Some(pos + 1);
            }
        }

        self.pos.unwrap() < self.results.len()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        if self.pos.is_none() && !self.next_resultset() {
            return false;
        }

        let result = &mut self.results[self.pos.unwrap()];
        match result.pos {
            None => {
                if result.rows.is_empty() {
                    return false;
                } else {
                    result.pos = Some(0);
                }
            },
            Some(pos) => {
                result.pos = Some(pos + 1);
            }
        }

        result.pos.unwrap() < result.rows.len()
    }

    pub fn has_rows(&self) -> bool {
        !self.results.is_empty()
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

    fn get_buffer(&mut self, col: impl Into<ColumnId>) -> Result<Option<(CS_DATAFMT, Rc<Vec<u8>>)>> {
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
                    if self.column_name(i)?.unwrap_or_else(|| String::from("")) == s {
                        column_index = Some(i);
                        break;
                    }
                }
                column_index.expect(&format!("Invalid column name: {}", s))
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
                Ok(Some((column.fmt, Rc::clone(buffer))))
            }
        }
    }

    fn convert<T>(&mut self, srcfmt: &CS_DATAFMT, srcbuffer: &[u8], dstdatatype: i32) -> Result<T>
    where
        T: Copy
    {
        let dstfmt = CS_DATAFMT {
            datatype: dstdatatype,
            maxlength: mem::size_of::<T>() as i32,
            format: CS_FMT_UNUSED as i32,
            count: 1,
            ..Default::default()
        };

        let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
        let dstlen = self.conn.convert(
            srcfmt, srcbuffer,
            &dstfmt,
            &mut dstdata)?;
        
        assert_eq!(dstlen, mem::size_of::<T>());
        unsafe {
            let buf: *const T = mem::transmute(dstdata.as_ptr());
            Ok(*buf)
        }
    }

    pub fn get_value(&mut self, col: impl Into<ColumnId>) -> Result<Option<ParamValue>> {
        if self.pos.is_none() {
            return Err(Error::from_message("Invalid state"));
        }
        
        let pos = self.pos.unwrap();
        if pos >= self.results.len() {
            return Err(Error::from_message("Invalid state"));
        }
        
        if self.results[pos].pos.is_none() {
            return Err(Error::from_message("Invalid state"));
        }

        let row_pos = self.results[pos].pos.unwrap();
        if row_pos >= self.results[pos].rows.len() {
            return Err(Error::from_message("Invalid state"));
        }

        let row = &self.results[pos].rows[row_pos];
        let col: ColumnId = col.into();
        let col: usize = match col {
            ColumnId::I32(i) => i.try_into().expect("Invalid column index"),
            ColumnId::String(s) => {
                let mut column_index: Option<usize> = None;
                for i in 0..self.column_count()? {
                    if self.column_name(i)?.unwrap_or_else(|| String::from("")) == s {
                        column_index = Some(i);
                        break;
                    }
                }
                column_index.expect("Invalid column name")
            }
        };

        if col >= self.results[pos].columns.len() {
            return Err(Error::from_message("Invalid column index"));
        }

        let column = self.results[pos].columns[col].clone();
        let buffer = row.buffers[col].clone();

        match buffer {
            None => {
                Ok(None)
            },
            Some(buffer) => {
                match column.fmt.datatype {
                    CS_BINARY_TYPE | CS_IMAGE_TYPE => {
                        Ok(Some(ParamValue::Blob(Rc::clone(&buffer).deref().clone())))
                    },
                    CS_CHAR_TYPE | CS_TEXT_TYPE => {
                        Ok(Some(ParamValue::String(String::from_utf8_lossy(&buffer).to_string())))
                    },
                    CS_UNICHAR_TYPE => {
                        let dstfmt = CS_DATAFMT {
                            datatype: CS_CHAR_TYPE,
                            maxlength: buffer.len() as i32,
                            format: CS_FMT_UNUSED as i32,
                            count: 1,
                            ..Default::default()
                        };

                        let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                        let dstlen = self.conn.convert(&column.fmt, &buffer, &dstfmt, &mut dstdata)?;
                        Ok(Some(ParamValue::String(String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string())))
                    },
                    CS_DATE_TYPE | CS_TIME_TYPE | CS_DATETIME_TYPE | CS_DATETIME4_TYPE => {
                        let datatype = column.fmt.datatype;
                        let daterec = self.convert_date(&column.fmt, &buffer)?;
                        Ok(Some(match datatype {
                            CS_DATE_TYPE => {
                                ParamValue::Date(
                                    NaiveDate::from_ymd_opt(
                                        daterec.dateyear,
                                        (daterec.datemonth + 1) as u32,
                                        daterec.datedmonth as u32
                                    )
                                    .ok_or_else(|| Error::from_message("Invalid date"))?
                                )
                            },
                            CS_TIME_TYPE => {
                                ParamValue::Time(
                                    NaiveTime::from_hms_milli_opt(
                                        daterec.datehour as u32,
                                        daterec.dateminute as u32,
                                        daterec.datesecond as u32,
                                        daterec.datemsecond as u32
                                    )
                                    .ok_or_else(|| Error::from_message("Invalid time"))?
                                )
                            },
                            CS_DATETIME_TYPE | CS_DATETIME4_TYPE => {
                                ParamValue::DateTime(
                                    NaiveDate::from_ymd_opt(
                                        daterec.dateyear,
                                        (daterec.datemonth + 1) as u32,
                                        daterec.datedmonth as u32
                                    )
                                    .ok_or_else(|| Error::from_message("Invalid date"))?
                                    .and_hms_milli_opt(
                                        daterec.datehour as u32,
                                        daterec.dateminute as u32,
                                        daterec.datesecond as u32,
                                        daterec.datemsecond as u32
                                    )
                                    .ok_or_else(|| Error::from_message("Invalid date"))?
                                )
                            },
                            _ => panic!("Invalid code path")
                        }))
                    },
                    CS_INT_TYPE => {
                        unsafe {
                            assert_eq!(buffer.len(), mem::size_of::<i32>());
                            let ptr: *const i32 = mem::transmute(buffer.as_ptr());
                            Ok(Some(ParamValue::I32(*ptr)))
                        }
                    },
                    CS_BIT_TYPE | CS_TINYINT_TYPE | CS_SMALLINT_TYPE => {
                        let dstfmt = CS_DATAFMT {
                            datatype: CS_INT_TYPE,
                            maxlength: mem::size_of::<i32>() as i32,
                            format: CS_FMT_UNUSED as i32,
                            count: 1,
                            ..Default::default()
                        };

                        let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                        let dstlen = self.conn.convert(
                            &column.fmt, &buffer,
                            &dstfmt,
                            &mut dstdata)?;
                        assert_eq!(dstlen, mem::size_of::<i32>());
                        unsafe {
                            let ptr: *const i32 = mem::transmute(buffer.as_ptr());
                            Ok(Some(ParamValue::I32(*ptr)))
                        }
                    },
                    CS_MONEY_TYPE | CS_MONEY4_TYPE | CS_DECIMAL_TYPE | CS_NUMERIC_TYPE => {
                        if column.fmt.precision == CS_DEF_PREC && column.fmt.scale == 0 {
                            let dstfmt = CS_DATAFMT {
                                datatype: CS_BIGINT_TYPE,
                                maxlength: mem::size_of::<i64>() as i32,
                                format: CS_FMT_UNUSED as i32,
                                count: 1,
                                ..Default::default()
                            };

                            let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                            let dstlen = self.conn.convert(
                                &column.fmt, &buffer,
                                &dstfmt,
                                &mut dstdata)?;
                            assert_eq!(dstlen, mem::size_of::<i64>());
                            unsafe {
                                let ptr: *const i64 = mem::transmute(dstdata.as_ptr());
                                Ok(Some(ParamValue::I64(*ptr)))
                            }
                        } else {
                            let dstfmt = CS_DATAFMT {
                                datatype : CS_CHAR_TYPE,
                                maxlength : 1024,
                                format : CS_FMT_UNUSED as i32,
                                precision : CS_SRC_VALUE,
                                scale : CS_SRC_VALUE,
                                count : 1,
                                ..Default::default()
                            };

                            let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                            let dstlen = self.conn.convert(&column.fmt, &buffer, &dstfmt, &mut dstdata)?;
                            let s = String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string();
                            Ok(Some(ParamValue::Decimal(Decimal::from_str_exact(&s).map_err(|_| Error::from_message("Invalid decimal"))?)))
                        }
                    },
                    CS_REAL_TYPE  => {
                        unsafe {
                            assert_eq!(buffer.len(), mem::size_of::<f32>());
                            let ptr: *const f32 = mem::transmute(buffer.as_ptr());
                            Ok(Some(ParamValue::F64(Into::<f64>::into(*ptr))))
                        }
                    },
                    CS_FLOAT_TYPE => {
                        unsafe {
                            assert_eq!(buffer.len(), mem::size_of::<f64>());
                            let ptr: *const f64 = mem::transmute(buffer.as_ptr());
                            Ok(Some(ParamValue::F64(*ptr)))
                        }
                    },
                    _ => {
                        Err(Error::from_message("Unsupported datatype"))
                    }
                }
            }
        }
    }

    pub fn get_i64(&mut self, col: impl Into<ColumnId>) -> Result<Option<i64>> {
        let buffer = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some((fmt, buffer)) => {
                match fmt.datatype {
                    CS_LONG_TYPE => {
                        unsafe {
                            assert_eq!(buffer.len(), mem::size_of::<i64>());
                            let buf: *const i64 = mem::transmute(buffer.deref().as_ptr());
                            Ok(Some(*buf))
                        }
                    },
                    _ => {
                        Ok(Some(self.convert::<i64>(&fmt, buffer.as_ref().as_slice(), CS_LONG_TYPE)?))
                    }
                }
            }
        }
    }

    pub fn get_i32(&mut self, col: impl Into<ColumnId>) -> Result<Option<i32>> {
        let buffer = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some((fmt, buffer)) => {
                match fmt.datatype {
                    CS_INT_TYPE => {
                        unsafe {
                            assert_eq!(buffer.len(), mem::size_of::<i32>());
                            let buf: *const i32 = mem::transmute(buffer.deref().as_ptr());
                            Ok(Some(*buf))
                        }
                    },
                    _ => {
                        Ok(Some(self.convert::<i32>(&fmt, buffer.as_ref().as_slice(), CS_INT_TYPE)?))
                    }
                }
            }
        }
    }

    pub fn get_bool(&mut self, col: impl Into<ColumnId>) -> Result<Option<bool>> {
        let val = self.get_i64(col)?;
        match val {
            None => Ok(None),
            Some(val) => Ok(Some(val != 0))
        }
    }

    pub fn get_f64(&mut self, col: impl Into<ColumnId>) -> Result<Option<f64>> {
        let buffer = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some((fmt, buffer)) => {
                match fmt.datatype {
                    CS_FLOAT_TYPE => {
                        unsafe {
                            assert_eq!(buffer.len(), mem::size_of::<f64>());
                            let buf: *const f64 = mem::transmute(buffer.deref().as_ptr());
                            Ok(Some(*buf))
                        }
                    },
                    CS_REAL_TYPE => {
                        unsafe {
                            assert_eq!(buffer.len(), mem::size_of::<f32>());
                            let buf: *const f32 = mem::transmute(buffer.deref().as_ptr());
                            Ok(Some(Into::<f64>::into(*buf)))
                        }
                    },
                    _ => {
                        Ok(Some(self.convert::<f64>(&fmt, buffer.as_ref().as_slice(), CS_FLOAT_TYPE)?))
                    }
                }
            }
        }
    }

    pub fn get_string(&mut self, col: impl Into<ColumnId>) -> Result<Option<String>> {
        let buffer = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some((fmt, buffer)) => {
                match fmt.datatype {
                    CS_CHAR_TYPE | CS_TEXT_TYPE => {
                        let value = String::from_utf8_lossy(&buffer);
                        Ok(Some(value.to_string()))
                    },
                    _ => {
                        let dstfmt = CS_DATAFMT {
                            datatype: CS_CHAR_TYPE,
                            maxlength: match fmt.datatype {
                                CS_BINARY_TYPE | CS_LONGBINARY_TYPE | CS_IMAGE_TYPE => {
                                    ((buffer.len() * 2) + 16) as i32
                                },
                                _ => {
                                    128
                                }
                            },
                            format: CS_FMT_UNUSED as i32,
                            count: 1,
                            ..Default::default()
                        };

                        let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                        let dstlen = self.conn.convert(
                            &fmt, &buffer,
                            &dstfmt,
                            &mut dstdata)?;
                        Ok(Some(String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string()))
                    }
                }
            }
        }
    }
    
    pub fn get_date(&mut self, col: impl Into<ColumnId>) -> Result<Option<NaiveDate>> {
        match self.get_daterec(col)? {
            None => Ok(None),
            Some(dt) => {
                Ok(
                    Some(
                        NaiveDate::from_ymd_opt(
                            dt.dateyear,
                            (dt.datemonth + 1) as u32,
                            dt.datedmonth as u32)
                        .ok_or_else(|| Error::from_message("Invalid date"))?
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
                    .ok_or_else(|| Error::from_message("Invalid time"))?))
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
                    .ok_or_else(|| Error::from_message("Invalid date"))?;
                Ok(Some(date.and_hms_milli_opt(date_rec.datehour as u32, 
                    date_rec.dateminute as u32, 
                    date_rec.datesecond as u32, 
                    date_rec.datemsecond as u32)
                    .ok_or_else(|| Error::from_message("Invalid time"))?))
            }
        }
    }

    pub fn get_blob(&mut self, col: impl Into<ColumnId>) -> Result<Option<Vec<u8>>> {
        match self.get_buffer(col)? {
            None => Ok(None),
            Some((fmt, buffer)) => {
                match fmt.datatype {
                    CS_BINARY_TYPE | CS_IMAGE_TYPE => {
                        Ok(Some(buffer.deref().clone()))
                    },
                    CS_VARBINARY_TYPE => {
                        panic!("Not implemented yet");
                    },
                    _ => {
                        let dstfmt = CS_DATAFMT {
                            datatype: CS_BINARY_TYPE,
                            maxlength: fmt.maxlength,
                            format: CS_FMT_UNUSED as i32,
                            count: 1,
                            ..Default::default()
                        };
        
                        let mut dstdata: Vec<u8> = Vec::new();
                        dstdata.resize(dstfmt.maxlength as usize, Default::default());
                        let dstlen = self.conn.convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                        dstdata.resize(dstlen, Default::default());
                        Ok(Some(dstdata))
                    }
                }
            }
        }
    }

    pub fn get_decimal(&mut self, col: impl Into<ColumnId>) -> Result<Option<Decimal>> {
        match self.get_buffer(col)? {
            None => Ok(None),
            Some((fmt, buffer)) => {
                match fmt.datatype {
                    CS_INT_TYPE | CS_BIT_TYPE | CS_TINYINT_TYPE | CS_SMALLINT_TYPE |
                    CS_NUMERIC_TYPE | CS_MONEY_TYPE | CS_MONEY4_TYPE | CS_DECIMAL_TYPE |
                    CS_REAL_TYPE | CS_FLOAT_TYPE => {
                        let dstfmt = CS_DATAFMT {
                            datatype: CS_CHAR_TYPE,
                            maxlength: 1024,
                            format: CS_FMT_UNUSED as i32,
                            precision: CS_SRC_VALUE,
                            scale: CS_SRC_VALUE,
                            count: 1,
                            ..Default::default()
                        };

                        let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                        let dstlen = self.conn.convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                        let s = String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string();
                        Ok(Some(Decimal::from_str_exact(&s).map_err(|_| Error::from_message("Invalid decimal"))?))
                    },
                    _ => {
                        Err(Error::from_message("Unsupported datatype"))
                    }
                }
            }
        }
    }

    fn convert_date(&mut self, fmt: &CS_DATAFMT, buffer: &[u8]) -> Result<CS_DATEREC> {
        match fmt.datatype {
            CS_DATE_TYPE => {
                unsafe {
                    assert!(buffer.len() == mem::size_of::<CS_DATE>());
                    let buf: *const CS_DATE = mem::transmute(buffer.as_ptr());
                    Ok(self.conn.crack_date(*buf)?)
                }
            },
            CS_TIME_TYPE => {
                unsafe {
                    assert!(buffer.len() == mem::size_of::<CS_TIME>());
                    let buf: *const CS_TIME = mem::transmute(buffer.as_ptr());
                    Ok(self.conn.crack_time(*buf)?)
                }
            },
            CS_DATETIME_TYPE => {
                unsafe {
                    assert!(buffer.len() == mem::size_of::<CS_DATETIME>());
                    let buf: *const CS_DATETIME = mem::transmute(buffer.as_ptr());
                    Ok(self.conn.crack_datetime(*buf)?)
                }
            },
            CS_DATETIME4_TYPE => {
                unsafe {
                    assert!(buffer.len() == mem::size_of::<CS_DATETIME4>());
                    let buf: *const CS_DATETIME4 = mem::transmute(buffer.as_ptr());
                    Ok(self.conn.crack_smalldatetime(*buf)?)
                }
            },
            _ => {
                Err(Error::from_message("Invalid conversion"))
            }
        }
    }

    fn get_daterec(&mut self, col: impl Into<ColumnId>) -> Result<Option<CS_DATEREC>> {
        let buffer = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some((fmt, buffer)) => {
                Ok(Some(self.convert_date(&fmt, buffer.deref())?))
            }
        }
    }

    pub fn status(&self) -> Option<i32> {
        self.status
    }

    pub fn messages(&self) -> &Vec<Error> {
        &self.messages
    }

    pub fn error(&self) -> Option<Error> {
        self.messages.first().cloned()
    }

}

