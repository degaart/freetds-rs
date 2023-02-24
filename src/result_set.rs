#![allow(clippy::expect_fun_call)]

use crate::{column_id::ColumnId, Connection, Error, Result, Value};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use freetds_sys::*;
use rust_decimal::Decimal;
use std::{mem, ops::Deref, rc::Rc};

#[derive(Debug, Default, Clone)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) fmt: CS_DATAFMT,
}

#[derive(Debug)]
pub struct Row {
    pub(crate) buffers: Vec<Option<Rc<Vec<u8>>>>,
}

#[derive(Debug)]
pub struct Rows {
    pub(crate) columns: Vec<Column>,
    pub(crate) rows: Vec<Row>,
    pub(crate) pos: Option<usize>,
}

impl Rows {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            pos: Default::default(),
        }
    }
}

#[derive(PartialEq)]
pub enum ResultType {
    None,
    Rows,
    Status,
    UpdateCount,
}

#[derive(Debug)]
pub(crate) enum SybResult {
    Rows(Rows),
    Status(i32),
    UpdateCount(u64),
}

pub struct ResultSet {
    pub(crate) conn: Connection,

    /* current result index */
    pub(crate) pos: Option<usize>,
    pub(crate) results: Vec<SybResult>,
    pub(crate) messages: Vec<Error>,
}

impl ResultSet {
    pub(crate) fn new(
        conn: Connection,
        results: Vec<SybResult>,
        messages: Vec<Error>,
    ) -> Self {
        Self {
            conn,
            pos: None,
            results,
            messages,
        }
    }

    pub fn next_results(&mut self) -> bool {
        match self.pos {
            None => {
                if self.results.is_empty() {
                    return false;
                } else {
                    self.pos = Some(0);
                }
            }
            Some(pos) => {
                self.pos = Some(pos + 1);
            }
        }

        self.pos.expect("Unexpected None value") < self.results.len()
    }

    pub fn next_results_of_type(&mut self, type_: ResultType) -> bool {
        if !self.next_results() {
            return false;
        }

        while self.result_type() != type_ {
            if !self.next_results() {
                return false;
            }
        }
        true
    }

    pub fn result_type(&self) -> ResultType {
        match self.pos {
            None => ResultType::None,
            Some(pos) => match self.results.get(pos) {
                None => ResultType::None,
                Some(r) => match r {
                    SybResult::Rows(_) => ResultType::Rows,
                    SybResult::Status(_) => ResultType::Status,
                    SybResult::UpdateCount(_) => ResultType::UpdateCount,
                }
            }
        }
    }

    /*
     * Seek self.result_index to next item in results which contains rows
     * and return the new result
     * If no more row result, return None and set result_index out of range value
     */
    fn next_row_result(&mut self) -> Option<usize> {
        for (i, r) in self.results.iter().skip(self.pos.unwrap_or(0)).enumerate() {
            if let SybResult::Rows(_) = r {
                self.pos = Some(i);
                return Some(i);
            }
        }
        self.pos = Some(self.results.len());
        return None;
    }

    fn next_status_result(&mut self) -> Option<usize> {
        for (i, r) in self.results.iter().skip(self.pos.unwrap_or(0)).enumerate() {
            if let SybResult::Status(_) = r {
                self.pos = Some(i);
                return Some(i);
            }
        }
        self.pos = Some(self.results.len());
        return None;
    }

    fn next_update_count_result(&mut self) -> Option<usize> {
        for (i, r) in self.results.iter().skip(self.pos.unwrap_or(0)).enumerate() {
            if let SybResult::UpdateCount(_) = r {
                self.pos = Some(i);
                return Some(i);
            }
        }
        self.pos = Some(self.results.len());
        return None;
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        /*
         * On the first call, we seek to next row results
         */
        if self.pos.is_none() {
            if self.next_row_result().is_none() {
                return false;
            }
        }

        /*
         * We can assume here that result_index is not None
         */
        let result_index = self.pos.expect("Unexpected None value");
        match self.results.get_mut(result_index) {
            Some(result) => {
                match result {
                    SybResult::Rows(rows) => {
                        if rows.rows.is_empty() {
                            rows.pos = Some(rows.rows.len());
                            return false;
                        }

                        if let Some(pos) = rows.pos {
                            rows.pos = Some(pos + 1);
                        } else {
                            rows.pos = Some(0);
                        }

                        rows.pos.expect("Unexpected None value") < rows.rows.len()
                    },
                    _ => false, /* Current results do not contain rows */
                }
            },

            /* End of results */
            None => false,
        }
    }

    /*
     * Returns true if current results contains SybResult::rows
     */
    pub fn is_rows(&self) -> bool {
        if let SybResult::Rows(_) = self.results[self.pos.unwrap_or(0)] {
            true
        } else {
            false
        }
    }

    /*
     * Returns true if current results contains SybResult::Status
     */
    pub fn is_status(&self) -> bool {
        if let SybResult::Status(_) = self.results[self.pos.unwrap_or(0)] {
            true
        } else {
            false
        }
    }

    /*
     * Returns true if current results contains SybResult::UpdateCount
     */
    pub fn is_update_count(&self) -> bool {
        if let SybResult::UpdateCount(_) = self.results[self.pos.unwrap_or(0)] {
            true
        } else {
            false
        }
    }

    /*
     * Seek to next row results if first call, then return the column count
     */
    pub fn column_count(&mut self) -> Result<usize> {
        if self.pos.is_none() {
            if self.next_row_result().is_none() {
                return Err(Error::from_message("Query did not return rows"));
            }
        }
        let pos = self.pos.expect("Unexpected None value");
        let results = self.results.get(pos);
        if let Some(results) = results {
            match results {
                SybResult::Rows(rows) => {
                    Ok(rows.columns.len())
                },
                _ => {
                    Err(Error::from_message("Current results do not contain rows"))
                }
            }
        } else {
            Err(Error::from_message("No more results"))
        }
    }

    pub fn column_name(&mut self, index: usize) -> Result<String> {
        if self.pos.is_none() {
            if self.next_row_result().is_none() {
                return Err(Error::from_message("Query did not return rows"));
            }
        }
        let pos = self.pos.expect("Unexpected None value");
        let results = self.results.get_mut(pos);
        if let Some(results) = results {
            match results {
                SybResult::Rows(rows) => {
                    if let Some(column) = rows.columns.get(index) {
                        Ok(column.name.clone())
                    } else {
                        Err(Error::from_message("Invalid column index"))
                    }
                },
                _ => {
                    Err(Error::from_message("Current results do not contain rows"))
                }
            }
        } else {
            Err(Error::from_message("No more results"))
        }
    }

    fn get_buffer(
        &self,
        col: impl Into<ColumnId>,
    ) -> Result<(CS_DATAFMT, Option<Rc<Vec<u8>>>)> {
        if self.pos.is_none() {
            return Err(Error::from_message("Invalid state"));
        }

        let pos = self.pos.expect("Unexpected None value");
        if pos >= self.results.len() {
            return Err(Error::from_message("ResultSet exhausted"));
        }

        if let Some(SybResult::Rows(rows)) = self.results.get(pos) {
            let col: usize = match Into::<ColumnId>::into(col) {
                ColumnId::I32(i) => match i.try_into() {
                    Ok(i) => i,
                    Err(_) => return Err(Error::from_message("Invalid column index")),
                },
                ColumnId::String(s) => {
                    let column_index = rows.columns.iter().enumerate()
                        .find(|(_,c)| c.name == s)
                        .map(|(i,_)| i);

                    if let Some(column_index) = column_index {
                        column_index
                    } else {
                        return Err(Error::from_message("Invalid column index"));
                    }
                }
            };

            if col >= rows.columns.len() {
                return Err(Error::from_message("Invalid column index"));
            }

            let column = &rows.columns[col];
            let row = if let Some(row) = rows.pos {
                &rows.rows[row]
            } else {
                return Err(Error::from_message("Invalid state"));
            };

            let buffer = &row.buffers[col];
            match buffer {
                None => Ok((column.fmt, None)),
                Some(buffer) => Ok((column.fmt, Some(Rc::clone(buffer)))),
            }
        } else {
            return Err(Error::from_message("Invalid state"));
        }
    }

    fn convert<T>(&mut self, srcfmt: &CS_DATAFMT, srcbuffer: &[u8], dstdatatype: i32) -> Result<T>
    where
        T: Copy,
    {
        let dstfmt = CS_DATAFMT {
            datatype: dstdatatype,
            maxlength: mem::size_of::<T>() as i32,
            format: CS_FMT_UNUSED as i32,
            count: 1,
            ..Default::default()
        };

        let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
        let dstlen = self
            .conn
            .convert(srcfmt, srcbuffer, &dstfmt, &mut dstdata)?;

        assert_eq!(dstlen, mem::size_of::<T>());
        unsafe {
            let buf: *const T = mem::transmute(dstdata.as_ptr());
            Ok(*buf)
        }
    }

    pub fn get_value(&mut self, col: impl Into<ColumnId>) -> Result<Value> {
        let (fmt, buffer) = self.get_buffer(col)?;

        match buffer {
            None => Ok(Value::Null),
            Some(buffer) => match fmt.datatype {
                CS_BINARY_TYPE | CS_IMAGE_TYPE => {
                    let buf: &Vec<u8> = &buffer;
                    Ok(Value::from(buf))
                }
                CS_CHAR_TYPE | CS_TEXT_TYPE => Ok(Value::from(
                    String::from_utf8_lossy(&buffer).to_string(),
                )),
                CS_UNICHAR_TYPE => {
                    let dstfmt = CS_DATAFMT {
                        datatype: CS_CHAR_TYPE,
                        maxlength: buffer.len() as i32,
                        format: CS_FMT_UNUSED as i32,
                        count: 1,
                        ..Default::default()
                    };

                    let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                    let dstlen = self
                        .conn
                        .convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                    Ok(Value::from(
                        String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string(),
                    ))
                }
                CS_DATE_TYPE | CS_TIME_TYPE | CS_DATETIME_TYPE | CS_DATETIME4_TYPE => {
                    let datatype = fmt.datatype;
                    let daterec = self.convert_date(&fmt, &buffer)?;
                    Ok(match datatype {
                        CS_DATE_TYPE => Value::from(
                            NaiveDate::from_ymd_opt(
                                daterec.dateyear,
                                (daterec.datemonth + 1) as u32,
                                daterec.datedmonth as u32,
                            )
                            .ok_or_else(|| Error::from_message("Invalid date"))?,
                        ),
                        CS_TIME_TYPE => Value::from(
                            NaiveTime::from_hms_milli_opt(
                                daterec.datehour as u32,
                                daterec.dateminute as u32,
                                daterec.datesecond as u32,
                                daterec.datemsecond as u32,
                            )
                            .ok_or_else(|| Error::from_message("Invalid time"))?,
                        ),
                        CS_DATETIME_TYPE | CS_DATETIME4_TYPE => Value::from(
                            NaiveDate::from_ymd_opt(
                                daterec.dateyear,
                                (daterec.datemonth + 1) as u32,
                                daterec.datedmonth as u32,
                            )
                            .ok_or_else(|| Error::from_message("Invalid date"))?
                            .and_hms_milli_opt(
                                daterec.datehour as u32,
                                daterec.dateminute as u32,
                                daterec.datesecond as u32,
                                daterec.datemsecond as u32,
                            )
                            .ok_or_else(|| Error::from_message("Invalid date"))?,
                        ),
                        _ => panic!("Invalid code path"),
                    })
                }
                CS_INT_TYPE => unsafe {
                    assert_eq!(buffer.len(), mem::size_of::<i32>());
                    let ptr: *const i32 = mem::transmute(buffer.as_ptr());
                    Ok(Value::from(*ptr))
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
                    let dstlen = self
                        .conn
                        .convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                    assert_eq!(dstlen, mem::size_of::<i32>());
                    unsafe {
                        let ptr: *const i32 = mem::transmute(buffer.as_ptr());
                        Ok(Value::from(*ptr))
                    }
                }
                CS_MONEY_TYPE | CS_MONEY4_TYPE | CS_DECIMAL_TYPE | CS_NUMERIC_TYPE => {
                    if fmt.precision == CS_DEF_PREC && fmt.scale == 0 {
                        let dstfmt = CS_DATAFMT {
                            datatype: CS_BIGINT_TYPE,
                            maxlength: mem::size_of::<i64>() as i32,
                            format: CS_FMT_UNUSED as i32,
                            count: 1,
                            ..Default::default()
                        };

                        let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                        let dstlen =
                            self.conn
                                .convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                        assert_eq!(dstlen, mem::size_of::<i64>());
                        unsafe {
                            let ptr: *const i64 = mem::transmute(dstdata.as_ptr());
                            Ok(Value::from(*ptr))
                        }
                    } else {
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
                        let dstlen =
                            self.conn
                                .convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                        let s = String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string();
                        Ok(Value::from(
                            Decimal::from_str_exact(&s)
                                .map_err(|_| Error::from_message("Invalid decimal"))?,
                        ))
                    }
                }
                CS_REAL_TYPE => unsafe {
                    assert_eq!(buffer.len(), mem::size_of::<f32>());
                    let ptr: *const f32 = mem::transmute(buffer.as_ptr());
                    Ok(Value::from(Into::<f64>::into(*ptr)))
                },
                CS_FLOAT_TYPE => unsafe {
                    assert_eq!(buffer.len(), mem::size_of::<f64>());
                    let ptr: *const f64 = mem::transmute(buffer.as_ptr());
                    Ok(Value::from(*ptr))
                },
                _ => Err(Error::from_message("Unsupported datatype")),
            },
        }
    }

    pub fn get_i64(&mut self, col: impl Into<ColumnId>) -> Result<Option<i64>> {
        let (fmt, buffer) = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some(buffer) => match fmt.datatype {
                CS_LONG_TYPE => unsafe {
                    assert_eq!(buffer.len(), mem::size_of::<i64>());
                    let buf: *const i64 = mem::transmute(buffer.deref().as_ptr());
                    Ok(Some(*buf))
                },
                _ => Ok(Some(self.convert::<i64>(
                    &fmt,
                    buffer.as_ref().as_slice(),
                    CS_LONG_TYPE,
                )?)),
            },
        }
    }

    pub fn get_i32(&mut self, col: impl Into<ColumnId>) -> Result<Option<i32>> {
        let (fmt, buffer) = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some(buffer) => match fmt.datatype {
                CS_INT_TYPE => unsafe {
                    assert_eq!(buffer.len(), mem::size_of::<i32>());
                    let buf: *const i32 = mem::transmute(buffer.deref().as_ptr());
                    Ok(Some(*buf))
                },
                _ => Ok(Some(self.convert::<i32>(
                    &fmt,
                    buffer.as_ref().as_slice(),
                    CS_INT_TYPE,
                )?)),
            },
        }
    }

    pub fn get_bool(&mut self, col: impl Into<ColumnId>) -> Result<Option<bool>> {
        let val = self.get_i64(col)?;
        match val {
            None => Ok(None),
            Some(val) => Ok(Some(val != 0)),
        }
    }

    pub fn get_f64(&mut self, col: impl Into<ColumnId>) -> Result<Option<f64>> {
        let (fmt, buffer) = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some(buffer) => match fmt.datatype {
                CS_FLOAT_TYPE => unsafe {
                    assert_eq!(buffer.len(), mem::size_of::<f64>());
                    let buf: *const f64 = mem::transmute(buffer.deref().as_ptr());
                    Ok(Some(*buf))
                },
                CS_REAL_TYPE => unsafe {
                    assert_eq!(buffer.len(), mem::size_of::<f32>());
                    let buf: *const f32 = mem::transmute(buffer.deref().as_ptr());
                    Ok(Some(Into::<f64>::into(*buf)))
                },
                _ => Ok(Some(self.convert::<f64>(
                    &fmt,
                    buffer.as_ref().as_slice(),
                    CS_FLOAT_TYPE,
                )?)),
            },
        }
    }

    pub fn get_string(&mut self, col: impl Into<ColumnId>) -> Result<Option<String>> {
        let (fmt, buffer) = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some(buffer) => match fmt.datatype {
                CS_CHAR_TYPE | CS_TEXT_TYPE => {
                    let value = String::from_utf8_lossy(&buffer);
                    Ok(Some(value.to_string()))
                }
                _ => {
                    let dstfmt = CS_DATAFMT {
                        datatype: CS_CHAR_TYPE,
                        maxlength: match fmt.datatype {
                            CS_BINARY_TYPE | CS_LONGBINARY_TYPE | CS_IMAGE_TYPE => {
                                ((buffer.len() * 2) + 16) as i32
                            }
                            _ => 128,
                        },
                        format: CS_FMT_UNUSED as i32,
                        count: 1,
                        ..Default::default()
                    };

                    let mut dstdata: Vec<u8> = vec![0u8; dstfmt.maxlength as usize];
                    let dstlen = self.conn.convert(&fmt, &buffer, &dstfmt, &mut dstdata)?;
                    Ok(Some(
                        String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string(),
                    ))
                }
            },
        }
    }

    pub fn get_date(&mut self, col: impl Into<ColumnId>) -> Result<Option<NaiveDate>> {
        match self.get_daterec(col)? {
            None => Ok(None),
            Some(dt) => Ok(Some(
                NaiveDate::from_ymd_opt(
                    dt.dateyear,
                    (dt.datemonth + 1) as u32,
                    dt.datedmonth as u32,
                )
                .ok_or_else(|| Error::from_message("Invalid date"))?,
            )),
        }
    }

    pub fn get_time(&mut self, col: impl Into<ColumnId>) -> Result<Option<NaiveTime>> {
        match self.get_daterec(col)? {
            None => Ok(None),
            Some(date_rec) => Ok(Some(
                NaiveTime::from_hms_milli_opt(
                    date_rec.datehour as u32,
                    date_rec.dateminute as u32,
                    date_rec.datesecond as u32,
                    date_rec.datemsecond as u32,
                )
                .ok_or_else(|| Error::from_message("Invalid time"))?,
            )),
        }
    }

    pub fn get_datetime(&mut self, col: impl Into<ColumnId>) -> Result<Option<NaiveDateTime>> {
        match self.get_daterec(col)? {
            None => Ok(None),
            Some(date_rec) => {
                let date = NaiveDate::from_ymd_opt(
                    date_rec.dateyear,
                    (date_rec.datemonth + 1) as u32,
                    date_rec.datedmonth as u32,
                )
                .ok_or_else(|| Error::from_message("Invalid date"))?;
                Ok(Some(
                    date.and_hms_milli_opt(
                        date_rec.datehour as u32,
                        date_rec.dateminute as u32,
                        date_rec.datesecond as u32,
                        date_rec.datemsecond as u32,
                    )
                    .ok_or_else(|| Error::from_message("Invalid time"))?,
                ))
            }
        }
    }

    pub fn get_blob(&mut self, col: impl Into<ColumnId>) -> Result<Option<Vec<u8>>> {
        let (fmt, buffer) = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some(buffer) => match fmt.datatype {
                CS_BINARY_TYPE | CS_IMAGE_TYPE => Ok(Some(buffer.deref().clone())),
                CS_VARBINARY_TYPE => {
                    todo!();
                }
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
            },
        }
    }

    pub fn get_decimal(&mut self, col: impl Into<ColumnId>) -> Result<Option<Decimal>> {
        let (fmt, buffer) = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some(buffer) => match fmt.datatype {
                CS_INT_TYPE | CS_BIT_TYPE | CS_TINYINT_TYPE | CS_SMALLINT_TYPE
                | CS_NUMERIC_TYPE | CS_MONEY_TYPE | CS_MONEY4_TYPE | CS_DECIMAL_TYPE
                | CS_REAL_TYPE | CS_FLOAT_TYPE => {
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
                    Ok(Some(
                        Decimal::from_str_exact(&s)
                            .map_err(|_| Error::from_message("Invalid decimal"))?,
                    ))
                }
                _ => Err(Error::from_message("Unsupported datatype")),
            },
        }
    }

    fn convert_date(&mut self, fmt: &CS_DATAFMT, buffer: &[u8]) -> Result<CS_DATEREC> {
        match fmt.datatype {
            CS_DATE_TYPE => unsafe {
                assert!(buffer.len() == mem::size_of::<CS_DATE>());
                let buf: *const CS_DATE = mem::transmute(buffer.as_ptr());
                Ok(self.conn.crack_date(*buf)?)
            },
            CS_TIME_TYPE => unsafe {
                assert!(buffer.len() == mem::size_of::<CS_TIME>());
                let buf: *const CS_TIME = mem::transmute(buffer.as_ptr());
                Ok(self.conn.crack_time(*buf)?)
            },
            CS_DATETIME_TYPE => unsafe {
                assert!(buffer.len() == mem::size_of::<CS_DATETIME>());
                let buf: *const CS_DATETIME = mem::transmute(buffer.as_ptr());
                Ok(self.conn.crack_datetime(*buf)?)
            },
            CS_DATETIME4_TYPE => unsafe {
                assert!(buffer.len() == mem::size_of::<CS_DATETIME4>());
                let buf: *const CS_DATETIME4 = mem::transmute(buffer.as_ptr());
                Ok(self.conn.crack_smalldatetime(*buf)?)
            },
            _ => Err(Error::from_message("Invalid conversion")),
        }
    }

    fn get_daterec(&mut self, col: impl Into<ColumnId>) -> Result<Option<CS_DATEREC>> {
        let (fmt, buffer) = self.get_buffer(col)?;
        match buffer {
            None => Ok(None),
            Some(buffer) => Ok(Some(self.convert_date(&fmt, buffer.deref())?)),
        }
    }

    pub fn status(&mut self) -> Result<i32> {
        if let None = self.pos {
            if self.next_status_result().is_none() {
                return Err(Error::from_message("Current ResultSet does not contain any status result"));
            }
        }

        match self.results.get(self.pos.expect("Unexpected None value")) {
            None => Err(Error::from_message("Invalid state")),
            Some(results) => {
                if let SybResult::Status(status) = results {
                    Ok(*status)
                } else {
                    Err(Error::from_message("ResultSet does not currently point to a status result"))
                }
            }
        }
    }

    pub fn update_count(&mut self) -> Result<u64> {
        if let None = self.pos {
            if self.next_update_count_result().is_none() {
                return Err(Error::from_message("Current ResultSet does not contain any update count result"));
            }
        }

        match self.results.get(self.pos.expect("Unexpected None value")) {
            None => Err(Error::from_message("Invalid state")),
            Some(results) => {
                if let SybResult::UpdateCount(update_count) = results {
                    Ok(*update_count)
                } else {
                    Err(Error::from_message("ResultSet does not currently point to an update count result"))
                }
            }
        }
    }

    pub fn messages(&self) -> &Vec<Error> {
        &self.messages
    }

    pub fn error(&self) -> Option<Error> {
        self.messages.first().cloned()
    }
}

