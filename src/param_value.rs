use chrono::{NaiveDate, NaiveTime, NaiveDateTime};

#[derive(Clone, Debug, PartialEq)]
pub enum ParamValue {
    Null,
    String(String),
    I32(i32),
    I64(i64),
    F64(f64),
    NaiveDate(NaiveDate),
    NaiveTime(NaiveTime),
    NaiveDateTime(NaiveDateTime),
    Blob(Vec<u8>),
}

impl<T> From<Option<T>> for ParamValue
where
    T: Into<ParamValue>
{
    fn from(v: Option<T>) -> Self {
        match v {
            None => Self::Null,
            Some(v) => v.into()
        }
    }
}

impl From<&str> for ParamValue {
    fn from(v: &str) -> Self {
        Self::String(String::from(v))
    }
}

impl From<String> for ParamValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<i32> for ParamValue {
    fn from(v: i32) -> Self {
        Self::I32(v)
    }
}

impl From<i64> for ParamValue {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<f32> for ParamValue {
    fn from(v: f32) -> Self {
        Self::F64(v.into())
    }
}

impl From<f64> for ParamValue {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}

impl From<NaiveDate> for ParamValue {
    fn from(v: NaiveDate) -> Self {
        Self::NaiveDate(v)
    }
}

impl From<NaiveTime> for ParamValue {
    fn from(v: NaiveTime) -> Self {
        Self::NaiveTime(v)
    }
}

impl From<NaiveDateTime> for ParamValue {
    fn from(v: NaiveDateTime) -> Self {
        Self::NaiveDateTime(v)
    }
}

impl From<Vec<u8>> for ParamValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Blob(v)
    }
}

impl From<&[u8]> for ParamValue {
    fn from(v: &[u8]) -> Self {
        Self::Blob(v.to_vec())
    }
}

impl From<Vec<i8>> for ParamValue {
    fn from(v: Vec<i8>) -> Self {
        Self::Blob(v.iter().map(|c| *c as u8).collect())
    }
}

impl From<&[i8]> for ParamValue {
    fn from(v: &[i8]) -> Self {
        Self::Blob(v.iter().map(|c| *c as u8).collect())
    }
}

