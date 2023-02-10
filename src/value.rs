#![allow(unused)]
use std::fmt::{Display, Formatter, Write};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    String(String),
    I32(i32),
    I64(i64),
    F64(f64),
    Decimal(Decimal),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Blob(Vec<u8>),
}

impl Value {
    fn new() -> Self {
        Self::Null
    }

    pub fn to_sql(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::String(s) => Self::write_string(f, s),
            Value::I32(i) => write!(f, "{i}"),
            Value::I64(i) => write!(f, "{i}"),
            Value::F64(d) => write!(f, "{d}"),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::Date(d) => Self::write_date(f, d),
            Value::Time(t) => Self::write_time(f, t),
            Value::DateTime(dt) => Self::write_datetime(f, dt),
            Value::Blob(b) => Self::write_blob(f, b),
        }
    }

    fn write_string(f: &mut Formatter, s: &str) -> std::fmt::Result {
        const QUOTE: char = '\'';
        f.write_char(QUOTE)?;
        for c in s.chars() {
            f.write_char(c)?;
            if c == QUOTE {
                f.write_char(c)?;
            }
        }
        f.write_char(QUOTE)?;
        Ok(())
    }

    fn write_date(f: &mut Formatter, d: &NaiveDate) -> std::fmt::Result {
        f.write_str(&d.format("'%Y/%m/%d'").to_string())
    }

    fn write_time(f: &mut Formatter, t: &NaiveTime) -> std::fmt::Result {
        f.write_str(&t.format("'%H:%M:%S%.f'").to_string())
    }

    fn write_datetime(f: &mut Formatter, dt: &NaiveDateTime) -> std::fmt::Result {
        f.write_str(&dt.format("'%Y/%m/%d %H:%M:%S%.f'").to_string())
    }

    fn write_blob(f: &mut Formatter, b: &[u8]) -> std::fmt::Result {
        f.write_str("0x")?;
        for c in b.iter() {
            write!(f, "{c:02X}")?;
        }
        Ok(())
    }

}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.to_sql(f)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<Decimal> for Value {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
    }
}

impl From<NaiveDate> for Value {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
    }
}

impl From<NaiveTime> for Value {
    fn from(value: NaiveTime) -> Self {
        Self::Time(value)
    }
}

impl From<NaiveDateTime> for Value {
    fn from(value: NaiveDateTime) -> Self {
        Self::DateTime(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::Blob(value.to_vec())
    }
}

impl From<Vec<i8>> for Value {
    fn from(value: Vec<i8>) -> Self {
        Self::Blob(value.iter().map(|c| *c as u8).collect())
    }
}

impl From<&[i8]> for Value {
    fn from(value: &[i8]) -> Self {
        Self::Blob(value.iter().map(|c| *c as u8).collect())
    }
}

impl<T> From<&T> for Value
where
    T: Into<Value> + Clone
{
    fn from(value: &T) -> Self {
        Into::<Value>::into(value.clone())
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value> + Clone
{
    fn from(value: Option<T>) -> Self {
        match value {
            None => Self::Null,
            Some(value) => value.into()
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
    use rust_decimal::Decimal;
    use super::Value;

    #[test]
    fn test_from() {
        Value::from(None::<String>);
        Value::from("deadbeef".to_string());
        Value::from(0_i32);
        Value::from(0_i64);
        Value::from(0.0_f64);
        Value::from(Decimal::from(42));
        Value::from(NaiveDate::from_ymd_opt(1980, 1, 1).unwrap());
        Value::from(NaiveTime::from_hms_opt(1, 1, 1).unwrap());
        Value::from(NaiveDateTime::from_timestamp_opt(0, 0).unwrap());

        Value::from(&None::<String>);
        Value::from("deadbeef");
        Value::from(&0_i32);
        Value::from(&0_i64);
        Value::from(&0.0_f64);
        Value::from(&Decimal::from(42));
        Value::from(&NaiveDate::from_ymd_opt(1980, 1, 1).unwrap());
        Value::from(&NaiveTime::from_hms_opt(1, 1, 1).unwrap());
        Value::from(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap());
    }

    #[test]
    fn test_to_sql() {
        assert_eq!("null", Value::from(None::<String>).to_string());
        assert_eq!("'dead''beef'", Value::from("dead'beef").to_string());
        assert_eq!("42", Value::from(42_i32).to_string());
        assert_eq!("42", Value::from(42_i64).to_string());
        assert_eq!("4.2", Value::from(4.2_f64).to_string());
        assert_eq!("42", Value::from(Decimal::from(42)).to_string());
        assert_eq!("'1980/02/03'", Value::from(NaiveDate::from_ymd_opt(1980, 2, 3).unwrap()).to_string());
        assert_eq!("'01:02:03'", Value::from(NaiveTime::from_hms_opt(1, 2, 3).unwrap()).to_string());
        assert_eq!(
            "'1980/02/03 01:02:03'",
            Value::from(NaiveDate::from_ymd_opt(1980, 2, 3).unwrap()
                .and_hms_opt(1, 2, 3)
                .unwrap())
                .to_string());
        assert_eq!("0xDEADBEEF", Value::from(vec![0xDE_u8, 0xAD, 0xBE, 0xEF]).to_string());
    }

}

