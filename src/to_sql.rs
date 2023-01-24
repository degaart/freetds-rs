use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use crate::{null::Null, ParamValue};

pub trait ToSql {
    fn to_sql(&self) -> String;
}

impl ToSql for Null {
    fn to_sql(&self) -> String {
        "null".to_string()
    }
}

impl ToSql for String {
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
        result
    }
}

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
        result
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> String {
        format!("{}", self)
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> String {
        format!("{}", self)
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> String {
        format!("{}", self)
    }
}

impl ToSql for Decimal {
    fn to_sql(&self) -> String {
        self.to_string()
    }
}

impl ToSql for NaiveDateTime {
    fn to_sql(&self) -> String {
        self.format("'%Y/%m/%d %H:%M:%S%.f'").to_string()
    }
}

impl ToSql for NaiveDate {
    fn to_sql(&self) -> String {
        self.format("'%Y/%m/%d'").to_string()
    }
}

impl ToSql for NaiveTime {
    fn to_sql(&self) -> String {
        self.format("%H:%M:%S%.f").to_string()
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> String {
        let mut result = String::new();
        result.push_str("0x");
        for c in self.iter() {
            result.push_str(&format!("{:02X}", c));
        }
        result
    }
}

impl ToSql for &[u8] {
    fn to_sql(&self) -> String {
        let mut result = String::new();
        result.push_str("0x");
        for c in self.iter() {
            result.push_str(&format!("{:02X}", c));
        }
        result
    }
}

impl<T> ToSql for Option<T>
where
    T: ToSql
{
    fn to_sql(&self) -> String {
        match self {
            Some(value) => {
                value.to_sql()
            },
            None => {
                "null".to_string()
            },
        }
    }
}

impl ToSql for ParamValue {
    fn to_sql(&self) -> String {
        match self {
            ParamValue::Null => String::from("null"),
            ParamValue::String(s) => s.to_sql(),
            ParamValue::I32(i) => i.to_sql(),
            ParamValue::I64(i) => i.to_sql(),
            ParamValue::F64(f) => f.to_sql(),
            ParamValue::Decimal(d) => d.to_sql(),
            ParamValue::Date(d) => d.to_sql(),
            ParamValue::Time(t) => t.to_sql(),
            ParamValue::DateTime(dt) => dt.to_sql(),
            ParamValue::Blob(b) => b.to_sql(),
        }
    }
}
