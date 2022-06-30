use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use crate::null::Null;

pub trait ToSql {
    fn to_sql(&self) -> String;

}

impl ToSql for Null {
    fn to_sql(&self) -> String {
        "null".to_string()
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
