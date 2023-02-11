use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
use rust_decimal::Decimal;

pub trait ToSql {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result;
}

fn write_string(f: &mut dyn std::fmt::Write, s: &str) -> std::fmt::Result {
    const QUOTE: char = '\'';
    f.write_char(QUOTE)?;
    for c in s.chars() {
        f.write_char(c)?;
        if c == QUOTE {
            f.write_char(c)?;
        }
    }
    f.write_char(QUOTE)
}

impl ToSql for &str {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write_string(f, self)
    }
}

impl ToSql for String {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write_string(f, self)
    }
}

impl ToSql for i32 {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl ToSql for i64 {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl ToSql for f64 {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl ToSql for Decimal {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl ToSql for NaiveDate {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        f.write_str(&self.format("'%Y/%m/%d'").to_string())
    }
}

impl ToSql for NaiveTime {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        f.write_str(&self.format("'%H:%M:%S%.f'").to_string())
    }
}

impl ToSql for NaiveDateTime {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        f.write_str(&self.format("'%Y/%m/%d %H:%M:%S%.f'").to_string())
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        f.write_str("0x")?;
        for c in self.iter() {
            write!(f, "{c:02X}")?;
        }
        Ok(())
    }
}

impl<T> ToSql for Option<T>
where
    T: ToSql
{
    fn to_sql(&self, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        match self {
            Some(value) => value.to_sql(f),
            None => write!(f, "null"),
        }
    }
}

