#![allow(unused)]
use std::fmt::Display;

pub enum Value {
    Null,
    I32(i32),
    I64(i64),
    String(String),
    Blob(Vec<u8>),
}

impl Value {
    fn new() -> Self {
        Self::Null
    }

    fn to_sql(&self) -> String {
        todo!();
    }
}

impl Display for Value {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!();
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

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
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

