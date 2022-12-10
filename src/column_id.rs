pub enum ColumnId {
    I32(i32),
    String(String),
}

impl From<i32> for ColumnId {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}

impl From<i64> for ColumnId {
    fn from(value: i64) -> Self {
        Self::I32(value.try_into().expect("Invalid column id"))
    }
}

impl From<usize> for ColumnId {
    fn from(value: usize) -> Self {
        Self::I32(value.try_into().expect("Invalid column id"))
    }
}

impl From<String> for ColumnId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ColumnId {
    fn from(value: &str) -> Self {
        Self::String(String::from(value))
    }
}
