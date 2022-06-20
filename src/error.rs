use crate::util::return_name;

#[derive(Debug, Clone)]
pub struct Error {
    code: Option<i32>,
    fn_name: Option<String>,
    desc: String,
}

impl Error {
    pub fn new(code: i32, fn_name: impl AsRef<str>) -> Self {
        Self {
            code: Some(code),
            fn_name: Some(fn_name.as_ref().to_string()),
            desc: format!("{} failed (ret: {})", fn_name.as_ref(), return_name(code).unwrap_or(&format!("{}", code)))
        }
    }

    pub fn from_message(desc: impl AsRef<str>) -> Self {
        Self {
            code: None,
            fn_name: None,
            desc: desc.as_ref().into()
        }
    }

    pub fn code(&self) -> Option<i32> {
        self.code
    }

    pub fn fn_name(&self) -> Option<&str> {
        if let Some(s) = &self.fn_name {
            Some(s)
        } else {
            None
        }
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.desc)
    }
}

impl std::error::Error for Error {

}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Self::from_message(e.to_string())
    }
}

impl From<std::ffi::NulError> for Error {
    fn from(e: std::ffi::NulError) -> Self {
        Self::from_message(e.to_string())
    }
}

macro_rules! err {
    ($code:ident, $fn_name:ident) => {
        Err(Error::new($code, stringify!($fn_name)))
    };
    ($desc:literal) => {
        Err(Error::from_message($desc))
    };
    ($desc:literal, $($arg:tt)*) => {
        Err(Error::from_message(format!($desc, $($arg)*)))
    };
}
use std::fmt::Display;

pub(crate) use err;

macro_rules! succeeded {
    ($code:ident, $fn_name:ident) => {
        if $code != CS_SUCCEED {
            return Err(Error::new($code, stringify!($fn_name)))
        }
    };
}
pub(crate) use succeeded;
