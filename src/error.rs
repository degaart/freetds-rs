use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum Type {
    Cs,
    Client,
    Server,
    Library
}

#[derive(Debug, Clone)]
pub struct Error {
    type_: Type,
    code: Option<i32>,
    desc: String,
}

impl Error {
    pub fn new(type_: Option<Type>, code: Option<i32>, desc: impl AsRef<str>) -> Self {
        Self {
            type_: match type_ {
                None => Type::Library,
                Some(type_) => type_
            },
            code: code,
            desc: desc.as_ref().to_string()
        }
    }

    pub fn from_message(desc: impl AsRef<str>) -> Self {
        Self::new(None, None, desc)
    }

    pub fn from_failure(fn_name: impl AsRef<str>) -> Self {
        Self::new(None, None, &format!("{} failed", fn_name.as_ref()))
    }

    pub fn code(&self) -> Option<i32> {
        self.code
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

    pub fn type_(&self) -> Type {
        self.type_
    }

}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.type_ {
            Type::Cs => { write!(f, "CS library error")?; },
            Type::Client => { write!(f, "Client library error")?; },
            Type::Server => { write!(f, "Server error")?; },
            Type::Library => { write!(f, "FreeTDS error")?; }
        };

        if let Some(code) = self.code {
            write!(f, " #{:04}", code)?;
        }

        write!(f, ": {}", self.desc)
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
