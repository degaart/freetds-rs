use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum Type {
    Cs,
    Client,
    Server,
    Library,
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Default for Type {
    fn default() -> Self {
        Self::Library
    }
}

#[derive(Debug, Clone)]
pub struct Error {
    pub(crate) type_: Type,
    pub(crate) code: Option<i32>,
    pub(crate) desc: String,
    pub(crate) severity: Option<i32>,
}

impl Error {
    pub fn from_message(desc: impl AsRef<str>) -> Self {
        Self {
            type_: Default::default(),
            code: None,
            desc: desc.as_ref().to_string(),
            severity: None,
        }
    }

    pub fn from_failure(fn_name: impl AsRef<str>) -> Self {
        Self {
            type_: Default::default(),
            code: None,
            desc: format!("{} failed", fn_name.as_ref()),
            severity: None,
        }
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

    pub fn severity(&self) -> Option<i32> {
        self.severity
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.type_ {
            Type::Cs => {
                write!(f, "CS library error")?;
            }
            Type::Client => {
                write!(f, "Client library error")?;
            }
            Type::Server => {
                write!(f, "Server error")?;
            }
            Type::Library => {
                write!(f, "FreeTDS error")?;
            }
        };

        if let Some(code) = self.code {
            write!(f, " #{:04}", code)?;
        }

        if let Some(severity) = self.severity {
            write!(f, " severity {}", severity)?;
        }

        write!(f, ": {}", self.desc)
    }
}

impl std::error::Error for Error {}

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
