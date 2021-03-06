pub mod connection;
pub mod command;
pub mod property;
pub mod error;
pub mod util;
pub mod to_sql;
pub mod null;

pub use connection::Connection;
pub use error::Error;
pub use null::NULL;

pub type Result<T, E = error::Error> = core::result::Result<T, E>;

#[cfg(test)]
mod tests {

}

