pub mod connection;
pub(crate) mod command;
pub mod property;
pub mod error;
pub mod util;
pub mod to_sql;
pub mod null;
pub mod column_id;
pub mod result_set;

pub use connection::Connection;
pub use error::Error;
pub use null::NULL;
pub use result_set::ResultSet;
pub use column_id::ColumnId;

pub type Result<T, E = error::Error> = core::result::Result<T, E>;

#[cfg(test)]
mod tests {

}

