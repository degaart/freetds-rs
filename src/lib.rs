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
use to_sql::ToSql;
pub type Result<T, E = error::Error> = core::result::Result<T, E>;

#[derive(PartialEq, Debug)]
pub(crate) enum TextPiece {
    Literal(String),
    Placeholder
}

pub(crate) struct ParsedQuery {
    pieces: Vec<TextPiece>,
    param_count: usize
}

pub(crate) fn parse_query(text: impl AsRef<str>) -> ParsedQuery {
    let mut pieces: Vec<TextPiece> = Vec::new();
    let mut param_count: usize = 0;
    let mut cur = String::new();
    let mut it = text.as_ref().chars().peekable();
    loop {
        let c = it.next();
        match c {
            None => {
                break;
            },
            Some(c) => {
                match c {
                    '\'' | '"' => {
                        cur.push(c);
                        while let Some(c1) = it.next() {
                            cur.push(c1);
                            if c1 == c {
                                break;
                            }
                        }
                    },
                    '/' => {
                        if it.peek().unwrap_or(&'\0') == &'*' {
                            cur.push(c);
                            while let Some(c1) = it.next() {
                                cur.push(c1);
                                if c1 == '*' && it.peek().unwrap_or(&'\0') == &'/' {
                                    break;
                                }
                            }
                        } else {
                            cur.push(c);
                        }
                    },
                    '-' => {
                        if it.peek().unwrap_or(&'\0') == &'-' {
                            cur.push(c);
                            while let Some(c1) = it.next() {
                                cur.push(c1);
                            }
                        }
                    },
                    '?' => {
                        if cur.len() > 0 {
                            pieces.push(TextPiece::Literal(cur.clone()));
                            cur.clear();
                        }
                        pieces.push(TextPiece::Placeholder);
                        param_count += 1;
                    },
                    _ => {
                        cur.push(c);
                    }
                }
            }
        }
    }

    if cur.len() > 0 {
        pieces.push(TextPiece::Literal(cur.clone()));
    }
    
    ParsedQuery { pieces, param_count }
}

pub(crate) fn generate_query(query: &ParsedQuery, params: &[&dyn ToSql]) -> String {
    let mut result = String::new();
    let mut params = params.iter();
    for piece in &query.pieces {
        result.push_str(&match piece {
            TextPiece::Literal(s) => {
                s.to_string()
            },
            TextPiece::Placeholder => {
                match params.next() {
                    Some(value) => {
                        value.to_sql()
                    },
                    None => {
                        "null".to_string()
                    }
                }
            }
        });
    }
    return result;
}


#[cfg(test)]
mod tests {
    use crate::{parse_query, TextPiece, Connection};

    fn connect() -> Connection {
        let mut conn = Connection::new();
        conn.set_client_charset("UTF-8").unwrap();
        conn.set_username("sa").unwrap();
        conn.set_password("").unwrap();
        conn.set_database("master").unwrap();
        conn.set_tds_version_50().unwrap();
        conn.set_login_timeout(5).unwrap();
        conn.set_timeout(5).unwrap();
        conn.connect("***REMOVED***:2025").unwrap();
        conn
    }

    #[test]
    fn test_parse_query() {
        let s = "?, '?', ?, \"?\", ? /* que? */, ? -- ?no?";
        let query = parse_query(s);
        assert_eq!(query.pieces.len(), 8);
        assert_eq!(query.param_count, 4);

        let concated: String = query.pieces.iter().map(
            |p| match p {
                TextPiece::Literal(s) => {
                    &s
                },
                TextPiece::Placeholder => {
                    "?"
                }
            })
            .collect();
        assert_eq!(s, concated);
    }

    #[test]
    fn test_quotes() {
        let mut conn = connect();
        let mut rs = conn
            .execute("select '''ab''', ?", &[&"\'cd\'"])
            .unwrap();
        assert!(rs.next());
        assert_eq!("\'ab\'", rs.get_string(0).unwrap().unwrap());
        assert_eq!("\'cd\'", rs.get_string(1).unwrap().unwrap());
    }

}

