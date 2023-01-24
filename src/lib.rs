pub mod connection;
pub(crate) mod command;
pub mod property;
pub mod error;
pub mod util;
pub mod to_sql;
pub mod null;
pub mod column_id;
pub mod result_set;
pub mod param_value;
pub mod statement;

pub use connection::Connection;
pub use error::Error;
pub use null::NULL;
pub use result_set::ResultSet;
pub use column_id::ColumnId;
use to_sql::ToSql;
pub type Result<T, E = error::Error> = core::result::Result<T, E>;
pub use param_value::ParamValue;
pub use statement::Statement;
pub use rust_decimal::Decimal;

#[derive(PartialEq, Debug, Clone)]
pub(crate) enum TextPiece {
    Literal(String),
    Placeholder
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedQuery {
    pieces: Vec<TextPiece>,
    params: Vec<Option<String>>,
}

impl ParsedQuery {
    
    pub(crate) fn param_index(&self, name: &str) -> Vec<usize> {
        let mut result = Vec::new();
        for (i, n) in self.params.iter().enumerate() {
            if let Some(n) = n {
                if n == name {
                    result.push(i);
                }
            }
        }
        result
    }

}

pub(crate) fn parse_query(text: impl AsRef<str>) -> ParsedQuery {
    let mut pieces: Vec<TextPiece> = Vec::new();
    let mut params: Vec<Option<String>> = Vec::new();
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

                        #[allow(clippy::while_let_on_iterator)]
                        while let Some(c1) = it.next() {
                            cur.push(c1);
                            if c1 == c {
                                break;
                            }
                        }
                    },
                    '/' => {
                        cur.push(c);
                        if it.peek().unwrap_or(&'\0') == &'*' {
                            #[allow(clippy::while_let_on_iterator)]
                            while let Some(c1) = it.next() {
                                cur.push(c1);
                                if c1 == '*' && it.peek().unwrap_or(&'\0') == &'/' {
                                    break;
                                }
                            }
                        }
                    },
                    '-' => {
                        cur.push(c);
                        if it.peek().unwrap_or(&'\0') == &'-' {
                            #[allow(clippy::while_let_on_iterator)]
                            while let Some(c1) = it.next() {
                                cur.push(c1);
                                if c1 == '\n' {
                                    break;
                                }
                            }
                        }
                    },
                    '?' => {
                        if !cur.is_empty() {
                            pieces.push(TextPiece::Literal(cur.clone()));
                            cur.clear();
                        }
                        pieces.push(TextPiece::Placeholder);
                        params.push(None);
                    },
                    ':' => {
                        if it.peek().is_none() {
                            cur.push(c);
                        } else {
                            if !cur.is_empty() {
                                pieces.push(TextPiece::Literal(cur.clone()));
                                cur.clear();
                            }

                            let mut name = String::new();
                            #[allow(clippy::while_let_on_iterator)]
                            while let Some(c) = it.peek() {
                                if c.is_alphanumeric() || *c == '_' {
                                    name.push(*c);
                                    it.next();
                                } else {
                                    break;
                                }
                            }

                            if name.is_empty() {
                                cur.push(c);
                            } else {
                                pieces.push(TextPiece::Placeholder);
                                params.push(Some(name));
                            }
                        }
                    },
                    _ => {
                        cur.push(c);
                    }
                }
            }
        }
    }

    if !cur.is_empty() {
        pieces.push(TextPiece::Literal(cur.clone()));
    }
    
    ParsedQuery { pieces, params }
}

pub(crate) fn generate_query<'a, I> (query: &ParsedQuery, mut params: I) -> String
where
    I: Iterator<Item = &'a dyn ToSql>
{
    let mut result = String::new();
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
    result
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
    fn test_named_param() {
        let s = ":param";
        let query = parse_query(s);
        println!("{:?}", query.pieces);
        assert_eq!(query.pieces[0], TextPiece::Placeholder);
        assert_eq!(query.params[0], Some(String::from("param")));
        assert_eq!(query.pieces.len(), 1);
        assert_eq!(query.params.len(), 1);

        let s = "select :param";
        let query = parse_query(s);
        println!("{:?}", query.pieces);
        assert_eq!(query.pieces[0], TextPiece::Literal(String::from("select ")));
        assert_eq!(query.pieces[1], TextPiece::Placeholder);
        assert_eq!(query.params[0], Some(String::from("param")));
        assert_eq!(query.pieces.len(), 2);
        assert_eq!(query.params.len(), 1);

        let s = "select :param,";
        let query = parse_query(s);
        println!("{:?}", query.pieces);
        assert_eq!(query.pieces[0], TextPiece::Literal(String::from("select ")));
        assert_eq!(query.pieces[1], TextPiece::Placeholder);
        assert_eq!(query.pieces[2], TextPiece::Literal(String::from(",")));
        assert_eq!(query.params[0], Some(String::from("param")));
        assert_eq!(query.pieces.len(), 3);
        assert_eq!(query.params.len(), 1);
    }

    #[test]
    fn test_parse_query() {
        let s = "?, '?', ?, \"?\", ? /* que? */, ? -- ?no?\nselect ?, :param1\n";
        let query = parse_query(s);

        assert_eq!(query.pieces[0], TextPiece::Placeholder);
        assert_eq!(query.pieces[1], TextPiece::Literal(String::from(", '?', ")));
        assert_eq!(query.pieces[2], TextPiece::Placeholder);
        assert_eq!(query.pieces[3], TextPiece::Literal(String::from(", \"?\", ")));
        assert_eq!(query.pieces[4], TextPiece::Placeholder);
        assert_eq!(query.pieces[5], TextPiece::Literal(String::from(" /* que? */, ")));
        assert_eq!(query.pieces[6], TextPiece::Placeholder);
        assert_eq!(query.pieces[7], TextPiece::Literal(String::from(" -- ?no?\nselect ")));
        assert_eq!(query.pieces[8], TextPiece::Placeholder);
        assert_eq!(query.pieces[9], TextPiece::Literal(String::from(", ")));
        assert_eq!(query.pieces[10], TextPiece::Placeholder);
        assert_eq!(query.pieces[11], TextPiece::Literal(String::from("\n")));

        println!("{:?}", query.pieces);
        println!("{:?}", query.params);
        
        assert_eq!(query.pieces.len(), 12);
        assert_eq!(query.params.len(), 6);

        let mut param_iter = query.params.iter();
        let concated: String = query.pieces.iter().map(
            |p| match p {
                TextPiece::Literal(s) => {
                    String::from(s)
                },
                TextPiece::Placeholder => {
                    let param = param_iter.next().unwrap();
                    match param {
                        Some(name) => format!(":{}", name),
                        None => String::from("?"),
                    }
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

