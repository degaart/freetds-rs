#![allow(clippy::expect_fun_call)]

use crate::{parse_query, ColumnId, ParsedQuery, Value};

pub struct Statement {
    pub(crate) text: String,
    pub(crate) query: ParsedQuery,
    pub(crate) params: Vec<Value>,
}

impl Statement {
    pub fn new(text: &str) -> Self {
        let query = parse_query(text);
        let mut params = Vec::with_capacity(query.params.len());
        params.resize(query.params.len(), Default::default());
        Self {
            text: String::from(text),
            query,
            params,
        }
    }

    pub fn set_param(&mut self, id: impl Into<ColumnId>, value: impl Into<Value>) {
        match id.into() {
            ColumnId::I32(i) => {
                let i: usize = i.try_into().expect(&format!("Invalid column index: {}", i));
                if i >= self.query.params.len() {
                    panic!("Invalid column index");
                }
                self.params[i] = value.into();
            }
            ColumnId::String(s) => {
                let indexes = self.query.param_index(&s);
                let param_val: Value = value.into();
                for i in indexes {
                    self.params[i] = param_val.clone();
                }
            }
        };
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn param_count(&self) -> usize {
        self.params.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::{generate_query, to_sql::ToSql, Statement, Value};

    #[test]
    fn test_set_param() {
        let mut st = Statement::new("select ?, ?, ?, :param3, :param4");
        assert_eq!(st.params.len(), 5);

        st.set_param(0, "Manger");
        st.set_param(1, String::from("Chier"));
        st.set_param(2, 42);
        st.set_param("param3", 42.0);
        st.set_param(4, "arf".as_bytes());

        assert_eq!(
            st.params[0],
            Value::from("Manger")
        );
        assert_eq!(
            st.params[1],
            Value::from("Chier")
        );
        assert_eq!(st.params[2], Value::I32(42));
        assert_eq!(st.params[3], Value::F64(42.0));
        assert_eq!(st.params[4], Value::Blob(vec![b'a', b'r', b'f']));
    }

    #[test]
    fn test_double_param() {
        let mut st = Statement::new(":owner, :name, :name");
        st.set_param("owner", "DIO");
        st.set_param("name", "ZA WARUDO");
        assert_eq!(st.params[0], Value::from("DIO"));
        assert_eq!(
            st.params[1],
            Value::from("ZA WARUDO")
        );

        let params: Vec<&dyn ToSql> = st
            .params
            .iter()
            .map(|v| v as &dyn ToSql)
            .collect();
        let mut text = String::new();
        generate_query(&mut text, &st.query, params.iter().map(|p| *p)).unwrap();

        let expected = "'DIO', 'ZA WARUDO', 'ZA WARUDO'";
        assert_eq!(expected, text);
    }
}
