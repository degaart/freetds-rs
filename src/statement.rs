use crate::{ParsedQuery, ColumnId, ParamValue, parse_query};

pub struct Statement {
    pub(crate) text: String,
    pub(crate) query: ParsedQuery,
    pub(crate) params: Vec<Option<ParamValue>>
}

impl Statement {

    pub fn new(text: &str) -> Self {
        let query = parse_query(text);
        let mut params = Vec::with_capacity(query.params.len());
        for _ in 0..query.params.len() {
            params.push(None);
        }
        Self {
            text: String::from(text),
            query,
            params
        }
    }

    pub fn set_param(&mut self, id: impl Into<ColumnId>, value: impl Into<ParamValue>) {
        let idx: usize = match id.into() {
            ColumnId::I32(i) => {
                let i: usize = i.try_into().expect("Invalid column index");
                if i >= self.query.params.len() {
                    panic!("Invalid column index");
                }
                i
            },
            ColumnId::String(s) => {
                match self.query.param_index(&s) {
                    None => panic!("Invalid param name"),
                    Some(idx) => idx
                }
            }
        };
        self.params[idx] = Some(value.into());
    }

    pub fn text(&self) -> &str {
        &self.text
    }

}

#[cfg(test)]
mod tests {
    use crate::{Statement, ParamValue};

    #[test]
    fn test_set_param() {
        let mut st = Statement::new("select ?, ?, ?, :param3, :param4");
        assert_eq!(st.params.len(), 5);

        st.set_param(0, "Manger");
        st.set_param(1, String::from("Chier"));
        st.set_param(2, 42);
        st.set_param("param3", 42.0);
        st.set_param(4, "arf".as_bytes());

        assert_eq!(st.params[0], Some(ParamValue::String(String::from("Manger"))));
        assert_eq!(st.params[1], Some(ParamValue::String(String::from("Chier"))));
        assert_eq!(st.params[2], Some(ParamValue::I32(42)));
        assert_eq!(st.params[3], Some(ParamValue::F64(42.0)));
        assert_eq!(st.params[4], Some(ParamValue::Blob(vec![b'a', b'r', b'f'])));


    }

}
