pub struct Null {}

impl Null {
    pub fn null() -> Self {
        Self {}
    }
}

#[allow(dead_code)]
pub static NULL: Null = Null {};
