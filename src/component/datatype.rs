#[derive(Debug, Clone)]
pub enum DataType {
    Char,
    Double,
    Float,
    Int,
    Varchar,
}

#[derive(Debug, Clone)]
pub enum Value {
    Char(u8, String),
    Double(f64),
    Float(f32),
    Int(i32),
    Varchar(u8, String),
}
