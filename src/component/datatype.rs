#[derive(Debug, Clone)]
pub enum DataType {
    Char(u8),
    Double,
    Float,
    Int,
    Varchar(u8),
}
