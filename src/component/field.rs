use component::datatype::DataType;
use component::datatype::Value;

#[derive(Debug, Clone)]
pub struct Field {
    name: String,
    datatype: DataType,
    value: Value,
    not_null: bool,
    check: Checker,
}

#[derive(Debug, Clone)]
pub enum Checker {
    None,
    Some(Operator, Value)
}

#[derive(Debug, Clone)]
enum Operator {
    LT, // <
    LE, // <=
    EQ, // =
    NE, // !=
    GT, // >
    GE, // >=
}

impl Field {
    fn new(name: &str, datatype: DataType, value: Value, not_null: bool, check: Checker) -> Field {
        Field {
            name: name.to_string(),
            datatype,
            value,
            not_null,
            check,
        }
    }
}
