use crate::component::datatype::DataType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub datatype: DataType,
    pub not_null: bool,
    pub default: Option<String>,
    pub check: Checker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Checker {
    None,
    Some(Operator, String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operator {
    LT, // <
    LE, // <=
    EQ, // =
    NE, // !=
    GT, // >
    GE, // >=
}

impl Field {
    pub fn new(name: &str, datatype: DataType) -> Field {
        Field {
            name: name.to_string(),
            datatype,
            not_null: false,
            default: None,
            check: Checker::None,
        }
    }

    pub fn new_all(name: &str, datatype: DataType, not_null: bool, default: Option<String>, check: Checker) -> Field {
        Field {
            name: name.to_string(),
            datatype,
            not_null,
            default,
            check,
        }
    }
}
