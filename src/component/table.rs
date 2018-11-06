use component::field::Field;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct Table {
    /* definition */
    pub name: String,
    pub fields: HashMap<String, Field>, // aka attributes
    pub primary_key: Vec<String>,
    pub foreign_key: Vec<String>,
    pub reference_table: Option<String>,

    /* value */
    pub rows: Vec<Row>,

    /* storage */
    pub page: u64,           // which page of this table
    pub cursors: (u64, u64), // cursors of range in a page
}

#[derive(Debug, Clone)]
pub struct Row(HashMap<String, String>);

impl Row {
    fn new() -> Row {
        Row(HashMap::new())
    }
}

#[derive(Debug, Clone)]
pub enum TableError {
    InsertFieldNotExisted(String),
    InsertFieldNotNullMismatched(String),
    InsertFieldDefaultMismatched(String),
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TableError::InsertFieldNotExisted(ref attr_name) => {
                write!(f, "Insert Error: the table doesn't have `{}` attribute.", attr_name)
            }
            TableError::InsertFieldNotNullMismatched(ref attr_name) => {
                write!(f, "Insert Error: {} could not be null", attr_name)
            }
            TableError::InsertFieldDefaultMismatched(ref attr_name) => write!(
                f,
                "Insert Error: {} has no default value. Need to declare the value.",
                attr_name
            ),
        }
    }
}

impl Table {
    fn new(name: &str) -> Table {
        Table {
            name: name.to_string(),
            fields: HashMap::new(),
            primary_key: vec![],
            foreign_key: vec![],
            reference_table: None,
        }
    }
}
