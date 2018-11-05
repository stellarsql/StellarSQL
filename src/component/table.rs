use component::field::Field;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    fields: HashMap<String, Field>,
    primary_key: Vec<String>,
    foreign_key: Vec<String>,
    reference_table: Option<String>,
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
