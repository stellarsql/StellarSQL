use component::table::Table;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new(name: &str) -> Database {
        Database {
            name: name.to_string(),
            tables: HashMap::new(),
        }
    }
}
