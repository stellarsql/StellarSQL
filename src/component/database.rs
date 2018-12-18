use crate::component::table::Table;
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

    pub fn insert_new_table(&mut self, table: Table) {
        self.tables.insert(table.name.to_string(), table);
    }

    // load the metadate of the database (without data) from storage
    pub fn load(name: &str) -> Database {
        // TODO: need load from the storage
        Database {
            name: name.to_string(),
            tables: HashMap::new(),
        }
    }

    // save the metadate and the data of the database to storage
    pub fn save(name: &str) {}
}
