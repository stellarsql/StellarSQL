use crate::component::table::Table;
use crate::storage::file::File;
use crate::storage::file::FileError;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

#[derive(Debug)]
pub enum DatabaseError {
    CausedByFile(FileError),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DatabaseError::CausedByFile(ref e) => write!(f, "error caused by file: {}", e),
        }
    }
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

    // load the metadate of the database and its tables
    pub fn load_db(username: &str, db_name: &str) -> Result<Database, DatabaseError> {
        let mut db = Database::new(db_name);
        let metas = File::load_tables_meta(username, db_name, None).map_err(|e| DatabaseError::CausedByFile(e))?;
        for meta in metas {
            let name = (&meta.name).to_string();
            let mut table = Table::new(&name);
            table.format_meta(meta);
            db.tables.insert(name, table.into());
        }
        Ok(db)
    }
}
