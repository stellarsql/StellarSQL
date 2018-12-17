use crate::component::database::Database;
use crate::component::table::Table;
use std::fmt;

pub struct SQL {
    pub database: Database,
}

#[derive(Debug)]
pub enum SQLError {}

impl fmt::Display for SQLError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {}
    }
}

impl SQL {
    // Create a new database
    pub fn create_database(name: &str) -> Result<SQL, SQLError> {
        Ok(SQL {
            database: Database::new(name),
        })
    }

    // Load the database and create a new table
    pub fn create_table(db_name: &str, table_name: &str) -> Result<SQL, SQLError> {
        let db = Database::load(db_name);
        // TODO: create table in database
        Ok(SQL { database: db })
    }
}
