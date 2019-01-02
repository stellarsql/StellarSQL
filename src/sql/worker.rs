use crate::component::database::Database;
use crate::component::database::DatabaseError;
use crate::component::table::Table;
use crate::sql::query::QueryData;
use std::fmt;

#[derive(Debug)]
pub struct SQL {
    pub username: String,
    pub database: Database,
    pub querydata: QueryData,
}

#[derive(Debug)]
pub enum SQLError {
    CauserByDatabase(DatabaseError),
    SemanticError(String),
}

impl fmt::Display for SQLError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SQLError::CauserByDatabase(ref e) => write!(f, "{}", e),
            SQLError::SemanticError(ref s) => write!(f, "semantic error: {}", s),
        }
    }
}

impl SQL {
    pub fn new(username: &str) -> Result<SQL, SQLError> {
        Ok(SQL {
            username: username.to_string(),
            database: Database::new(""), // empty db
            querydata: QueryData::new(),
        })
    }

    // Create a new database
    pub fn create_database(&mut self, db_name: &str) -> Result<(), SQLError> {
        self.database = Database::new(db_name);
        Ok(())
    }

    /// Load a database
    pub fn load_database(&mut self, db_name: &str) -> Result<(), SQLError> {
        self.database = Database::load_db(&self.username, db_name).map_err(|e| SQLError::CauserByDatabase(e))?;
        Ok(())
    }

    /// Load the database and create a new table
    pub fn create_table(&mut self, table: &Table) -> Result<(), SQLError> {
        self.database.insert_new_table(table.clone());
        Ok(())
    }

    /// Insert new rows into the table
    pub fn insert_into_table(
        &mut self,
        table_name: &str,
        attrs: Vec<String>,
        rows: Vec<Vec<String>>,
    ) -> Result<(), SQLError> {
        let table = self
            .database
            .tables
            .get_mut(table_name)
            .ok_or(SQLError::SemanticError("table not exists".to_string()))?;

        for row in rows {
            let mut row_in_pair: Vec<(&str, &str)> = Vec::new();
            for i in 0..attrs.len() {
                row_in_pair.push((&attrs[i], &row[i]));
            }
            table
                .insert_row(row_in_pair)
                .map_err(|e| SQLError::SemanticError(format!("{}", e)))?;
        }

        Ok(())
    }
}
