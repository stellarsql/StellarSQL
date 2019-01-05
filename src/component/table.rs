use crate::component::field::Field;
use crate::storage::file::File;
use crate::storage::file::FileError;
use crate::storage::file::TableMeta;
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
    pub reference_attr: Option<String>,

    /* value */
    pub rows: Vec<Row>,

    /* storage */
    pub is_data_loaded: bool, // if load the data from storage
    pub is_dirty: bool,
    pub dirty_cursor: u32, // where is the dirty data beginning
    pub is_delete: bool,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub data: HashMap<String, String>,
    pub is_dirty: bool,
    pub is_delete: bool,
}

impl Row {
    pub fn new() -> Row {
        Row {
            data: HashMap::new(),
            is_dirty: true,
            is_delete: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TableError {
    InsertFieldNotExisted(String),
    InsertFieldNotNullMismatched(String),
    InsertFieldDefaultMismatched(String),
    CausedByFile(FileError),
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
            TableError::CausedByFile(ref e) => write!(f, "error caused by file: {}", e),
        }
    }
}

impl Table {
    pub fn new(name: &str) -> Table {
        Table {
            name: name.to_string(),
            fields: HashMap::new(),
            rows: vec![],
            primary_key: vec![],
            foreign_key: vec![],
            reference_table: None,
            reference_attr: None,

            is_data_loaded: false,
            is_dirty: true,
            dirty_cursor: 0,
            is_delete: false,
        }
    }

    /// Load a table with meta data
    #[allow(dead_code)]
    pub fn load_meta(username: &str, db_name: &str, table_name: &str) -> Result<Table, TableError> {
        let meta =
            File::load_table_meta(username, db_name, table_name, None).map_err(|e| TableError::CausedByFile(e))?;
        let mut table = Table::new(table_name);

        table.format_meta(meta);

        Ok(table)
    }

    /// format metadata into table
    pub fn format_meta(&mut self, meta: TableMeta) {
        self.fields = meta.attrs;
        self.primary_key = meta.primary_key;
        self.foreign_key = meta.foreign_key;
        self.reference_table = meta.reference_table;
        self.reference_attr = meta.reference_attr;
        self.is_dirty = false;
    }

    pub fn load_rows_data(&mut self, username: &str, db_name: &str) -> Result<(), TableError> {
        // TODO: read index file, find all row data range, call fetch_rows
        //let row_data = File::fetch_rows(username, db_name, self.name, , None).unwrap().map_err(|e| TableError::CauseByFile(e))?;
        //self.rows = row_data;
        self.is_data_loaded = true;
        Ok(())
    }

    pub fn insert_new_field(&mut self, field: Field) {
        self.fields.insert(field.name.clone(), field);
    }

    /// `insert` row into the table
    /// `key` and `value` are `&str`, and will be formated to the right type.
    pub fn insert_row(&mut self, row: Vec<(&str, &str)>) -> Result<(), TableError> {
        let mut new_row = Row::new();

        // insert data into row
        for (key, value) in row {
            match self.fields.get(key) {
                Some(field) => {
                    if field.not_null && value == "null" {
                        return Err(TableError::InsertFieldNotNullMismatched(field.clone().name));
                    }
                    new_row.data.insert(key.to_string(), value.to_string());
                }
                None => return Err(TableError::InsertFieldNotExisted(key.to_string())),
            }
        }

        // check if the row fits the field
        for (key, field) in self.fields.iter() {
            match new_row.data.get(key) {
                Some(_) => {}
                None => {
                    match field.clone().default {
                        // if the attribute has default value, then insert with the default value.
                        Some(value) => new_row.data.insert(key.to_string(), value.to_string()),
                        None => return Err(TableError::InsertFieldDefaultMismatched(key.to_string())),
                    };
                }
            };
        }

        self.rows.push(new_row);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::datatype::DataType;
    use crate::component::field;

    #[test]
    fn test_insert_row() {
        let mut table = Table::new("table_1");
        table.fields.insert(
            "attr_1".to_string(),
            Field::new_all(
                "attr_1",
                DataType::Int,
                true,                    // not_null is true
                Some("123".to_string()), // default is 123
                field::Checker::None,
                false,
            ),
        );
        table.fields.insert(
            "attr_2".to_string(),
            Field::new_all(
                "attr_2",
                DataType::Int,
                true, // not_null is true
                None, // no default
                field::Checker::None,
                false,
            ),
        );
        table.fields.insert(
            "attr_3".to_string(),
            Field::new_all(
                "attr_3",
                DataType::Int,
                false, // not null is false
                None,  // no default
                field::Checker::None,
                false,
            ),
        );

        println!("correct data");
        let data = vec![("attr_1", "123"), ("attr_2", "123"), ("attr_3", "123")];
        assert!(table.insert_row(data).is_ok());

        println!("`attr_2` is null while its not_null is true");
        let data = vec![("attr_1", "123"), ("attr_2", "null"), ("attr_3", "123")];
        assert!(table.insert_row(data).is_err());

        println!("`attr_3` is null while its not_null is false");
        let data = vec![("attr_1", "123"), ("attr_2", "123"), ("attr_3", "null")];
        assert!(table.insert_row(data).is_ok());

        println!("none given value `attr_2` while its default is None");
        let data = vec![("attr_1", "123"), ("attr_3", "123")];
        assert!(table.insert_row(data).is_err());

        println!("none given value `attr_1` while it has default");
        let data = vec![("attr_2", "123"), ("attr_3", "123")];
        assert!(table.insert_row(data).is_ok());

        println!("fields mismatched");
        let data = vec![
            ("attr_1", "123"),
            ("attr_2", "123"),
            ("attr_3", "123"),
            ("attr_4", "123"),
        ];
        assert!(table.insert_row(data).is_err());
        let data = vec![("attr_1", "123")];
        assert!(table.insert_row(data).is_err());
    }
}
