use crate::component::datatype::DataType;
use crate::component::field;
use crate::component::field::Field;
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
    pub fn new(name: &str) -> Table {
        Table {
            name: name.to_string(),
            fields: HashMap::new(),
            rows: vec![],
            primary_key: vec![],
            foreign_key: vec![],
            reference_table: None,
            page: 0,
            cursors: (0, 0),
        }
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
                    new_row.0.insert(key.to_string(), value.to_string());
                }
                None => return Err(TableError::InsertFieldNotExisted(key.to_string())),
            }
        }

        // check if the row fits the field
        for (key, field) in self.fields.iter() {
            match new_row.0.get(key) {
                Some(_) => {}
                None => {
                    match field.clone().default {
                        // if the attribute has default value, then insert with the default value.
                        Some(value) => new_row.0.insert(key.to_string(), value.to_string()),
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
