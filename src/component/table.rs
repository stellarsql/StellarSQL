use crate::component::datatype::DataType;
use crate::component::field::Field;
use crate::storage::diskinterface::{DiskError, DiskInterface, TableMeta};
use std::collections::HashMap;
use std::collections::HashSet;
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

    /* virtual table */
    is_predicate_init: bool, // if ever filter rows for predicate
    row_set: HashSet<usize>, // record rows for predicate

    /* encryption */
    pub public_key: i32,
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

#[derive(Debug, Serialize)]
pub struct SelectData {
    pub fields: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl SelectData {
    pub fn new() -> SelectData {
        SelectData {
            fields: vec![],
            rows: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub enum TableError {
    InsertFieldNotExisted(String),
    InsertFieldNotNullMismatched(String),
    InsertFieldDefaultMismatched(String),
    SelectFieldNotExisted(String),
    CausedByFile(DiskError),
    KeyNotExist,
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
            TableError::SelectFieldNotExisted(ref name) => write!(f, "Selected field not exists: {}", name),
            TableError::CausedByFile(ref e) => write!(f, "error caused by file: {}", e),
            TableError::KeyNotExist => write!(f, "encrypt error: public key is not existed"),
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

            is_predicate_init: false,
            row_set: HashSet::new(),

            public_key: 0,
        }
    }

    /// Load a table with meta data
    #[allow(dead_code)]
    pub fn load_meta(username: &str, db_name: &str, table_name: &str) -> Result<Table, TableError> {
        let meta = DiskInterface::load_table_meta(username, db_name, table_name, None)
            .map_err(|e| TableError::CausedByFile(e))?;
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

    /// load the particular range of rows from storage
    pub fn load_rows_data(&mut self, username: &str, db_name: &str) -> Result<(), TableError> {
        // TODO: read index file, find all row data range, call fetch_rows
        //let row_data = DiskInterface::fetch_rows(username, db_name, self.name, , None).unwrap().map_err(|e| TableError::CauseByFile(e))?;
        //self.rows = row_data;
        self.is_data_loaded = true;
        Ok(())
    }

    /// load the all data from storage
    pub fn load_all_rows_data(&mut self, username: &str, db_name: &str) -> Result<(), TableError> {
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

        for (key, field) in self.fields.iter() {
            if field.encrypt {
                if self.public_key == 0 {
                    // 0 is default key value, which is not a valid key
                    return Err(TableError::KeyNotExist);
                }
                let value = new_row.data.get_mut(key).unwrap();
                // TODO: encrypt value with self.public_key
            }
        }
        self.rows.push(new_row);

        Ok(())
    }

    /// return the set of all rows' id of the table
    pub fn get_all_rows_set(&self) -> HashSet<usize> {
        let mut set: HashSet<usize> = HashSet::new();
        for i in 0..self.rows.len() {
            set.insert(i);
        }
        set
    }

    /// filter rows by the predicate and update the row_set
    ///
    /// Note: this assume all data of rows and the predicate follow the rules, so there is no check for
    /// data type and field name.
    pub fn operator_filter_rows(
        &mut self,
        field_name: &str,
        operator: &str,
        value: &str,
    ) -> Result<HashSet<usize>, TableError> {
        let data_type = self.fields.get(field_name).unwrap().datatype.clone();
        let mut set = HashSet::new();

        // if the first time, the predicate range is the range of all rows.
        if !self.is_predicate_init {
            for i in 0..self.rows.len() {
                self.row_set.insert(i);
            }
            // TODO: analyse when to set true.
            // currently always false, so it will get all rows every times.HashSet
            // We need to figure out when to let it to be true, as when there is `OR` then
            // it should keep false.

            // self.is_predicate_init = true;
        }

        for i in self.row_set.iter() {
            let row = &self.rows[*i];
            if match data_type {
                DataType::Int => {
                    let data = row.data.get(field_name).unwrap().parse::<i32>().unwrap();
                    let value = value.parse::<i32>().unwrap();
                    cmp(data, operator, value)
                }
                DataType::Float => {
                    let data = row.data.get(field_name).unwrap().parse::<f32>().unwrap();
                    let value = value.parse::<f32>().unwrap();
                    cmp(data, operator, value)
                }
                DataType::Double => {
                    let data = row.data.get(field_name).unwrap().parse::<f64>().unwrap();
                    let value = value.parse::<f64>().unwrap();
                    cmp(data, operator, value)
                }
                DataType::Char(_) => {
                    let data = row.data.get(field_name).unwrap().clone();
                    cmp(data, operator, value.to_string())
                }
                DataType::Varchar(_) => {
                    let data = row.data.get(field_name).unwrap().clone();
                    cmp(data, operator, value.to_string())
                }
            } {
                set.insert(*i);
            }
        }

        self.row_set = set;
        Ok(self.row_set.clone())
    }

    /// set the new row set
    pub fn set_row_set(&mut self, set: HashSet<usize>) {
        self.row_set = set;
        self.is_predicate_init = true;
    }

    /// select fields from rows in row_set of the table
    pub fn select(&mut self, field_names: Vec<String>) -> Result<SelectData, TableError> {
        let mut data = SelectData::new();
        for name in &field_names {
            data.fields.push(name.to_string());
        }
        // if no predicate, select all data
        if !self.is_predicate_init {
            for i in 0..self.rows.len() {
                self.row_set.insert(i);
            }
            self.is_predicate_init = true;
        }
        // only which is in row_set will be picked
        for i in &self.row_set {
            let row = &self.rows[*i];
            let mut r = vec![];
            for name in &field_names {
                r.push(
                    row.data
                        .get::<str>(name)
                        .ok_or(TableError::SelectFieldNotExisted(name.to_string()))?
                        .clone(),
                );
            }
            data.rows.push(r);
        }
        data.rows.sort();
        Ok(data)
    }
}

#[inline]
fn cmp<T: PartialOrd>(left: T, operator: &str, right: T) -> bool {
    match operator {
        "=" => left == right,
        ">" => left > right,
        ">=" => left >= right,
        "<" => left < right,
        "<=" => left <= right,
        "!=" => left != right,
        "<>" => left != right,
        _ => false, // never happen
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

    #[test]
    #[rustfmt::skip]
    fn test_operator_filter_rows() {
        let mut table = Table::new("table_1");
        table.fields.insert("a1".to_string(), Field::new("attr_1", DataType::Int));
        table.fields.insert("a2".to_string(), Field::new("attr_1", DataType::Char(20)));
        let data = vec![("a1", "1"), ("a2", "aaa")];
        let _ = table.insert_row(data).unwrap();
        let data = vec![("a1", "2"), ("a2", "bbb")];
        let _ = table.insert_row(data).unwrap();
        let data = vec![("a1", "3"), ("a2", "aaa")];
        let _ = table.insert_row(data).unwrap();
        let data = vec![("a1", "4"), ("a2", "bbb")];
        let _ = table.insert_row(data).unwrap();

        let set = table.operator_filter_rows("a1", ">", "2").unwrap();
        table.set_row_set(set);
        let select_data = table.select(vec!["a1".to_string(), "a2".to_string()]).unwrap();
        assert_eq!(select_data.rows, vec![["3", "aaa"], ["4", "bbb"]]);

        let set = table.operator_filter_rows("a2", "=", "bbb").unwrap();
        table.set_row_set(set);
        let select_data = table.select(vec!["a1".to_string(), "a2".to_string()]).unwrap();
        assert_eq!(select_data.rows, vec![vec!["4", "bbb"]]);
    }
}
