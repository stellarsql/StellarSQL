use crate::component::field::Field;
use crate::component::table::Row;
use crate::component::table::Table;
use crate::storage::file;
use crate::storage::file::File;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct DiskInterface {
    /* definition */
// Ideally, DiskInterface is a stateless struct
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableMeta {
    pub name: String,
    pub username: String,
    pub db_name: String,
    pub path_tsv: String,
    pub path_bin: String,
    pub primary_key: Vec<String>,
    pub foreign_key: Vec<String>,
    pub reference_table: Option<String>,
    pub reference_attr: Option<String>,
    pub row_length: u32,
    pub attrs: HashMap<String, Field>,
    pub attrs_order: Vec<String>,
    pub attr_offset_ranges: Vec<Vec<u32>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DiskInterfaceError {
    File,
    // Index,
}

impl From<file::FileError> for DiskInterfaceError {
    fn from(_err: file::FileError) -> DiskInterfaceError {
        DiskInterfaceError::File
    }
}

#[allow(dead_code)]
impl DiskInterfaceError {
    pub fn create_username(username: &str, file_base_path: Option<&str>) -> Result<(), DiskInterfaceError> {
        Ok(File::create_username(username, file_base_path)?)
    }

    pub fn get_usernames(file_base_path: Option<&str>) -> Result<Vec<String>, DiskInterfaceError> {
        Ok(File::get_usernames(file_base_path)?)
    }

    pub fn remove_username(username: &str, file_base_path: Option<&str>) -> Result<(), DiskInterfaceError> {
        Ok(File::remove_username(username, file_base_path)?)
    }

    pub fn create_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), DiskInterfaceError> {
        Ok(File::create_db(username, db_name, file_base_path)?)
    }

    pub fn get_dbs(username: &str, file_base_path: Option<&str>) -> Result<Vec<String>, DiskInterfaceError> {
        Ok(File::get_dbs(username, file_base_path)?)
    }

    pub fn remove_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), DiskInterfaceError> {
        Ok(File::remove_db(username, db_name, file_base_path)?)
    }

    pub fn create_table(
        username: &str,
        db_name: &str,
        table: &Table,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskInterfaceError> {
        Ok(File::create_table(username, db_name, table, file_base_path)?)
    }

    pub fn get_tables(
        username: &str,
        db_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<Vec<String>, DiskInterfaceError> {
        Ok(File::get_tables(username, db_name, file_base_path)?)
    }

    pub fn load_tables_meta(
        username: &str,
        db_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<Vec<TableMeta>, DiskInterfaceError> {
        Ok(File::load_tables_meta(username, db_name, file_base_path)?)
    }

    pub fn load_table_meta(
        username: &str,
        db_name: &str,
        table_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<TableMeta, DiskInterfaceError> {
        Ok(File::load_table_meta(username, db_name, table_name, file_base_path)?)
    }

    pub fn drop_table(
        username: &str,
        db_name: &str,
        table_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskInterfaceError> {
        Ok(File::drop_table(username, db_name, table_name, file_base_path)?)
    }

    pub fn append_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        rows: &Vec<Row>,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskInterfaceError> {
        Ok(File::append_rows(username, db_name, table_name, rows, file_base_path)?)
    }

    pub fn fetch_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        row_range: &Vec<u32>,
        file_base_path: Option<&str>,
    ) -> Result<Vec<Row>, DiskInterfaceError> {
        Ok(File::fetch_rows(
            username,
            db_name,
            table_name,
            row_range,
            file_base_path,
        )?)
    }

    pub fn delete_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        row_range: &Vec<u32>,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskInterfaceError> {
        Ok(File::delete_rows(
            username,
            db_name,
            table_name,
            row_range,
            file_base_path,
        )?)
    }

    pub fn modify_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        row_range: &Vec<u32>,
        new_rows: &Vec<Row>,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskInterfaceError> {
        Ok(File::modify_rows(
            username,
            db_name,
            table_name,
            row_range,
            new_rows,
            file_base_path,
        )?)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
// }
