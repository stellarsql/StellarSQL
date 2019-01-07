use crate::component::datatype::DataType;
use crate::component::field::Field;
use crate::component::table::Row;
use crate::component::table::Table;
use crate::storage::bytescoder;
use crate::storage::file::File;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct DiskInterface {
    /* definition */
// Ideally, DiskInterface is a stateless struct
}

// structure of `usernames.json`

#[derive(Debug, Serialize, Deserialize)]
pub struct UsernamesJson {
    pub usernames: Vec<UsernameInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UsernameInfo {
    pub name: String,
    pub path: String,
}

// structure of `dbs.json`

#[derive(Debug, Serialize, Deserialize)]
pub struct DbsJson {
    pub dbs: Vec<DbInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbInfo {
    pub name: String,
    pub path: String,
}

// structure of `tables.json`

#[derive(Debug, Serialize, Deserialize)]
pub struct TablesJson {
    pub tables: Vec<TableMeta>,
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
pub enum DiskError {
    Io,
    BaseDirExists,
    BaseDirNotExists,
    UsernamesJsonNotExists,
    UsernameExists,
    UsernameNotExists,
    UsernameDirNotExists,
    DbsJsonNotExists,
    DbExists,
    DbNotExists,
    DbDirNotExists,
    TablesJsonNotExists,
    TableExists,
    TableNotExists,
    TableBinNotExists,
    TableTsvNotExists,
    TableIdxFileNotExists,
    JsonParse,
    RangeContainsDeletedRecord,
    RangeExceedLatestRecord,
    RangeAndNumRowsMismatch,
    AttrNotExists,
    BytesError,
}

impl From<io::Error> for DiskError {
    fn from(_err: io::Error) -> DiskError {
        DiskError::Io
    }
}

impl From<serde_json::Error> for DiskError {
    fn from(_err: serde_json::Error) -> DiskError {
        DiskError::JsonParse
    }
}

impl From<bytescoder::BytesCoderError> for DiskError {
    fn from(_err: bytescoder::BytesCoderError) -> DiskError {
        DiskError::BytesError
    }
}

impl fmt::Display for DiskError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DiskError::Io => write!(f, "No such file or directory."),
            DiskError::BaseDirExists => write!(f, "Base dir already exists and cannot be created again."),
            DiskError::BaseDirNotExists => write!(f, "Base data directory not exists. All data lost."),
            DiskError::UsernamesJsonNotExists => write!(f, "The file `usernames.json` is lost"),
            DiskError::UsernameExists => write!(f, "User name already exists and cannot be created again."),
            DiskError::UsernameNotExists => {
                write!(f, "Specified user name not exists. Please create this username first.")
            }
            DiskError::UsernameDirNotExists => write!(f, "Username exists but corresponding data folder is lost."),
            DiskError::DbsJsonNotExists => write!(f, "The `dbs.json` of the username is lost"),
            DiskError::DbExists => write!(f, "DB already exists and cannot be created again."),
            DiskError::DbNotExists => write!(f, "DB not exists. Please create DB first."),
            DiskError::DbDirNotExists => write!(f, "DB exists but correspoding data folder is lost."),
            DiskError::TablesJsonNotExists => write!(f, "The `tables.json` of the DB is lost."),
            DiskError::TableExists => write!(f, "Table already exists and cannot be created again."),
            DiskError::TableNotExists => write!(f, "Table not exists. Please create table first."),
            DiskError::TableBinNotExists => write!(f, "Table exists but correspoding bin file is lost."),
            DiskError::TableTsvNotExists => write!(f, "Table exists but correspoding tsv file is lost."),
            DiskError::TableIdxFileNotExists => write!(
                f,
                "Index file does not exist. Please build and save it before you can load from it."
            ),
            DiskError::JsonParse => write!(f, "JSON parsing error."),
            DiskError::RangeContainsDeletedRecord => write!(f, "The range of rows to fetch contains deleted records."),
            DiskError::RangeExceedLatestRecord => {
                write!(f, "The range of rows to fetch exceeds the latest record on the table.")
            }
            DiskError::RangeAndNumRowsMismatch => {
                write!(f, "The range of rows does not match number of rows to be modified.")
            }
            DiskError::AttrNotExists => write!(f, "The row does not contain specified attribute."),
            DiskError::BytesError => write!(f, "Error raised from BytesCoder."),
        }
    }
}

#[allow(dead_code)]
impl DiskInterface {
    pub fn create_file_base(file_base_path: Option<&str>) -> Result<(), DiskError> {
        Ok(File::create_file_base(file_base_path)?)
    }

    pub fn create_username(username: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        Ok(File::create_username(username, file_base_path)?)
    }

    pub fn get_usernames(file_base_path: Option<&str>) -> Result<Vec<String>, DiskError> {
        Ok(File::get_usernames(file_base_path)?)
    }

    pub fn remove_username(username: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        Ok(File::remove_username(username, file_base_path)?)
    }

    pub fn create_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        Ok(File::create_db(username, db_name, file_base_path)?)
    }

    pub fn get_dbs(username: &str, file_base_path: Option<&str>) -> Result<Vec<String>, DiskError> {
        Ok(File::get_dbs(username, file_base_path)?)
    }

    pub fn remove_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        Ok(File::remove_db(username, db_name, file_base_path)?)
    }

    pub fn create_table(
        username: &str,
        db_name: &str,
        table: &Table,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        Ok(File::create_table(username, db_name, table, file_base_path)?)
    }

    pub fn get_tables(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<Vec<String>, DiskError> {
        Ok(File::get_tables(username, db_name, file_base_path)?)
    }

    pub fn load_tables_meta(
        username: &str,
        db_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<Vec<TableMeta>, DiskError> {
        Ok(File::load_tables_meta(username, db_name, file_base_path)?)
    }

    pub fn load_table_meta(
        username: &str,
        db_name: &str,
        table_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<TableMeta, DiskError> {
        Ok(File::load_table_meta(username, db_name, table_name, file_base_path)?)
    }

    pub fn drop_table(
        username: &str,
        db_name: &str,
        table_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        Ok(File::drop_table(username, db_name, table_name, file_base_path)?)
    }

    pub fn append_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        rows: &Vec<Row>,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        Ok(File::append_rows(username, db_name, table_name, rows, file_base_path)?)
    }

    pub fn fetch_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        row_range: &Vec<u32>,
        file_base_path: Option<&str>,
    ) -> Result<Vec<Row>, DiskError> {
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
    ) -> Result<(), DiskError> {
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
    ) -> Result<(), DiskError> {
        Ok(File::modify_rows(
            username,
            db_name,
            table_name,
            row_range,
            new_rows,
            file_base_path,
        )?)
    }

    pub fn storage_hierarchy_check(
        base_path: &str,
        username: Option<&str>,
        db_name: Option<&str>,
        table_name: Option<&str>,
    ) -> Result<(), DiskError> {
        // check if base directory exists
        if !Path::new(base_path).exists() {
            return Err(DiskError::BaseDirNotExists);
        }

        // check if `usernames.json` exists
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        if !Path::new(&usernames_json_path).exists() {
            return Err(DiskError::UsernamesJsonNotExists);
        }

        // base level check passed
        if username == None {
            return Ok(());
        }

        // check if username exists
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;
        if !usernames_json
            .usernames
            .iter()
            .map(|username_info| username_info.name.clone())
            .collect::<Vec<String>>()
            .contains(&username.unwrap().to_string())
        {
            return Err(DiskError::UsernameNotExists);
        }

        // check if username directory exists
        let username_path = format!("{}/{}", base_path, username.unwrap());
        if !Path::new(&username_path).exists() {
            return Err(DiskError::UsernameDirNotExists);
        }

        // check if `dbs.json` exists
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        if !Path::new(&dbs_json_path).exists() {
            return Err(DiskError::DbsJsonNotExists);
        }

        // username level check passed
        if db_name == None {
            return Ok(());
        }

        // check if db exists
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;
        if !dbs_json
            .dbs
            .iter()
            .map(|db_info| db_info.name.clone())
            .collect::<Vec<String>>()
            .contains(&db_name.unwrap().to_string())
        {
            return Err(DiskError::DbNotExists);
        }

        // check if db directory exists
        let db_path = format!("{}/{}", username_path, db_name.unwrap());
        if !Path::new(&db_path).exists() {
            return Err(DiskError::DbDirNotExists);
        }

        // check if `tables.json` exists
        let tables_json_path = format!("{}/{}", db_path, "tables.json");
        if !Path::new(&tables_json_path).exists() {
            return Err(DiskError::TablesJsonNotExists);
        }

        // db level check passed
        if table_name == None {
            return Ok(());
        }

        // check if table exists
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;
        if !tables_json
            .tables
            .iter()
            .map(|table_meta| table_meta.name.clone())
            .collect::<Vec<String>>()
            .contains(&table_name.unwrap().to_string())
        {
            return Err(DiskError::TableNotExists);
        }

        // check if table bin exists
        let table_bin_path = format!("{}/{}.bin", db_path, table_name.unwrap());
        if !Path::new(&table_bin_path).exists() {
            return Err(DiskError::TableBinNotExists);
        }

        if dotenv!("ENABLE_TSV") == "true" {
            // check if table tsv exists
            let table_tsv_path = format!("{}/{}.tsv", db_path, table_name.unwrap());
            if !Path::new(&table_tsv_path).exists() {
                return Err(DiskError::TableTsvNotExists);
            }
        }

        Ok(())
    }

    pub fn get_datatype_size(datatype: &DataType) -> u32 {
        match datatype {
            DataType::Char(length) => length.clone() as u32,
            DataType::Double => 8,
            DataType::Float => 4,
            DataType::Int => 4,
            DataType::Varchar(length) => length.clone() as u32,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
// }
