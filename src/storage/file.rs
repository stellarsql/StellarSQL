use crate::component::datatype::DataType;
use crate::component::field::Field;
use crate::component::table::Row;
use crate::component::table::Table;
use crate::storage::bytescoder::BytesCoder;
use crate::storage::diskinterface::{
    DbInfo, DbsJson, DiskError, DiskInterface, TableMeta, TablesJson, UsernameInfo, UsernamesJson,
};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct File {
    /* definition */
// Ideally, File is a stateless struct
}

// TODO: add table-level folders to storage hierarchy
#[allow(dead_code)]
impl File {
    pub fn create_file_base(file_base_path: Option<&str>) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // create base data folder if not exists
        if !Path::new(base_path).exists() {
            fs::create_dir_all(base_path)?;
        } else {
            return Err(DiskError::BaseDirExists);
        }

        // create and save an initialized `usernames.json`
        let usernames_json = UsernamesJson { usernames: Vec::new() };
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        let mut usernames_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(usernames_json_path)?;
        usernames_file.write_all(serde_json::to_string_pretty(&usernames_json)?.as_bytes())?;

        Ok(())
    }

    pub fn create_username(username: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward base level
        DiskInterface::storage_hierarchy_check(base_path, None, None, None).map_err(|e| e)?;

        // load current usernames from `usernames.json`
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let mut usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;

        // check if the username exists
        for username_info in &usernames_json.usernames {
            if username_info.name == username {
                return Err(DiskError::UsernameExists);
            }
        }

        // create new username json instance
        let new_username_info = UsernameInfo {
            name: username.to_string(),
            path: username.to_string(),
        };

        // insert the new username record into `usernames.json`
        usernames_json.usernames.push(new_username_info);

        // save `usernames.json`
        let mut usernames_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(usernames_json_path)?;
        usernames_file.write_all(serde_json::to_string_pretty(&usernames_json)?.as_bytes())?;

        // create corresponding directory for the new username
        let username_path = format!("{}/{}", base_path, username);
        fs::create_dir_all(&username_path)?;

        // create corresponding `dbs.json` for the new username
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        let mut dbs_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dbs_json_path)?;
        let dbs_json = DbsJson { dbs: Vec::new() };
        dbs_file.write_all(serde_json::to_string_pretty(&dbs_json)?.as_bytes())?;

        Ok(())
    }

    pub fn get_usernames(file_base_path: Option<&str>) -> Result<Vec<String>, DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward base level
        DiskInterface::storage_hierarchy_check(base_path, None, None, None).map_err(|e| e)?;

        // read and parse `usernames.json`
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;

        // create a vector of usernames
        let usernames = usernames_json
            .usernames
            .iter()
            .map(|username_info| username_info.name.clone())
            .collect::<Vec<String>>();
        Ok(usernames)
    }

    pub fn remove_username(username: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward base level
        DiskInterface::storage_hierarchy_check(base_path, None, None, None).map_err(|e| e)?;

        // read and parse `usernames.json`
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let mut usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;

        // remove if the username exists; otherwise raise error
        let idx_to_remove = usernames_json
            .usernames
            .iter()
            .position(|username_info| &username_info.name == username);
        match idx_to_remove {
            Some(idx) => usernames_json.usernames.remove(idx),
            None => return Err(DiskError::UsernameNotExists),
        };

        // remove corresponding username directory
        let username_path = format!("{}/{}", base_path, username);
        if Path::new(&username_path).exists() {
            fs::remove_dir_all(&username_path)?;
        }

        // overwrite `usernames.json`
        let mut usernames_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(usernames_json_path)?;
        usernames_file.write_all(serde_json::to_string_pretty(&usernames_json)?.as_bytes())?;

        Ok(())
    }

    pub fn create_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward username level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), None, None).map_err(|e| e)?;

        // load current dbs from `dbs.json`
        let dbs_json_path = format!("{}/{}/{}", base_path, username, "dbs.json");
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let mut dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // check if the db exists
        for db_info in &dbs_json.dbs {
            if db_info.name == db_name {
                return Err(DiskError::DbExists);
            }
        }

        // create new db json instance
        let new_db_info = DbInfo {
            name: db_name.to_string(),
            path: db_name.to_string(),
        };

        // insert the new db record into `dbs.json`
        dbs_json.dbs.push(new_db_info);

        // save `dbs.json`
        let mut dbs_file = fs::OpenOptions::new().write(true).truncate(true).open(dbs_json_path)?;
        dbs_file.write_all(serde_json::to_string_pretty(&dbs_json)?.as_bytes())?;

        // create corresponding directory for the db
        let db_path = format!("{}/{}/{}", base_path, username, db_name);
        fs::create_dir_all(&db_path)?;

        // create corresponding `tables.json` for the new db
        let tables_json_path = format!("{}/{}", db_path, "tables.json");
        let mut tables_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(tables_json_path)?;
        let tables_json = TablesJson { tables: Vec::new() };
        tables_file.write_all(serde_json::to_string_pretty(&tables_json)?.as_bytes())?;

        Ok(())
    }

    pub fn get_dbs(username: &str, file_base_path: Option<&str>) -> Result<Vec<String>, DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward username level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), None, None).map_err(|e| e)?;

        // read and parse `dbs.json`
        let dbs_json_path = format!("{}/{}/{}", base_path, username, "dbs.json");
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // create a vector of dbs
        let dbs = dbs_json
            .dbs
            .iter()
            .map(|db_info| db_info.name.clone())
            .collect::<Vec<String>>();
        Ok(dbs)
    }

    pub fn remove_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward username level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), None, None).map_err(|e| e)?;

        // load current dbs from `dbs.json`
        let dbs_json_path = format!("{}/{}/{}", base_path, username, "dbs.json");
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let mut dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // remove if the db exists; otherwise raise error
        let idx_to_remove = dbs_json.dbs.iter().position(|db_info| &db_info.name == db_name);
        match idx_to_remove {
            Some(idx) => dbs_json.dbs.remove(idx),
            None => return Err(DiskError::DbNotExists),
        };

        // remove corresponding db directory
        let db_path = format!("{}/{}/{}", base_path, username, db_name);
        if Path::new(&db_path).exists() {
            fs::remove_dir_all(&db_path)?;
        }

        // overwrite `dbs.json`
        let mut dbs_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dbs_json_path)?;
        dbs_file.write_all(serde_json::to_string_pretty(&dbs_json)?.as_bytes())?;

        Ok(())
    }

    pub fn create_table(
        username: &str,
        db_name: &str,
        table: &Table,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward db level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), None).map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let mut tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // check if the table exists
        for table_meta in &tables_json.tables {
            if table_meta.name == table.name {
                return Err(DiskError::TableExists);
            }
        }

        // create new table json instance
        let mut new_table_meta = TableMeta {
            name: table.name.to_string(),
            username: username.to_string(),
            db_name: db_name.to_string(),
            path_tsv: format!("{}.tsv", table.name),
            path_bin: format!("{}.bin", table.name),
            primary_key: table.primary_key.clone(),
            foreign_key: table.foreign_key.clone(),
            reference_table: table.reference_table.clone(),
            reference_attr: table.reference_attr.clone(),
            row_length: 0,
            attrs_order: vec![],
            attrs: table.fields.clone(),
            attr_offset_ranges: vec![],
        };

        // determine storing order of attrs in .tsv and .bin
        // `__valid__` and primary key attrs are always at first
        new_table_meta.attrs_order = vec!["__valid__".to_string()];
        new_table_meta
            .attrs_order
            .extend_from_slice(&new_table_meta.primary_key);
        let mut other_attrs: Vec<String> = vec![];
        for (k, _v) in table.fields.iter() {
            if !new_table_meta.primary_key.contains(&k) {
                other_attrs.push(k.clone());
            }
        }
        other_attrs.sort();
        new_table_meta.attrs_order.extend_from_slice(&other_attrs);

        // determine the starting offset of each attribute in a row of bin file
        new_table_meta.attr_offset_ranges = vec![vec![0, 1]];
        let attr_sizes: Vec<u32> = new_table_meta.attrs_order[1..]
            .iter()
            .map(|attr_name| File::get_datatype_size(&new_table_meta.attrs[attr_name].datatype))
            .collect();

        // `__valid__` attr occupies 1 byte
        let mut curr_offset = 1;
        for attr_size in attr_sizes {
            new_table_meta
                .attr_offset_ranges
                .push(vec![curr_offset, curr_offset + attr_size]);
            curr_offset += attr_size;
        }
        new_table_meta.row_length = curr_offset;

        // create corresponding bin for the table, which is empty
        let table_bin_path = format!("{}/{}/{}/{}", base_path, username, db_name, new_table_meta.path_bin);
        let mut table_bin_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(table_bin_path)?;
        table_bin_file.write_all("".as_bytes())?;

        // perform equivalent operation on table tsv
        if dotenv!("ENABLE_TSV") == "true" {
            // create corresponding tsv for the table, with the title line
            let table_tsv_path = format!("{}/{}/{}/{}", base_path, username, db_name, new_table_meta.path_tsv);
            let mut table_tsv_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(table_tsv_path)?;
            table_tsv_file.write_all(new_table_meta.attrs_order.join("\t").as_bytes())?;
        }

        // insert the new table record into `tables.json`
        tables_json.tables.push(new_table_meta);

        // save `tables.json`
        let mut tables_file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(tables_json_path)?;
        tables_file.write_all(serde_json::to_string_pretty(&tables_json)?.as_bytes())?;

        Ok(())
    }

    /// get the list of tables in a database
    /// for `show tables`
    pub fn get_tables(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<Vec<String>, DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward db level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), None).map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;
        let tables = tables_json
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<String>>();

        // return the vector of table name
        Ok(tables)
    }

    /// load metadata of all tables from the database
    pub fn load_tables_meta(
        username: &str,
        db_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<Vec<TableMeta>, DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward db level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), None).map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // return the vector of table meta
        Ok(tables_json.tables)
    }

    /// load a particular table meta data
    pub fn load_table_meta(
        username: &str,
        db_name: &str,
        table_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<TableMeta, DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward db level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), None).map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // return the vector of table meta
        for table in tables_json.tables {
            if &table.name == table_name {
                return Ok(table);
            }
        }
        Err(DiskError::TableNotExists)
    }

    pub fn drop_table(
        username: &str,
        db_name: &str,
        table_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward table level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), Some(table_name))
            .map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let mut tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // remove if the table exists; otherwise raise error
        let idx_to_remove = tables_json
            .tables
            .iter()
            .position(|table_meta| &table_meta.name == table_name);
        match idx_to_remove {
            Some(idx) => tables_json.tables.remove(idx),
            None => return Err(DiskError::TableNotExists),
        };

        // remove corresponding bin file
        let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
        if Path::new(&table_bin_path).exists() {
            fs::remove_file(&table_bin_path)?;
        }

        // perform equivalent operation on table tsv
        if dotenv!("ENABLE_TSV") == "true" {
            // remove corresponding tsv file
            let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
            if Path::new(&table_tsv_path).exists() {
                fs::remove_file(&table_tsv_path)?;
            }
        }

        // overwrite `tables.json`
        let mut tables_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tables_json_path)?;
        tables_file.write_all(serde_json::to_string_pretty(&tables_json)?.as_bytes())?;

        Ok(())
    }

    pub fn append_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        rows: &Vec<Row>,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward table level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), Some(table_name))
            .map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // locate meta of target table
        let idx_target = tables_json
            .tables
            .iter()
            .position(|table_meta| &table_meta.name == table_name);

        let table_meta_target: &TableMeta = match idx_target {
            Some(idx) => &tables_json.tables[idx],
            None => return Err(DiskError::TableNotExists),
        };

        // create chunk of bytes to be inserted
        let mut chunk_bytes = vec![];
        for row in rows {
            chunk_bytes.extend_from_slice(&BytesCoder::row_to_bytes(&table_meta_target, row)?);
        }

        // append chunk of bytes to table bin
        let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
        let mut table_bin_file = fs::OpenOptions::new().append(true).open(table_bin_path)?;
        table_bin_file.write_all(&chunk_bytes)?;

        // perform equivalent operation on table tsv
        if dotenv!("ENABLE_TSV") == "true" {
            // create chunk of rows to be inserted
            let mut chunk = String::new();
            for row in rows {
                // set `__valid__` to 1
                let mut raw_row = vec!["1".to_string()];
                for attr in table_meta_target.attrs_order[1..].iter() {
                    raw_row.push(row.data.get(attr).ok_or_else(|| DiskError::AttrNotExists)?.clone());
                }
                chunk += &("\n".to_string() + &raw_row.join("\t"));
            }

            // append chunk to table tsv
            let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
            let mut table_tsv_file = fs::OpenOptions::new().append(true).open(table_tsv_path)?;
            table_tsv_file.write_all(chunk.as_bytes())?;
        }

        Ok(())
    }

    pub fn fetch_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        row_range: &Vec<u32>,
        file_base_path: Option<&str>,
    ) -> Result<Vec<Row>, DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward table level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), Some(table_name))
            .map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // locate meta of target table
        let idx_target = tables_json
            .tables
            .iter()
            .position(|table_meta| &table_meta.name == table_name);

        let table_meta_target: &TableMeta = match idx_target {
            Some(idx) => &tables_json.tables[idx],
            None => return Err(DiskError::TableNotExists),
        };

        // load corresponding chunk of bytes from table bin
        let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
        let table_bin_file = fs::File::open(&table_bin_path)?;
        let mut buffered = BufReader::new(table_bin_file);

        let mut chunk_bytes = vec![];
        buffered.seek(SeekFrom::Start((row_range[0] * table_meta_target.row_length) as u64))?;
        let mut raw = buffered.take(((row_range[1] - row_range[0]) * table_meta_target.row_length) as u64);
        raw.read_to_end(&mut chunk_bytes)?;

        if chunk_bytes.len() != ((row_range[1] - row_range[0]) * table_meta_target.row_length) as usize {
            return Err(DiskError::RangeExceedLatestRecord);
        }

        // parse chunk of bytes to vector of rows
        let mut rows: Vec<Row> = vec![];
        for row_bytes in chunk_bytes.chunks(table_meta_target.row_length as usize) {
            if row_bytes[0] == 0 as u8 {
                return Err(DiskError::RangeContainsDeletedRecord);
            }
            rows.push(BytesCoder::bytes_to_row(&table_meta_target, &row_bytes.to_vec())?);
        }

        // DEPRECATED: load rows from table tsv (functionally same with loading from bin)
        // let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
        // let table_tsv_file = fs::File::open(&table_tsv_path)?;
        // let buffered = BufReader::new(table_tsv_file);

        // let mut rows: Vec<Row> = vec![];
        // let mut curr_idx = row_range[0];
        // for line in buffered.lines().skip((row_range[0] + 1) as usize) {
        //     let raw_line: Vec<String> = line?.replace('\n', "").split('\t').map(|e| e.to_string()).collect();
        //     if raw_line[0] == "0".to_string() {
        //         return Err(DiskError::RangeContainsDeletedRecord);
        //     }
        //     let mut new_row = Row::new();
        //     for i in 1..raw_line.len() {
        //         new_row
        //             .data
        //             .insert(table_meta_target.attrs_order[i].clone(), raw_line[i].clone());
        //     }
        //     rows.push(new_row);
        //     curr_idx += 1;
        //     if curr_idx == row_range[1] {
        //         break;
        //     }
        // }
        // if rows.len() < ((row_range[1] - row_range[0]) as usize) {
        //     return Err(DiskError::RangeExceedLatestRecord);
        // }

        Ok(rows)
    }

    pub fn delete_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        row_range: &Vec<u32>,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward table level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), Some(table_name))
            .map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // locate meta of target table
        let idx_target = tables_json
            .tables
            .iter()
            .position(|table_meta| &table_meta.name == table_name);

        let table_meta_target: &TableMeta = match idx_target {
            Some(idx) => &tables_json.tables[idx],
            None => return Err(DiskError::TableNotExists),
        };

        // open table bin for read
        let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
        let table_bin_file = fs::OpenOptions::new().read(true).open(table_bin_path)?;
        let mut buffered = BufReader::new(table_bin_file);

        // check if row range contains deleted record
        for row_id in row_range[0]..row_range[1] {
            buffered.seek(SeekFrom::Start((row_id * table_meta_target.row_length) as u64))?;
            let mut valid_byte = [0; 1];
            match buffered.read_exact(&mut valid_byte) {
                Ok(_) => (),
                Err(_) => return Err(DiskError::RangeExceedLatestRecord),
            };
            if valid_byte[0] == 0 as u8 {
                return Err(DiskError::RangeContainsDeletedRecord);
            }
        }

        // open table bin for write
        let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
        let table_bin_file = fs::OpenOptions::new().write(true).open(table_bin_path)?;
        let mut buffered = BufWriter::new(table_bin_file);

        // locate the `__valid__` byte for each row and overwrite them to 0
        for row_id in row_range[0]..row_range[1] {
            buffered.seek(SeekFrom::Start((row_id * table_meta_target.row_length) as u64))?;
            let mut del_valid_byte = [0; 1];
            buffered.write_all(&mut del_valid_byte)?;
        }

        // perform equivalent operation on table tsv
        if dotenv!("ENABLE_TSV") == "true" {
            // open table tsv
            let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
            let table_tsv_file = fs::File::open(&table_tsv_path)?;
            let buffered = BufReader::new(table_tsv_file);

            let mut new_content: Vec<String> = vec![];

            // overwrite the `__valid__` attr of rows to delete
            let mut rows_deleted = 0;
            for (idx, line) in buffered.lines().enumerate() {
                if idx < (row_range[0] + 1) as usize || idx > row_range[1] as usize {
                    new_content.push(line?);
                } else {
                    let l = line?;
                    if (&l).starts_with("0") {
                        return Err(DiskError::RangeContainsDeletedRecord);
                    }
                    new_content.push(format!("0{}", &l[1..]));
                    rows_deleted += 1;
                }
            }
            if rows_deleted < row_range[1] - row_range[0] {
                return Err(DiskError::RangeExceedLatestRecord);
            }

            // overwrite table tsv file
            let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
            let mut table_tsv_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(table_tsv_path)?;
            table_tsv_file.write_all(new_content.join("\n").as_bytes())?;
        }

        Ok(())
    }

    pub fn modify_rows(
        username: &str,
        db_name: &str,
        table_name: &str,
        row_range: &Vec<u32>,
        new_rows: &Vec<Row>,
        file_base_path: Option<&str>,
    ) -> Result<(), DiskError> {
        if row_range[1] - row_range[0] != new_rows.len() as u32 {
            return Err(DiskError::RangeAndNumRowsMismatch);
        }

        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward table level
        DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), Some(table_name))
            .map_err(|e| e)?;

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // locate meta of target table
        let idx_target = tables_json
            .tables
            .iter()
            .position(|table_meta| &table_meta.name == table_name);

        let table_meta_target: &TableMeta = match idx_target {
            Some(idx) => &tables_json.tables[idx],
            None => return Err(DiskError::TableNotExists),
        };

        // open table bin for read
        let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
        let table_bin_file = fs::OpenOptions::new().read(true).open(table_bin_path)?;
        let mut buffered = BufReader::new(table_bin_file);

        // check if row range contains deleted record
        for row_id in row_range[0]..row_range[1] {
            buffered.seek(SeekFrom::Start((row_id * table_meta_target.row_length) as u64))?;
            let mut valid_byte = [0; 1];
            match buffered.read_exact(&mut valid_byte) {
                Ok(_) => (),
                Err(_) => return Err(DiskError::RangeExceedLatestRecord),
            };
            if valid_byte[0] == 0 as u8 {
                return Err(DiskError::RangeContainsDeletedRecord);
            }
        }

        // open table bin for write
        let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
        let table_bin_file = fs::OpenOptions::new().write(true).open(table_bin_path)?;
        let mut buffered = BufWriter::new(table_bin_file);

        // create modified chunk of bytes
        let mut modified_chunk_bytes: Vec<u8> = vec![];
        for row in new_rows {
            modified_chunk_bytes.extend_from_slice(&BytesCoder::row_to_bytes(&table_meta_target, row)?);
        }

        // overwrite modified chunk of bytes
        buffered.seek(SeekFrom::Start((row_range[0] * table_meta_target.row_length) as u64))?;
        buffered.write_all(&mut modified_chunk_bytes)?;

        // perform equivalent operation on table tsv
        if dotenv!("ENABLE_TSV") == "true" {
            // create modified rows
            let mut modified_content: Vec<String> = vec![];
            for row in new_rows {
                // set `__valid__` to 1
                let mut raw_row = vec!["1".to_string()];
                for attr in table_meta_target.attrs_order[1..].iter() {
                    raw_row.push(row.data.get(attr).ok_or_else(|| DiskError::AttrNotExists)?.clone());
                }
                modified_content.push(raw_row.join("\t"));
            }

            // open table tsv
            let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
            let table_tsv_file = fs::File::open(&table_tsv_path)?;
            let buffered = BufReader::new(table_tsv_file);

            // overwrite the rows to be modified
            let mut new_content: Vec<String> = vec![];
            let mut rows_modified = 0;
            for (idx, line) in buffered.lines().enumerate() {
                if idx < (row_range[0] + 1) as usize || idx > row_range[1] as usize {
                    new_content.push(line?);
                } else {
                    let l = line?;
                    if (&l).starts_with("0") {
                        return Err(DiskError::RangeContainsDeletedRecord);
                    }
                    new_content.push(modified_content[idx - (row_range[0] + 1) as usize].clone());
                    rows_modified += 1;
                }
            }
            if rows_modified < row_range[1] - row_range[0] {
                return Err(DiskError::RangeExceedLatestRecord);
            }

            // overwrite table tsv file
            let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
            let mut table_tsv_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(table_tsv_path)?;
            table_tsv_file.write_all(new_content.join("\n").as_bytes())?;
        }

        Ok(())
    }

    fn get_datatype_size(datatype: &DataType) -> u32 {
        match datatype {
            DataType::Char(length) => length.clone() as u32,
            DataType::Double => 8,
            DataType::Float => 4,
            DataType::Int => 4,
            DataType::Varchar(length) => length.clone() as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::datatype::DataType;
    use crate::component::field;
    #[test]
    pub fn test_create_file_base() {
        let file_base_path = "data0";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();

        assert!(Path::new(file_base_path).exists());

        let usernames_json_path = format!("{}/{}", file_base_path, "usernames.json");
        assert!(Path::new(&usernames_json_path).exists());

        assert_eq!(
            File::create_file_base(Some(file_base_path)).unwrap_err(),
            DiskError::BaseDirExists
        );
    }

    #[test]
    pub fn test_create_username() {
        let file_base_path = "data1";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();

        let usernames_json_path = format!("{}/{}", file_base_path, "usernames.json");
        let usernames_json = fs::read_to_string(usernames_json_path).unwrap();
        let usernames_json: UsernamesJson = serde_json::from_str(&usernames_json).unwrap();

        let ideal_usernames_json = UsernamesJson {
            usernames: vec![
                UsernameInfo {
                    name: "crazyguy".to_string(),
                    path: "crazyguy".to_string(),
                },
                UsernameInfo {
                    name: "happyguy".to_string(),
                    path: "happyguy".to_string(),
                },
            ],
        };

        assert_eq!(usernames_json.usernames[0].name, ideal_usernames_json.usernames[0].name);
        assert_eq!(usernames_json.usernames[1].name, ideal_usernames_json.usernames[1].name);
        assert_eq!(usernames_json.usernames[0].path, ideal_usernames_json.usernames[0].path);
        assert_eq!(usernames_json.usernames[1].path, ideal_usernames_json.usernames[1].path);

        assert!(Path::new(&format!("{}/{}", file_base_path, "crazyguy")).exists());
        assert!(Path::new(&format!("{}/{}", file_base_path, "happyguy")).exists());

        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "dbs.json")).exists());
        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "happyguy", "dbs.json")).exists());

        let dbs_json = fs::read_to_string(format!("{}/{}/{}", file_base_path, "crazyguy", "dbs.json")).unwrap();
        let dbs_json: DbsJson = serde_json::from_str(&dbs_json).unwrap();

        assert_eq!(dbs_json.dbs.len(), 0);

        assert_eq!(
            File::create_username("happyguy", Some(file_base_path)).unwrap_err(),
            DiskError::UsernameExists
        );
    }

    #[test]
    pub fn test_get_usernames() {
        let file_base_path = "data2";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "happyguy"]);
    }

    #[test]
    pub fn test_remove_username() {
        let file_base_path = "data3";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();
        File::create_username("sadguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "happyguy", "sadguy"]);

        File::remove_username("happyguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "sadguy"]);

        assert_eq!(
            File::remove_username("happyguy", Some(file_base_path)).unwrap_err(),
            DiskError::UsernameNotExists
        );

        File::remove_username("sadguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy"]);

        File::remove_username("crazyguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames.len(), 0);
    }

    #[test]
    pub fn test_create_db() {
        let file_base_path = "data4";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs_json_path = format!("{}/{}/{}", file_base_path, "crazyguy", "dbs.json");
        assert!(Path::new(&dbs_json_path).exists());

        let dbs_json = fs::read_to_string(dbs_json_path).unwrap();
        let dbs_json: DbsJson = serde_json::from_str(&dbs_json).unwrap();

        let ideal_dbs_json = DbsJson {
            dbs: vec![
                DbInfo {
                    name: "BookerDB".to_string(),
                    path: "BookerDB".to_string(),
                },
                DbInfo {
                    name: "MovieDB".to_string(),
                    path: "MovieDB".to_string(),
                },
            ],
        };

        assert_eq!(dbs_json.dbs[0].name, ideal_dbs_json.dbs[0].name);
        assert_eq!(dbs_json.dbs[1].name, ideal_dbs_json.dbs[1].name);
        assert_eq!(dbs_json.dbs[0].path, ideal_dbs_json.dbs[0].path);
        assert_eq!(dbs_json.dbs[1].path, ideal_dbs_json.dbs[1].path);

        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "BookerDB")).exists());
        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "MovieDB")).exists());

        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "tables.json"
        ))
        .exists());
        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "MovieDB", "tables.json"
        ))
        .exists());

        let tables_json = fs::read_to_string(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "tables.json"
        ))
        .unwrap();
        let tables_json: TablesJson = serde_json::from_str(&tables_json).unwrap();

        assert_eq!(tables_json.tables.len(), 0);

        assert_eq!(
            File::create_db("happyguy", "BookerDB", Some(file_base_path)).unwrap_err(),
            DiskError::UsernameNotExists
        );
        assert_eq!(
            File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap_err(),
            DiskError::DbExists
        );
    }

    #[test]
    pub fn test_get_dbs() {
        let file_base_path = "data5";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs.len(), 0);

        File::create_db("happyguy", "BookerDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB"]);

        File::create_db("happyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "MovieDB"]);

        assert_eq!(
            File::get_dbs("sadguy", Some(file_base_path)).unwrap_err(),
            DiskError::UsernameNotExists
        );
    }

    #[test]
    pub fn test_remove_db() {
        let file_base_path = "data6";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "PhotoDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "MovieDB", "PhotoDB"]);

        File::remove_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "PhotoDB"]);

        assert_eq!(
            File::remove_db("happyguy", "BookerDB", Some(file_base_path)).unwrap_err(),
            DiskError::UsernameNotExists
        );

        File::remove_db("crazyguy", "PhotoDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB"]);

        assert_eq!(
            File::remove_db("crazyguy", "PhotoDB", Some(file_base_path)).unwrap_err(),
            DiskError::DbNotExists
        );

        File::remove_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs.len(), 0);

        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "BookerDB")).exists());
        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "MovieDB")).exists());
        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "PhotoDB")).exists());
    }

    #[test]
    pub fn test_create_get_load_drop_table() {
        let file_base_path = "data7";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        let mut aff_table = Table::new("Affiliates");
        aff_table.fields.insert(
            "AffID".to_string(),
            Field::new_all("AffID", DataType::Int, true, None, field::Checker::None, false),
        );
        aff_table.fields.insert(
            "AffName".to_string(),
            Field::new_all(
                "AffName",
                DataType::Varchar(40),
                true,
                None,
                field::Checker::None,
                false,
            ),
        );
        aff_table.fields.insert(
            "AffEmail".to_string(),
            Field::new_all(
                "AffEmail",
                DataType::Varchar(50),
                true,
                None,
                field::Checker::None,
                false,
            ),
        );
        aff_table.fields.insert(
            "AffPhoneNum".to_string(),
            Field::new_all(
                "AffPhoneNum",
                DataType::Varchar(20),
                false,
                Some("+886900000000".to_string()),
                field::Checker::None,
                false,
            ),
        );
        aff_table.primary_key.push("AffID".to_string());

        File::create_table("crazyguy", "BookerDB", &aff_table, Some(file_base_path)).unwrap();

        let mut htl_table = Table::new("Hotels");
        htl_table.fields.insert(
            "HotelID".to_string(),
            Field::new_all("HotelID", DataType::Int, true, None, field::Checker::None, false),
        );
        htl_table.fields.insert(
            "HotelName".to_string(),
            Field::new_all(
                "HotelName",
                DataType::Varchar(40),
                true,
                None,
                field::Checker::None,
                false,
            ),
        );
        htl_table.fields.insert(
            "HotelType".to_string(),
            Field::new_all(
                "HotelType",
                DataType::Varchar(20),
                false,
                Some("Homestay".to_string()),
                field::Checker::None,
                false,
            ),
        );
        htl_table.fields.insert(
            "HotelAddr".to_string(),
            Field::new_all(
                "HotelAddr",
                DataType::Varchar(50),
                false,
                Some("".to_string()),
                field::Checker::None,
                false,
            ),
        );
        htl_table.primary_key.push("HotelID".to_string());

        File::create_table("crazyguy", "BookerDB", &htl_table, Some(file_base_path)).unwrap();

        let ideal_tables = vec![
            TableMeta {
                name: "Affiliates".to_string(),
                username: "crazyguy".to_string(),
                db_name: "BookerDB".to_string(),
                path_tsv: "Affiliates.tsv".to_string(),
                path_bin: "Affiliates.bin".to_string(),
                primary_key: vec!["AffID".to_string()],
                foreign_key: vec![],
                reference_table: None,
                reference_attr: None,
                attr_offset_ranges: vec![vec![0, 1], vec![1, 5], vec![5, 55], vec![55, 95], vec![95, 115]],
                row_length: 115,
                // ignore attrs checking
                attrs_order: vec![],
                attrs: HashMap::new(),
            },
            TableMeta {
                name: "Hotels".to_string(),
                username: "crazyguy".to_string(),
                db_name: "BookerDB".to_string(),
                path_tsv: "Hotels.tsv".to_string(),
                path_bin: "Hotels.bin".to_string(),
                primary_key: vec!["HotelID".to_string()],
                foreign_key: vec![],
                reference_table: None,
                reference_attr: None,
                attr_offset_ranges: vec![vec![0, 1], vec![1, 5], vec![5, 55], vec![55, 95], vec![95, 115]],
                row_length: 115,
                // ignore attrs checking
                attrs_order: vec![],
                attrs: HashMap::new(),
            },
        ];

        let table_names = File::get_tables("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        for i in 0..ideal_tables.len() {
            assert_eq!(table_names[i], ideal_tables[i].name);
        }

        // load_table_meta
        assert!(File::load_table_meta("crazyguy", "BookerDB", "Affiliates", Some(file_base_path)).is_ok());
        assert!(File::load_table_meta("crazyguy", "BookerDB", "none_table", Some(file_base_path)).is_err());

        let tables = File::load_tables_meta("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        assert_eq!(tables.len(), 2);

        for i in 0..tables.len() {
            assert_eq!(tables[i].name, ideal_tables[i].name);
            assert_eq!(tables[i].username, ideal_tables[i].username);
            assert_eq!(tables[i].db_name, ideal_tables[i].db_name);
            assert_eq!(tables[i].path_tsv, ideal_tables[i].path_tsv);
            assert_eq!(tables[i].path_bin, ideal_tables[i].path_bin);
            assert_eq!(tables[i].primary_key, ideal_tables[i].primary_key);
            assert_eq!(tables[i].foreign_key, ideal_tables[i].foreign_key);
            assert_eq!(tables[i].reference_table, ideal_tables[i].reference_table);
            assert_eq!(tables[i].reference_attr, ideal_tables[i].reference_attr);
            assert_eq!(tables[i].row_length, ideal_tables[i].row_length);
            assert_eq!(tables[i].attr_offset_ranges, ideal_tables[i].attr_offset_ranges);
        }

        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Affiliates.bin"
        ))
        .exists());
        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Hotels.bin"
        ))
        .exists());

        if dotenv!("ENABLE_TSV") == "true" {
            assert!(Path::new(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
            ))
            .exists());
            assert!(Path::new(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Hotels.tsv"
            ))
            .exists());

            let aff_tsv_content: Vec<String> = fs::read_to_string(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
            ))
            .unwrap()
            .split('\t')
            .map(|s| s.to_string())
            .collect();

            assert_eq!(aff_tsv_content[0], "__valid__".to_string());
            assert_eq!(aff_tsv_content[1], "AffID".to_string());
            assert_eq!(aff_tsv_content.len(), 5);
        }

        assert_eq!(
            File::create_table("happyguy", "BookerDB", &htl_table, Some(file_base_path)).unwrap_err(),
            DiskError::UsernameNotExists
        );
        assert_eq!(
            File::create_table("crazyguy", "MusicDB", &htl_table, Some(file_base_path)).unwrap_err(),
            DiskError::DbNotExists
        );
        assert_eq!(
            File::create_table("crazyguy", "BookerDB", &htl_table, Some(file_base_path)).unwrap_err(),
            DiskError::TableExists
        );

        File::drop_table("crazyguy", "BookerDB", "Affiliates", Some(file_base_path)).unwrap();

        assert!(!Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Affiliates.bin"
        ))
        .exists());

        if dotenv!("ENABLE_TSV") == "true" {
            assert!(!Path::new(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
            ))
            .exists());
        }

        let tables = File::load_tables_meta("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        assert_eq!(tables.len(), 1);

        assert_eq!(
            File::drop_table("crazyguy", "BookerDB", "Affiliates", Some(file_base_path)).unwrap_err(),
            DiskError::TableNotExists
        );

        File::drop_table("crazyguy", "BookerDB", "Hotels", Some(file_base_path)).unwrap();

        assert!(!Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Hotels.bin"
        ))
        .exists());

        if dotenv!("ENABLE_TSV") == "true" {
            assert!(!Path::new(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Hotels.tsv"
            ))
            .exists());
        }

        let tables = File::load_tables_meta("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        assert_eq!(tables.len(), 0);
    }

    #[test]
    pub fn test_append_fetch_delete_modify_rows() {
        let file_base_path = "data8";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_file_base(Some(file_base_path)).unwrap();
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        let mut aff_table = Table::new("Affiliates");
        aff_table.fields.insert(
            "AffID".to_string(),
            Field::new_all("AffID", DataType::Int, true, None, field::Checker::None, false),
        );
        aff_table.fields.insert(
            "AffName".to_string(),
            Field::new_all(
                "AffName",
                DataType::Varchar(40),
                true,
                None,
                field::Checker::None,
                false,
            ),
        );
        aff_table.fields.insert(
            "AffEmail".to_string(),
            Field::new_all(
                "AffEmail",
                DataType::Varchar(50),
                true,
                None,
                field::Checker::None,
                false,
            ),
        );
        aff_table.fields.insert(
            "AffPhoneNum".to_string(),
            Field::new_all(
                "AffPhoneNum",
                DataType::Varchar(20),
                false,
                Some("+886900000000".to_string()),
                field::Checker::None,
                false,
            ),
        );
        aff_table.primary_key.push("AffID".to_string());

        File::create_table("crazyguy", "BookerDB", &aff_table, Some(file_base_path)).unwrap();

        let data = vec![
            ("AffID", "1"),
            ("AffName", "Tom"),
            ("AffEmail", "tom@foo.com"),
            ("AffPhoneNum", "+886900000001"),
        ];
        aff_table.insert_row(data).unwrap();

        File::append_rows(
            "crazyguy",
            "BookerDB",
            "Affiliates",
            &aff_table.rows,
            Some(file_base_path),
        )
        .unwrap();

        if dotenv!("ENABLE_TSV") == "true" {
            let aff_tsv_content: Vec<String> = fs::read_to_string(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
            ))
            .unwrap()
            .split('\n')
            .map(|s| s.to_string())
            .collect();

            assert_eq!(aff_tsv_content.len(), 2);
            assert_eq!(aff_tsv_content[1], "1\t1\ttom@foo.com\tTom\t+886900000001".to_string());
        }

        let data = vec![
            ("AffID", "2"),
            ("AffName", "Ben"),
            ("AffEmail", "ben@foo.com"),
            ("AffPhoneNum", "+886900000002"),
        ];
        aff_table.insert_row(data).unwrap();
        let data = vec![
            ("AffID", "3"),
            ("AffName", "Leo"),
            ("AffEmail", "leo@dee.com"),
            ("AffPhoneNum", "+886900000003"),
        ];
        aff_table.insert_row(data).unwrap();

        File::append_rows(
            "crazyguy",
            "BookerDB",
            "Affiliates",
            &aff_table.rows[1..].iter().cloned().collect(),
            Some(file_base_path),
        )
        .unwrap();

        if dotenv!("ENABLE_TSV") == "true" {
            let aff_tsv_content: Vec<String> = fs::read_to_string(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
            ))
            .unwrap()
            .split('\n')
            .map(|s| s.to_string())
            .collect();

            assert_eq!(aff_tsv_content.len(), 4);
            assert_eq!(aff_tsv_content[1], "1\t1\ttom@foo.com\tTom\t+886900000001".to_string());
            assert_eq!(aff_tsv_content[2], "1\t2\tben@foo.com\tBen\t+886900000002".to_string());
            assert_eq!(aff_tsv_content[3], "1\t3\tleo@dee.com\tLeo\t+886900000003".to_string());
        }

        let rows: Vec<Row> =
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![0, 1], Some(file_base_path)).unwrap();

        assert_eq!(rows.len(), 1);

        for (attr, val) in aff_table.rows[0].data.iter() {
            assert_eq!(val.clone(), rows[0].data[attr]);
        }

        let rows: Vec<Row> =
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![0, 3], Some(file_base_path)).unwrap();

        assert_eq!(rows.len(), 3);

        for i in 0..3 {
            for (attr, val) in aff_table.rows[i].data.iter() {
                assert_eq!(val.clone(), rows[i].data[attr]);
            }
        }

        let rows: Vec<Row> =
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![1, 3], Some(file_base_path)).unwrap();

        assert_eq!(rows.len(), 2);

        for i in 0..2 {
            for (attr, val) in aff_table.rows[i + 1].data.iter() {
                assert_eq!(val.clone(), rows[i].data[attr]);
            }
        }

        assert_eq!(
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![2, 4], Some(file_base_path)).unwrap_err(),
            DiskError::RangeExceedLatestRecord
        );

        File::delete_rows("crazyguy", "BookerDB", "Affiliates", &vec![1, 2], Some(file_base_path)).unwrap();

        assert_eq!(
            File::delete_rows("crazyguy", "BookerDB", "Affiliates", &vec![1, 2], Some(file_base_path)).unwrap_err(),
            DiskError::RangeContainsDeletedRecord,
        );

        assert_eq!(
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![0, 2], Some(file_base_path)).unwrap_err(),
            DiskError::RangeContainsDeletedRecord,
        );

        File::delete_rows("crazyguy", "BookerDB", "Affiliates", &vec![0, 1], Some(file_base_path)).unwrap();

        if dotenv!("ENABLE_TSV") == "true" {
            let aff_tsv_content: Vec<String> = fs::read_to_string(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
            ))
            .unwrap()
            .split('\n')
            .map(|s| s.to_string())
            .collect();

            assert_eq!(aff_tsv_content.len(), 4);
            assert_eq!(aff_tsv_content[1], "0\t1\ttom@foo.com\tTom\t+886900000001".to_string());
            assert_eq!(aff_tsv_content[2], "0\t2\tben@foo.com\tBen\t+886900000002".to_string());
            assert_eq!(aff_tsv_content[3], "1\t3\tleo@dee.com\tLeo\t+886900000003".to_string());
        }

        let data = vec![
            ("AffID", "4"),
            ("AffName", "John"),
            ("AffEmail", "john@dee.com"),
            ("AffPhoneNum", "+886900000004"),
        ];
        aff_table.insert_row(data).unwrap();

        let data = vec![
            ("AffID", "5"),
            ("AffName", "Ray"),
            ("AffEmail", "ray@dee.com"),
            ("AffPhoneNum", "+886900000005"),
        ];
        aff_table.insert_row(data).unwrap();

        let data = vec![
            ("AffID", "6"),
            ("AffName", "Bryn"),
            ("AffEmail", "bryn@dee.com"),
            ("AffPhoneNum", "+886900000006"),
        ];
        aff_table.insert_row(data).unwrap();

        File::append_rows(
            "crazyguy",
            "BookerDB",
            "Affiliates",
            &aff_table.rows[3..].iter().cloned().collect(),
            Some(file_base_path),
        )
        .unwrap();

        if dotenv!("ENABLE_TSV") == "true" {
            let aff_tsv_content: Vec<String> = fs::read_to_string(&format!(
                "{}/{}/{}/{}",
                file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
            ))
            .unwrap()
            .split('\n')
            .map(|s| s.to_string())
            .collect();

            assert_eq!(aff_tsv_content.len(), 7);
        }

        assert_eq!(
            File::delete_rows("crazyguy", "BookerDB", "Affiliates", &vec![5, 7], Some(file_base_path)).unwrap_err(),
            DiskError::RangeExceedLatestRecord
        );

        File::delete_rows("crazyguy", "BookerDB", "Affiliates", &vec![5, 6], Some(file_base_path)).unwrap();

        assert_eq!(
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![5, 6], Some(file_base_path)).unwrap_err(),
            DiskError::RangeContainsDeletedRecord,
        );

        let rows: Vec<Row> =
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![2, 5], Some(file_base_path)).unwrap();

        assert_eq!(rows.len(), 3);

        for i in 0..3 {
            for (attr, val) in aff_table.rows[i + 2].data.iter() {
                assert_eq!(val.clone(), rows[i].data[attr]);
            }
        }

        *aff_table.rows[2].data.get_mut("AffName").unwrap() = "Leow".to_string();
        *aff_table.rows[4].data.get_mut("AffEmail").unwrap() = "raymond@dee.com".to_string();
        *aff_table.rows[4].data.get_mut("AffPhoneNum").unwrap() = "+886900000015".to_string();
        File::modify_rows(
            "crazyguy",
            "BookerDB",
            "Affiliates",
            &vec![2, 5],
            &aff_table.rows[2..5].iter().cloned().collect(),
            Some(file_base_path),
        )
        .unwrap();

        let rows: Vec<Row> =
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![2, 5], Some(file_base_path)).unwrap();

        assert_eq!(rows.len(), 3);

        for i in 0..3 {
            for (attr, val) in aff_table.rows[i + 2].data.iter() {
                assert_eq!(val.clone(), rows[i].data[attr]);
            }
        }

        assert_eq!(
            File::modify_rows(
                "crazyguy",
                "BookerDB",
                "Affiliates",
                &vec![1, 4],
                &aff_table.rows[1..4].iter().cloned().collect(),
                Some(file_base_path)
            )
            .unwrap_err(),
            DiskError::RangeContainsDeletedRecord
        );

        let data = vec![
            ("AffID", "7"),
            ("AffName", "Eric"),
            ("AffEmail", "eric@doo.com"),
            ("AffPhoneNum", "+886900000007"),
        ];
        aff_table.insert_row(data).unwrap();

        let data = vec![
            ("AffID", "8"),
            ("AffName", "Vinc"),
            ("AffEmail", "vinc@doo.com"),
            ("AffPhoneNum", "+886900000008"),
        ];
        aff_table.insert_row(data).unwrap();

        File::append_rows(
            "crazyguy",
            "BookerDB",
            "Affiliates",
            &aff_table.rows[6..].iter().cloned().collect(),
            Some(file_base_path),
        )
        .unwrap();

        assert_eq!(
            File::modify_rows(
                "crazyguy",
                "BookerDB",
                "Affiliates",
                &vec![6, 10],
                &aff_table.rows[1..4].iter().cloned().collect(),
                Some(file_base_path)
            )
            .unwrap_err(),
            DiskError::RangeAndNumRowsMismatch
        );

        assert_eq!(
            File::modify_rows(
                "crazyguy",
                "BookerDB",
                "Affiliates",
                &vec![6, 9],
                &aff_table.rows[1..4].iter().cloned().collect(),
                Some(file_base_path)
            )
            .unwrap_err(),
            DiskError::RangeExceedLatestRecord
        );

        let rows: Vec<Row> =
            File::fetch_rows("crazyguy", "BookerDB", "Affiliates", &vec![6, 8], Some(file_base_path)).unwrap();

        assert_eq!(rows.len(), 2);

        for i in 0..2 {
            for (attr, val) in aff_table.rows[i + 6].data.iter() {
                assert_eq!(val.clone(), rows[i].data[attr]);
            }
        }
    }
}
