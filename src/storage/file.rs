extern crate serde_json;
use crate::component::field::Field;
use std::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct File {
    /* definition */
// Ideally, File is a stateless struct
}

#[derive(Debug)]
pub enum FileError {
    Io,
    UsernameExists,
    UsernameNotExists,
    UsernameDirNotExists,
    DbsJsonNotExists,
    DbExists,
    DbNotExists,
    JsonParse,
}

#[derive(Debug, Serialize, Deserialize)]
struct UsernamesJson {
    usernames: Vec<UsernameInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UsernameInfo {
    name: String,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DbsJson {
    dbs: Vec<DbInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DbInfo {
    name: String,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TablesJson {
    tables: Vec<TableInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableInfo {
    name: String,
    path_tsv: String,
    path_bin: String,
    primary_key: Vec<String>,
    foreign_key: Vec<String>,
    reference_table: Option<String>,
    reference_attr: Option<String>,
    attrs: Vec<Field>,
}

impl From<io::Error> for FileError {
    fn from(_err: io::Error) -> FileError {
        FileError::Io
    }
}

impl From<serde_json::Error> for FileError {
    fn from(_err: serde_json::Error) -> FileError {
        FileError::JsonParse
    }
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FileError::Io => write!(f, "No such file or directory."),
            FileError::UsernameExists => write!(f, "User name already exists and cannot be created again."),
            FileError::UsernameNotExists => {
                write!(f, "Specified user name not exists. Please create this username first.")
            }
            FileError::UsernameDirNotExists => write!(f, "Username exists but corresponding data folder losed."),
            FileError::DbsJsonNotExists => write!(f, "The `dbs.json` of the username is losed"),
            FileError::DbExists => write!(f, "DB already exists and cannot be created again."),
            FileError::DbNotExists => write!(f, "DB not exists. Please create DB first."),
            FileError::JsonParse => write!(f, "JSON parsing error."),
        }
    }
}

impl File {
    pub fn create_username(username: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // check if the base data path exists
        if !Path::new(base_path).exists() {
            fs::create_dir_all(base_path)?;
        }

        // load current usernames from `usernames.json`
        let mut usernames_json: UsernamesJson;
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        if Path::new(&usernames_json_path).exists() {
            let usernames_file = fs::File::open(&usernames_json_path)?;
            usernames_json = serde_json::from_reader(usernames_file)?;
        } else {
            usernames_json = UsernamesJson { usernames: Vec::new() };
        }

        // check if the username exists
        for username_info in &usernames_json.usernames {
            if username_info.name == username {
                return Err(FileError::UsernameExists);
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

    pub fn get_usernames(file_base_path: Option<&str>) -> Result<Vec<String>, FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // read and parse `usernames.json`
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;

        // create a vector of usernames
        let mut usernames = Vec::new();
        for username_info in &usernames_json.usernames {
            usernames.push(username_info.name.clone());
        }
        Ok(usernames)
    }

    pub fn remove_username(username: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

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
            None => return Err(FileError::UsernameNotExists),
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

    pub fn create_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // check if username exists
        let usernames = File::get_usernames(file_base_path)?;
        if !usernames.contains(&username.to_string()) {
            return Err(FileError::UsernameNotExists);
        }

        // check if username directory exists
        let username_path = format!("{}/{}", base_path, username);
        if !Path::new(&username_path).exists() {
            return Err(FileError::UsernameDirNotExists);
        }

        // check if `dbs.json` exists
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        if !Path::new(&dbs_json_path).exists() {
            return Err(FileError::DbsJsonNotExists);
        }

        // load current dbs from `dbs.json`
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let mut dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // check if the db exists
        for db_info in &dbs_json.dbs {
            if db_info.name == db_name {
                return Err(FileError::DbExists);
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
        let db_path = format!("{}/{}", username_path, db_name);
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

    pub fn get_dbs(username: &str, file_base_path: Option<&str>) -> Result<Vec<String>, FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // check if username exists
        let usernames = File::get_usernames(file_base_path)?;
        if !usernames.contains(&username.to_string()) {
            return Err(FileError::UsernameNotExists);
        }

        // check if username directory exists
        let username_path = format!("{}/{}", base_path, username);
        if !Path::new(&username_path).exists() {
            return Err(FileError::UsernameDirNotExists);
        }

        // check if `dbs.json` exists
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        if !Path::new(&dbs_json_path).exists() {
            return Err(FileError::DbsJsonNotExists);
        }

        // read and parse `dbs.json`
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // create a vector of dbs
        let mut dbs = Vec::new();
        for dbs_info in &dbs_json.dbs {
            dbs.push(dbs_info.name.clone());
        }
        Ok(dbs)
    }

    pub fn remove_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // check if username exists
        let usernames = File::get_usernames(file_base_path)?;
        if !usernames.contains(&username.to_string()) {
            return Err(FileError::UsernameNotExists);
        }

        // check if username directory exists
        let username_path = format!("{}/{}", base_path, username);
        if !Path::new(&username_path).exists() {
            return Err(FileError::UsernameDirNotExists);
        }

        // check if `dbs.json` exists
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        if !Path::new(&dbs_json_path).exists() {
            return Err(FileError::DbsJsonNotExists);
        }

        // load current dbs from `dbs.json`
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let mut dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // remove if the db exists; otherwise raise error
        let idx_to_remove = dbs_json.dbs.iter().position(|db_info| &db_info.name == db_name);
        match idx_to_remove {
            Some(idx) => dbs_json.dbs.remove(idx),
            None => return Err(FileError::DbNotExists),
        };

        // remove corresponding db directory
        let db_path = format!("{}/{}", username_path, db_name);
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

    // TODO: create_table(username: &str, db_name: &str, table: &TableInfo, file_base_path: Option<&str>) -> Result<(), FileError>

    // TODO: load_tables(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<Vec<TableInfo>, FileError>

    // TODO: append_rows(username: &str, db_name: &str, table_name: &str, rows: &Vec<Row>, file_base_path: Option<&str>) -> Result<Vec<u32>, FileError>

    // TODO: fetch_rows(username: &str, db_name: &str, table_name: &str, row_id_range: &Vec<u32>, file_base_path: Option<&str>) -> Result<Vec<Row>, FileError>

    // TODO: delete_rows(username: &str, db_name: &str, table_name: &str, row_id_range: &Vec<u32>, file_base_path: Option<&str>) -> Result<(), FileError>

    // TODO: modify_row(username: &str, db_name: &str, table_name: &str, row_id: u32, new_row: &Row, file_base_path: Option<&str>) -> Result<(), FileError>
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn test_create_username() {
        let file_base_path = "data1";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();

        assert!(Path::new(file_base_path).exists());

        let usernames_json_path = format!("{}/{}", file_base_path, "usernames.json");
        assert!(Path::new(&usernames_json_path).exists());

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

        match File::create_username("happyguy", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "User name already exists and cannot be created again."
            ),
        };
    }

    #[test]
    pub fn test_get_usernames() {
        let file_base_path = "data2";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
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
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();
        File::create_username("sadguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "happyguy", "sadguy"]);

        File::remove_username("happyguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "sadguy"]);

        match File::remove_username("happyguy", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };

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

        match File::create_db("happyguy", "BookerDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };

        match File::create_db("crazyguy", "BookerDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "DB already exists and cannot be created again."),
        };
    }

    #[test]
    pub fn test_get_dbs() {
        let file_base_path = "data5";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_username("happyguy", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs.len(), 0);

        File::create_db("happyguy", "BookerDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB"]);

        File::create_db("happyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "MovieDB"]);

        match File::get_dbs("sadguy", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };
    }

    #[test]
    pub fn test_remove_db() {
        let file_base_path = "data6";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "PhotoDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "MovieDB", "PhotoDB"]);

        File::remove_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "PhotoDB"]);

        match File::remove_db("happyguy", "BookerDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };

        File::remove_db("crazyguy", "PhotoDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB"]);

        match File::remove_db("crazyguy", "PhotoDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "DB not exists. Please create DB first."),
        };

        File::remove_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs.len(), 0);

        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "BookerDB")).exists());
        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "MovieDB")).exists());
        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "PhotoDB")).exists());
    }
}
