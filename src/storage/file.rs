extern crate serde_json;
use crate::component::table::Table;
use crate::component::datatype::DataType;
use crate::component::field::Field;
use std::io;
use std::fs;
use std::fmt;
use std::path::Path;
use std::io::Write;


static FILE_BASE_PATH: &str = "data";

#[derive(Debug, Clone)]
pub struct File {
    /* definition */
}

#[derive(Debug)]
pub enum FileError {
    Io,
    UsernameExisted,
    DbExisted,
    DbNotExisted,
    TableExisted,
    JsonParse,
    JsonKeyNotFound,
    JsonArrayMismatch,
    UnknownError,
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

// #[derive(Debug, Serialize, Deserialize)]
// struct TablesJson {
//     dbs: Vec<TableInfo>,
// }

// #[derive(Debug, Serialize, Deserialize)]
// struct TableInfo {
//     name: String,
//     path_tsv: String,
//     path_bin: String,
//     primary_key: Vec<String>,
//     foreign_key: Vec<String>,
//     reference_table: Option<String>,
//     reference_attr: Option<String>,
//     attrs: Vec<Field>,
// }

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
            FileError::UsernameExisted => write!(f, "User name already existed and cannot be created again."),
            FileError::DbExisted => write!(f, "DB already existed and cannot be created again."),
            FileError::DbNotExisted => write!(f, "DB not existed. Please create DB first."),
            FileError::TableExisted => write!(f, "Table already existed and cannot be created again."),
            FileError::JsonParse => write!(f, "JSON parsing error."),
            FileError::JsonKeyNotFound => write!(f, "Key cannot be found in the JSON structure."),
            FileError::JsonArrayMismatch => write!(f, "The value is expected to be an array."),
            FileError::UnknownError => write!(f, "Unknown error."),
        }
    }
}

impl File {
    pub fn create_username(username: &str) -> Result<(), FileError> {
        // check if the base data path exists
        if !Path::new(FILE_BASE_PATH).exists() {
            fs::create_dir_all(FILE_BASE_PATH)?;
        }

        // load current usernames from `usernames.json`
        let mut usernames_json: UsernamesJson;
        let usernames_json_path = format!("{}/{}", FILE_BASE_PATH, "usernames.json");
        if Path::new(&usernames_json_path).exists() {
            let usernames_file = fs::File::open(&usernames_json_path)?;
            usernames_json = serde_json::from_reader(usernames_file)?;
        } else {
            usernames_json = UsernamesJson {
                usernames: Vec::new()
            };
        }

        // check if the username existed
        for username_info in &usernames_json.usernames {
            if username_info.name == username {
                return Err(FileError::UsernameExisted);
            }
        }

        // create new username json instance
        let new_username_info = UsernameInfo {
            name: username.to_string(),
            path: username.to_string()
        };

        // insert the new username record into `usernames.json`
        usernames_json.usernames.push(new_username_info);

        // save `usernames.json`
        let mut usernames_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(usernames_json_path)?;
        usernames_file.write_all(serde_json::to_string_pretty(&usernames_json)?.as_bytes())?;

        // create corresponding directory for the new username
        let username_path = format!("{}/{}", FILE_BASE_PATH, username);
        fs::create_dir_all(&username_path)?;

        // create corresponding `dbs.json` for the new username
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        let mut dbs_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dbs_json_path)?;
        let dbs_json = DbsJson{
            dbs: Vec::new()
        };
        dbs_file.write_all(serde_json::to_string_pretty(&dbs_json)?.as_bytes())?;

        Ok(())
    }

    // pub fn create_db(db_name: &str) -> Result<(), FileError> {
    //     // check if the base data path exists
    //     if !Path::new(FILE_BASE_PATH).exists() {
    //         fs::create_dir_all(FILE_BASE_PATH)?;
    //     }

    //     // insert the new db record into `dbs.json`
    //     let mut dbs: serde_json::Value;
    //     if Path::new(DBS_JSON_PATH).exists() {
    //         let dbs_file = fs::File::open(DBS_JSON_PATH)?;
    //         dbs = serde_json::from_reader(dbs_file)?;
    //     } else {
    //         dbs = json!({
    //             "dbs": json!([])
    //         });
    //     }

    //     let new_db = json!({
    //         "name": db_name,
    //         "path": db_name
    //     });

    //     // check if the db existed
    //     for db in dbs.get("dbs")
    //         .ok_or(FileError::JsonKeyNotFound)?
    //         .as_array().ok_or(FileError::JsonArrayMismatch)? {
    //         if db.get("name").ok_or(FileError::JsonKeyNotFound)? == new_db.get("name").ok_or(FileError::JsonKeyNotFound)? {
    //             return Err(FileError::DbExisted);
    //         }
    //     }

    //     dbs.get_mut("dbs").ok_or(FileError::JsonKeyNotFound)?.as_array_mut().ok_or(FileError::JsonArrayMismatch)?.push(new_db);

    //     // save `dbs.json`
    //     let mut dbs_file = fs::OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(DBS_JSON_PATH)?;
    //     dbs_file.write_all(serde_json::to_string_pretty(&dbs)?.as_bytes())?;

    //     // create corresponding db directory for the new db
    //     let db_path = format!("{}/{}", FILE_BASE_PATH, db_name);
    //     fs::create_dir_all(&db_path)?;

    //     // create corresponding `tables.json` for the new db
    //     let tables_json_path = format!("{}/{}", db_path, "tables.json");
    //     let mut tables_file = fs::OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(tables_json_path)?;
    //     let tables = json!({
    //         "tables": json!([])
    //     });
    //     tables_file.write_all(serde_json::to_string_pretty(&tables)?.as_bytes())?;

    //     Ok(())
    // }

    // pub fn get_db_names() -> Result<Vec<String>, FileError> {
    //     // read and parse `dbs.json`
    //     let dbs = fs::read_to_string(DBS_JSON_PATH)?;
    //     let mut dbs = serde_json::from_str(&dbs)?;

    //     // create a vector of db names
    //     let mut ret = Vec::new();
    //     for db in dbs["dbs"].members_mut() {
    //         let db_name = db["name"].take_string().ok_or(FileError::DbNotExisted)?;
    //         ret.push(db_name);
    //     }
    //     Ok(ret)
    // }

    // pub fn create_table(db_name: &str, new_table: &Table) -> Result<(), FileError> {
    //     // check if db exists
    //     let db_path = format!("{}/{}", FILE_BASE_PATH, db_name);
    //     if !Path::new(&db_path).exists() {
    //         return Err(FileError::DbNotExisted);          
    //     }

    //     // parse tables.json
    //     let tables_json_path = format!("{}/{}", db_path, "tables.json");
    //     let tables_json_str = fs::read_to_string(&tables_json_path)?;
    //     let mut tables = serde_json::from_str(&tables_json_str)?;

    //     // check if the table exists
    //     for t in tables["tables"].members() {
    //         if t["name"] == new_table["name"] {
    //             return Err(FileError::TableExisted);
    //         }
    //     }

    //     // convert table to json object
    //     let mut new_table_json = object!{
    //         "name" => new_table.name,
    //         "path_tsv" => format!("{}.tsv", new_table.name),
    //         "path_bin" => format!("{}.bin", new_table.name),
    //         "primary_key" => array!(new_table.primary_key),
    //         "foreign_key" => array!(new_table.foreign_key),
    //         "reference_table" => array!(new_table.reference_table),
    //         "reference_attr" => array!(new_table.reference_attr),
    //         "attrs": array![],
    //     };

    //     // deal with attrs
    //     let mut attr_names = vec![];
    //     for (attr_name, attr) in &new_table {
    //         attr_names.push(attr_name);
    //         new_table_json["attrs"].push(object!{
    //             "name" => attr_name,
    //             "datatype" => datatype_to_obj(&attr.datatype),
    //             "not_null" => attr.not_null,
    //             "default" => attr.default.unwrap_or("__")
    //             // TODO: Checker for field
    //         });
    //     }

    //     // insert table json to `tables.json`
    //     tables["tables"].push(new_table_json)?;

    //     // save `tables.json`
    //     let mut tables_file = fs::OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(tables_json_path)?;
    //     tables_file.write_all(tables.pretty(4).as_bytes())?;
        
    //     // write tsv
    //     let table_tsv_path = format!("{}/{}/{}.tsv", db_path, new_table.name, new_table.name);
    //     let mut table_tsv = fs::OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(table_tsv_path)?;
    //     tables_file.write_all(attr_names.join("\t").as_bytes())?;

    //     Ok(())
    // }

    // load_table

    // fetch rows

    // pub fn datatype_to_obj(datatype: &DataType) -> json::JsonValue {
    //     match datatype {
    //         DataType::Char(length) => object!{
    //             "type" => "char",
    //             "length" => length
    //         },
    //         DataType::Double => object!{
    //             "type" => "double",
    //             "length" => 64
    //         },
    //         DataType::Float => object!{
    //             "type" => "float",
    //             "length" => 32
    //         },
    //         DataType::Int => object!{
    //             "type" => "int",
    //             "length" => 32
    //         },
    //         DataType::Varchar(length) => object!{
    //             "type" => "varchar",
    //             "length" => length
    //         },
    //     }
    // }

    // pub fn obj_to_datatype(obj: &json::JsonValue) -> DataType {
    //     DataType::get(obj["type"], obj["length"])
    // }
}

#[test]
pub fn test_create_username() {
    if Path::new(FILE_BASE_PATH).exists() {
        fs::remove_dir_all(FILE_BASE_PATH).unwrap();
    }
    File::create_username("tom6311tom6311").unwrap();
    File::create_username("happyguy").unwrap();

    assert!(Path::new(FILE_BASE_PATH).exists());

    let usernames_json_path = format!("{}/{}", FILE_BASE_PATH, "usernames.json");
    assert!(Path::new(&usernames_json_path).exists());

    let usernames_json = fs::read_to_string(usernames_json_path).unwrap();
    let usernames_json: UsernamesJson = serde_json::from_str(&usernames_json).unwrap();

    let ideal_usernames_json = UsernamesJson {
        usernames: vec![
            UsernameInfo {
                name: "tom6311tom6311".to_string(),
                path: "tom6311tom6311".to_string()
            },
            UsernameInfo {
                name: "happyguy".to_string(),
                path: "happyguy".to_string()
            }
        ]
    };

    assert_eq!(usernames_json.usernames[0].name, ideal_usernames_json.usernames[0].name);
    assert_eq!(usernames_json.usernames[1].name, ideal_usernames_json.usernames[1].name);
    assert_eq!(usernames_json.usernames[0].path, ideal_usernames_json.usernames[0].path);
    assert_eq!(usernames_json.usernames[1].path, ideal_usernames_json.usernames[1].path);

    assert!(Path::new(&format!("{}/{}", FILE_BASE_PATH, "tom6311tom6311")).exists());
    assert!(Path::new(&format!("{}/{}", FILE_BASE_PATH, "happyguy")).exists());

    assert!(Path::new(&format!("{}/{}/{}", FILE_BASE_PATH, "tom6311tom6311", "dbs.json")).exists());
    assert!(Path::new(&format!("{}/{}/{}", FILE_BASE_PATH, "happyguy", "dbs.json")).exists());

    let dbs_json = fs::read_to_string(format!("{}/{}/{}", FILE_BASE_PATH, "tom6311tom6311", "dbs.json")).unwrap();
    let dbs_json: DbsJson = serde_json::from_str(&dbs_json).unwrap();

    assert_eq!(dbs_json.dbs.len(), 0);

    match File::create_username("happyguy") {
        Ok(_) => {}
        Err(e) => assert_eq!(format!("{}", e), "User name already existed and cannot be created again.")
    }
}

// #[test]
// pub fn test_create_db() {
//     if Path::new("data").exists() {
//         fs::remove_dir_all("data").unwrap();
//     }
//     File::create_db("BookerDB").unwrap();
//     File::create_db("MovieDB").unwrap();

//     assert!(Path::new("data").exists());
//     assert!(Path::new("data/dbs.json").exists());

//     let dbs = fs::read_to_string("data/dbs.json").unwrap();
//     let dbs: serde_json::Value = serde_json::from_str(&dbs).unwrap();

//     assert_eq!(dbs, json!({
//         "dbs": json!([
//             json!({
//                 "name": "BookerDB",
//                 "path": "BookerDB"
//             }),
//             json!({
//                 "name": "MovieDB",
//                 "path": "MovieDB"
//             })
//         ])
//     }));

//     assert!(Path::new("data/BookerDB").exists());
//     assert!(Path::new("data/MovieDB").exists());

//     assert!(Path::new("data/BookerDB/tables.json").exists());
//     assert!(Path::new("data/MovieDB/tables.json").exists());

//     let tables = fs::read_to_string("data/BookerDB/tables.json").unwrap();
//     let tables: serde_json::Value = serde_json::from_str(&tables).unwrap();

//     assert_eq!(tables, json!({
//         "tables": json!([])
//     }));

//     match File::create_db("MovieDB") {
//         Ok(_) => {}
//         Err(e) => assert_eq!(format!("{}", e), "DB already existed and cannot be created again.")
//     }
// }

// #[test]
// pub fn test_get_db_names() {
//     if Path::new("data").exists() {
//         fs::remove_dir_all("data").unwrap();
//     }
//     File::create_db("BookerDB").unwrap();
//     File::create_db("MovieDB").unwrap();

//     let db_names: Vec<String> = File::get_db_names().unwrap();
//     assert_eq!(db_names, vec!["BookerDB", "MovieDB"]);
// }
