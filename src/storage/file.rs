extern crate json;
use std::io;
use std::fs;
use std::fmt;
use std::path::Path;
use std::io::Write;
use json::object;
use json::array;

static FILE_BASE_PATH: &str = "data";
static DBS_JSON_PATH: &str = "data/dbs.json";

#[derive(Debug, Clone)]
pub struct File {
    /* definition */
}

#[derive(Debug)]
pub enum FileError {
    Io,
    Json,
    DbNameNotFound,
    DbExisted,
    UnknownError,
}

impl From<io::Error> for FileError {
    fn from(err: io::Error) -> FileError {
        FileError::Io
    }
}

impl From<json::Error> for FileError {
    fn from(err: json::Error) -> FileError {
        FileError::Json
    }
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FileError::Io => write!(f, "No such file or directory"),
            FileError::Json => write!(f, "JSON parsing error"),
            FileError::DbNameNotFound => write!(f, "DB name cannot be found in json file"),
            FileError::DbExisted => write!(f, "DB already existed and cannot be created again"),
            FileError::UnknownError => write!(f, "Unknown error"),
        }
    }
}

impl File {
    pub fn create_db(db_name: &str) -> Result<(), FileError> {
        // check if the base data path exists
        if !Path::new(FILE_BASE_PATH).exists() {
            fs::create_dir_all(FILE_BASE_PATH)?;
        }

        // insert the new db record into `dbs.json`
        let mut dbs;
        if Path::new(DBS_JSON_PATH).exists() {
            let dbs_str = fs::read_to_string(DBS_JSON_PATH)?;
            dbs = json::parse(&dbs_str)?;            
        } else {
            dbs = object!{
                "dbs" => array![]
            };
        }

        let new_db = object!{
            "name" => db_name,
            "path" => db_name
        };

        // check if the db existed
        for db in dbs["dbs"].members() {
            if db["name"] == new_db["name"] {
                return Err(FileError::DbExisted);
            }
        }

        dbs["dbs"].push(new_db)?;

        // save `dbs.json`
        let mut dbs_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(DBS_JSON_PATH)?;
        dbs_file.write_all(dbs.pretty(4).as_bytes())?;
        
        // create corresponding db directory for the new db
        let db_path = format!("{}/{}", FILE_BASE_PATH, db_name);
        fs::create_dir_all(&db_path)?;

        // create corresponding `tables.json` for the new db
        let tables_json_path = format!("{}/{}", db_path, "tables.json");
        let mut tables_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(tables_json_path)?;
        let tables = object!{
            "tables" => array![]
        };
        tables_file.write_all(tables.pretty(4).as_bytes())?;

        Ok(())
    }

    pub fn get_db_names() -> Result<Vec<String>, FileError> {
        // read and parse `dbs.json`
        let dbs = fs::read_to_string(DBS_JSON_PATH)?;
        let mut dbs = json::parse(&dbs)?;

        // create a vector of db names
        let mut ret = Vec::new();
        for db in dbs["dbs"].members_mut() {
            let db_name = db["name"].take_string().ok_or(FileError::DbNameNotFound)?;
            ret.push(db_name);
        }
        Ok(ret)
    }
}


#[test]
pub fn test_create_db() {
    if Path::new("data").exists() {
        fs::remove_dir_all("data").unwrap();
    }
    File::create_db("BookerDB").unwrap();
    File::create_db("MovieDB").unwrap();

    assert!(Path::new("data").exists());
    assert!(Path::new("data/dbs.json").exists());

    let dbs = fs::read_to_string("data/dbs.json").unwrap();
    let dbs = json::parse(&dbs).unwrap();

    assert_eq!(dbs, object!{
        "dbs" => array![
            object!{
                "name" => "BookerDB",
                "path" => "BookerDB"
            },
            object!{
                "name" => "MovieDB",
                "path" => "MovieDB"
            }
        ]
    });

    assert!(Path::new("data/BookerDB").exists());
    assert!(Path::new("data/MovieDB").exists());

    assert!(Path::new("data/BookerDB/tables.json").exists());
    assert!(Path::new("data/MovieDB/tables.json").exists());

    let tables = fs::read_to_string("data/BookerDB/tables.json").unwrap();
    let tables = json::parse(&tables).unwrap();

    assert_eq!(tables, object!{
        "tables" => array![]
    });

    match File::create_db("MovieDB") {
        Ok(_) => {}
        Err(e) => assert_eq!(format!("{}", e), "DB already existed and cannot be created again")
    }
}

#[test]
pub fn test_get_db_names() {
    if Path::new("data").exists() {
        fs::remove_dir_all("data").unwrap();
    }
    File::create_db("BookerDB").unwrap();
    File::create_db("MovieDB").unwrap();

    let db_names: Vec<String> = File::get_db_names().unwrap();
    assert_eq!(db_names, vec!["BookerDB", "MovieDB"]);
}
