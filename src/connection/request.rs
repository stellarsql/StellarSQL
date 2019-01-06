use crate::manager::pool::{Pool, PoolError};
use crate::sql::parser::{Parser, ParserError};
use crate::storage::file::{File, FileError};
use crate::Response;
use std::fmt;

use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Request {
    pub username: String,
    pub addr: String,
}

#[derive(Debug)]
pub enum RequestError {
    PoolError(PoolError),
    CauseByParser(ParserError),
    FileError(FileError),
    UserNotExist(String),
    CreateDBBeforeCmd,
    BadRequest,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RequestError::PoolError(ref e) => write!(f, "error caused by pool: {}", e),
            RequestError::CauseByParser(ref e) => write!(f, "error caused by parser: {}", e),
            RequestError::FileError(ref e) => write!(f, "error caused by file: {}", e),
            RequestError::UserNotExist(ref s) => write!(f, "user: {} not found", s),
            RequestError::CreateDBBeforeCmd => write!(f, "please create a database before any other commands"),
            RequestError::BadRequest => write!(f, "BadRequest, invalid request format"),
        }
    }
}

impl Request {
    pub fn new(new_addr: String) -> Request {
        Request {
            username: "".to_string(),
            addr: new_addr,
        }
    }
    pub fn parse(input: &str, mutex: &Arc<Mutex<Pool>>, req: &mut Request) -> Result<Response, RequestError> {
        /*
         * request format
         * case0: init (must be first request in each connection)
         * username||||||key
         * case1: normal
         * username||dbname||command;
         * case2: user without a database
         * username||||create database dbname;
         *
         */
        let split_str: Vec<&str> = input.split("||").collect();

        // first connection
        if req.username == "" {
            if split_str.len() != 4 || split_str[1] != "" || split_str[2] != "" {
                return Err(RequestError::BadRequest);
            }
            let username = split_str[0];
            let key = split_str[3];
            // TODO: save key

            // initialize username
            match Request::user_verify(username) {
                Ok(()) => req.username = username.to_string(),
                Err(ret) => return Err(ret),
            }
            return Ok(Response::OK {
                msg: "Login OK!".to_string(),
            });
        }

        if split_str.len() != 3 {
            return Err(RequestError::BadRequest);
        }

        let username = split_str[0];
        let dbname = split_str[1];
        let cmd = format!("{};", split_str[2]);

        // load sql object from memory pool
        let mut pool = mutex.lock().unwrap();
        let mut sql = match pool.get(username, dbname, req.addr.clone()) {
            Ok(tsql) => tsql,
            Err(ret) => return Err(RequestError::PoolError(ret)),
        };
        // check dbname
        if dbname != "" {
            let parser = Parser::new(&cmd).unwrap();
            match parser.parse(&mut sql) {
                Err(ret) => return Err(RequestError::CauseByParser(ret)),
                Ok(_) => {}
            }
        } else {
            // check cmd if it is "create database dbname;"
            let mut iter = cmd.split_whitespace();
            if iter.next() != Some("create") || iter.next() != Some("database") {
                return Err(RequestError::CreateDBBeforeCmd);
            }
            let parser = Parser::new(&cmd).unwrap();
            match parser.parse(&mut sql) {
                Err(ret) => return Err(RequestError::CauseByParser(ret)),
                Ok(_) => {}
            }
        }
        Ok(Response::OK {
            msg: "Query OK!".to_string(),
        })
        //Ok(Response::OK { msg: format!("{}, user:{}",input, sql.username) })
    }
    fn user_verify(name: &str) -> Result<(), RequestError> {
        // auto create new users for now
        if name == "" {
            return Err(RequestError::UserNotExist(name.to_string()));
        } else {
            let users = match File::get_usernames(Some(dotenv!("FILE_BASE_PATH"))) {
                Ok(us) => us,
                Err(ret) => return Err(RequestError::FileError(ret)),
            };
            if !users.contains(&name.to_string()) {
                match File::create_username(name, Some(dotenv!("FILE_BASE_PATH"))) {
                    Ok(_) => {}
                    Err(ret) => return Err(RequestError::FileError(ret)),
                }
            }
        }
        Ok(())
    }
}
