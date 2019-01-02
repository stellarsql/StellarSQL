use crate::sql::parser::Parser;
use crate::sql::parser::ParserError;
use crate::sql::worker::SQLError;
use crate::sql::worker::SQL;
use crate::Response;
use std::fmt;

#[derive(Debug)]
pub struct Request {}

#[derive(Debug)]
pub enum RequestError {
    SQLError(SQLError),
    CauseByParser(ParserError),
    UserNotExist(String),
    // DBNotExist(String),
    CreateDBBeforeCmd,
    BadRequest,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RequestError::SQLError(ref e) => write!(f, "error caused by worker: {}", e),
            RequestError::CauseByParser(ref e) => write!(f, "error caused by parser: {}", e),
            RequestError::UserNotExist(ref s) => write!(f, "user: {} not found", s),
            // RequestError::DBNotExist(ref s) => write!(f, "database: {} not found", s),
            RequestError::CreateDBBeforeCmd => write!(f, "please create a database before any other commands"),
            RequestError::BadRequest => write!(f, "BadRequest, invalid request format"),
        }
    }
}

impl Request {
    pub fn parse(input: &str, mut sql: &mut SQL) -> Result<Response, RequestError> {
        /*
         * request format
         * case1:
         * username||databasename||command;
         * case2:
         * username||||create dbname;
         *
         */
        let split_str: Vec<&str> = input.split("||").collect();
        if split_str.len() != 3 {
            return Err(RequestError::BadRequest);
        }

        let username = split_str[0];
        let dbname = split_str[1];
        let cmd = format!("{};", split_str[2]);

        // initialize username
        if sql.username == "" {
            if Request::user_verify(username).is_ok() {
                sql.username = username.to_string();
            } else {
                // user not existed
                return Err(RequestError::UserNotExist(username.to_string()));
            }
        }

        // check dbname
        if dbname != "" {
            if sql.database.name == "" {
                match sql.load_database(dbname) {
                    Err(ret) => return Err(RequestError::SQLError(ret)),
                    Ok(_) => {}
                }
            }
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
    fn user_verify(name: &str) -> Result<(), ()> {
        if name == "" {
            return Err(());
        }
        Ok(())
    }
}
