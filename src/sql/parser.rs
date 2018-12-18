use crate::component::datatype::DataType;
use crate::component::field::Field;
use crate::component::table::Table;
use crate::sql::lexer::LexerError;
use crate::sql::lexer::Scanner;
use crate::sql::symbol::Group;
use crate::sql::symbol::Symbol;
use crate::sql::symbol::Token;
use crate::sql::worker::SQLError;
use crate::sql::worker::SQL;
use std::fmt;

#[derive(Debug)]
struct Parser {
    tokens: Vec<Symbol>,
}

#[derive(Debug)]
enum ParserError {
    CauseByLexer(LexerError),
    TokenLengthZero,
    SyntaxError,
    SemanticError(SQLError),
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParserError::CauseByLexer(ref e) => write!(f, "error caused by lexer: {}", e),
            ParserError::TokenLengthZero => write!(f, "error caused by a zero length tokens"),
            ParserError::SyntaxError => write!(f, "error caused by wrong syntax"),
            ParserError::SemanticError(ref e) => write!(f, "error caused by semantic: {}", e),
        }
    }
}

impl Parser {
    fn new(message: &str) -> Result<Parser, ParserError> {
        let mut s: Scanner = Scanner::new(message);
        match s.scan_tokens() {
            Ok(tokens) => {
                if tokens.len() == 0 {
                    return Err(ParserError::TokenLengthZero);
                }
                Ok(Parser { tokens })
            }
            Err(e) => Err(ParserError::CauseByLexer(e)),
        }
    }
    fn parse(&self, db_name: Option<String>) -> Result<SQL, ParserError> {
        println!("Parser parsing started...");

        let mut iter = self.tokens.iter().peekable();
        let category = iter.next();

        match category {
            Some(symbol) => match symbol.token {
                Token::CreateDatabase => {
                    let db_name_sym = iter.next().ok_or(ParserError::SyntaxError)?;

                    // name should not be as same as keywords
                    if db_name_sym.group != Group::Identifier {
                        return Err(ParserError::SyntaxError);
                    }

                    let sql = SQL::create_database(&db_name_sym.name).map_err(|e| ParserError::SemanticError(e))?;

                    return Ok(sql);
                }
                Token::CreateTable => {
                    println!("-> create table");

                    let table_name_sym = iter.next().ok_or(ParserError::SyntaxError)?;

                    if table_name_sym.group != Group::Identifier {
                        return Err(ParserError::SyntaxError);
                    }

                    let table_name = table_name_sym.name.clone();
                    println!("   - table name: {}", table_name);

                    if iter.next().ok_or(ParserError::SyntaxError)?.token != Token::ParentLeft {
                        return Err(ParserError::SyntaxError);
                    };

                    // create table.
                    let mut table = Table::new(&table_name);
                    loop {
                        println!("   -- new field:");

                        let mut field;

                        match iter.peek() {
                            // setting a field
                            Some(s) if s.group == Group::Identifier => {
                                // 1. column
                                let var_name = iter.next().ok_or(ParserError::SyntaxError)?.name.clone();
                                println!("   --- field name: {}", var_name);

                                // 2. datatype
                                let var_type_sym = iter.next().ok_or(ParserError::SyntaxError)?;
                                println!("   --- field type: {}", var_type_sym.name);

                                // 2.1 case: varchar, char
                                if var_type_sym.token == Token::Varchar || var_type_sym.token == Token::Char {
                                    if iter.next().ok_or(ParserError::SyntaxError)?.token != Token::ParentLeft {
                                        return Err(ParserError::SyntaxError);
                                    };

                                    let varchar_len_str = iter.next().ok_or(ParserError::SyntaxError)?.name.clone();
                                    let varchar_len =
                                        varchar_len_str.parse::<u8>().map_err(|_| ParserError::SyntaxError)?;
                                    println!("   --- field type length: {}", varchar_len);

                                    let datatype = DataType::get(&var_type_sym.name, Some(varchar_len))
                                        .ok_or(ParserError::SyntaxError)?;
                                    field = Field::new(&var_name, datatype);

                                    if iter.next().ok_or(ParserError::SyntaxError)?.token != Token::ParentRight {
                                        return Err(ParserError::SyntaxError);
                                    };
                                // 2.2 case: other type
                                } else {
                                    let datatype =
                                        DataType::get(&var_type_sym.name, None).ok_or(ParserError::SyntaxError)?;
                                    field = Field::new(&var_name, datatype);
                                }
                                // 3. column properties
                                loop {
                                    match iter.peek() {
                                        Some(s) if s.token == Token::Comma => {
                                            iter.next();
                                            println!("   go next field");
                                            break;
                                        }
                                        Some(s) if s.token == Token::NotNull => {
                                            iter.next();
                                            field.not_null = true
                                        }
                                        Some(s) if s.token == Token::Default => {
                                            iter.next();
                                            let default_value =
                                                iter.next().ok_or(ParserError::SyntaxError)?.name.clone();
                                            field.default = Some(default_value);
                                        }
                                        Some(s) if s.token == Token::Check => {
                                            // TODO: handle check syntax. Do not use `check` in sql now.
                                            return Err(ParserError::SyntaxError);
                                        }
                                        // end of table block
                                        Some(s) if s.token == Token::ParentRight => break,
                                        Some(_) | None => return Err(ParserError::SyntaxError),
                                    }
                                }
                            }

                            // setting table properties
                            Some(s) if s.group == Group::Keyword => {
                                // TODO: primary key, foreign key & reference
                                return Err(ParserError::SyntaxError);
                            }

                            // finish table block
                            Some(s) if s.token == Token::ParentRight => {
                                println!("   - fields setting done.");
                                break;
                            }

                            Some(_) | None => return Err(ParserError::SyntaxError),
                        }

                        table.insert_new_field(field);
                        println!("   - insert new field into table");
                    }

                    let db_name = db_name.ok_or(ParserError::SyntaxError)?;

                    let sql = SQL::create_table(&db_name, &table).map_err(|e| ParserError::SemanticError(e))?;

                    return Ok(sql);
                }
                _ => {
                    return Err(ParserError::SyntaxError);
                }
            },
            None => {
                return Err(ParserError::SyntaxError);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_parse_database() {
        let query = "create database db1;";
        let sql = Parser::new(query).unwrap().parse(None).unwrap();
        assert_eq!(sql.database.name, "db1");
    }

    #[test]
    fn test_parser_parse_table() {
        let query = "create table t1 (a1 int, b1 char(7), c1 double);";
        let sql = Parser::new(query).unwrap().parse(Some("db1".to_string())).unwrap();
        let db = sql.database.clone();
        let table = db.tables.get("t1").unwrap();
        assert!(table.fields.contains_key("a1"));
        assert!(table.fields.contains_key("b1"));
        assert!(table.fields.contains_key("c1"));

        let query = "create table t1 (a1 int not null default 5, b1 char(7) not null, c1 double default 1.2);";
        let sql = Parser::new(query).unwrap().parse(Some("db1".to_string())).unwrap();
        let db = sql.database.clone();
        let table = db.tables.get("t1").unwrap();
        let a1 = table.fields.get("a1").unwrap();
        let b1 = table.fields.get("b1").unwrap();
        let c1 = table.fields.get("c1").unwrap();
        assert_eq!(a1.not_null, true);
        assert_eq!(a1.default.clone().unwrap(), "5");
        assert_eq!(b1.not_null, true);
        assert_eq!(c1.default.clone().unwrap(), "1.2");
    }

    #[test]
    fn test_parser_new_error() {
        let query = "create table $1234;";
        match Parser::new(query) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "error caused by lexer: please use ascii character."),
        }
    }
}
