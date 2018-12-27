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
use std::iter::Peekable;
use std::slice::Iter;

#[derive(Debug)]
pub struct Parser {
    tokens: Vec<Symbol>,
}

#[derive(Debug)]
pub enum ParserError {
    CauseByLexer(LexerError),
    TokenLengthZero,
    SyntaxError(String),
    SQLError(SQLError),
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParserError::CauseByLexer(ref e) => write!(f, "error caused by lexer: {}", e),
            ParserError::TokenLengthZero => write!(f, "error caused by a zero length tokens"),
            ParserError::SyntaxError(ref s) => write!(f, "error caused by wrong syntax `{}`", s),
            ParserError::SQLError(ref e) => write!(f, "error caused by semantic: {}", e),
        }
    }
}

impl Parser {
    pub fn new(message: &str) -> Result<Parser, ParserError> {
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
    pub fn parse(&self, sql: &mut SQL) -> Result<(), ParserError> {
        debug!("Parser parsing started...");

        let mut iter = self.tokens.iter().peekable();

        match iter.peek() {
            Some(symbol) => match symbol.token {
                Token::CreateDatabase => {
                    let _ = iter.next(); // "create database"

                    let db_name_sym = iter
                        .next()
                        .ok_or(ParserError::SyntaxError(String::from("no db name")))?;
                    check_id(db_name_sym)?;

                    sql.create_database(&db_name_sym.name)
                        .map_err(|e| ParserError::SQLError(e))?;

                    return Ok(());
                }
                Token::CreateTable => {
                    debug!("-> create table");
                    let _ = iter.next();

                    let table_name_sym = iter
                        .next()
                        .ok_or(ParserError::SyntaxError(String::from("no table name")))?;
                    check_id(table_name_sym)?;

                    let table_name = table_name_sym.name.clone();
                    debug!("   - table name: {}", table_name);

                    assert_token(iter.next(), Token::ParentLeft)?;

                    // create table.
                    let mut table = Table::new(&table_name);
                    loop {
                        debug!("   -- new field:");

                        let mut field;

                        match iter.peek() {
                            // setting a field
                            Some(s) if s.group == Group::Identifier => {
                                // 1. column
                                let var_name = iter
                                    .next()
                                    .ok_or(ParserError::SyntaxError(String::from("miss column name")))?
                                    .name
                                    .clone();
                                debug!("   --- field name: {}", var_name);

                                // 2. datatype
                                let var_type_sym = iter
                                    .next()
                                    .ok_or(ParserError::SyntaxError(String::from("miss column type")))?;
                                debug!("   --- field type: {}", var_type_sym.name);

                                // 2.1 case: varchar, char
                                if var_type_sym.token == Token::Varchar || var_type_sym.token == Token::Char {
                                    assert_token(iter.next(), Token::ParentLeft)?;

                                    let varchar_len_str = iter
                                        .next()
                                        .ok_or(ParserError::SyntaxError(String::from("miss column type length")))?
                                        .name
                                        .clone();
                                    let varchar_len = varchar_len_str
                                        .parse::<u8>()
                                        .map_err(|_| ParserError::SyntaxError(String::from("type length invalid")))?;
                                    debug!("   --- field type length: {}", varchar_len);

                                    let datatype = DataType::get(&var_type_sym.name, Some(varchar_len))
                                        .ok_or(ParserError::SyntaxError(String::from("invalid type")))?;
                                    field = Field::new(&var_name, datatype);

                                    assert_token(iter.next(), Token::ParentRight)?;

                                // 2.2 case: other type
                                } else {
                                    let datatype = DataType::get(&var_type_sym.name, None)
                                        .ok_or(ParserError::SyntaxError(String::from("invalid type")))?;
                                    field = Field::new(&var_name, datatype);
                                }
                                // 3. column properties
                                loop {
                                    match iter.peek() {
                                        Some(s) if s.token == Token::Comma => {
                                            iter.next();
                                            debug!("   go next field");
                                            break;
                                        }
                                        Some(s) if s.token == Token::NotNull => {
                                            iter.next();
                                            field.not_null = true
                                        }
                                        Some(s) if s.token == Token::Default => {
                                            iter.next();
                                            let default_value = iter
                                                .next()
                                                .ok_or(ParserError::SyntaxError(String::from("miss default value")))?
                                                .name
                                                .clone();
                                            field.default = Some(default_value);
                                        }
                                        Some(s) if s.token == Token::Check => {
                                            // TODO: handle check syntax. Do not use `check` in sql now.
                                            return Err(ParserError::SyntaxError(String::from("check syntax error")));
                                        }
                                        // end of table block
                                        Some(s) if s.token == Token::ParentRight => break,
                                        Some(_) | None => return Err(ParserError::SyntaxError(String::from(""))),
                                    }
                                }
                            }

                            // setting table properties
                            Some(s) if s.group == Group::Keyword => {
                                // TODO: primary key, foreign key & reference
                                return Err(ParserError::SyntaxError(String::from("")));
                            }

                            // finish table block
                            Some(s) if s.token == Token::ParentRight => {
                                debug!("   - fields setting done.");
                                break;
                            }

                            Some(_) | None => return Err(ParserError::SyntaxError(String::from(""))),
                        }

                        table.insert_new_field(field);
                        debug!("   - insert new field into table");
                    }

                    sql.create_table(&table).map_err(|e| ParserError::SQLError(e))?;

                    return Ok(());
                }
                Token::InsertInto => {
                    debug!("-> insert into table");
                    let (table_name, attrs, rows) = parser_insert_into_table(&mut iter)?;
                    sql.insert_into_table(&table_name, attrs, rows)
                        .map_err(|e| ParserError::SQLError(e))?;

                    Ok(())
                }
                Token::InsertInto => {
                    debug!("-> select table");
                    let (table_name, attrs, rows) = parser_insert_into_table(&mut iter)?;
                    sql.insert_into_table(&table_name, attrs, rows)
                        .map_err(|e| ParserError::SQLError(e))?;

                    Ok(())
                }
                _ => {
                    return Err(ParserError::SyntaxError(String::from("unknown keyword")));
                }
            },
            None => {
                return Err(ParserError::SyntaxError(String::from("miss query")));
            }
        }
    }
}

#[inline]
fn parser_insert_into_table(
    iter: &mut Peekable<Iter<Symbol>>,
) -> Result<(String, Vec<String>, Vec<Vec<String>>), ParserError> {
    let _ = iter.next();

    let table_name_sym = iter
        .next()
        .ok_or(ParserError::SyntaxError(String::from("miss table name")))?;
    check_id(table_name_sym)?;

    let table_name = table_name_sym.name.clone();
    debug!("   - table name: {}", table_name);

    // get attributes
    let attrs = get_id_list(iter, true)?;
    debug!("   -- attributes: {:?}", attrs);

    assert_token(iter.next(), Token::Values)?;

    let mut rows: Vec<Vec<String>> = Vec::new();
    loop {
        match iter.peek() {
            Some(s) if s.token == Token::ParentLeft => {
                let row = get_id_list(iter, true)?;
                debug!("   -- row: {:?}", row);
                if attrs.len() != row.len() {
                    return Err(ParserError::SyntaxError(String::from(
                        "tuple length mismatch definition",
                    )));
                }
                rows.push(row);
            }
            Some(s) if s.token == Token::Comma => {
                iter.next();
                continue;
            }
            Some(_) | None => break,
        }
    }

    assert_token(iter.next(), Token::Semicolon)?;
    Ok((table_name, attrs, rows))
}

/// Get a list of identifiers, which in form as
///
/// `is_parent` parameter
/// -  is_parent: `(a1, a2, a3, a4)`
/// - !is_parent: `a1, a2, a3, a4`
fn get_id_list(iter: &mut Peekable<Iter<Symbol>>, is_parent: bool) -> Result<Vec<String>, ParserError> {
    let mut v = vec![];
    if is_parent {
        assert_token(iter.next(), Token::ParentLeft)?;
    }
    loop {
        match iter.next() {
            Some(s) if s.token == Token::Identifier => {
                v.push(s.name.clone());
                match iter.peek() {
                    Some(s) if s.token == Token::Comma => {
                        iter.next();
                        continue;
                    }
                    Some(_) | None => break,
                }
            }
            Some(_) | None => return Err(ParserError::SyntaxError(String::from("invalid syntax"))),
        }
    }
    if is_parent {
        assert_token(iter.next(), Token::ParentRight)?;
    }
    Ok(v)
}

/// Check if the symbol is an identifier
#[inline]
fn check_id(sym: &Symbol) -> Result<(), ParserError> {
    if sym.group != Group::Identifier {
        return Err(ParserError::SyntaxError(format!("{} is not an", &sym.name)));
    }
    Ok(())
}

/// Check if the symbol is the expected token.
#[inline]
fn assert_token(sym: Option<&Symbol>, token: Token) -> Result<(), ParserError> {
    if sym
        .ok_or(ParserError::SyntaxError(String::from("invalid syntax")))?
        .token
        != token
    {
        return Err(ParserError::SyntaxError(String::from("invalid syntax")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;

    fn fake_sql() -> SQL {
        let mut sql = SQL::new("Jenny").unwrap();
        sql.create_database("db1").unwrap();
        sql
    }

    #[test]
    fn test_parser_create_database() {
        let mut sql = fake_sql();

        let query = "create database db2;";
        let parser = Parser::new(query).unwrap();
        parser.parse(&mut sql).unwrap();
        assert_eq!(sql.database.name, "db2");
    }

    #[test]
    fn test_parser_create_table() {
        let mut sql = fake_sql();

        let query = "create table t1 (a1 int, b1 char(7), c1 double);";
        let parser = Parser::new(query).unwrap();
        parser.parse(&mut sql).unwrap();

        let db = sql.database.clone();
        let table = db.tables.get("t1").unwrap();
        assert!(table.fields.contains_key("a1"));
        assert!(table.fields.contains_key("b1"));
        assert!(table.fields.contains_key("c1"));

        let query = "create table t1 (a1 int not null default 5, b1 char(7) not null, c1 double default 1.2);";
        let parser = Parser::new(query).unwrap();
        parser.parse(&mut sql).unwrap();

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
    fn test_insert_into_table1() {
        env_logger::init();
        let query = "insert into t1(a1, a2, a3) values (1, 2, 3), (4, 5, 6);";
        let parser = Parser::new(query).unwrap();
        let mut iter = parser.tokens.iter().peekable();
        let (table_name, attrs, rows) = parser_insert_into_table(&mut iter).unwrap();
        assert_eq!(table_name, "t1");
        assert_eq!(attrs, vec![String::from("a1"), String::from("a2"), String::from("a3")]);
        assert_eq!(
            rows,
            vec![
                vec![String::from("1"), String::from("2"), String::from("3")],
                vec![String::from("4"), String::from("5"), String::from("6")]
            ]
        );

        let query = "insert into t1(a1, a2, a3) values (1, 2, 3);";
        let parser = Parser::new(query).unwrap();
        let mut iter = parser.tokens.iter().peekable();
        let (table_name, attrs, rows) = parser_insert_into_table(&mut iter).unwrap();
        assert_eq!(table_name, "t1");
        assert_eq!(attrs, vec![String::from("a1"), String::from("a2"), String::from("a3")]);
        assert_eq!(
            rows,
            vec![vec![String::from("1"), String::from("2"), String::from("3")],]
        );

        let query = "insert into t1(a1) values (1);";
        let parser = Parser::new(query).unwrap();
        let mut iter = parser.tokens.iter().peekable();
        let (table_name, attrs, rows) = parser_insert_into_table(&mut iter).unwrap();
        assert_eq!(table_name, "t1");
        assert_eq!(attrs, vec![String::from("a1")]);
        assert_eq!(rows, vec![vec![String::from("1")]]);
    }

    #[test]
    fn test_insert_into_table_syntax_error() {
        // values not match attributes
        let query = "insert into t1(a1, a2, a3) values (1, 2);";
        let parser = Parser::new(query).unwrap();
        let mut iter = parser.tokens.iter().peekable();
        assert!(parser_insert_into_table(&mut iter).is_err());

        let query = "insert into t1(a1, a2, a3) values (1, 2, 3, 4);";
        let parser = Parser::new(query).unwrap();
        let mut iter = parser.tokens.iter().peekable();
        assert!(parser_insert_into_table(&mut iter).is_err());
    }

    #[test]
    fn test_parser_insert_into_table() {
        let mut sql = fake_sql();

        let query = "create table t1 (a1 int, b1 char(7), c1 double);";
        let parser = Parser::new(query).unwrap();
        parser.parse(&mut sql).unwrap();

        let query = "insert into t1(a1, b1, c1) values (33, 'abc', 3.43);";
        let parser = Parser::new(query).unwrap();
        assert!(parser.parse(&mut sql).is_ok());
    }

    #[test]
    fn test_parser_insert_into_table_error() {
        let mut sql = fake_sql();

        let query = "create table t1 (a1 int, b1 char(7), c1 double);";
        let parser = Parser::new(query).unwrap();
        parser.parse(&mut sql).unwrap();

        // miss the attribute, but it has no default value
        let query = "insert into t1(a1, c1) values (33,  3.43);";
        let parser = Parser::new(query).unwrap();
        assert!(parser.parse(&mut sql).is_err());
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
