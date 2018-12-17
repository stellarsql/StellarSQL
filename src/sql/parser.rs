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
    fn parse(&self) -> Result<SQL, ParserError> {
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
                    return Err(ParserError::SyntaxError);
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
    fn test_parser_new() {
        let query = "create database db1;";
        let sql = Parser::new(query).unwrap().parse().unwrap();
        assert_eq!(sql.database.name, "db1");
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
