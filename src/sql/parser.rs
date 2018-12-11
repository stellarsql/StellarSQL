use sql::lexer::LexerError;
use sql::lexer::Scanner;
use sql::symbol::Symbol;
use sql::symbol::Token;
use std::fmt;

struct Parser {
    tokens: Vec<Symbol>,
}

enum ParserError {
    CauseByLexer(LexerError),
    TokenLengthZero,
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParserError::CauseByLexer(ref e) => write!(f, "error caused by lexer: {}", e),
            ParserError::TokenLengthZero => write!(f, "error caused by a zero length tokens"),
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
    fn parse(&self) {
        let mut iter = self.tokens.iter();
        let category = iter.next();

        match category {
            Some(symbol) => match symbol.token {
                Token::CreateDatabase | Token::CreateTable => {}
                _ => {}
            },
            None => {}
        }
    }
}

#[test]
fn test_parser_new_error() {
    let message = "create table $1234";
    match Parser::new(message) {
        Ok(_) => {}
        Err(e) => assert_eq!(format!("{}", e), "error caused by lexer: please use ascii character."),
    }
}
