use crate::component::datatype::DataType;
use crate::component::field::Field;
use crate::component::table::Table;
use crate::sql::lexer::LexerError;
use crate::sql::lexer::Scanner;
use crate::sql::query::Node;
use crate::sql::query::QueryData;
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
                    let table = parser_create_table(&mut iter)?;
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
                Token::Select => {
                    debug!("-> select table");
                    sql.querydata = parse_select(&mut iter)?;
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

fn parser_create_table(iter: &mut Peekable<Iter<Symbol>>) -> Result<Table, ParserError> {
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
                        Some(s) if s.token == Token::Encrypt => {
                            iter.next();
                            field.encrypt = true;
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

    Ok(table)
}

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

/// Parse select query
///
/// Syntax:
/// ```
///     SELECT   f1, f2
///     FROM     t1, t2
///     WHERE    predicate
///     GROUP BY f1, f2
///     ORDER BY f1 DES, f2 ASC
/// ```
#[inline]
fn parse_select(iter: &mut Peekable<Iter<Symbol>>) -> Result<QueryData, ParserError> {
    let _ = iter.next(); // select

    let mut query_data = QueryData::new();

    query_data.fields = get_id_list(iter, false)?;

    assert_token(iter.next(), Token::From)?;

    query_data.tables = get_id_list(iter, false)?;

    match iter.peek() {
        Some(s) if s.token == Token::Where => {
            let _ = iter.next();
            let mut symbols: Vec<&Symbol> = vec![];
            loop {
                match iter.peek() {
                    Some(s)
                        if s.token == Token::GroupBy || s.token == Token::OrderBy || s.token == Token::Semicolon =>
                    {
                        break
                    }
                    Some(_) => symbols.push(iter.next().unwrap()),
                    None => break,
                }
            }
            query_data.predicate = Some(parse_predicate(symbols)?);
        }
        Some(_) | None => {}
    }

    match iter.peek() {
        Some(s) if s.token == Token::GroupBy => {
            // TODO:
        }
        Some(_) | None => {}
    }

    match iter.peek() {
        Some(s) if s.token == Token::OrderBy => {
            // TODO:
        }
        Some(_) | None => {}
    }

    Ok(query_data)
}

/// Parse a predicate as a tree
fn parse_predicate(symbols: Vec<&Symbol>) -> Result<Box<Node>, ParserError> {
    let postfix_vec = parse_infix_postfix(symbols)?;
    parse_postfix_tree(postfix_vec)
}

/// Parse a postfix to a binary tree, and do semantic check
fn parse_postfix_tree(symbols: Vec<&Symbol>) -> Result<Box<Node>, ParserError> {
    let mut iter = symbols.iter();
    let mut nodes_stack: Vec<Node> = vec![];
    loop {
        match iter.next() {
            Some(s) if s.group == Group::Identifier => nodes_stack.push(Node::new(s.name.clone())),
            Some(s) if s.group == Group::Operator => match s.token {
                Token::AND | Token::OR => {
                    let tree = Node::new(s.name.clone())
                        .right(nodes_stack.pop().unwrap())
                        .left(nodes_stack.pop().unwrap());
                    nodes_stack.push(tree);
                }
                Token::NOT => {
                    let tree = Node::new(s.name.clone()).right(nodes_stack.pop().unwrap());
                    nodes_stack.push(tree);
                }
                Token::LT | Token::LE | Token::EQ | Token::NE | Token::GT | Token::GE => {
                    let right = nodes_stack.pop().unwrap();
                    let left = nodes_stack.pop().unwrap();

                    // TODO: check right value
                    // TODO: check left value

                    let tree = Node::new(s.name.clone()).right(right).left(left);
                    nodes_stack.push(tree);
                }
                _ => {}
            },
            Some(_) => return Err(ParserError::SyntaxError(String::from("invalid predicate syntax"))),
            None => break,
        }
    }

    let tree = nodes_stack.pop().unwrap();

    if nodes_stack.len() != 0 {
        return Err(ParserError::SyntaxError(String::from("invalid predicate syntax")));
    }

    Ok(Box::new(tree))
}

/// in order traversal
fn in_order(node: Box<Node>, vec: &mut Vec<String>) {
    if node.left.is_some() {
        in_order(node.left.unwrap(), vec);
    }
    vec.push(node.root.clone());
    if node.right.is_some() {
        in_order(node.right.unwrap(), vec);
    }
}

/// parse predicate tokens from infix to postfix
fn parse_infix_postfix(symbols: Vec<&Symbol>) -> Result<Vec<&Symbol>, ParserError> {
    let mut iter = symbols.iter();
    let mut stack: Vec<&Symbol> = vec![];
    let mut output: Vec<&Symbol> = vec![];
    loop {
        let mut parent_counter = 0;
        match iter.next() {
            Some(s) if s.token == Token::ParentLeft => {
                parent_counter += 1;
                stack.push(*s);
            }
            Some(s) if s.token == Token::ParentRight => loop {
                match stack.pop() {
                    Some(s_) if s_.token == Token::ParentLeft => {
                        parent_counter -= 1;
                        break;
                    }
                    Some(s_) => output.push(s_),
                    None => break,
                }
            },
            Some(s) if s.group == Group::Operator => {
                loop {
                    match stack.last() {
                        Some(last) if last.group == Group::Operator => {
                            let l = operator_priority(&last.token);
                            let r = operator_priority(&s.token);
                            if l >= r {
                                output.push(*last);
                                stack.pop();
                            } else {
                                break;
                            }
                        }
                        Some(_) | None => break,
                    }
                }
                stack.push(*s);
            }
            Some(s) => output.push(*s),
            None => {
                if parent_counter > 0 {
                    return Err(ParserError::SyntaxError(String::from("invalid predicate syntax")));
                }
                loop {
                    match stack.pop() {
                        Some(s_) => output.push(s_),
                        None => break,
                    }
                }
                break;
            }
        }
    }

    Ok(output)
}

#[inline]
fn operator_priority(t: &Token) -> u32 {
    match t {
        &Token::NOT => 2,
        &Token::AND | &Token::OR => 1,
        _ => 3, // >=, >, =, <, <=
    }
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
    use crate::sql::query::*;
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

        let query = "create table t1 (a1 int not null default 5 encrypt, b1 char(7) not null, c1 double default 1.2);";
        let parser = Parser::new(query).unwrap();
        parser.parse(&mut sql).unwrap();

        let db = sql.database.clone();
        let table = db.tables.get("t1").unwrap();
        let a1 = table.fields.get("a1").unwrap();
        let b1 = table.fields.get("b1").unwrap();
        let c1 = table.fields.get("c1").unwrap();
        assert_eq!(a1.not_null, true);
        assert_eq!(a1.default.clone().unwrap(), "5");
        assert_eq!(a1.encrypt, true);
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

    fn assert_parse_infix_postfix(query: &str, answer: Vec<&str>) {
        let mut parser = Parser::new(query).unwrap();
        parser.tokens.pop(); // `;`
        let mut iter = parser.tokens.iter();
        let mut tokens: Vec<&Symbol> = vec![];
        loop {
            match iter.next() {
                Some(s) => tokens.push(s),
                None => break,
            }
        }
        let postfix = parse_infix_postfix(tokens).unwrap();
        println!("{:?}", postfix);
        println!("{:?}", answer);
        for i in 0..answer.len() {
            assert_eq!(&postfix[i].name, answer[i]);
        }
    }

    #[test]
    fn test_parse_infix_postfix() {
        let query = "not a1 = 3 and b2 >= 5;";
        let answer = ["a1", "3", "=", "not", "b2", "5", ">=", "and"];
        assert_parse_infix_postfix(query, answer.to_vec());

        let query = "not(not a1 = 3 and not (b2 >= 5 or c1 < 7));";
        let answer = [
            "a1", "3", "=", "not", "b2", "5", ">=", "c1", "7", "<", "or", "not", "and", "not",
        ];
        assert_parse_infix_postfix(query, answer.to_vec());
    }

    fn assert_parse_postfix_tree(query: &str, answer: Vec<&str>) {
        let mut parser = Parser::new(query).unwrap();
        parser.tokens.pop(); // `;`
        let mut iter = parser.tokens.iter();
        let mut tokens: Vec<&Symbol> = vec![];
        loop {
            match iter.next() {
                Some(s) => tokens.push(s),
                None => break,
            }
        }
        let mut output = vec![];
        let tree = parse_postfix_tree(tokens).unwrap();
        in_order(tree, &mut output);
        println!("{:?}", output);
        println!("{:?}", answer);
        for i in 0..answer.len() {
            assert_eq!(&output[i], answer[i]);
        }
    }

    #[test]
    fn test_parse_postfix_tree() {
        let postfix = "a1 3 = not b2 5 >= and ;";
        let answer = ["not", "a1", "=", "3", "and", "b2", ">=", "5"];
        assert_parse_postfix_tree(postfix, answer.to_vec());

        let postfix = "a1 3 = not b2 5 >= c1 7 < or not and not ;";
        let answer = [
            "not", "not", "a1", "=", "3", "and", "not", "b2", ">=", "5", "or", "c1", "<", "7",
        ];
        assert_parse_postfix_tree(postfix, answer.to_vec());
    }

    fn assert_parse_predicate(query: &str, answer: &str) {
        let mut parser = Parser::new(query).unwrap();
        parser.tokens.pop(); // `;`
        let mut iter = parser.tokens.iter();
        let mut tokens: Vec<&Symbol> = vec![];
        loop {
            match iter.next() {
                Some(s) => tokens.push(s),
                None => break,
            }
        }
        let mut output = vec![];
        let tree = parse_predicate(tokens).unwrap();
        in_order(tree, &mut output);
        let mut in_order_string = "".to_string();
        for i in output {
            in_order_string += &i;
            in_order_string += " ";
        }
        in_order_string += ";";
        assert_eq!(in_order_string, answer);
    }

    #[test]
    fn test_parse_predicate() {
        let query = "a1 >= 3 and b3 <= 7 or c1 = 4 and not d1 = 3 ;"; // a space before `;` is required
        assert_parse_predicate(query, query);

        let query = "not (a1 >= 3 and b3 <= 7) or not (c1 = 4 and d1 = 3);";
        let answer = "not a1 >= 3 and b3 <= 7 or not c1 = 4 and d1 = 3 ;"; // a space before `;` is required
        assert_parse_predicate(query, answer);
    }

    #[test]
    fn test_parser_select() {
        let query = "select t1.a1, t1.a2, t1.a3 from t1, t2 where t1.a1 > 4 and t1.a1 = t2.a1;";
        let mut sql = fake_sql();
        let parser = Parser::new(query).unwrap();
        parser.parse(&mut sql).unwrap();

        assert_eq!(
            sql.querydata.fields,
            vec![String::from("t1.a1"), String::from("t1.a2"), String::from("t1.a3")]
        );
        assert_eq!(sql.querydata.tables, vec![String::from("t1"), String::from("t2")]);
    }

}
