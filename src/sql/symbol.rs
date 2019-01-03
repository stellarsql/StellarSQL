use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Symbol {
    pub name: String,
    pub len: usize,
    pub token: Token,
    pub group: Group,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Group {
    DataType,
    Function,
    Keyword,
    Operator,   // >, >=, =, !=, <>, <, <=
    Identifier, // t1, a, b
    Delimiter,  // `,`, (, )
}

/// Token includes keywords, functions, and data types (by alphabetical order)
#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    /* SQL Keywords */
    Add,
    AddConstraint,
    AlterColumn,
    AlterTable,
    All,
    Any,
    As,
    Asc,
    Between,
    Case,
    Check,
    Column,
    Constraint,
    Create,
    CreateDatabase,
    CreateIndex,
    CreateOrReplaceView,
    CreateTable,
    CreateProcedure,
    CreateUniqueIndex,
    CreateView,
    Database,
    Default,
    Delete,
    Desc,
    Distinct,
    DropColumn,
    DropConstraint,
    DropDatabase,
    DropDefault,
    DropIndex,
    DropTable,
    DropView,
    Exec,
    Exists,
    ForeignKey,
    From,
    FullOuterJoin,
    GroupBy,
    Having,
    In,
    Index,
    InnerJoin,
    InsertInto,
    IsNull,
    IsNotNull,
    LeftJoin,
    Like,
    Limit,
    NotNull,
    OrderBy,
    Percent,
    PrimaryKey,
    Procedure,
    RightJoin,
    Rownum,
    Select,
    Set,
    Table,
    Top,
    TruncateTable,
    Union,
    UnionAll,
    Unique,
    Update,
    Values,
    View,
    Where,

    /* SQL Function */
    Avg,
    Count,
    Max,
    Min,
    Sum,

    /* SQL Data Type */
    Char,
    Double,
    Float,
    Int,
    Varchar,

    /* Operator */
    LT, // <
    LE, // <=
    EQ, // =
    NE, // !=, <>
    GT, // >
    GE, // >=
    AND,
    NOT,
    OR,

    /* Delimiter */
    ParentLeft,  // (
    ParentRight, // )
    Comma,       // ,
    Semicolon,   // ;

    /* Any Identifier */
    Identifier,

    /* Define by StellarSQL */
    Encrypt,
}

pub fn sym(name: &str, token: Token, group: Group) -> Symbol {
    Symbol {
        name: name.to_string(),
        len: name.len(),
        token,
        group,
    }
}

lazy_static! {
    /// A static struct of token hashmap storing all tokens
    pub static ref SYMBOLS: HashMap<&'static str, Symbol> = {
        let mut m = HashMap::new();

        // The following is maintained by hand according to `Token`

        /* SQL Keywords */
        m.insert("add", sym("add", Token::Add, Group::Keyword));
        m.insert("add constraint", sym("add constraint", Token::AddConstraint, Group::Keyword));
        m.insert("alter column", sym("alter column", Token::AlterColumn, Group::Keyword));
        m.insert("alter table", sym("alter table", Token::AlterTable, Group::Keyword));
        m.insert("all", sym("all", Token::All, Group::Keyword));
        m.insert("any", sym("any", Token::Any, Group::Keyword));
        m.insert("as", sym("as", Token::As, Group::Keyword));
        m.insert("asc", sym("asc", Token::Asc, Group::Keyword));
        m.insert("between", sym("between", Token::Between, Group::Keyword));
        m.insert("case", sym("case", Token::Case, Group::Keyword));
        m.insert("check", sym("check", Token::Check, Group::Keyword));
        m.insert("column", sym("column", Token::Column, Group::Keyword));
        m.insert("constraint", sym("constraint", Token::Constraint, Group::Keyword));
        m.insert("create", sym("create", Token::Create, Group::Keyword));
        m.insert("create database", sym("create database", Token::CreateDatabase, Group::Keyword));
        m.insert("create index", sym("create index", Token::CreateIndex, Group::Keyword));
        m.insert("create or replace view", sym("create or replace view", Token::CreateOrReplaceView, Group::Keyword));
        m.insert("create table", sym("create table", Token::CreateTable, Group::Keyword));
        m.insert("create procedure", sym("create procedure", Token::CreateProcedure, Group::Keyword));
        m.insert("create unique index", sym("create unique index", Token::CreateUniqueIndex, Group::Keyword));
        m.insert("create view", sym("create view", Token::CreateView, Group::Keyword));
        m.insert("database", sym("database", Token::Database, Group::Keyword));
        m.insert("default", sym("default", Token::Default, Group::Keyword));
        m.insert("delete", sym("delete", Token::Delete, Group::Keyword));
        m.insert("desc", sym("desc", Token::Desc, Group::Keyword));
        m.insert("distinct", sym("distinct", Token::Distinct, Group::Keyword));
        m.insert("drop column", sym("drop column", Token::DropColumn, Group::Keyword));
        m.insert("drop constraint", sym("drop constraint", Token::DropConstraint, Group::Keyword));
        m.insert("drop database", sym("drop database", Token::DropDatabase, Group::Keyword));
        m.insert("drop default", sym("drop default", Token::DropDefault, Group::Keyword));
        m.insert("drop index", sym("drop index", Token::DropIndex, Group::Keyword));
        m.insert("drop table", sym("drop table", Token::DropTable, Group::Keyword));
        m.insert("drop view", sym("drop view", Token::DropView, Group::Keyword));
        m.insert("exec", sym("exec", Token::Exec, Group::Keyword));
        m.insert("exists", sym("exists", Token::Exists, Group::Keyword));
        m.insert("foreign key", sym("foreign key", Token::ForeignKey, Group::Keyword));
        m.insert("from", sym("from", Token::From, Group::Keyword));
        m.insert("full outer join", sym("full outer join", Token::FullOuterJoin, Group::Keyword));
        m.insert("group by", sym("group by", Token::GroupBy, Group::Keyword));
        m.insert("having", sym("having", Token::Having, Group::Keyword));
        m.insert("in", sym("in", Token::In, Group::Keyword));
        m.insert("index", sym("index", Token::Index, Group::Keyword));
        m.insert("inner join", sym("inner join", Token::InnerJoin, Group::Keyword));
        m.insert("insert into", sym("insert into", Token::InsertInto, Group::Keyword));
        m.insert("is null", sym("is null", Token::IsNull, Group::Keyword));
        m.insert("is not null", sym("is not null", Token::IsNotNull, Group::Keyword));
        m.insert("left join", sym("left join", Token::LeftJoin, Group::Keyword));
        m.insert("like", sym("like", Token::Like, Group::Keyword));
        m.insert("limit", sym("limit", Token::Limit, Group::Keyword));
        m.insert("not null", sym("not null", Token::NotNull, Group::Keyword));
        m.insert("order by", sym("order by", Token::OrderBy, Group::Keyword));
        m.insert("percent", sym("percent", Token::Percent, Group::Keyword));
        m.insert("primary key", sym("primary key", Token::PrimaryKey, Group::Keyword));
        m.insert("procedure", sym("procedure", Token::Procedure, Group::Keyword));
        m.insert("right join", sym("right join", Token::RightJoin, Group::Keyword));
        m.insert("rownum", sym("rownum", Token::Rownum, Group::Keyword));
        m.insert("select", sym("select", Token::Select, Group::Keyword));
        m.insert("set", sym("set", Token::Set, Group::Keyword));
        m.insert("table", sym("table", Token::Table, Group::Keyword));
        m.insert("top", sym("top", Token::Top, Group::Keyword));
        m.insert("truncate table", sym("truncate table", Token::TruncateTable, Group::Keyword));
        m.insert("union", sym("union", Token::Union, Group::Keyword));
        m.insert("union all", sym("union all", Token::UnionAll, Group::Keyword));
        m.insert("unique", sym("unique", Token::Unique, Group::Keyword));
        m.insert("update", sym("update", Token::Update, Group::Keyword));
        m.insert("values", sym("values", Token::Values, Group::Keyword));
        m.insert("view", sym("view", Token::View, Group::Keyword));
        m.insert("where", sym("where", Token::Where, Group::Keyword));

        /* SQL Function */
        m.insert("avg", sym("avg", Token::Avg, Group::Function));
        m.insert("count", sym("count", Token::Count, Group::Function));
        m.insert("max", sym("max", Token::Max, Group::Function));
        m.insert("min", sym("min", Token::Min, Group::Function));
        m.insert("sum", sym("sum", Token::Sum, Group::Function));

        /* SQL Data Type */
        m.insert("char", sym("char", Token::Char, Group::DataType));
        m.insert("double", sym("double", Token::Double, Group::DataType));
        m.insert("float", sym("float", Token::Float, Group::DataType));
        m.insert("int", sym("int", Token::Int, Group::DataType));
        m.insert("varchar", sym("varchar", Token::Varchar, Group::DataType));

        /* Operator */
        m.insert(">", sym(">", Token::GT, Group::Operator));
        m.insert(">=", sym(">=", Token::GE, Group::Operator));
        m.insert("=", sym("=", Token::EQ, Group::Operator));
        m.insert("!=", sym("!=", Token::NE, Group::Operator));
        m.insert("<>", sym("<>", Token::NE, Group::Operator));
        m.insert("<", sym("<", Token::LT, Group::Operator));
        m.insert("<=", sym("<=", Token::LE, Group::Operator));
        m.insert("and", sym("and", Token::AND, Group::Operator));
        m.insert("not", sym("not", Token::NOT, Group::Operator));
        m.insert("or", sym("or", Token::OR, Group::Operator));

        /* StellarSQL */
        m.insert("encrypt", sym("encrypt", Token::Encrypt, Group::Keyword));

        m //return m
    };
}

impl Symbol {
    pub fn match_delimiter(ch: char) -> Option<Symbol> {
        match ch {
            '(' => Some(sym("(", Token::ParentLeft, Group::Delimiter)),
            ')' => Some(sym(")", Token::ParentRight, Group::Delimiter)),
            ',' => Some(sym(",", Token::Comma, Group::Delimiter)),
            ';' => Some(sym(";", Token::Semicolon, Group::Delimiter)),
            _ => None,
        }
    }
}

/// Check if the word is the first word of any multi-word keywords, and then
/// return how many words of all possible keywords.
/// ex: `alter` could be `alter table` and `alter column`, so return `Some(vec![2])`
///     `is` could be `is null` and `is not null`, so return `Some(vec![2, 3])`
pub fn check_multi_keywords_front(s: &str) -> Option<Vec<u32>> {
    match s {
        "add" => Some(vec![2]),
        "alter" => Some(vec![2]),
        "create" => Some(vec![2, 3, 4]),
        "drop" => Some(vec![2]),
        "foreign" => Some(vec![2]),
        "full" => Some(vec![2]),
        "group" => Some(vec![2]),
        "inner" => Some(vec![2]),
        "insert" => Some(vec![2]),
        "is" => Some(vec![2, 3]),
        "left" => Some(vec![2]),
        "not" => Some(vec![2]),
        "order" => Some(vec![2]),
        "outer" => Some(vec![2]),
        "primary" => Some(vec![2]),
        "right" => Some(vec![2]),
        "select" => Some(vec![2]),
        "truncate" => Some(vec![2]),
        "union" => Some(vec![2]),
        _ => return None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test if `SYMBOLS` initialize.
    #[test]
    fn test_symbols() {
        let s = SYMBOLS.get("add").unwrap();
        assert_eq!(s.name, "add");
        assert_eq!(s.len, 3);
        assert_eq!(s.token, Token::Add);
        assert_eq!(s.group, Group::Keyword);
        let s = SYMBOLS.get(">").unwrap();
        assert_eq!(s.name, ">");
        assert_eq!(s.len, 1);
        assert_eq!(s.token, Token::GT);
        assert_eq!(s.group, Group::Operator);
    }

    #[test]
    fn test_match_delimiter() {
        let mut chs = "){".chars();
        let x = chs.next().unwrap();
        let s = Symbol::match_delimiter(x).unwrap();
        assert_eq!(s.token, Token::ParentRight);
        let x = chs.next().unwrap();
        assert!(Symbol::match_delimiter(x).is_none());
    }

    #[test]
    fn test_check_multi_keywords_front() {
        assert_eq!(check_multi_keywords_front("alter"), Some(vec![2]));
        assert!(check_multi_keywords_front("not_match").is_none());
    }
}
