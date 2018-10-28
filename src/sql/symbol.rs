use std::collections::HashMap;

#[derive(Debug)]
pub struct Symbol<'a> {
    name: &'a str,
    len: usize,
    token: Token,
    group: Group,
}

#[derive(Debug, PartialEq)]
pub enum Group {
    DataType,
    DoubleKeyword,
    MultiKeyword,
    Function,
    Keyword,
    Operator, // >, >=, ==, !=, <, <=
    Number,
    Identifier, // t1, a, b
}

/// Token includes keywords, functions, and data types (by alphabetical order)
#[derive(Debug, PartialEq)]
pub enum Token {
    /* SQL Keywords */
    Add,
    AddConstraint,
    Alter,
    AlterColumn,
    AlterTable,
    All,
    And,
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
    Drop,
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
    Join,
    LeftJoin,
    Like,
    Limit,
    Not,
    NotNull,
    Or,
    OrderBy,
    OuterJoin,
    PrimaryKey,
    Procedure,
    RightJoin,
    Rownum,
    Select,
    SelectDistinct,
    SelectTop,
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
    EQ, // ==
    NE, // !=
    GT, // >
    GE, // >=
}

fn sym(name: &str, token: Token, group: Group) -> Symbol {
    Symbol {
        name,
        len: name.len(),
        token,
        group,
    }
}

lazy_static! {
    /// A static struct of token hashmap storing all tokens
    pub static ref SYMBOLS: HashMap<&'static str, Symbol<'static>> = {
        let mut m = HashMap::new();

        // The following is maintained by hand according to `Token`

        /* SQL Keywords */
        m.insert("add", sym("add", Token::Add, Group::Keyword));
        m.insert("add constraint", sym("add constraint", Token::AddConstraint, Group::DoubleKeyword));
        m.insert("alter", sym("alter", Token::Alter, Group::Keyword));
        m.insert("alter column", sym("alter column", Token::AlterColumn, Group::DoubleKeyword));
        m.insert("alter table", sym("alter table", Token::AlterTable, Group::DoubleKeyword));
        m.insert("all", sym("all", Token::All, Group::Keyword));
        m.insert("and", sym("and", Token::And, Group::Keyword));
        m.insert("any", sym("any", Token::Any, Group::Keyword));
        m.insert("as", sym("as", Token::As, Group::Keyword));
        m.insert("asc", sym("asc", Token::Asc, Group::Keyword));
        m.insert("between", sym("between", Token::Between, Group::Keyword));
        m.insert("case", sym("case", Token::Case, Group::Keyword));
        m.insert("check", sym("check", Token::Check, Group::Keyword));
        m.insert("column", sym("column", Token::Column, Group::Keyword));
        m.insert("constraint", sym("constraint", Token::Constraint, Group::Keyword));
        m.insert("create", sym("create", Token::Create, Group::Keyword));
        m.insert("create database", sym("create database", Token::CreateDatabase, Group::DoubleKeyword));
        m.insert("create index", sym("create index", Token::CreateIndex, Group::DoubleKeyword));
        m.insert("create or replace view", sym("create or replace view", Token::CreateOrReplaceView, Group::MultiKeyword));
        m.insert("create table", sym("create table", Token::CreateTable, Group::DoubleKeyword));
        m.insert("create procedure", sym("create procedure", Token::CreateProcedure, Group::DoubleKeyword));
        m.insert("create unique index", sym("create unique index", Token::CreateUniqueIndex, Group::MultiKeyword));
        m.insert("create view", sym("create view", Token::CreateView, Group::DoubleKeyword));
        m.insert("database", sym("database", Token::Database, Group::Keyword));
        m.insert("default", sym("default", Token::Default, Group::Keyword));
        m.insert("delete", sym("delete", Token::Delete, Group::Keyword));
        m.insert("desc", sym("desc", Token::Desc, Group::Keyword));
        m.insert("distinct", sym("distinct", Token::Distinct, Group::Keyword));
        m.insert("drop", sym("drop", Token::Drop, Group::Keyword));
        m.insert("drop column", sym("drop column", Token::DropColumn, Group::DoubleKeyword));
        m.insert("drop constraint", sym("drop constraint", Token::DropConstraint, Group::DoubleKeyword));
        m.insert("drop database", sym("drop database", Token::DropDatabase, Group::DoubleKeyword));
        m.insert("drop default", sym("drop default", Token::DropDefault, Group::DoubleKeyword));
        m.insert("drop index", sym("drop index", Token::DropIndex, Group::DoubleKeyword));
        m.insert("drop table", sym("drop table", Token::DropTable, Group::DoubleKeyword));
        m.insert("drop view", sym("drop view", Token::DropView, Group::DoubleKeyword));
        m.insert("exec", sym("exec", Token::Exec, Group::Keyword));
        m.insert("exists", sym("exists", Token::Exists, Group::Keyword));
        m.insert("foreign key", sym("foreign key", Token::ForeignKey, Group::DoubleKeyword));
        m.insert("from", sym("from", Token::From, Group::Keyword));
        m.insert("full outer join", sym("full outer join", Token::FullOuterJoin, Group::MultiKeyword));
        m.insert("group by", sym("group by", Token::GroupBy, Group::DoubleKeyword));
        m.insert("having", sym("having", Token::Having, Group::Keyword));
        m.insert("in", sym("in", Token::In, Group::Keyword));
        m.insert("index", sym("index", Token::Index, Group::Keyword));
        m.insert("inner join", sym("inner join", Token::InnerJoin, Group::DoubleKeyword));
        m.insert("insert into", sym("insert into", Token::InsertInto, Group::DoubleKeyword));
        m.insert("is null", sym("is null", Token::IsNull, Group::Keyword));
        m.insert("is not null", sym("is not null", Token::IsNotNull, Group::MultiKeyword));
        m.insert("join", sym("join", Token::Join, Group::Keyword));
        m.insert("left join", sym("left join", Token::LeftJoin, Group::DoubleKeyword));
        m.insert("like", sym("like", Token::Like, Group::Keyword));
        m.insert("limit", sym("limit", Token::Limit, Group::Keyword));
        m.insert("not", sym("not", Token::Not, Group::Keyword));
        m.insert("not null", sym("not null", Token::NotNull, Group::DoubleKeyword));
        m.insert("or", sym("or", Token::Or, Group::Keyword));
        m.insert("order by", sym("order by", Token::OrderBy, Group::DoubleKeyword));
        m.insert("outer join", sym("outer join", Token::OuterJoin, Group::DoubleKeyword));
        m.insert("primary key", sym("primary key", Token::PrimaryKey, Group::DoubleKeyword));
        m.insert("procedure", sym("procedure", Token::Procedure, Group::Keyword));
        m.insert("right join", sym("right join", Token::RightJoin, Group::DoubleKeyword));
        m.insert("rownum", sym("rownum", Token::Rownum, Group::Keyword));
        m.insert("select", sym("select", Token::Select, Group::Keyword));
        m.insert("select distinct", sym("select distinct", Token::SelectDistinct, Group::DoubleKeyword));
        m.insert("select top", sym("select top", Token::SelectTop, Group::DoubleKeyword));
        m.insert("set", sym("set", Token::Set, Group::Keyword));
        m.insert("table", sym("table", Token::Table, Group::Keyword));
        m.insert("top", sym("top", Token::Top, Group::Keyword));
        m.insert("truncate table", sym("truncate table", Token::TruncateTable, Group::DoubleKeyword));
        m.insert("union", sym("union", Token::Union, Group::Keyword));
        m.insert("union all", sym("union all", Token::UnionAll, Group::DoubleKeyword));
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
        m.insert("avg", sym("avg", Token::Avg, Group::DataType));
        m.insert("count", sym("count", Token::Count, Group::DataType));
        m.insert("max", sym("max", Token::Max, Group::DataType));
        m.insert("min", sym("min", Token::Min, Group::DataType));
        m.insert("sum", sym("sum", Token::Sum, Group::DataType));

        /* Operator */
        m.insert(">", sym(">", Token::GT, Group::Operator));
        m.insert(">=", sym(">=", Token::GE, Group::Operator));
        m.insert("==", sym("==", Token::EQ, Group::Operator));
        m.insert("!=", sym("!=", Token::NE, Group::Operator));
        m.insert("<", sym("<", Token::LT, Group::Operator));
        m.insert("<=", sym("<=", Token::LE, Group::Operator));

        m //return m
    };
}

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
