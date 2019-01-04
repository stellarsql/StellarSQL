/// Data for `select`
#[derive(Debug)]
pub struct QueryData {
    pub fields: Vec<String>,
    pub tables: Vec<String>,
    pub joins: Vec<Join>,
    pub predicate: Option<Box<Node>>,
    pub group_fields: Vec<String>,
    pub aggregation_fn: Vec<String>,
    pub sort_fields: Vec<String>,
    pub sort_dir: SortDirection,
    pub is_distinct: bool,
    pub top: TopType,
}

impl QueryData {
    pub fn new() -> QueryData {
        QueryData {
            fields: vec![],
            tables: vec![],
            joins: vec![],
            predicate: None,
            group_fields: vec![],
            aggregation_fn: vec![],
            sort_fields: vec![],
            sort_dir: SortDirection::None,
            is_distinct: false,
            top: TopType::None,
        }
    }
}

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum SortDirection {
    Asc,
    Desc,
    None,
}

#[derive(Debug, PartialEq)]
pub enum TopType {
    Percent(f32),
    Number(u32),
    None,
}

#[derive(Debug)]
pub struct Join {
    pub join_type: JoinType,
    pub table: String,
    pub condition: Option<Box<Node>>,
}

impl Join {
    pub fn new(name: &str) -> Join {
        Join {
            join_type: JoinType::get(name).unwrap(),
            table: "".to_string(),
            condition: None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    InnerJoin,
    FullOuterJoin,
    RightJoin,
    LeftJoin,
}

impl JoinType {
    fn get(name: &str) -> Option<JoinType> {
        let t = match name {
            "inner join" => JoinType::InnerJoin,
            "full outer join" => JoinType::FullOuterJoin,
            "left join" => JoinType::LeftJoin,
            "right join" => JoinType::RightJoin,
            _ => return None,
        };
        Some(t)
    }
}

#[derive(Default, Debug)]
pub struct Node {
    pub root: String,
    pub left: Option<Box<Node>>,
    pub right: Option<Box<Node>>,
}

impl Node {
    pub fn new(root: String) -> Node {
        Node {
            root: root,
            ..Default::default()
        }
    }

    pub fn left(mut self, leaf: Node) -> Self {
        self.left = Some(Box::new(leaf));
        self
    }

    pub fn right(mut self, leaf: Node) -> Self {
        self.right = Some(Box::new(leaf));
        self
    }
}
