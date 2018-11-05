use component::table::Table;

#[derive(Debug, Clone)]
pub struct Database {
    name: String,
    tables: Vec<Table>,
}

impl Database {
    fn new(name: &str) -> Database {
        Database {
            name: name.to_string(),
            tables: vec![],
        }
    }
}
