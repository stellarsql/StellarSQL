pub enum Response {
    OK { msg: String },
    Error { msg: String },
}

impl Response {
    pub fn serialize(&self) -> String {
        match *self {
            Response::OK { ref msg } => format!("{}\n", msg),
            Response::Error { ref msg } => format!("Error: {}\n", msg),
        }
    }
}
