use Response;

pub enum Request {}

impl Request {
    pub fn parse(input: &str) -> Result<Response, String> {
        Ok(Response::OK { msg: input.to_string() })
    }
}
