use std::io::{self, BufRead};
use std::mem;

use futures::{Poll, Stream};

use AsyncRead;

/// Combinator created by the top-level `message` method which is a stream over
/// the message of text on an I/O object.
#[derive(Debug)]
pub struct Message<A> {
    io: A,
    message: String,
}

/// Creates a new stream from the I/O object given representing the message of
/// input that are found on `A`.
///
/// This method takes an asynchronous I/O object, `a`, and returns a `Stream` of
/// message that the object contains. The returned stream will reach its end once
/// `a` reaches EOF.
pub fn new<A>(a: A) -> Message<A>
where
    A: AsyncRead + BufRead,
{
    Message {
        io: a,
        message: String::new(),
    }
}

impl<A> Stream for Message<A>
where
    A: AsyncRead + BufRead,
{
    type Item = String;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<String>, io::Error> {
        let n = match self.io.read_line(&mut self.message) {
            Ok(t) => t,
            Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                return Ok(::futures::Async::NotReady)
            }
            Err(e) => return Err(e.into()),
        };
        if n == 0 && self.message.len() == 0 {
            return Ok(None.into());
        }
        if self.message.ends_with("\n") {
            self.message.pop();
            if self.message.ends_with("\r") {
                self.message.pop();
            }
        }
        Ok(Some(mem::replace(&mut self.message, String::new())).into())
    }
}
