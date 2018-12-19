#[macro_use]
extern crate clap;
#[macro_use]
extern crate dotenv_codegen;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

mod component;
mod connection;
mod sql;
mod storage;

use clap::App;
use std::io::BufReader;
use tokio::io::write_all;

use crate::connection::message;
use crate::connection::request::Request;
use crate::connection::response::Response;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

fn main() {
    println!("Hello, StellarSQL!");

    // Parse arguments
    let yml = load_yaml!("../cli.yml");
    let m = App::from_yaml(yml).get_matches();

    let port = if let Some(port_) = m.value_of("port") {
        port_
    } else {
        dotenv!("PORT")
    };

    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Bind a TCP listener to the socket address.
    // Note that this is the Tokio TcpListener, which is fully async.
    let listener = TcpListener::bind(&addr).unwrap();

    // The server task asynchronously iterates over and processes each
    // incoming connection.
    let server = listener
        .incoming()
        .for_each(move |socket| {
            let addr = socket.peer_addr().unwrap();
            println!("New Connection: {}", addr);

            // Spawn a task to process the connection
            process(socket);

            Ok(())
        })
        .map_err(|err| {
            println!("accept error = {:?}", err);
        });

    println!("StellarSQL running on {} port", port);
    tokio::run(server);
}

fn process(socket: TcpStream) {
    let (reader, writer) = socket.split();

    let messages = message::new(BufReader::new(reader));

    // note the `move` keyword on the closure here which moves ownership
    // of the reference into the closure, which we'll need for spawning the
    // client below.
    //
    // The `map` function here means that we'll run some code for all
    // requests (lines) we receive from the client. The actual handling here
    // is pretty simple, first we parse the request and if it's valid we
    // generate a response.
    let responses = messages.map(move |message| match Request::parse(&message) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    });

    // At this point `responses` is a stream of `Response` types which we
    // now want to write back out to the client. To do that we use
    // `Stream::fold` to perform a loop here, serializing each response and
    // then writing it out to the client.
    let writes = responses.fold(writer, |writer, response| {
        let response = response.serialize().into_bytes();
        write_all(writer, response).map(|(w, _)| w)
    });

    // `spawn` this client to ensure it
    // runs concurrently with all other clients, for now ignoring any errors
    // that we see.
    let connection = writes.then(move |_| Ok(()));

    // Spawn the task. Internally, this submits the task to a thread pool.
    tokio::spawn(connection);
}
