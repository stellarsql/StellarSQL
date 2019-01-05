//! # StellarSQL
//! A minimal SQL DBMS written in Rust
//!
#[macro_use]
extern crate clap;
#[macro_use]
extern crate dotenv_codegen;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

mod component;
mod connection;
mod manager;
mod sql;
mod storage;

use clap::App;
use std::io::BufReader;
use tokio::io::write_all;

use crate::connection::message;
use crate::connection::request::Request;
use crate::connection::response::Response;
use crate::manager::pool::Pool;
use env_logger;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use std::sync::{Arc, Mutex};

/// The entry of the program
///
/// Use `Tokio` to handle each TCP connection and spawn a thread to handle the request.
fn main() {
    info!("Hello, StellarSQL!");

    // start logger
    env_logger::init();

    // Parse arguments
    let yml = load_yaml!("../cli.yml");
    let m = App::from_yaml(yml).get_matches();

    let port = if let Some(port_) = m.value_of("port") {
        port_
    } else {
        dotenv!("PORT")
    };

    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    lazy_static! {
        static ref mutex: Arc<Mutex<Pool>> = Arc::new(Mutex::new(Pool::new(dotenv!("POOL_SIZE").parse().unwrap())));
    }
    // Bind a TCP listener to the socket address.
    // Note that this is the Tokio TcpListener, which is fully async.
    let listener = TcpListener::bind(&addr).unwrap();

    // The server task asynchronously iterates over and processes each
    // incoming connection.
    let server = listener
        .incoming()
        .for_each(move |socket| {
            let addr = socket.peer_addr().unwrap();
            info!("New Connection: {}", addr);

            // Spawn a task to process the connection
            process(socket, &mutex, addr);

            Ok(())
        })
        .map_err(|err| {
            error!("accept error = {:?}", err);
        });

    info!("StellarSQL running on {} port", port);
    tokio::run(server);
}

/// Process the TCP socket connection
///
/// The request message pass to [`Response`](connection/request/index.html) and get [`Response`](connection/response/index.html)
fn process(socket: TcpStream, mutex: &'static Arc<Mutex<Pool>>, addr: std::net::SocketAddr) {
    let (reader, writer) = socket.split();

    let messages = message::new(BufReader::new(reader));

    let mut requests = Request::new(addr.to_string());

    // note the `move` keyword on the closure here which moves ownership
    // of the reference into the closure, which we'll need for spawning the
    // client below.
    //
    // The `map` function here means that we'll run some code for all
    // requests (lines) we receive from the client. The actual handling here
    // is pretty simple, first we parse the request and if it's valid we
    // generate a response.
    let responses = messages.map(move |message| match Request::parse(&message, &mutex, &mut requests) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: format!("{}", e) },
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
    let connection = writes.then(move |_| {
        // write back
        let mut pool = mutex.lock().unwrap();
        pool.write_back(addr.to_string());
        Ok(())
    });

    // Spawn the task. Internally, this submits the task to a thread pool.
    tokio::spawn(connection);
}
