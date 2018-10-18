#[macro_use]
extern crate clap;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate dotenv_codegen;
extern crate tokio;

use clap::App;
#[macro_use]
use tokio::io;
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
            // Spawn a task to process the connection
            // TODO process()
            Ok(())
        }).map_err(|err| {
            println!("accept error = {:?}", err);
        });

    tokio::run(server);
    println!("StellarSQL running on {} port", port);
}
