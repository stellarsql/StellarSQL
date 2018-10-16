#[macro_use]
extern crate clap;
#[macro_use]
extern crate dotenv_codegen;

use clap::App;

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
    println!("StellarSQL running on {} port", port);
}
