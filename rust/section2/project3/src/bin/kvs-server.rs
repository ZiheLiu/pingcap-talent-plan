use std::net;

use clap::arg_enum;
use structopt::StructOpt;

use kvs::{Result};

#[derive(Debug, StructOpt)]
#[structopt(about = "Start the server")]
struct Config {
    /// Valid socket address `IP:PORT`, where IP address is either v4 or v6.
    #[structopt(
        long,
        help = "Sets the server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str),
    )]
    addr: net::SocketAddr,
    /// Valid engine name, either "kvs" or "slde".
    #[structopt(
        long,
        help = "Sets the storage engine",
        value_name = "ENGINE-NAME",
        possible_values = &Engine::variants(),
        case_insensitive = true,
    )]
    engine: Option<Engine>,
}

arg_enum! {
    #[derive(Debug)]
    enum Engine {
        Kvs,
        Slde
    }
}

fn main() -> Result<()> {
    let config = Config::from_args();
    println!("{:?}", config);

    Ok(())
}
