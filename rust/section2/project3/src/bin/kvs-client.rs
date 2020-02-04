use std::net;
use std::process;

use structopt::StructOpt;

use kvs::{KvsClient, KvsError, Result};

#[derive(Debug, StructOpt)]
enum Config {
    #[structopt(about = "Set the value of a string key to a string")]
    Set {
        #[structopt(help = "A string key", name = "KEY")]
        key: String,
        #[structopt(help = "The string value of the key", name = "VALUE")]
        value: String,
        #[structopt(
            long,
            help = "Set the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: net::SocketAddr,
    },
    #[structopt(about = "Get the string value of a given string key")]
    Get {
        #[structopt(help = "A string key", name = "KEY")]
        key: String,
        #[structopt(
            long,
            help = "Set the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: net::SocketAddr,
    },
    #[structopt(about = "Remove a given key")]
    Rm {
        #[structopt(help = "A string key", name = "KEY")]
        key: String,
        #[structopt(
            long,
            help = "Set the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: net::SocketAddr,
    },
}

fn main() -> Result<()> {
    let config: Config = Config::from_args();

    match config {
        Config::Set { key, value, addr } => {
            let mut client = KvsClient::new(addr)?;
            client.set(key, value)?;
        }
        Config::Get { key, addr } => {
            let mut client = KvsClient::new(addr)?;
            match client.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        Config::Rm { key, addr } => {
            let mut client = KvsClient::new(addr)?;
            match client.remove(key) {
                Ok(()) => {}
                Err(KvsError::RemoteError(err_msg)) => {
                    eprintln!("{}", err_msg);
                    process::exit(1);
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}
