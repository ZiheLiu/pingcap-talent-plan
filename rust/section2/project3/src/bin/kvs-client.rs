use std::process;
use std::env::current_dir;
use std::net;

use structopt::StructOpt;

use kvs::{KvStore, KvsError, Result};

#[derive(Debug, StructOpt)]
enum Config {
    #[structopt(about = "Set the value of a string key to a string")]
    Set {
        #[structopt(help = "A string key", name= "KEY")]
        key: String,
        #[structopt(help = "The string value of the key", name= "VALUE")]
        value: String,
        #[structopt(
            long,
            help = "Set the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str),
        )]
        addr: net::SocketAddr,
    },
    #[structopt(about = "Get the string value of a given string key")]
    Get {
        #[structopt(help = "A string key", name= "KEY")]
        key: String,
        #[structopt(
            long,
            help = "Set the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str),
        )]
        addr: net::SocketAddr,
    },
    #[structopt(about = "Remove a given key")]
    Rm {
        #[structopt(help = "A string key", name= "KEY")]
        key: String,
        #[structopt(
            long,
            help = "Set the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str),
        )]
        addr: net::SocketAddr,
    },
}

fn main() -> Result<()> {
    let config = Config::from_args();

    match config {
        Config::Set { key, value, .. } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?;
        }
        Config::Get { key, .. } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        Config::Rm { key, .. } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("{}", KvsError::KeyNotFound);
                    process::exit(1);
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}
