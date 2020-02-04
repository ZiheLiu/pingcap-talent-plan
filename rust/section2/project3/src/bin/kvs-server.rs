use std::fs;
use std::net;

use clap::arg_enum;
use structopt::StructOpt;

use kvs::{KvStore, KvsError, KvsServer, Result};
use log::{info, warn, LevelFilter};
use std::env::current_dir;

#[derive(Debug, StructOpt)]
#[structopt(about = "Start the server")]
struct Config {
    /// Valid socket address `IP:PORT`, where IP address is either v4 or v6.
    #[structopt(
        long,
        help = "Sets the server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: net::SocketAddr,
    /// Valid engine name, either "kvs" or "slde".
    #[structopt(
        long,
        help = "Sets the storage engine",
        value_name = "ENGINE-NAME",
        possible_values = &EngineType::variants(),
        case_insensitive = true,
    )]
    engine: Option<EngineType>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, PartialEq)]
    enum EngineType {
        kvs,
        sled
    }
}

const DEFAULT_ENGINE: EngineType = EngineType::kvs;
const CONFIG_FILENAME: &str = "engine_config";

impl EngineType {
    /// Get the previous engine type from disk.
    ///
    /// If the `CONFIG_FILENAME` file exists and the content is valid `Engine`, return it.
    /// Otherwise, return None.
    ///
    /// # Errors
    ///
    /// It propagates I/O errors.
    fn previous_engine_type() -> Result<Option<EngineType>> {
        let engine_config_path = current_dir()?.join(CONFIG_FILENAME);
        if !engine_config_path.exists() {
            Ok(None)
        } else {
            match fs::read_to_string(engine_config_path)?.parse() {
                Ok(engine) => Ok(Some(engine)),
                Err(e) => {
                    warn!("File {} content is invalid: {}.", CONFIG_FILENAME, e);
                    Ok(None)
                }
            }
        }
    }

    /// Check the given engine with the previous engine and return it.
    ///
    /// If the previous engine exists, the given engine must be same as it.
    /// Otherwise, return `WrongEngineType` error.
    ///
    /// if the previous one does not exist, return the given engine or DEFAULT_ENGINE.
    ///
    /// # Errors
    /// It propagates I/O errors or returns `WrongEngineType` error..
    fn new(engine_op: Option<EngineType>) -> Result<EngineType> {
        match Self::previous_engine_type()? {
            Some(pre_engine) => match engine_op {
                Some(engine) => {
                    if engine != pre_engine {
                        Err(KvsError::WrongEngineType)
                    } else {
                        Ok(pre_engine)
                    }
                }
                None => Ok(pre_engine),
            },
            None => match engine_op {
                Some(engine) => Ok(engine),
                None => Ok(DEFAULT_ENGINE),
            },
        }
    }

    /// Dump the engine config into disk.
    ///
    /// # Errors
    ///
    /// It propagates I/O errors.
    fn dump_config(&self) -> Result<()> {
        let engine_config_path = current_dir()?.join(CONFIG_FILENAME);
        fs::write(engine_config_path, self.to_string())?;
        Ok(())
    }
}

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();

    let config: Config = Config::from_args();
    let engine = EngineType::new(config.engine)?;
    engine.dump_config()?;

    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("Storage Engine: {}", engine);
    info!("Socket Address: {}", config.addr);

    let engine = match engine {
        EngineType::kvs => KvStore::open(current_dir()?)?,
        EngineType::sled => {
            unimplemented!("Sled");
        }
    };
    let mut server = KvsServer::new(config.addr, engine);
    server.start()?;

    Ok(())
}
