use std::process;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Config {
    #[structopt(about = "Set the value of a string key to a string")]
    Set {
        #[structopt(help = "A string key")]
        key: String,
        #[structopt(help = "The string value of the key")]
        value: String,
    },
    #[structopt(about = "Get the string value of a given string key")]
    Get {
        #[structopt(help = "A string key")]
        key: String,
    },
    #[structopt(about = "Remove a given key")]
    Rm {
        #[structopt(help = "A string key")]
        key: String,
    },
}

fn main() {
    let config = Config::from_args();

    match config {
        Config::Set { .. } => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        Config::Get { .. } => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        Config::Rm { .. } => {
            eprintln!("unimplemented");
            process::exit(1);
        }
    }
}
