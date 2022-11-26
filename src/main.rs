mod broker;
mod config;
mod logger;
mod monitor;
mod protocols;
mod route;
mod script_engine;
mod state;
mod storage;

use std::error::Error as StdErr;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
/// Akasa MQTT broker
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the broker
    Start {
        /// The socket address to bind
        #[clap(long, value_name = "IP:PORT", default_value = "127.0.0.1:1883")]
        bind: SocketAddr,

        /// The config file path
        #[clap(long, value_name = "FILE")]
        config: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn StdErr>> {
    logger::init();

    let cli = Cli::parse();
    log::debug!("{:?}", cli);

    match cli.command {
        Commands::Start { bind, config } => broker::start(bind, config)?,
    }
    Ok(())
}
