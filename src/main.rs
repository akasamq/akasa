mod config;
mod hook;
mod logger;
mod protocols;
mod server;
mod state;
mod storage;

#[cfg(test)]
mod tests;

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};

use crate::config::Config;
use crate::state::GlobalState;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
/// Akasa MQTT server
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the server
    Start {
        /// The socket address to bind
        #[clap(long, value_name = "IP:PORT", default_value = "127.0.0.1:1883")]
        bind: SocketAddr,

        /// The config file path
        #[clap(long, value_name = "FILE")]
        config: PathBuf,

        /// Async runtime
        #[cfg(target_os = "linux")]
        #[clap(long, default_value_t = Runtime::Glommio, value_enum)]
        runtime: Runtime,
        #[cfg(not(target_os = "linux"))]
        #[clap(long, default_value_t = Runtime::Tokio, value_enum)]
        runtime: Runtime,
    },

    /// Generate default config to stdout
    DefaultConfig {
        /// Allow anonymous user connect
        #[clap(long)]
        allow_anonymous: bool,
    },
}

#[derive(ValueEnum, Clone, Debug)]
enum Runtime {
    #[cfg(target_os = "linux")]
    Glommio,
    Tokio,
}

fn main() -> anyhow::Result<()> {
    logger::init();

    let cli = Cli::parse();
    log::debug!("{:#?}", cli);

    match cli.command {
        Commands::Start {
            bind,
            config,
            runtime,
        } => {
            let config: Config = {
                let content = fs::read_to_string(config)?;
                serde_yaml::from_str(&content)
                    .map_err(|err| anyhow!("invalid config format {}", err))?
            };
            log::debug!("config: {:#?}", config);
            if !config.is_valid() {
                bail!("invalid config");
            }
            log::info!("Listen on {}", bind);
            let global = Arc::new(GlobalState::new(bind, config));
            match runtime {
                #[cfg(target_os = "linux")]
                Runtime::Glommio => server::rt_glommio::start(global)?,
                Runtime::Tokio => server::rt_tokio::start(global)?,
            }
        }
        Commands::DefaultConfig { allow_anonymous } => {
            let config = if allow_anonymous {
                Config::new_allow_anonymous()
            } else {
                Config::default()
            };
            println!("{}", serde_yaml::to_string(&config).expect("serde yaml"));
        }
    }
    Ok(())
}
