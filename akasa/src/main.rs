mod default_hook;
mod logger;

use std::fs;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;

use akasa_core::{
    auth::{Auth, Claims},
    dump_passwords, hash_password, load_passwords, server, AuthPassword, Config, GlobalState,
    HashAlgorithm as CoreHashAlgorithm, MIN_SALT_LEN,
};
use anyhow::{anyhow, bail, Context};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use dashmap::DashMap;
use rand::{rngs::OsRng, RngCore};
use serde::de::DeserializeOwned;

use default_hook::DefaultHook;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
        /// The config file path
        #[clap(long, value_name = "FILE")]
        config: PathBuf,
    },

    /// Generate default config to stdout
    DefaultConfig {
        /// Allow anonymous user connect
        #[clap(long)]
        allow_anonymous: bool,
    },

    /// Insert a password to the password file
    InsertPassword {
        /// The password file path
        #[clap(long, value_name = "FILE")]
        path: PathBuf,

        /// Create new password file, this overwrite existing file.
        #[clap(long)]
        create: bool,

        /// The user name
        #[clap(long, value_name = "STRING")]
        username: String,

        /// The password hash algorithm
        #[clap(long, default_value_t = HashAlgorithm::Sha512Pkbdf2, value_enum)]
        hash_algorithm: HashAlgorithm,

        /// When use pkbdf2 algorithm, this argument specific the iterations
        #[clap(long, value_name = "NUM", default_value = "128")]
        iterations: Option<NonZeroU32>,
    },

    /// Remove a password from the password file
    RemovePassword {
        /// The password file path
        #[clap(long, value_name = "FILE")]
        path: PathBuf,

        /// The user name
        #[clap(long, value_name = "STRING")]
        username: String,
    },

    /// Generate jwt token
    JwtGen {
        /// The secrets file path
        #[clap(long, value_name = "FILE")]
        path: PathBuf,

        /// The username
        #[clap(long, visible_alias = "sub", value_name = "STRING")]
        username: Option<String>,

        /// The superuser
        #[clap(long, action = ArgAction::SetTrue)]
        superuser: bool,
    },
}

#[derive(ValueEnum, Clone, Debug)]
enum HashAlgorithm {
    Sha256,
    Sha512,
    Sha256Pkbdf2,
    Sha512Pkbdf2,
}

fn load<T: DeserializeOwned>(f: &PathBuf) -> anyhow::Result<T> {
    let s = fs::read_to_string(f).context(format!("read {f:?}"))?;
    let r = serde_yaml::from_str(&s).context(format!("parse {f:?}"))?;
    Ok(r)
}

fn main() -> anyhow::Result<()> {
    logger::init();

    let cli = Cli::parse();
    log::debug!("{:#?}", cli);

    match cli.command {
        Commands::Start { config } => {
            let config: Config = {
                let content = fs::read_to_string(config)?;
                serde_yaml::from_str(&content)
                    .map_err(|err| anyhow!("invalid config format {}", err))?
            };
            log::debug!("config: {:#?}", config);
            if !config.is_valid() {
                bail!("invalid config");
            }
            log::info!("Listen on {:#?}", config.listeners);
            let hook_handler = DefaultHook;
            let mut auth = Auth::default();
            if config.auth.enable {
                let path = config.auth.password_file.as_ref().expect("pass file");
                let file =
                    fs::File::open(path).map_err(|err| anyhow!("load passwords: {}", err))?;
                auth.update_passwords(load_passwords(file)?);
            }
            if let Some(ref f) = config.auth.jwt.secrets_file {
                let secrets = load(f).context(format!("Jwt secrets load error {f:?}"))?;
                auth.update_jwt(&secrets);
            }
            let mut global_state = GlobalState::new(config);
            global_state.auth = auth;
            let global = Arc::new(global_state);
            server::rt::start(hook_handler, global)?;
        }
        Commands::DefaultConfig { allow_anonymous } => {
            let config = if allow_anonymous {
                Config::new_allow_anonymous()
            } else {
                Config::default()
            };
            println!("{}", serde_yaml::to_string(&config).expect("serde yaml"));
        }
        Commands::InsertPassword {
            path,
            create,
            username,
            hash_algorithm,
            iterations,
        } => {
            let password = rpassword::prompt_password("Password: ")?;
            let re_password = rpassword::prompt_password("Repeat Password: ")?;
            if password != re_password {
                bail!("repeat password not match!");
            }
            let hash_algorithm = match hash_algorithm {
                HashAlgorithm::Sha256 => CoreHashAlgorithm::Sha256,
                HashAlgorithm::Sha512 => CoreHashAlgorithm::Sha512,
                HashAlgorithm::Sha256Pkbdf2 => {
                    let iterations =
                        iterations.ok_or_else(|| anyhow!("missing iterations argument"))?;
                    CoreHashAlgorithm::Sha256Pkbdf2 { iterations }
                }
                HashAlgorithm::Sha512Pkbdf2 => {
                    let iterations =
                        iterations.ok_or_else(|| anyhow!("missing iterations argument"))?;
                    CoreHashAlgorithm::Sha512Pkbdf2 { iterations }
                }
            };
            let mut salt = vec![0u8; MIN_SALT_LEN];
            OsRng.fill_bytes(&mut salt);
            let hashed_password = hash_password(hash_algorithm, &salt, password.as_bytes());

            let auth_passwords = if create {
                DashMap::new()
            } else {
                load_passwords(&fs::File::open(&path)?)?
            };

            auth_passwords.insert(
                username.clone(),
                AuthPassword {
                    hash_algorithm,
                    hashed_password,
                    salt,
                },
            );
            let file = fs::File::create(&path)?;
            dump_passwords(&file, &auth_passwords)?;
            println!("add/update user={username} to {path:?}");
        }
        Commands::RemovePassword { path, username } => {
            let auth_passwords = load_passwords(&fs::File::open(&path)?)?;
            if auth_passwords.remove(&username).is_some() {
                let file = fs::File::create(&path)?;
                dump_passwords(&file, &auth_passwords)?;
                println!("removed user={username} from {path:?}");
            } else {
                println!("user={username} not found");
            }
        }
        Commands::JwtGen {
            path: ref f,
            username,
            superuser,
        } => {
            let mut auth = Auth::default();
            let secrets = load(f).context(format!("Jwt secrets load error {f:?}"))?;
            auth.update_jwt(&secrets);
            let mut claims = Claims::default();
            claims.sub = username.unwrap_or_default();
            claims.superuser = superuser;
            let token = auth.jwt.encode(claims)?;
            println!("JWT: {token}");
        }
    }
    Ok(())
}
