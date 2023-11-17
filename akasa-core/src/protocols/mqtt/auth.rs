use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::num::NonZeroU32;

use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
use hashbrown::HashMap;
use parking_lot::RwLock;
use ring::{
    digest::{Context, SHA256, SHA256_OUTPUT_LEN, SHA512, SHA512_OUTPUT_LEN},
    pbkdf2::{self, PBKDF2_HMAC_SHA256, PBKDF2_HMAC_SHA512},
};

use crate::state::{AuthPassword, HashAlgorithm};

pub const MIN_SALT_LEN: usize = 12;

pub fn load_passwords<R: Read>(input: R) -> io::Result<RwLock<HashMap<String, AuthPassword>>> {
    let reader = BufReader::with_capacity(2048, input);
    let passwords = RwLock::new(HashMap::new());
    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        let text = line.trim();
        let parse_iterations = |s: &str| -> io::Result<NonZeroU32> {
            match s.parse() {
                Ok(raw) => {
                    if let Some(n) = NonZeroU32::new(raw) {
                        Ok(n)
                    } else {
                        log::error!(
                            "invalid hash algorithm iterations(line:#{}): {}",
                            line_num,
                            line
                        );
                        Err(io::ErrorKind::InvalidData.into())
                    }
                }
                Err(_) => {
                    log::error!(
                        "invalid hash algorithm iterations(line:#{}): {}",
                        line_num,
                        line
                    );
                    Err(io::ErrorKind::InvalidData.into())
                }
            }
        };
        let parse_salt = |s: &str| -> io::Result<Vec<u8>> {
            match STANDARD_NO_PAD.decode(s) {
                Ok(salt) => {
                    if salt.len() < MIN_SALT_LEN {
                        log::error!("password salt not enough(line#{}): {}", line_num, line);
                        Err(io::ErrorKind::InvalidData.into())
                    } else {
                        Ok(salt)
                    }
                }
                Err(_err) => {
                    log::error!("invalid password salt(line#{}): {}", line_num, line);
                    Err(io::ErrorKind::InvalidData.into())
                }
            }
        };
        let parse_hashed_password = |s: &str| -> io::Result<Vec<u8>> {
            match STANDARD_NO_PAD.decode(s) {
                Ok(pass) => Ok(pass),
                Err(_err) => {
                    log::error!("invalid hashed password (line#{}): {}", line_num, line);
                    Err(io::ErrorKind::InvalidData.into())
                }
            }
        };

        if text.is_empty() {
            log::debug!("skip empty password line: #{}", line_num);
        } else if let Some((username, password)) = text.rsplit_once(':') {
            let passwd_parts: Vec<_> = password.split(',').collect();
            if passwd_parts.len() < 3 {
                log::error!("invalid password part (line#{}): {}", line_num, line);
                return Err(io::ErrorKind::InvalidData.into());
            }
            let hash_algorithm = match passwd_parts[0] {
                "sha256" => HashAlgorithm::Sha256,
                "sha512" => HashAlgorithm::Sha512,
                "sha256-pkbdf2" => {
                    let iterations = parse_iterations(passwd_parts[1])?;
                    HashAlgorithm::Sha256Pkbdf2 { iterations }
                }
                "sha512-pkbdf2" => {
                    let iterations = parse_iterations(passwd_parts[1])?;
                    HashAlgorithm::Sha512Pkbdf2 { iterations }
                }
                algo => {
                    log::error!(
                        "invalid password hash algorithm (line#{}): {}",
                        line_num,
                        algo
                    );
                    return Err(io::ErrorKind::InvalidData.into());
                }
            };
            let (salt, hashed_password) = match hash_algorithm {
                HashAlgorithm::Sha256 | HashAlgorithm::Sha512 => {
                    if passwd_parts.len() != 3 {
                        log::error!("invalid password line(#{}): {}", line_num, line);
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                    let salt = parse_salt(passwd_parts[1])?;
                    let hashed_password = parse_hashed_password(passwd_parts[2])?;
                    (salt, hashed_password)
                }
                HashAlgorithm::Sha256Pkbdf2 { .. } | HashAlgorithm::Sha512Pkbdf2 { .. } => {
                    if passwd_parts.len() != 4 {
                        log::error!("invalid password line(#{}): {}", line_num, line);
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                    let salt = parse_salt(passwd_parts[2])?;
                    let hashed_password = parse_hashed_password(passwd_parts[3])?;
                    (salt, hashed_password)
                }
            };
            let item = AuthPassword {
                hash_algorithm,
                hashed_password,
                salt,
            };
            passwords.write().insert(username.to_owned(), item);
        } else {
            log::error!("invalid password line(#{}): {}", line_num, line);
            return Err(io::ErrorKind::InvalidData.into());
        }
    }
    Ok(passwords)
}

pub fn dump_passwords<W: Write>(
    output: W,
    passwords: &RwLock<HashMap<String, AuthPassword>>,
) -> io::Result<()> {
    let mut writer = BufWriter::new(output);
    for item in passwords.read().iter() {
        let username = item.0;
        let AuthPassword {
            hash_algorithm,
            hashed_password,
            salt,
        } = item.1;
        let base64_hashed_password = STANDARD_NO_PAD.encode(hashed_password);
        let base64_salt = STANDARD_NO_PAD.encode(salt);
        let line = match hash_algorithm {
            HashAlgorithm::Sha256 => {
                format!("{username}:sha256,{base64_salt},{base64_hashed_password}\n")
            }
            HashAlgorithm::Sha512 => {
                format!("{username}:sha512,{base64_salt},{base64_hashed_password}\n")
            }
            HashAlgorithm::Sha256Pkbdf2 { iterations } => {
                format!("{username}:sha256-pkbdf2,{iterations},{base64_salt},{base64_hashed_password}\n")
            }
            HashAlgorithm::Sha512Pkbdf2 { iterations } => {
                format!("{username}:sha512-pkbdf2,{iterations},{base64_salt},{base64_hashed_password}\n")
            }
        };
        writer.write_all(line.as_bytes())?;
    }
    writer.flush()
}

pub fn check_password(
    passwords: &RwLock<HashMap<String, AuthPassword>>,
    username: &str,
    password: &[u8],
) -> bool {
    if let Some(item) = passwords.read().get(username) {
        match item.hash_algorithm {
            HashAlgorithm::Sha256 => {
                let mut ctx = Context::new(&SHA256);
                ctx.update(password);
                ctx.update(&item.salt);
                ctx.finish().as_ref() == item.hashed_password
            }
            HashAlgorithm::Sha512 => {
                let mut ctx = Context::new(&SHA512);
                ctx.update(password);
                ctx.update(&item.salt);
                ctx.finish().as_ref() == item.hashed_password
            }
            HashAlgorithm::Sha256Pkbdf2 { iterations } => pbkdf2::verify(
                PBKDF2_HMAC_SHA256,
                iterations,
                &item.salt,
                password,
                &item.hashed_password,
            )
            .is_ok(),
            HashAlgorithm::Sha512Pkbdf2 { iterations } => pbkdf2::verify(
                PBKDF2_HMAC_SHA512,
                iterations,
                &item.salt,
                password,
                &item.hashed_password,
            )
            .is_ok(),
        }
    } else {
        false
    }
}

pub fn hash_password(hash_algorithm: HashAlgorithm, salt: &[u8], password: &[u8]) -> Vec<u8> {
    match hash_algorithm {
        HashAlgorithm::Sha256 => {
            let mut ctx = Context::new(&SHA256);
            ctx.update(password);
            ctx.update(salt);
            ctx.finish().as_ref().to_vec()
        }
        HashAlgorithm::Sha512 => {
            let mut ctx = Context::new(&SHA512);
            ctx.update(password);
            ctx.update(salt);
            ctx.finish().as_ref().to_vec()
        }
        HashAlgorithm::Sha256Pkbdf2 { iterations } => {
            let mut output = vec![0u8; SHA256_OUTPUT_LEN];
            pbkdf2::derive(PBKDF2_HMAC_SHA256, iterations, salt, password, &mut output);
            output
        }
        HashAlgorithm::Sha512Pkbdf2 { iterations } => {
            let mut output = vec![0u8; SHA512_OUTPUT_LEN];
            pbkdf2::derive(PBKDF2_HMAC_SHA512, iterations, salt, password, &mut output);
            output
        }
    }
}
