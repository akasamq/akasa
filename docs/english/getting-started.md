
# Getting Started Guide

## Install dependency

If openssl not install on your system, please follow [the guide](https://docs.rs/openssl/latest/openssl/#automatic) to install it first.

## Build the project

```shell
git clone https://github.com/akasamq/akasa.git && cd akasa
cargo build --release
```

## Run the server

You can show all the subcommands from akasa cli.
```shell
./target/release/akasa --help
# Commands:
#  start            Start the server
#  default-config   Generate default config to stdout
#  insert-password  Insert a password to the password file
#  remove-password  Remove a password from the password file
#  help             Print this message or the help of the given subcommand(s)
```

We need create a config file first, so it can be used to start the server:

```shell
./target/release/akasa default-config > akasa.yaml
```

If you want to enable file based authentication, you need to create a password file and insert a username/password pair to it:

```shell
./target/release/akasa insert-password --create --path ./password.txt --username user1
# Password:
# Repeat Password:
# add/update user=user1 to "./password.txt"
```

Insert more username/password pairs:
```shell
./target/release/akasa insert-password --path ./password.txt --username user2
# Password:
# Repeat Password:
# add/update user=user2 to "./password.txt"
```

Then you need to update the config `auth` section to enable it:
```yaml
auth:
  enable: true
  password_file: ./password.txt
```

The final step is to start the server:
```shell
./target/release/akasa start --config ./akasa.yaml
#[2023-00-00T00:00:00Z INFO  akasa] Listen on Listeners {
#        mqtt: Some(
#            Listener {
#                addr: 127.0.0.1:1883,
#                proxy_mode: None,
#            },
#        ),
#        mqtts: None,
#        ws: None,
#        wss: None,
#    }
#[2023-00-00T00:00:00Z INFO  akasa_core::server::rt] Listen mqtt@127.0.0.1:1883 success!
```
