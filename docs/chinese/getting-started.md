
# 入门指南

## 安装依赖

如果你的系统里还没安装 openssl, 请先根据[这个指南](https://docs.rs/openssl/latest/openssl/#automatic)安装 openssl.

## 构建项目

```shell
git clone https://github.com/akasamq/akasa.git && cd akasa
cargo build --release
```

## 启动服务器

你可以通过 akasa 命令行工具的 `--help` 选项看到所有的子命令。
```shell
./target/release/akasa --help
# Commands:
#  start            Start the server
#  default-config   Generate default config to stdout
#  insert-password  Insert a password to the password file
#  remove-password  Remove a password from the password file
#  help             Print this message or the help of the given subcommand(s)
```

首先我们需要创建一个配置文件，用来作为之后启动服务器的参数：

```shell
./target/release/akasa default-config > akasa.yaml
```

如果你想启用基于密码文件的认证方式，你需要先创建一个密码文件然后插入一些 username/password:

```shell
./target/release/akasa insert-password --create --path ./password.txt --username user1
# Password:
# Repeat Password:
# add/update user=user1 to "./password.txt"
```

插入更多的 username/password:
```shell
./target/release/akasa insert-password --path ./password.txt --username user2
# Password:
# Repeat Password:
# add/update user=user2 to "./password.txt"
```

然后你需要修改 `auth` 部分来启用它:
```yaml
auth:
  enable: true
  password_file: ./password.txt
```

最后一步就是启动服务器了:
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
#[2023-00-00T00:00:00Z INFO  akasa_core::server::rt] Listen mqtt@127.0.0.1:1883 success! (tokio)
```
