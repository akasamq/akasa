# Akasa (आकाश)

![CI](https://github.com/akasamq/akasa/actions/workflows/rust.yml/badge.svg)

English | [简体中文](README-CN.md) 

Akasa is a high performance, low latency and high extendable MQTT server in Rust.

It uses [glommio][glommio] for high performance and low latency network IO. The underlying MQTT protocol message codec ([mqtt-proto][mqtt-proto]) is carefully crafted for high performance and async environment.

## Features
- [x] Full support MQTT v3.1/v3.1.1/v5.0
- [x] Support TLS (include two-way authentication)
- [x] Support WebSocket (include TLS support)
- [x] Support [Proxy Protocol V2][proxy-protocol]
- [x] Use `io_uring` ([glommio][glommio]) for high performance low latency IO (can use tokio on non-Linux OS)
- [x] Use a [Hook trait][hook-trait] to extend the server
- [x] Simple password file based authentication
- [ ] Raft based cluster (*coming soon*)

## How to Use

```shell
git clone https://github.com/akasamq/akasa.git && cd akasa
# You may also need to install openssl: https://docs.rs/openssl/latest/openssl/#automatic
cargo build --release

./target/release/akasa --help
# Commands:
#  start            Start the server
#  default-config   Generate default config to stdout
#  insert-password  Insert a password to the password file
#  remove-password  Remove a password from the password file
#  help             Print this message or the help of the given subcommand(s)
```

For more documentation:

- [Config Options Explanation](docs/english/config.md)

## Benchmark
TODO

## Testing
Testing is very important for reliable software. Akasa currently include 100+ test cases, those test cases are collected by reading the specification and catch the functional points and limitations.

The underlying codec ([mqtt-proto][mqtt-proto]) also include significant amount of test cases and also some [fuzz][mqtt-proto-fuzz] testing.

## Community

If you have any questions or if you would like to get involved in our community, please check out:

- English Community on [GitHub Discussions][github-group]
- English Community on [Discord][discord-group]
- Chinese Community on [Telegram][telegram-group]
- Chinese Community on QQ Group (862039269)

## License
Akasa is licensed under **MIT** license ([LICENSE](LICENSE))

## Enterprise Edition

Akasa will have an enterprise edition. In this edition, it provides:

- [x] WebAssembly based Rule Engine
  * can run very complex logic (such as [TensorFlow][tensorflow])
  * very high performance compare to scripting language
  * hot reload
- [ ] HTTP API to interact with Akasa
- [ ] Statistics metrics
- [ ] Rate limit
- [ ] Data Integration (route message to MySQL/Kafka/InfluxDB...)


[mqtt-proto]: https://github.com/akasamq/mqtt-proto
[mqtt-proto-fuzz]: https://github.com/akasamq/mqtt-proto/tree/master/fuzz
[proxy-protocol]: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
[glommio]: https://github.com/DataDog/glommio
[bsl]: https://mariadb.com/bsl-faq-mariadb/
[hook-trait]: https://github.com/akasamq/akasa/blob/5ade2d788d9a919671f81b01d720155caf8e4e2d/akasa-core/src/hook.rs#L43
[tensorflow]: https://blog.tensorflow.org/2020/09/supercharging-tensorflowjs-webassembly.html
[github-group]: https://github.com/akasamq/akasa/discussions
[discord-group]: https://discord.gg/Geg7hXWM
[telegram-group]: https://t.me/+UCBpJs-6ddI4MjE1
