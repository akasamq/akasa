# Akasa (आकाश)

![CI](https://github.com/akasamq/akasa/actions/workflows/rust.yml/badge.svg)

English | [简体中文](README-CN.md) 

Akasa is a high performance, low latency and high extendable MQTT server in Rust.

The underlying MQTT protocol message codec ([mqtt-proto][mqtt-proto]) is carefully crafted for high performance and async environment.

## Features
- [x] Full support MQTT v3.1/v3.1.1/v5.0
- [x] Support TLS (include two-way authentication)
- [x] Support WebSocket (include TLS support)
- [x] Support [Proxy Protocol V2][proxy-protocol]
- [x] Use a [Hook trait][hook-trait] to extend the server
- [x] Simple password file based authentication
- [ ] Cluster mode (*coming soon*)

## How to Use

The easist way is use docker:
```shell
docker run --init -it --rm -p 1883:1883 -v "$HOME/local/etc":/opt thewawar/akasa:0.1.1 akasa start --config /opt/akasa-config.yaml
```

Or build from source:
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

More documentation:

- [Getting Started Guide](docs/english/getting-started.md)
- [Config Options Explanation](docs/english/config.md)

## Benchmark
```yaml
# Environment
CPU    : Intel® Xeon® E5-2678 v3 × 48
Memory : 32GB DDR4/2133
System : Arch Linux

# Players
FlashMQ : v1.6.9
  Akasa : v0.1.0
   EMQX : v5.2.1
VerneMQ : v1.13.0

# Connections (clean_session=false)
FlashMQ : 250k connections, 0.9GB memory
  Akasa : 250k connections, 2.8GB memory
   EMQX : 250k connections,   5GB memory
VerneMQ :  50k connections,  20GB memory

# Message/s
FlashMQ : 40k coonections, 600k message/s, 0.6GB memory, CPU  550%
  Akasa : 40k connections, 600k message/s, 0.8GB memory, CPU  580%
   EMQX : 20k connections, 300k message/s, 3.2GB memory, CPU 3000%
VerneMQ : 25k connections, 370k message/s, 6.0GB memory, CPU 2600%
```


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
[bsl]: https://mariadb.com/bsl-faq-mariadb/
[hook-trait]: https://github.com/akasamq/akasa/blob/5ade2d788d9a919671f81b01d720155caf8e4e2d/akasa-core/src/hook.rs#L43
[tensorflow]: https://blog.tensorflow.org/2020/09/supercharging-tensorflowjs-webassembly.html
[github-group]: https://github.com/akasamq/akasa/discussions
[discord-group]: https://discord.gg/Geg7hXWM
[telegram-group]: https://t.me/+UCBpJs-6ddI4MjE1
