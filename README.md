# akasa (आकाश)
An advanced MQTT server in Rust


## Features
* Full support MQTT v3.1/v3.1.1/v5.0
* Support TLS (mqtt/mqtts)
* Support WebSocket (ws/wss)
* Proxy Protocol
* Using `io_uring` (glommio) for high performance low latency IO (can fallback to tokio on non-linux OS)
* Use a Hook trait to extend the server


## How to Use

```shell
cargo run -- --help
```
