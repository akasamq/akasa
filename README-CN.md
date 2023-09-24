# Akasa (आकाश)
[English](README.md) | 简体中文

Akasa 是一个 Rust 写的高性能，低延迟，高度可扩展的 MQTT 服务器。

Akasa 用 [glommio][glommio] 来实现高性能低延迟的网络 IO. 它底层的 MQTT 协议消息编解码器 ([mqtt-proto][mqtt-proto]) 是为了高性能和 async 环境而精心设计实现的。

## 特性
- [x] 完全支持 MQTT v3.1/v3.1.1/v5.0
- [x] 支持 TLS (包括双向认证)
- [x] 支持 WebSocket (包括 TLS 支持)
- [x] 支持 [Proxy Protocol V2][proxy-protocol]
- [x] 使用 `io_uring` ([glommio][glommio]) 来实现高性能低延迟 IO (非 Linux 环境可以用 tokio)
- [x] 使用 [Hook trait][hook-trait] 来扩展服务器
- [x] 用一个密码文件来支持简单的认证
- [ ] 基于 Raft 的服务器集群 (*敬请期待*)

## 如何使用

```shell
git clone https://github.com/akasamq/akasa.git && cd akasa
cargo build --release

./target/release/akasa --help
# Commands:
#  start            Start the server
#  default-config   Generate default config to stdout
#  insert-password  Insert a password to the password file
#  remove-password  Remove a password from the password file
#  help             Print this message or the help of the given subcommand(s)
```

## 性能测试
TODO

## 正确性测试
对于可靠的软件来说测试是非常重要的工作。Akasa 目前有 100+ 测试用例， 这些测试用例都是通过仔细阅读 MQTT 规格说明书中的功能点和限制点来收集的。

底层的编解码器 ([mqtt-proto][mqtt-proto]) 也包含了相当数量的测试和一些 [fuzz][mqtt-proto-fuzz] 测试。

## License
Akasa 使用 **MIT** license ([LICENSE](LICENSE))

## 企业版
Akasa 会有一个企业版本, 它会提供一个受限的二进制程序作为公开的使用和测试。对于商业用户, Akasa 企业版的源码可以以类似 MariaDB Business Source License ([BSL][bsl]) 来提供， 而且还会提供相应的技术支持。企业版中的额外功能包括:

- [x] 基于 Webassembly 的规则引擎
  * 可以跑非常复杂的业务逻辑 (比如 [TensorFlow][tensorflow])
  * 相对于脚本语言来说有绝对的性能优势
  * 在线更新
- [ ] 和 Akasa 交互的 HTTP API 
- [ ] 统计指标
- [ ] 流量控制
- [ ] 数据集成 (路由数据到 MySQL/Kafka/InfluxDB...)


[mqtt-proto]: https://github.com/akasamq/mqtt-proto
[mqtt-proto-fuzz]: https://github.com/akasamq/mqtt-proto/tree/master/fuzz
[proxy-protocol]: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
[glommio]: https://github.com/DataDog/glommio
[bsl]: https://mariadb.com/bsl-faq-mariadb/
[hook-trait]: https://github.com/akasamq/akasa/blob/5ade2d788d9a919671f81b01d720155caf8e4e2d/akasa-core/src/hook.rs#L43
[tensorflow]: https://blog.tensorflow.org/2020/09/supercharging-tensorflowjs-webassembly.html
