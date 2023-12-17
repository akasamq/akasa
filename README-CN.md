# Akasa (आकाश)
[English](README.md) | 简体中文

Akasa 是一个 Rust 写的高性能，低延迟，高度可扩展的 MQTT 服务器。

它底层的 MQTT 协议消息编解码器 ([mqtt-proto][mqtt-proto]) 是为了高性能和 async 环境而精心设计实现的。

## 特性
- [x] 完全支持 MQTT v3.1/v3.1.1/v5.0
- [x] 支持 TLS (包括双向认证)
- [x] 支持 WebSocket (包括 TLS 支持)
- [x] 支持 [Proxy Protocol V2][proxy-protocol]
- [x] 使用 [Hook trait][hook-trait] 来扩展服务器
- [x] 用一个密码文件来支持简单的认证
- [ ] 基于 Raft 的服务器集群 (*敬请期待*)

## 如何使用
最简单的方法是通过 docker 来使用:
```shell
docker run --init -it --rm -p 1883:1883 -v "$HOME/local/etc":/opt thewawar/akasa:0.1.1 akasa start --config /opt/akasa-config.yaml
```
或者你可以直接从源码编译:
```shell
git clone https://github.com/akasamq/akasa.git && cd akasa
# 可能你需要先安装 openssl: https://docs.rs/openssl/latest/openssl/#automatic
cargo build --release

./target/release/akasa --help
# Commands:
#  start            Start the server
#  default-config   Generate default config to stdout
#  insert-password  Insert a password to the password file
#  remove-password  Remove a password from the password file
#  help             Print this message or the help of the given subcommand(s)
```

更多文档:

- [入门指南](docs/chinese/getting-started.md)
- [配置项说明](docs/chinese/config.md)

## 性能测试
```yaml
# 测试环境
CPU    : Intel® Xeon® E5-2678 v3 × 48
Memory : 32GB DDR4/2133
System : Arch Linux

# 参与者
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

## 正确性测试
对于可靠的软件来说测试是非常重要的工作。Akasa 目前有 100+ 测试用例， 这些测试用例都是通过仔细阅读 MQTT 规格说明书中的功能点和限制点来收集的。

底层的编解码器 ([mqtt-proto][mqtt-proto]) 也包含了相当数量的测试和一些 [fuzz][mqtt-proto-fuzz] 测试。

## 社区

如果你有任何问题或者想参与到我们的社区中来，可以通过以下方式进入：

- [GitHub][github-group] 英文社区
- [Discord][discord-group] 英文社区
- [Telegram][telegram-group] 中文社区
- QQ 群 (号码：862039269)

## License
Akasa 使用 **MIT** license ([LICENSE](LICENSE))

## 企业版
Akasa 会有一个企业版本，企业版中的额外功能包括:

- [x] 基于 WebAssembly 的规则引擎
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
[bsl]: https://mariadb.com/bsl-faq-mariadb/
[hook-trait]: https://github.com/akasamq/akasa/blob/5ade2d788d9a919671f81b01d720155caf8e4e2d/akasa-core/src/hook.rs#L43
[tensorflow]: https://blog.tensorflow.org/2020/09/supercharging-tensorflowjs-webassembly.html
[github-group]: https://github.com/akasamq/akasa/discussions
[discord-group]: https://discord.gg/Geg7hXWM
[telegram-group]: https://t.me/+UCBpJs-6ddI4MjE1
