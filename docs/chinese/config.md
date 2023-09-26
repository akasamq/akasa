

## 配置项说明
```yaml
# 网络监听器
listeners:
  # (可选) 监听 TCP 地址
  mqtt:
    # 绑定的 Socket 地址
    addr: 127.0.0.1:1883
    # (可选) proxy protocol 模式, 可能的选项:
    #    null           : 不启用 proxy protocol
    #    Normal         : 客户端非 TLS, 服务端非 TLS
    #    TlsTermination : 客户端 TLS, proxy 处理 TLS, 服务端非 TLS (会从 proxy protocol header 中读取 host name(SNI))
    proxy_mode: null
  # (可选) 监听 TCP+TLS 地址
  mqtts:
    # 绑定的 Socket 地址
    addr: 127.0.0.1:8883
    # 是否开启 proxy protocol v2
    proxy: false
    # 用来认证客户端的 CA 文件, 如果 `verify_peer` 是 true 这个字段必须填上
    ca_file: /path/to/mqtts.ca
    # key 文件
    key_file: /path/to/mqtts.key
    # cert 文件
    cert_file: /path/to/mqtts.cert
    # 校验客户端的的证书是被信任的
    verify_peer: true
    # 如果在握手阶段客户端没发送它的证书马上终止连接。需要先开启 `verify_peer` 这个配置项才有效.
    fail_if_no_peer_cert: true
  # (同 `listeners.mqtt`) WebSocket 监听器
  ws: null
  # (同 `listeners.mqtts`) WebSocket+TLS 监听器
  wss: null
# 基于密码文件的认证，密码用来校验 connect 数据包中的 username/password 字段
auth:
  enable: true
  # 密码文件, 请使用 insert-password/remove-password 子命令来管理密码
  password_file: /path/to/passwords/file

# (v5.0 专有) 用于 MQTT v5.0 增强认证的 Scram 用户列表
#   生成 hashed password 的方法: https://github.com/akasamq/akasa/blob/8503adb566c46074d57bfea8dbe39a6fd3403e28/akasa-core/src/tests/protocols/mqtt/v5/v500/auth.rs#L21-L24
scram_users:
  user:
    hashed_password: 2a2a2a
    iterations: 4096
    salt: 73616c74
# (v5.0 专有) MQTT v5.0 增强认证支持的 sasl 机制, 当前只支持 `SCRAM-SHA-256`
sasl_mechanisms:
- SCRAM-SHA-256

# 通过 MQTT v3.1 协议连接的时候, 如果设置这个选项为 true 服务器会拒绝所有 client identifier 长度超过 23 字节的连接.
check_v310_client_id_length: false
# (v5.0 专有) 共享订阅模式, 可选项: [Random, HashClientId, HashTopicName]
shared_subscription_mode: Random
# 客户端允许使用的最高 QoS 级别
max_allowed_qos: 2
# 重发消息的超时时间 (单位: 秒)
inflight_timeout: 15
# 最大允许服务端同时发送给客户端的消息数量的默认值
max_inflight_client: 10
# 最大允许客户端同时发送给服务端的消息数量
max_inflight_server: 10
# 最大允许的存储在内存中的待发消息
max_in_mem_pending_messages: 256
# (未使用) 最大允许的存储在数据库中的待发消息
max_in_db_pending_messages: 65536
# (v5.0 专有) 最小允许的 keep alive 值
min_keep_alive: 10
# (v5.0 专有) 最大允许的 keep alive 值
max_keep_alive: 65535
# (v5.0 专有, 未使用)
multiple_subscription_id_in_publish: false
# (v5.0 专有) 可以在 connect packet 中设置的最大的 session expiry interval 值 (单位: 秒)
max_session_expiry_interval: 4294967295
# (v5.0 专有) 客户端限制服务端可以发送的最大 packet 提价 (单位: 字节)
max_packet_size_client: 268435460
# (v5.0 专有) 服务端限制客户端可以发送的最大 packet 体积 (单位: 字节)
max_packet_size_server: 268435460
# (v5.0 专有) publish 消息中 topic alias 的最大值
topic_alias_max: 65535
# (v5.0 专有) 是否支持保留消息
retain_available: true
# (v5.0 专有) 是否支持共享订阅
shared_subscription_available: true
# (v5.0 专有) 是否支持订阅 ID
subscription_id_available: true
# (v5.0 专有) 是否支持通配符订阅
wildcard_subscription_available: true
# 控制哪些 hook 函数被调用
hook:
  enable_before_connect: true
  enable_after_connect: true
  enable_publish: true
  enable_subscribe: true
  enable_unsubscribe: true
```
