

## Config Options Explanation
```yaml
# Network Listeners
listeners:
  # (optional) Listen on TCP socket
  mqtt:
    # The socket address to bind
    addr: 127.0.0.1:1883
    # Allows the socket to bind to an in-use port to increase connection accept speed
    reuse_port: false
    # (optional) proxy protocol mode, can be:
    #    null           : Disable proxy protocol
    #    Normal         : Client side non-TLS, server side non-TLS
    #    TlsTermination : Client side TLS, proxy handle TLS, server side non-TLS (read host name(SNI) from proxy protocol header)
    proxy_mode: null
  # (optional) Listen on TCP socket with TLS
  mqtts:
    # The socket address to bind
    addr: 127.0.0.1:8883
    # Allows the socket to bind to an in-use port to increase connection accept speed
    reuse_port: false
    # Enable proxy protocol v2 or not
    proxy: false
    # This CA file is for verify client certificate, if `verify_peer` is true this field MUST be presented.
    ca_file: /path/to/mqtts.ca
    # key file
    key_file: /path/to/mqtts.key
    # cert file
    cert_file: /path/to/mqtts.cert
    # Verifies that the client’s certificate is trusted.
    verify_peer: true
    # Abort the handshake if the client did not send a certificate. This should be paired with `verify_peer`.
    fail_if_no_peer_cert: true
  # (same with `listeners.mqtt`) WebSocket listener
  ws: null
  # (same with `listeners.mqtts`) WebSocket with TLS listener
  wss: null
# Password file based authentication, the config used to check username/password fields in connect packet.
auth:
  enable: true
  # The password file, please use insert-password/remove-password subcommand to manage the passwords
  password_file: /path/to/passwords/file
  # (optional) Use JWT for authorization instead of password
  jwt:
    # The file with secrets
    secrets_file: /path/to/jwt.yaml
    # File contains secrets map
    # default:  # name of secret
    #   alg: HS512
    #   secret: *****
    # my_old_deprecated_secret:  # drop after 2035-03-12
    #   alg: HS256
    #   secret: *****

# (v5.0 only) Scram users used in MQTT v5.0 enhanced authentication.
#   To generate the hashed password: https://github.com/akasamq/akasa/blob/8503adb566c46074d57bfea8dbe39a6fd3403e28/akasa-core/src/tests/protocols/mqtt/v5/v500/auth.rs#L21-L24
scram_users:
  user:
    hashed_password: 2a2a2a
    iterations: 4096
    salt: 73616c74
# (v5.0 only) The sasl mechanisms supported in MQTT v5.0 enhanced authentication, now only `SCRAM-SHA-256` is supported
sasl_mechanisms:
- SCRAM-SHA-256

# When client connect with MQTT v3.1 protocol, if set this option to true, server will forbid client identifier length greater than 23.
check_v310_client_id_length: false
# (v5.0 only) The shared subscription mode, can be: [Random, HashClientId, HashTopicName]
shared_subscription_mode: Random
# Maximum allowed QoS the client can publish or subscribe
max_allowed_qos: 2
# Timeout seconds to resend inflight pending messages (unit: second)
inflight_timeout: 15
# Maximum inflight pending messages for client default value
max_inflight_client: 10
# Maximum inflight pending messages the server will handle
max_inflight_server: 10
# Maximum allowed pending messages in memory
max_in_mem_pending_messages: 256
# (unused) Maximum allowed pending messages in database
max_in_db_pending_messages: 65536
# (v5.0 only) The minimum allowed keep alive
min_keep_alive: 10
# (v5.0 only) The maximum allowed keep alive
max_keep_alive: 65535
# (v5.0 only, unused)
multiple_subscription_id_in_publish: false
# (v5.0 only) The maximum session expiry interval value can set in connect packet (unit: second)
max_session_expiry_interval: 4294967295
# (v5.0 only) The maximum packet size given by client (to limit server, unit: byte)
max_packet_size_client: 268435460
# (v5.0 only) The maximum packet size given by server (to limit client, unit: byte)
max_packet_size_server: 268435460
# (v5.0 only) The maximum topic alias value can be used in publish packet
topic_alias_max: 65535
# (v5.0 only) Whether support retained message
retain_available: true
# (v5.0 only) Whether support shared subscription
shared_subscription_available: true
# (v5.0 only) Whether support subscription identifiers
subscription_id_available: true
# (v5.0 only) Whether supports wildcard subscriptions
wildcard_subscription_available: true
# The value indicate whether call certain hook function
hook:
  enable_before_connect: true
  enable_after_connect: true
  enable_publish: true
  enable_subscribe: true
  enable_unsubscribe: true
```
