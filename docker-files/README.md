
### The binary is build by:
```shell
alias rust-musl-builder='docker run --rm -it -v "$(pwd)":/home/rust/src messense/rust-musl-cross:aarch64-musl'
rust-musl-builder cargo build --release
```

### The way to run the container:
```shell
docker run --init -it --rm -p 1883:1883 -v "$HOME/local/etc":/opt akasa:0.1.1 akasa start --config /opt/akasa-config.yaml
```

### The way to build the image:

For multiple platforms:
```
cp target/aarch64-unknown-linux-musl/release/akasa docker-files/bins/arm64
cp target/x86_64-unknown-linux-musl/release/akasa docker-files/bins/amd64

docker buildx create --name multiarch --driver docker-container --platform linux/amd64,linux/arm64/v8 --use
docker buildx build --push --platform linux/amd64,linux/arm64/v8 --tag thewawar/akasa:0.1.1 .
```

Just for one platform:
```shell
docker build -t akasa:0.1.1 . --build-arg AKASA_ARCH="aarch64-unknown-linux-musl"
docker image tag akasa:0.1.1 thewawar/akasa:0.1.1
docker image push thewawar/akasa:0.1.1
```
