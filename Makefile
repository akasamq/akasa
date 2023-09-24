
fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all --all-targets --all-features

test:
	cargo test

ci: fmt clippy test

.PHONY: clippy fmt test ci
