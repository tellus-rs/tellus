ARG RUST_VERSION
FROM rust:$RUST_VERSION-slim AS builder
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --locked --release -p tellus-cluster-demo \
    && mkdir -p /out/config \
    && cp target/release/node target/release/verifier /out/ \
    && cp tellus-cluster-demo/config/*.yaml /out/config/

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2
COPY --from=builder /out/node /usr/local/bin/node
COPY --from=builder /out/verifier /usr/local/bin/verifier
COPY --from=builder /out/config /opt/tellus-cluster-demo/config
WORKDIR /opt/tellus-cluster-demo
CMD ["node"]
