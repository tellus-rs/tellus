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

FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132
COPY --from=builder /out/node /usr/local/bin/node
COPY --from=builder /out/verifier /usr/local/bin/verifier
COPY --from=builder /out/config /opt/tellus-cluster-demo/config
WORKDIR /opt/tellus-cluster-demo
CMD ["node"]
