FROM rust:1.75 as builder

WORKDIR /build

RUN apt-get update -q && apt-get install -yq \
    cmake make g++ \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/

RUN cargo build --release --bin hub

FROM ubuntu:22.04

RUN apt-get update -q && apt-get install -yq \
    libgomp1 curl ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/hub /usr/local/bin/

ENV RUST_LOG=info
ENV MODEL_NAME=unknown
ENV MODEL_LAYERS=32
ENV HIDDEN_SIZE=4096
ENV HUB_PORT=8080
ENV WORKER_PORT=50051

EXPOSE 8080 50051

CMD ["hub"]