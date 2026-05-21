FROM ubuntu:22.04 AS builder

ARG LLAMACPP_VERSION=a8681a0

RUN apt-get update -q && apt-get install -yq \
    git cmake make g++ curl ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN git clone --depth 100 https://github.com/ggml-org/llama.cpp . \
    && git checkout $LLAMACPP_VERSION

RUN cmake -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DGGML_RPC=ON \
      -DBUILD_SHARED_LIBS=OFF \
      -DLLAMA_CURL=ON \
    && cmake --build build --config Release -j$(nproc) \
         --target llama-server

FROM ubuntu:22.04

RUN apt-get update -q && apt-get install -yq \
    libgomp1 curl ca-certificates jq \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/build/bin/llama-server /usr/local/bin/llama-server
COPY entrypoint.sh /entrypoint.sh
COPY switch-model.sh /usr/local/bin/switch-model
RUN chmod +x /entrypoint.sh /usr/local/bin/switch-model

VOLUME ["/models"]
EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]