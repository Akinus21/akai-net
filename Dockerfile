FROM ubuntu:22.04 AS builder

ARG LLAMACPP_VERSION=a8681a0

RUN apt-get update -q && apt-get install -yq \
    git cmake make g++ curl ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN git clone https://github.com/ggml-org/llama.cpp . \
    && git checkout $LLAMACPP_VERSION

RUN cmake -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DGGML_RPC=ON \
      -DGGML_AVX512=OFF \
      -DGGML_AVX512_VBMI=OFF \
      -DGGML_AVX512_VNNI=OFF \
      -DBUILD_SHARED_LIBS=OFF \
      -DLLAMA_CURL=ON \
      -DCMAKE_C_FLAGS="-mno-avx512f -mno-avx512dq -mno-avx512bw -mno-avx512vl" \
      -DCMAKE_CXX_FLAGS="-mno-avx512f -mno-avx512dq -mno-avx512bw -mno-avx512vl" \
    && cmake --build build --config Release -j$(nproc) \
         --target llama-server

FROM ubuntu:22.04

RUN apt-get update -q && apt-get install -yq \
    libgomp1 curl ca-certificates jq python3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/build/bin/llama-server /usr/local/bin/llama-server
COPY entrypoint.sh /entrypoint.sh
COPY healthd.py /app/healthd.py
COPY switch-model.sh /usr/local/bin/switch-model
RUN chmod +x /entrypoint.sh /usr/local/bin/switch-model

VOLUME ["/models"]
EXPOSE 8080 8081

ENTRYPOINT ["/entrypoint.sh"]