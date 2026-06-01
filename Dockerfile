FROM ubuntu:22.04

RUN apt-get update -q && apt-get install -yq \
    libgomp1 curl ca-certificates jq python3 python3-pip python3-venv \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install PyTorch CPU (hub does no inference, but Petals client needs it)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Install Petals and dependencies
RUN pip install --no-cache-dir \
    petals \
    transformers>=4.36.0 \
    accelerate>=0.25.0

COPY pipeline_hub.py /app/pipeline_hub.py
COPY healthd.py /app/healthd.py
COPY switch-model.sh /usr/local/bin/switch-model
RUN chmod +x /app/pipeline_hub.py /usr/local/bin/switch-model

ENV PYTHONUNBUFFERED=1
ENV QUEUE_URL=http://ollama-queue:8000
ENV TUNNEL_HOST=tunnel.akinus21.com
ENV TUNNEL_PORT=443

VOLUME ["/models"]
EXPOSE 8080 8081

CMD ["python3", "-u", "/app/pipeline_hub.py"]