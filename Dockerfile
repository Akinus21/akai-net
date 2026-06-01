FROM ubuntu:22.04

RUN apt-get update -q && apt-get install -yq \
    libgomp1 curl ca-certificates jq python3 python3-pip python3-venv \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install aiohttp and httpx for the hub server
RUN pip install --no-cache-dir aiohttp httpx

# Install Petals (includes transformers, accelerate, torch as dependencies)
RUN pip install --no-cache-dir petals

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