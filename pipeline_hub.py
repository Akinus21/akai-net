#!/usr/bin/env python3
"""
Akai-Net Hub using Petals Distributed Inference

Petals provides layer pipeline parallelism through a BitTorrent-style DHT swarm.
Workers host contiguous transformer blocks and are discovered dynamically.

Key difference from previous llama.cpp RPC approach:
  - Petals handles peer discovery via DHT (no queue needed)
  - Workers connect directly to swarm (no tunnel needed for Petals traffic)
  - Hub acts as Petals client, connecting to the swarm

For our tunnel infrastructure: still needed for worker registration,
heartbeat, and meta-coordination - but NOT for inference traffic.
"""

import os
import sys
import json
import time
import asyncio
import logging
import argparse
from typing import AsyncIterator, Optional, Dict, Any
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("akai-net")


def ensure_petals_installed():
    """Ensure Petals is installed."""
    import subprocess
    try:
        result = subprocess.run(
            ["python3", "-m", "pip", "show", "petals"],
            capture_output=True,
            timeout=10,
        )
        if result.returncode == 0:
            return True
    except:
        pass

    log.info("Installing Petals...")
    result = subprocess.run(
        ["python3", "-m", "pip", "install", "git+https://github.com/bigscience-workshop/petals"],
        capture_output=True,
        timeout=300,
    )
    if result.returncode != 0:
        log.error(f"Petals installation failed: {result.stderr.decode()}")
        return False
    log.info("Petals installed successfully")
    return True


class PetalsHub:
    """Hub using Petals for distributed inference."""

    def __init__(
        self,
        model_name: str,
        queue_url: Optional[str] = None,
    ):
        self.model_name = model_name
        self.queue_url = queue_url
        self.model = None
        self.tokenizer = None

    async def load(self):
        """Load Petals model and tokenizer."""
        if not ensure_petals_installed():
            raise RuntimeError("Petals installation failed")

        from petals import AutoDistributedModelForCausalLM
        from transformers import AutoTokenizer

        log.info(f"Loading Petals model: {self.model_name}")
        log.info("Connecting to distributed swarm...")
        log.info("Workers should run: petals.cli.run_server <model>")

        try:
            self.model = AutoDistributedModelForCausalLM.from_pretrained(
                self.model_name,
                timeout=300,
            )
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            log.info(f"Petals model loaded. Serving {self.model_name}")
        except Exception as e:
            log.error(f"Failed to load Petals model: {e}")
            raise

    async def infer(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        temperature: float = 0.7,
        top_p: float = 0.9,
    ) -> AsyncIterator[dict]:
        """Run inference through Petals swarm."""
        if self.model is None or self.tokenizer is None:
            yield {"error": "Model not loaded"}
            return

        try:
            inputs = self.tokenizer(prompt, return_tensors="pt")["input_ids"]

            for seq in self.model.generate(
                inputs,
                max_new_tokens=max_new_tokens,
                do_sample=True,
                temperature=temperature,
                top_p=top_p,
                streaming=True,
            ):
                text = self.tokenizer.decode(seq[0], skip_special_tokens=True)
                yield {"content": text[len(prompt):]}

        except Exception as e:
            yield {"error": str(e)}

    async def health_check(self) -> dict:
        """Check hub health and swarm status."""
        return {
            "model": self.model_name,
            "status": "ok" if self.model is not None else "loading",
            "note": "Petals swarm - workers join via 'petals.cli.run_server'",
        }


async def main():
    parser = argparse.ArgumentParser(description="Akai-Net Hub (Petals)")
    parser.add_argument("--model", default="meta-llama/Meta-Llama-3.1-8B-Instruct",
                        help="Model name on HuggingFace")
    parser.add_argument("--port", type=int, default=8080, help="Hub port")
    parser.add_argument("--queue-url", default=os.environ.get("QUEUE_URL", ""),
                        help="Optional queue URL for worker coordination")
    args = parser.parse_args()

    from aiohttp import web

    hub = PetalsHub(args.model, queue_url=args.queue_url or None)
    await hub.load()

    async def health(request):
        return web.json_response(await hub.health_check())

    async def completions(request):
        """OpenAI-compatible /v1/completions endpoint."""
        data = await request.json()
        prompt = data.get("prompt", "")
        max_tokens = data.get("max_tokens", 256)
        temperature = data.get("temperature", 0.7)
        top_p = data.get("top_p", 0.9)
        stream = data.get("stream", True)

        if stream:
            async def generate():
                async for chunk in hub.infer(prompt, max_tokens, temperature, top_p):
                    if "error" in chunk:
                        yield f"data: {json.dumps({'error': chunk['error']})}\n\n"
                    elif "content" in chunk:
                        yield f"data: {json.dumps({'choices': [{'text': chunk['content']}]})}\n\n"
                yield "data: [DONE]\n\n"
            return web.Response(
                text=generate(),
                content_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        else:
            result = None
            async for chunk in hub.infer(prompt, max_tokens, temperature, top_p):
                if "error" not in chunk:
                    result = chunk
            return web.json_response({
                "choices": [{"text": result.get("content", "") if result else ""}]
            })

    async def chat_completions(request):
        """OpenAI-compatible /v1/chat/completions endpoint."""
        data = await request.json()
        messages = data.get("messages", [])
        max_tokens = data.get("max_tokens", 256)
        temperature = data.get("temperature", 0.7)
        top_p = data.get("top_p", 0.9)
        stream = data.get("stream", True)

        prompt = "\n".join([
            f"{m.get('role', 'user')}: {m.get('content', '')}"
            for m in messages if m.get('content')
        ])

        if stream:
            async def generate():
                async for chunk in hub.infer(prompt, max_tokens, temperature, top_p):
                    if "error" in chunk:
                        yield f"data: {json.dumps({'error': {'message': chunk['error']}})}\n\n"
                    elif "content" in chunk:
                        yield f"data: {json.dumps({'choices': [{'delta': {'content': chunk['content']}}]})}\n\n"
                yield "data: [DONE]\n\n"
            return web.Response(
                text=generate(),
                content_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        else:
            result = None
            async for chunk in hub.infer(prompt, max_tokens, temperature, top_p):
                if "error" not in chunk:
                    result = chunk
            return web.json_response({
                "choices": [{
                    "message": {"role": "assistant", "content": result.get("content", "") if result else ""}
                }]
            })

    async def models(request):
        """OpenAI-compatible /v1/models endpoint."""
        return web.json_response({
            "object": "list",
            "data": [{
                "id": args.model,
                "object": "model",
                "created": int(time.time()),
                "owned_by": "akai-net",
            }]
        })

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/v1/models", models)
    app.router.add_post("/v1/completions", completions)
    app.router.add_post("/v1/chat/completions", chat_completions)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", args.port)
    await site.start()

    log.info(f"Akai-Net Hub (Petals) ready on 0.0.0.0:{args.port}")
    log.info(f"Model: {args.model}")
    log.info("Workers should run: petals.cli.run_server <model>")

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())