import http.server
import json
import os
import sys
import time

STATE_FILE = "/tmp/rpc_workers.json"
TUNNEL_STATE_FILE = os.getenv("TUNNEL_STATE_FILE", "/app/data/tunnel_workers.json")


def read_tunnel_state():
    try:
        with open(TUNNEL_STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def write_state(workers):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(workers, f)
    os.replace(tmp, STATE_FILE)


class NotifyHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/notify":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.end_headers()
                return

            raw_workers = data.get("workers", [])

            validated = []
            seen = set()
            for w in raw_workers:
                wid = w.get("worker_id", "")
                local_port = w.get("local_port", 0)
                if not wid or not local_port:
                    wg_ip = w.get("wg_ip", "")
                    if wg_ip:
                        validated.append({"wg_ip": wg_ip, "port": w.get("port", 50052), "rpc_ok": True})
                    continue
                if wid in seen:
                    continue
                seen.add(wid)
                validated.append({
                    "worker_id": wid,
                    "local_port": local_port,
                    "rpc_ok": True,
                })

            write_state(validated)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "validated": len(validated),
                "total": len(raw_workers),
                "workers": validated,
            }).encode())
            return

        if self.path == "/tunnel-notify":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.end_headers()
                return

            tunnel = data.get("workers", {})
            current = []
            try:
                with open(STATE_FILE) as f:
                    current = json.load(f)
            except Exception:
                pass

            existing = {w.get("worker_id"): w for w in current if w.get("worker_id")}
            for wid, info in tunnel.items():
                existing[wid] = {
                    "worker_id": wid,
                    "local_port": info["local_port"],
                    "rpc_ok": True,
                }

            surviving = [existing[wid] for wid in tunnel if wid in existing]
            write_state(surviving)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"workers": len(surviving)}).encode())
            return

        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            try:
                with open(STATE_FILE) as f:
                    workers = json.load(f)
            except Exception:
                workers = []
            self.wfile.write(json.dumps({"workers": workers}).encode())
            return

        if self.path == "/tunnel":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(read_tunnel_state()).encode())
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt, *args):
        ts = time.strftime("%H:%M:%S")
        sys.stderr.write(f"[healthd] {ts} {fmt % args}\n")


def run_server(port=8081):
    server = http.server.HTTPServer(("0.0.0.0", port), NotifyHandler)
    server.serve_forever()


if __name__ == "__main__":
    write_state([])
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
    print(f"[healthd] listening on :{port}", flush=True)
    run_server(port)