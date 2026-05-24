import http.server
import json
import socket
import threading
import os
import time
import sys

STATE_FILE = "/tmp/rpc_workers.json"
RPC_PORT = 50052
TCP_TIMEOUT = 10


def check_rpc_port(wg_ip, port=RPC_PORT, timeout=TCP_TIMEOUT):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((wg_ip, port))
        s.close()
        return True
    except Exception as e:
        print(f"[healthd] TCP check {wg_ip}:{port} FAILED: {e}", flush=True)
        return False


def write_state(workers):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(workers, f)
    os.replace(tmp, STATE_FILE)


class NotifyHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/notify":
            self.send_response(404)
            self.end_headers()
            return

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
        for w in raw_workers:
            ip = w.get("wg_ip", "")
            if not ip:
                continue
            reachable = check_rpc_port(ip)
            entry = {"wg_ip": ip, "port": w.get("port", RPC_PORT), "rpc_ok": reachable}
            if reachable:
                validated.append(entry)

        write_state(validated)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "validated": len(validated),
            "total": len(raw_workers),
            "workers": validated,
        }).encode())

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