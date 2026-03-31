import socket
import threading
import json
import time

HOST = '127.0.0.1'
PORT = 5000

active_nodes = {}
dead_nodes = set()
metadata = {}
event_log = []
all_nodes = set()


def log(msg):
    timestamp = time.strftime("%H:%M:%S")
    event_log.append(f"[{timestamp}] {msg}")


def handle_client(conn, addr):
    try:
        data = conn.recv(4096).decode()
        request = json.loads(data)
    except:
        conn.close()
        return

    if request["type"] == "upload":
        filename = request["filename"]
        chunks = request["chunks"]

        metadata[filename] = chunks

        log(f"🚀 File '{filename}' injected into cluster")
        for c in chunks:
            log(f"🧩 {c} distributed → {chunks[c]}")

        conn.send(json.dumps({"status": "ok"}).encode())

    elif request["type"] == "download":
        filename = request["filename"]

        if filename in metadata:
            log(f"📡 Retrieval protocol initiated → '{filename}'")
            conn.send(json.dumps({
                "status": "ok",
                "chunks": metadata[filename]
            }).encode())
        else:
            log(f"⚠️ Retrieval failed → '{filename}' not found")
            conn.send(json.dumps({"status": "error"}).encode())

    elif request["type"] == "heartbeat":
        node = request["node"]

        all_nodes.add(node)

        if node in dead_nodes:
            return

        active_nodes[node] = time.time()

    elif request["type"] == "kill_node":
        node = request["node"]

        dead_nodes.add(node)
        log(f"💀 Node {node} terminated by operator")

        if node in active_nodes:
            del active_nodes[node]

        re_replicate(node)

    elif request["type"] == "get_logs":
        conn.send(json.dumps(event_log[-30:]).encode())

    elif request["type"] == "get_nodes":
        response = {
            "active": list(active_nodes.keys()),
            "all": list(all_nodes)
        }
        conn.send(json.dumps(response).encode())

    conn.close()


def monitor_nodes():
    while True:
        now = time.time()

        for node in list(active_nodes.keys()):
            if now - active_nodes[node] > 5:
                log(f"💀 Node {node} lost signal (timeout)")
                del active_nodes[node]
                re_replicate(node)

        time.sleep(2)


def re_replicate(dead_node):
    for file in metadata:
        for chunk in metadata[file]:
            nodes = metadata[file][chunk]

            if dead_node in nodes:
                log(f"🔁 Reconstructing {chunk}")
                nodes.remove(dead_node)

                if len(nodes) == 0:
                    log(f"☠️ Data loss detected → {chunk}")
                    continue

                source = nodes[0]
                data = get_chunk(source, chunk)

                if not data:
                    log(f"⚠️ Extraction failed → {chunk}")
                    continue

                for new_node in active_nodes:
                    if new_node not in nodes:
                        if send_chunk(new_node, chunk, data):
                            nodes.append(new_node)
                            log(f"🧬 {chunk} replicated → node{new_node}")
                        break


def get_chunk(port, chunk):
    try:
        s = socket.socket()
        s.connect(("127.0.0.1", port))
        s.send(f"GET:::{chunk}".encode())

        data = b""
        while True:
            part = s.recv(4096)
            if not part:
                break
            data += part

        s.close()
        return data if data else None
    except:
        return None


def send_chunk(port, chunk, data):
    try:
        s = socket.socket()
        s.connect(("127.0.0.1", port))

        header = f"STORE:{chunk:<44}".encode()
        s.send(header + data)

        s.recv(1024)
        s.close()
        return True
    except:
        return False


def start_master():
    server = socket.socket()
    server.bind((HOST, PORT))
    server.listen()

    threading.Thread(target=monitor_nodes, daemon=True).start()

    print("🧠 MASTER ONLINE")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    start_master()