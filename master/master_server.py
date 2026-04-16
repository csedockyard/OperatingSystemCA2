import socket
import threading
import json
import time
import os

HOST = '127.0.0.1'
PORT = 5000

# Change from active_nodes = {} to:
active_nodes = {}  # Will now store: { node_port: [timestamp1, timestamp2, ...] }
dead_nodes = set()
metadata = {}
event_log = []
all_nodes = set()
METADATA_FILE = "master/metadata.json"
# Add this below METADATA_FILE = "master/metadata.json"
metadata_lock = threading.Lock()

def load_metadata():
    global metadata
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, "r") as f:
                metadata = json.load(f)
            log("💾 Metadata loaded from persistent storage")
        except:
            log("⚠️ Metadata file corrupted, starting fresh")
            metadata = {}

def save_metadata():
    try:
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=4)
    except Exception as e:
        log(f"❌ Failed to save metadata: {str(e)}")


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

        # Acquire lock before modifying shared memory
        with metadata_lock:
            metadata[filename] = chunks
            save_metadata()

        log(f"🚀 File '{filename}' injected into cluster")
        # ... rest of the upload logic
        for c in chunks:
            log(f"🧩 {c} distributed → {chunks[c]}")

        conn.send(json.dumps({"status": "ok"}).encode())

    elif request["type"] == "download":
        filename = request["filename"]

        if filename in metadata:
            log(f"Retrieval protocol initiated → '{filename}'")
            conn.send(json.dumps({
                "status": "ok",
                "chunks": metadata[filename]
            }).encode())
        else:
            log(f"⚠️ Retrieval failed → '{filename}' not found")
            conn.send(json.dumps({"status": "error"}).encode())


    elif request["type"] == "delete":
        filename = request["filename"]
        
        # Acquire lock to ensure thread safety during deletion
        with metadata_lock:
            if filename in metadata:
                del metadata[filename]
                save_metadata()
                log(f"🗑️ File '{filename}' purged from cluster metadata")
                conn.send(json.dumps({"status": "ok"}).encode())
            else:
                conn.send(json.dumps({"status": "error"}).encode())

    


    elif request["type"] == "heartbeat":
        node = request["node"]
        all_nodes.add(node)

        if node in dead_nodes:
            return

        # Keep the last 5 heartbeat timestamps for time-series analysis
        if node not in active_nodes:
            active_nodes[node] = []
        active_nodes[node].append(time.time())
        if len(active_nodes[node]) > 5:
            active_nodes[node].pop(0)

    elif request["type"] == "kill_node":
        node = request["node"]

        dead_nodes.add(node)
        log(f"Node {node} terminated by operator")

        if node in active_nodes:
            del active_nodes[node]

        re_replicate(node)

    elif request["type"] == "get_logs":
        conn.send(json.dumps(event_log[-30:]).encode())

    elif request["type"] == "get_nodes":
        degraded_list = []
        now = time.time()
        
        # Calculate which nodes are struggling based on the timestamps we are now saving
        for node, timestamps in active_nodes.items():
            if len(timestamps) == 5:
                intervals = [timestamps[i] - timestamps[i-1] for i in range(1, 5)]
                avg_latency = sum(intervals) / len(intervals)
                if avg_latency > 3.5:  # Our threshold
                    degraded_list.append(node)

        response = {
            "active": list(active_nodes.keys()),
            "degraded": degraded_list, # Send to frontend
            "all": list(all_nodes)
        }
        conn.send(json.dumps(response).encode())

    elif request["type"] == "get_stats":
        file_stats = []
        for name, chunks in metadata.items():
            avg_replication = sum(len(nodes) for nodes in chunks.values()) / len(chunks) if chunks else 0
            file_stats.append({
                "name": name,
                "chunks": len(chunks),
                "health": round((avg_replication / 2.0) * 100, 1) # assuming 2 is target
            })
        conn.send(json.dumps(file_stats).encode())

    conn.close()


def monitor_nodes():
    while True:
        now = time.time()
        nodes_to_remove = []

        for node, timestamps in list(active_nodes.items()):
            if not timestamps:
                continue
                
            last_beat = timestamps[-1]
            time_since_last = now - last_beat

            # 1. Hard Failure Detection
            if time_since_last > 5:
                log(f"❌ Node {node} hard fault (timeout).")
                nodes_to_remove.append(node)
            
            # 2. Predictive/Heuristic Degradation Detection
            elif len(timestamps) == 5:
                # Calculate average latency between the last 5 heartbeats
                intervals = [timestamps[i] - timestamps[i-1] for i in range(1, 5)]
                avg_latency = sum(intervals) / len(intervals)
                
                # If average latency drifts too high (expected is ~2s), disk/network is struggling
                if avg_latency > 3.5 and time_since_last > 3:
                    log(f"⚠️ Warning: Node {node} showing severe latency (Avg: {avg_latency:.2f}s). Preemptive replication advised.")
                    # You could trigger a lighter version of re_replicate here for critical files

        for node in nodes_to_remove:
            del active_nodes[node]
            re_replicate(node)

        time.sleep(2)


def re_replicate(dead_node):
    for file in metadata:
        for chunk in metadata[file]:
            nodes = metadata[file][chunk]

            if dead_node in nodes:
                log(f" 🔁 RECONSTRUCTING {chunk}")
                nodes.remove(dead_node)

                if len(nodes) == 0:
                    log(f" DATA LOSS DETECTED → {chunk}")
                    continue

                source = nodes[0]
                data = get_chunk(source, chunk)

                if not data:
                    log(f"⚠️ EXTRACTION FAILED → {chunk}")
                    continue

                for new_node in active_nodes:
                    if new_node not in nodes:
                        if send_chunk(new_node, chunk, data):
                            nodes.append(new_node)
                            log(f"{chunk} REPLICATED → node{new_node}")
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
    load_metadata()
    server = socket.socket()
    server.bind((HOST, PORT))
    server.listen()

    threading.Thread(target=monitor_nodes, daemon=True).start()

    print("MASTER ONLINE")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    start_master()