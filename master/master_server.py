import socket
import threading
import json
import time
import os

HOST = '127.0.0.1'

# Ask operator to define Master role
try:
    PORT = int(input("Enter Master Port (5000 for Primary, 5001 for Backup): "))
except ValueError:
    PORT = 5000

active_nodes = {}  
dead_nodes = set()
metadata = {}
event_log = []
all_nodes = set()
METADATA_FILE = "master/metadata.json"
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
        os.makedirs("master", exist_ok=True)
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=4)
    except Exception as e:
        log(f"❌ Failed to save metadata: {str(e)}")

def log(msg):
    timestamp = time.strftime("%H:%M:%S")
    event_log.append(f"[{timestamp}] {msg}")
    print(f"[{timestamp}] {msg}")

def handle_client(conn, addr):
    try:
        # 1. Safely read payloads of ANY size (crucial for big files)
        full_data = b""
        while True:
            packet = conn.recv(4096)
            if not packet:
                break
            full_data += packet
            
        request = json.loads(full_data.decode('utf-8'))
    except Exception as e:
        conn.close()
        return

    if request["type"] == "upload":
        filename = request["filename"]
        chunks = request["chunks"]

        with metadata_lock:
            metadata[filename] = chunks
            save_metadata()

        log(f"🚀 File '{filename}' injected into cluster")
        for c in chunks:
            log(f"🧩 {c} distributed → {chunks[c]['ports']}")

        conn.sendall(json.dumps({"status": "ok"}).encode())

    elif request["type"] == "download":
        filename = request["filename"]

        if filename in metadata:
            log(f"Retrieval protocol initiated → '{filename}'")
            conn.sendall(json.dumps({
                "status": "ok",
                "chunks": metadata[filename]
            }).encode())
        else:
            log(f"⚠️ Retrieval failed → '{filename}' not found")
            conn.sendall(json.dumps({"status": "error"}).encode())

    elif request["type"] == "delete":
        filename = request["filename"]
        
        with metadata_lock:
            if filename in metadata:
                del metadata[filename]
                save_metadata()
                log(f"🗑️ File '{filename}' purged from cluster metadata")
                conn.sendall(json.dumps({"status": "ok"}).encode())
            else:
                conn.sendall(json.dumps({"status": "error"}).encode())

    elif request["type"] == "heartbeat":
        node = request["node"]
        all_nodes.add(node)

        if node in dead_nodes:
            conn.close()
            return

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
        conn.sendall(b"OK")

    elif request["type"] == "get_logs":
        conn.sendall(json.dumps(event_log).encode())

    elif request["type"] == "get_nodes":
        degraded_list = []
        now = time.time()
        
        for node, timestamps in active_nodes.items():
            if len(timestamps) == 5:
                intervals = [timestamps[i] - timestamps[i-1] for i in range(1, 5)]
                avg_latency = sum(intervals) / len(intervals)
                if avg_latency > 3.5:  
                    degraded_list.append(node)

        # 🧠 Calculate the Global Adaptive Cluster Replication Factor
        total_chunks = 0
        total_replicas = 0
        for file, chunks in metadata.items():
            for chunk_data in chunks.values():
                total_chunks += 1
                total_replicas += len(chunk_data["ports"])
        
        cluster_rf = round(total_replicas / total_chunks, 1) if total_chunks > 0 else 0.0

        response = {
            "active": list(active_nodes.keys()),
            "degraded": degraded_list, 
            "all": list(all_nodes),
            "cluster_rf": cluster_rf # Send dynamic global RF to UI
        }
        conn.sendall(json.dumps(response).encode())

    elif request["type"] == "get_stats":
        file_stats = []
        for name, chunks in metadata.items():
            if not chunks: continue
            
            target_rf = 3 if len(chunks) <= 10 else 2
            avg_replication = sum(len(chunk_data["ports"]) for chunk_data in chunks.values()) / len(chunks)
            
            file_stats.append({
                "name": name,
                "chunks": len(chunks),
                "health": min(round((avg_replication / target_rf) * 100, 1), 100),
                "actual_rf": round(avg_replication, 1), # Send per-file exact RF to UI
                "target_rf": target_rf
            })
        conn.sendall(json.dumps(file_stats).encode())

def monitor_nodes():
    while True:
        now = time.time()
        nodes_to_remove = []

        for node, timestamps in list(active_nodes.items()):
            if not timestamps:
                continue
                
            last_beat = timestamps[-1]
            time_since_last = now - last_beat

            if time_since_last > 5:
                log(f"❌ Node {node} hard fault (timeout).")
                nodes_to_remove.append(node)
            
            elif len(timestamps) == 5:
                intervals = [timestamps[i] - timestamps[i-1] for i in range(1, 5)]
                avg_latency = sum(intervals) / len(intervals)
                
                if avg_latency > 3.5 and time_since_last > 3:
                    log(f"⚠️ Warning: Node {node} showing severe latency (Avg: {avg_latency:.2f}s). Preemptive replication advised.")

        for node in nodes_to_remove:
            del active_nodes[node]
            re_replicate(node)

        time.sleep(2)

def re_replicate(dead_node):
    tasks = []
    
    # STEP 1: Safely lock metadata, remove the dead node, and queue tasks
    with metadata_lock:
        for file in metadata:
            for chunk in metadata[file]:
                ports = metadata[file][chunk]["ports"]
                if dead_node in ports:
                    ports.remove(dead_node)
                    if len(ports) > 0:
                        # Queue the chunk to be copied from a surviving node
                        tasks.append((file, chunk, ports[0]))
                    else:
                        log(f"💀 FATAL DATA LOSS → {chunk}")

    # STEP 2: Do the slow network transfers outside the lock so the server stays alive
    for file, chunk, source_node in tasks:
        log(f" 🔁 RECONSTRUCTING {chunk} from Node {source_node}")
        data = get_chunk(source_node, chunk)
        
        if not data:
            log(f"⚠️ EXTRACTION FAILED → {chunk}")
            continue

        target_node = None
        
        # Briefly lock to find a node that doesn't already have this chunk
        with metadata_lock:
            current_ports = metadata[file][chunk]["ports"]
            for node in active_nodes:
                if node not in current_ports:
                    target_node = node
                    break
                    
        # If we found a target, transfer the data and save
        if target_node:
            if send_chunk(target_node, chunk, data):
                with metadata_lock:
                    metadata[file][chunk]["ports"].append(target_node)
                    save_metadata()
                log(f"{chunk} REPLICATED → Node {target_node}")
                if tasks:
                    log(f"✅ CLUSTER STABILIZED: {len(tasks)} chunks successfully recovered.")

def get_chunk(port, chunk):
    try:
        s = socket.socket()
        s.settimeout(3)
        s.connect(("127.0.0.1", port))
        s.sendall(f"GET:::{chunk}".encode())
        s.shutdown(socket.SHUT_WR)

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
        s.settimeout(3)
        s.connect(("127.0.0.1", port))
        header = f"STORE:{chunk:<44}".encode()
        s.sendall(header + data)
        s.shutdown(socket.SHUT_WR)
        
        res = b""
        while True:
            packet = s.recv(1024)
            if not packet: break
            res += packet
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

    print(f"MASTER ONLINE [PORT {PORT}]")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()

if __name__ == "__main__":
    start_master()