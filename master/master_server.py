import socket
import threading
import json
import time
import os
import queue # NEW: For OS-Level Task Scheduling
from collections import OrderedDict

HOST = '127.0.0.1'

try:
    PORT = int(input("Enter Master Port (5000 for Primary, 5001 for Backup): "))
except ValueError:
    PORT = 5000

active_nodes = {}  
dead_nodes = set()
metadata = {}
event_log = []
all_nodes = set()
live_telemetry = {} 
METADATA_FILE = "master/metadata.json"
metadata_lock = threading.Lock()

# ==========================================
# 🧠 OS CONCEPT: PRIORITY CPU SCHEDULER
# ==========================================
cpu_scheduler = queue.PriorityQueue()

# ==========================================
# 🧠 OS CONCEPT: LRU PAGE REPLACEMENT CACHE
# ==========================================
class LRUMetadataCache:
    def __init__(self, capacity=3):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get_routing(self, filename):
        if filename in self.cache:
            # CACHE HIT: Move to end to mark as "Most Recently Used"
            self.cache.move_to_end(filename)
            log(f"[SYS] LRU Cache Hit: '{filename}' served instantly from RAM.")
            return self.cache[filename]
        else:
            # PAGE FAULT: Not in RAM.
            return None

    def put_routing(self, filename, routing_data):
        self.cache[filename] = routing_data
        self.cache.move_to_end(filename)
        # LRU EVICTION: If we exceed capacity, pop the oldest item (First In)
        if len(self.cache) > self.capacity:
            oldest_file, _ = self.cache.popitem(last=False)
            log(f"[SYS] LRU Eviction: RAM full. Purged '{oldest_file}' to make space.")

# Initialize the RAM Cache with a limit of 3 active files
routing_cache = LRUMetadataCache(capacity=3)
# ==========================================

def os_dispatcher():
    """
    This daemon thread acts as the CPU Scheduler. 
    It constantly pulls tasks from the queue. If a Priority 1 and Priority 3 
    task arrive at the same time, Priority 1 ALWAYS gets the CPU first.
    """
    while True:
        priority, task_name, func, args = cpu_scheduler.get()
        
        # We only print logs for high-priority tasks to keep the UI clean
        if priority == 1:
            log(f"[SYS] CPU Dispatcher halted routine tasks. Executing Priority {priority}: {task_name}")
            
        try:
            func(*args)
        except Exception as e:
            log(f"[ERROR] Dispatcher failed on task {task_name}: {e}")
        finally:
            cpu_scheduler.task_done()

# Start the CPU Scheduler daemon immediately
threading.Thread(target=os_dispatcher, daemon=True).start()
# ==========================================

def load_metadata():
    global metadata
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, "r") as f:
                metadata = json.load(f)
            log("[SYS] Metadata loaded from persistent storage")
        except:
            log("[WARN] Metadata file corrupted, starting fresh")
            metadata = {}

def save_metadata():
    try:
        os.makedirs("master", exist_ok=True)
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=4)
        # log("[SYS] Deferred I/O: Metadata flushed to physical disk.") # Optional log
    except Exception as e:
        log(f"[ERROR] Failed to save metadata: {str(e)}")

def log(msg):
    timestamp = time.strftime("%H:%M:%S")
    event_log.append(f"[{timestamp}] {msg}")
    print(f"[{timestamp}] {msg}")

def process_heartbeat(request):
    """Extracted logic so the Dispatcher can execute it"""
    node = request["node"]
    all_nodes.add(node)

    if node in dead_nodes:
        return

    if node not in active_nodes:
        active_nodes[node] = []
    active_nodes[node].append(time.time())
    if len(active_nodes[node]) > 5:
        active_nodes[node].pop(0)

    if "ram_buffer_usage" in request:
        live_telemetry[str(node)] = {
            "ram_buffer_usage": request["ram_buffer_usage"]
        }

def handle_client(conn, addr):
    try:
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
            # PRIORITY 3: Deferred I/O. Don't block the network waiting for the SSD.
            cpu_scheduler.put((3, f"DEFERRED_SAVE_{filename}", save_metadata, ()))

        log(f"[INFO] File '{filename}' injected into cluster")
        
        # --- NEW FIX: Restore the shard mapping logs ---
        for chunk_name, chunk_info in chunks.items():
            log(f"[INFO] Shard {chunk_name} mapped to Units {chunk_info['ports']}")
        # -----------------------------------------------

        conn.sendall(json.dumps({"status": "ok"}).encode())

    elif request["type"] == "download":
        filename = request["filename"]

        # STEP 1: Ask the LRU Cache first
        cached_data = routing_cache.get_routing(filename)

        if cached_data:
            # Fast Path: Served from RAM
            conn.sendall(json.dumps({
                "status": "ok",
                "chunks": cached_data
            }).encode())
        else:
            # STEP 2: Page Fault! We must do a slow lookup from the main metadata
            if filename in metadata:
                log(f"[WARN] Page Fault: '{filename}' not in RAM. Fetching from main storage...")
                
                # Fetch it and put it in the RAM cache for next time
                file_data = metadata[filename]
                routing_cache.put_routing(filename, file_data)
                
                log(f"[INFO] Retrieval protocol initiated for '{filename}'")
                conn.sendall(json.dumps({
                    "status": "ok",
                    "chunks": file_data
                }).encode())
            else:
                conn.sendall(json.dumps({"status": "error"}).encode())

    elif request["type"] == "delete":
        filename = request["filename"]
        
        with metadata_lock:
            if filename in metadata:
                del metadata[filename]
                # PRIORITY 3: Deferred Disk Write
                cpu_scheduler.put((3, f"DEFERRED_SAVE_{filename}", save_metadata, ()))
                log(f"[SYS] File '{filename}' purged from namespace")
                conn.sendall(json.dumps({"status": "ok"}).encode())
            else:
                conn.sendall(json.dumps({"status": "error"}).encode())

    elif request["type"] == "heartbeat":
        # PRIORITY 2: Queue the telemetry update
        cpu_scheduler.put((2, f"HEARTBEAT_{request['node']}", process_heartbeat, (request,)))

    elif request["type"] == "kill_node":
        node = request["node"]
        dead_nodes.add(node)
        log(f"[CRITICAL] Unit {node} terminated by operator")
        if node in active_nodes:
            del active_nodes[node]
            
        # NEW FIX: Purge the ghost telemetry so the UI clears the red lock
        if str(node) in live_telemetry:
            del live_telemetry[str(node)]
            
        # PRIORITY 1: Emergency Crash Recovery!
        cpu_scheduler.put((1, f"CRASH_RECOVERY_{node}", re_replicate, (node,)))
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
            "cluster_rf": cluster_rf,
            "telemetry": live_telemetry 
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
                "actual_rf": round(avg_replication, 1), 
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
                log(f"[ERROR] Unit {node} hard fault (timeout).")
                nodes_to_remove.append(node)
            
            elif len(timestamps) == 5:
                intervals = [timestamps[i] - timestamps[i-1] for i in range(1, 5)]
                avg_latency = sum(intervals) / len(intervals)
                
                if avg_latency > 3.5 and time_since_last > 3:
                    log(f"[WARN] Unit {node} showing severe latency. Preemptive replication advised.")

        for node in nodes_to_remove:
            if node in active_nodes:
                del active_nodes[node]
                
            # NEW FIX: Purge the ghost telemetry for auto-detected crashes
            if str(node) in live_telemetry:
                del live_telemetry[str(node)]
                
            # PRIORITY 1: Auto-detected crash recovery
            cpu_scheduler.put((1, f"AUTO_RECOVERY_{node}", re_replicate, (node,)))

        time.sleep(2)

def re_replicate(dead_node):
    tasks = []
    with metadata_lock:
        for file in metadata:
            for chunk in metadata[file]:
                ports = metadata[file][chunk]["ports"]
                if dead_node in ports:
                    ports.remove(dead_node)
                    if len(ports) > 0:
                        tasks.append((file, chunk, ports[0]))
                    else:
                        log(f"[FATAL] DATA LOSS IN SHARD → {chunk}")

    for file, chunk, source_node in tasks:
        log(f"[INFO] Reconstructing {chunk} from Unit {source_node}")
        data = get_chunk(source_node, chunk)
        
        if not data:
            log(f"[ERROR] Extraction failed for {chunk}")
            continue

        target_node = None
        
        with metadata_lock:
            current_ports = metadata[file][chunk]["ports"]
            for node in active_nodes:
                if node not in current_ports:
                    target_node = node
                    break
                    
        if target_node:
            if send_chunk(target_node, chunk, data):
                with metadata_lock:
                    metadata[file][chunk]["ports"].append(target_node)
                    # Priority 3: Defer the SSD save
                    cpu_scheduler.put((3, "DEFERRED_SAVE_RECOVERY", save_metadata, ()))
                log(f"[SUCCESS] {chunk} replicated to Unit {target_node}")
                
    if tasks:
        log(f"[SUCCESS] CLUSTER STABILIZED: {len(tasks)} shards recovered.")

def get_chunk(port, chunk):
    try:
        s = socket.socket()
        s.settimeout(5)
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