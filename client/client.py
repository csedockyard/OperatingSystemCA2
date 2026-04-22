import socket
import json
import os
import hashlib

MASTER_HOST = '127.0.0.1'
MASTER_PORTS = [5000, 5001]

CHUNK_SIZE = 1024 * 512 # 512KB chunks
REPLICATION_FACTOR = 2


def _contact_master(payload):
    """Helper function to handle Master HA Failover safely"""
    for port in MASTER_PORTS:
        try:
            s = socket.socket()
            s.settimeout(5)
            s.connect((MASTER_HOST, port))
            
            s.sendall(json.dumps(payload).encode())
            s.shutdown(socket.SHUT_WR)
            
            res = b""
            while True:
                packet = s.recv(4096)
                if not packet:
                    break
                res += packet
                
            s.close()
            return res
        except Exception as e:  # 🔧 better debugging
            print(f"Master {port} failed: {e}")
            continue
    return None


def get_active_nodes():
    res = _contact_master({"type": "get_nodes"})
    if res:
        try:
            return json.loads(res.decode('utf-8')).get("active", [])
        except:
            return []
    return []


def calculate_checksum(data):
    return hashlib.sha256(data).hexdigest()


def split_file(filepath):
    chunks = []
    
    if not os.path.exists(filepath):  # 🔧 safety check
        return chunks

    with open(filepath, "rb") as f:
        i = 0
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            chunk_name = f"chunk_{i}"
            chunks.append((chunk_name, data))
            i += 1
    return chunks


def send_to_node(port, chunk_name, data):
    try:
        s = socket.socket()
        s.settimeout(5)
        s.connect(('127.0.0.1', port))
        
        header = f"STORE:{chunk_name:<44}".encode()
        s.sendall(header + data) 
        s.shutdown(socket.SHUT_WR) 
        
        res = b""
        while True:
            packet = s.recv(1024)
            if not packet:
                break
            res += packet
            
        s.close()
        return res
    except Exception as e:
        print(f"Error sending chunk to node {port}: {e}")
        return None


def upload_file(filepath):
    chunks = split_file(filepath)
    mapping = {}

    if not chunks:  # 🔧 file validation
        return "Error: File not found or empty"

    active_nodes = get_active_nodes()
    if not active_nodes:
        return "Error: No active nodes"

    # ==========================================
    # 🧠 THE AI HEURISTIC: Size-Based Tiering
    # ==========================================
    
    if len(chunks) <= 10:
        target_rf = 3
        print(f"Heuristic: Small file detected ({len(chunks)} chunks). Assigning High-Availability Tier (3.0x)")
    else:
        target_rf = 2
        print(f"Heuristic: Large file detected ({len(chunks)} chunks). Assigning Storage-Efficient Tier (2.0x)")

    actual_rf = min(target_rf, len(active_nodes))
    # ==========================================

    for i, (name, data) in enumerate(chunks):
        assigned_nodes = []
        checksum = calculate_checksum(data)

        for r in range(actual_rf):
            node_port = active_nodes[(i + r) % len(active_nodes)]
            
            res = send_to_node(node_port, name, data)
            if res is None:  # 🔧 retry protection
                continue

            assigned_nodes.append(node_port)

        if not assigned_nodes:  # 🔧 fail-safe
            return f"Error: Failed to store chunk {name}"

        mapping[name] = {
            "ports": assigned_nodes,
            "checksum": checksum
        }

    request = {
        "type": "upload",
        "filename": os.path.basename(filepath),
        "chunks": mapping
    }
    
    res = _contact_master(request)
    return res.decode('utf-8') if res else "Error: All masters offline"


def get_chunk_from_node(port, chunk_name):
    try:
        s = socket.socket()
        s.settimeout(5)
        s.connect(('127.0.0.1', port))
        
        s.sendall(f"GET:::{chunk_name}".encode())
        s.shutdown(socket.SHUT_WR)

        data = b""
        while True:
            packet = s.recv(4096)
            if not packet:
                break
            data += packet

        s.close()
        return data if data else None
    except Exception as e:  # 🔧 debug info
        print(f"Node {port} fetch failed: {e}")
        return None


def download_file(filename):
    res = _contact_master({
        "type": "download",
        "filename": filename
    })

    if not res:
        return "Error: All masters offline"

    try:
        response = json.loads(res.decode('utf-8'))
    except:
        return "Error: Invalid response from master"

    if response.get("status") != "ok":
        return "File not found"

    file_data = b""

    for chunk_name in sorted(response["chunks"].keys(), key=lambda x: int(x.split('_')[1])):
        chunk_info = response["chunks"][chunk_name]
        chunk_data = None
        
        for node in chunk_info["ports"]:
            data = get_chunk_from_node(node, chunk_name)
            if data:
                if calculate_checksum(data) == chunk_info["checksum"]:
                    chunk_data = data
                    break
                else:
                    print(f"⚠️ Checksum mismatch for {chunk_name} on node {node}")

        if chunk_data:
            file_data += chunk_data
        else:
            return f"Error: Missing or corrupted chunk {chunk_name}"

    try:
        with open("downloaded_" + filename, "wb") as f:
            f.write(file_data)
    except Exception as e:
        return f"Error writing file: {e}"

    return "Download complete"