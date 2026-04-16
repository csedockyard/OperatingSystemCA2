import socket
import json
import os
import hashlib

MASTER_HOST = '127.0.0.1'
MASTER_PORT = 5000

CHUNK_SIZE = 1024 * 512 # 512KB chunks
REPLICATION_FACTOR = 2

def get_active_nodes():
    try:
        s = socket.socket()
        s.connect((MASTER_HOST, MASTER_PORT))
        s.send(json.dumps({"type": "get_nodes"}).encode())
        res = json.loads(s.recv(4096).decode())
        s.close()
        return res["active"]
    except:
        return []

def calculate_checksum(data):
    return hashlib.sha256(data).hexdigest()


def split_file(filepath):
    chunks = []
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
    s = socket.socket()
    s.connect(('127.0.0.1', port))

    header = f"STORE:{chunk_name:<44}".encode()
    s.send(header + data)

    res = s.recv(1024)
    s.close()
    return res


def upload_file(filepath):
    chunks = split_file(filepath)
    mapping = {}

    active_nodes = get_active_nodes()
    if not active_nodes:
        return "Error: No active nodes"

    for i, (name, data) in enumerate(chunks):
        assigned_nodes = []
        checksum = calculate_checksum(data)

        for r in range(REPLICATION_FACTOR):
            node_port = active_nodes[(i + r) % len(active_nodes)]
            send_to_node(node_port, name, data)
            assigned_nodes.append(node_port)

        mapping[name] = {
            "ports": assigned_nodes,
            "checksum": checksum
        }

    s = socket.socket()
    s.connect((MASTER_HOST, MASTER_PORT))

    request = {
        "type": "upload",
        "filename": os.path.basename(filepath),
        "chunks": mapping
    }

    s.send(json.dumps(request).encode())
    res = s.recv(4096)
    s.close()

    return res.decode()


def get_chunk_from_node(port, chunk_name):
    try:
        s = socket.socket()
        s.settimeout(3)
        s.connect(('127.0.0.1', port))

        s.send(f"GET:::{chunk_name}".encode())

        data = b""
        while True:
            packet = s.recv(4096)
            if not packet:
                break
            data += packet

        s.close()
        return data if data else None
    except:
        return None


def download_file(filename):
    s = socket.socket()
    s.connect((MASTER_HOST, MASTER_PORT))

    s.send(json.dumps({
        "type": "download",
        "filename": filename
    }).encode())

    response = json.loads(s.recv(4096).decode())
    s.close()

    if response["status"] != "ok":
        return "File not found"

    file_data = b""

    for chunk_name in sorted(response["chunks"].keys()):
        chunk_info = response["chunks"][chunk_name]
        chunk_data = None
        
        for node in chunk_info["ports"]:
            data = get_chunk_from_node(node, chunk_name)
            if data:
                # Verify checksum
                if calculate_checksum(data) == chunk_info["checksum"]:
                    chunk_data = data
                    break
                else:
                    print(f"⚠️ Checksum mismatch for {chunk_name} on node {node}")

        if chunk_data:
            file_data += chunk_data
        else:
            return "Error: Data missing or corrupted"

    with open("downloaded_" + filename, "wb") as f:
        f.write(file_data)

    return "Download complete"