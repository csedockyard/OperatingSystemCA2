import socket
import threading
import json
import time 

HOST = '127.0.0.1'
PORT = 5000
active_nodes = {}

# file -> chunk -> nodes mapping
metadata = {}

def handle_client(conn, addr):
    print(f"[MASTER] Connected: {addr}")
    
    data = conn.recv(4096).decode()
    request = json.loads(data)

    if request["type"] == "upload":
        filename = request["filename"]
        chunks = request["chunks"]

        metadata[filename] = chunks

        response = {"status": "ok", "message": "Metadata stored"}
        conn.send(json.dumps(response).encode())

    elif request["type"] == "download":
        filename = request["filename"]

        if filename in metadata:
            response = {"status": "ok", "chunks": metadata[filename]}
        else:
            response = {"status": "error", "message": "File not found"}

        conn.send(json.dumps(response).encode())
    
    elif request["type"] == "heartbeat":
        node = request["node"]
        active_nodes[node] = time.time()
        print(f"[MASTER] Heartbeat from node {node}")

    conn.close()

def start_master():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"[MASTER] Running on {HOST}:{PORT}")
    threading.Thread(target=monitor_nodes, daemon=True).start()

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()

def monitor_nodes():
    while True:
        now = time.time()

        for node in list(active_nodes.keys()):
            if now - active_nodes[node] > 5:
                print(f"[MASTER] Node {node} DEAD")
                del active_nodes[node]
                re_replicate(node)

        time.sleep(2)

def re_replicate(dead_node):
    for filename in metadata:
        for chunk in metadata[filename]:
            nodes = metadata[filename][chunk]

            if dead_node in nodes:
                print(f"[MASTER] Re-replicating {chunk}")

                nodes.remove(dead_node)

                if len(nodes) == 0:
                    print(f"[MASTER] No source available for {chunk}")
                    continue

                source_node = nodes[0]

                # fetch chunk from alive node
                data = get_chunk_from_node(source_node, chunk)

                if data is None:
                    print(f"[MASTER] Failed to fetch {chunk} from node{source_node}")
                    continue

                # find new node
                for new_node in active_nodes:
                    if new_node not in nodes:
                        print(f"[MASTER] Trying to copy {chunk} from {source_node} → {new_node}")
                        success = send_chunk_to_node(new_node, chunk, data)

                        if success:
                            nodes.append(new_node)
                            print(f"[MASTER] {chunk} copied to node{new_node}")
                        else:
                            print(f"[MASTER] Failed to send {chunk} to node{new_node}")

                        break

def get_chunk_from_node(port, chunk_name):
    try:
        s = socket.socket()
        s.settimeout(3)
        s.connect(('127.0.0.1', port))

        header = f"GET:::{chunk_name}".encode()
        s.send(header)

        data = b""

        while True:
            try:
                packet = s.recv(4096)
                if not packet:
                    break
                data += packet
            except:
                break

        s.close()

        if data == b"" or data == b"NOTFOUND":
            return None

        return data

    except Exception as e:
        print(f"[MASTER] Error fetching chunk: {e}")
        return None
    
def send_chunk_to_node(port, chunk_name, data):
    try:
        s = socket.socket()
        s.connect(('127.0.0.1', port))

        header = f"STORE:{chunk_name:<44}".encode()
        s.send(header + data)

        response = s.recv(1024)
        s.close()

        return response == b"STORED"
    except Exception as e:
        print(f"[MASTER] Error sending chunk: {e}")
        return False

if __name__ == "__main__":
    start_master()