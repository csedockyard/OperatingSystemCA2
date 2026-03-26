import socket
import json
import os

REPLICATION_FACTOR = 2

MASTER_HOST = '127.0.0.1'
MASTER_PORT = 5000

NODES = [5001, 5002, 5003]

CHUNK_SIZE = 1024


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

    print(s.recv(1024))
    s.close()


def upload(filepath):
    chunks = split_file(filepath)

    mapping = {}
    
    for i, (name, data) in enumerate(chunks):
        assigned_nodes = []

    for r in range(REPLICATION_FACTOR):
        node_port = NODES[(i + r) % len(NODES)]
        send_to_node(node_port, name, data)
        assigned_nodes.append(node_port)

        print(f"{name} stored in node{node_port}")

    mapping[name] = assigned_nodes

    # send metadata to master
    s = socket.socket()
    s.connect((MASTER_HOST, MASTER_PORT))

    request = {
        "type": "upload",
        "filename": os.path.basename(filepath),
        "chunks": mapping
    }

    s.send(json.dumps(request).encode())
    print(s.recv(1024))
    s.close()


def get_chunk_from_node(port, chunk_name):
    try:
        s = socket.socket()
        s.settimeout(2)  # fail fast
        s.connect(('127.0.0.1', port))

        header = f"GET:::{chunk_name}".encode()
        s.send(header)

        data = s.recv(4096)
        s.close()

        if data == b"NOTFOUND":
            return None
        
        return data

    except:
        return None  # node failed or unreachable

def download(filename):
    # ask master for chunk locations
    s = socket.socket()
    s.connect((MASTER_HOST, MASTER_PORT))

    request = {
        "type": "download",
        "filename": filename
    }

    s.send(json.dumps(request).encode())
    response = json.loads(s.recv(4096).decode())
    s.close()

    if response["status"] != "ok":
        print("File not found")
        return

    chunks = response["chunks"]

    file_data = b""

    for chunk_name in sorted(chunks.keys()):
        node_list = chunks[chunk_name]

        chunk_data = None

        for node in node_list:
            chunk_data = get_chunk_from_node(node, chunk_name)

            if chunk_data:
                print(f"{chunk_name} retrieved from node{node}")
                break
            else:
                print(f"node{node} failed for {chunk_name}")

        if chunk_data is None:
            print(f"FAILED to retrieve {chunk_name}")
            return

        file_data += chunk_data

    with open("downloaded_" + filename, "wb") as f:
        f.write(file_data)

    print("File reconstructed successfully!")


if __name__ == "__main__":
    choice = input("1. Upload\n2. Download\nChoose: ")

    if choice == "1":
        filepath = input("Enter file path: ")
        upload(filepath)

    elif choice == "2":
        filename = input("Enter filename: ")
        download(filename)