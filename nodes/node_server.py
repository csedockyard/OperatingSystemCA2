import socket
import os
import threading 
import time
import json

MASTER_HOST = '127.0.0.1'
MASTER_PORT = 5000

HOST = '127.0.0.1'
PORT = int(input("Enter node port: "))
STORAGE_DIR = f"nodes/node{PORT}/"

os.makedirs(STORAGE_DIR, exist_ok=True)

def handle_client(conn):
    data = conn.recv(4096)

    command = data[:6].decode()

    if command == "STORE:":
        filename = data[6:50].decode().strip()
        content = data[50:]

        with open(STORAGE_DIR + filename, "wb") as f:
            f.write(content)

        conn.send(b"STORED")

    elif command == "GET:::":
        filename = data[6:].decode().strip()

        filepath = STORAGE_DIR + filename

        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                while True:
                   chunk = f.read(4096)
                   if not chunk:
                       break
                   conn.sendall(chunk)
        else:
            conn.send(b"NOTFOUND")

    conn.close()

def start_node():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"[NODE {PORT}] Running...")
    threading.Thread(target=send_heartbeat, daemon=True).start()

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

def send_heartbeat():
    while True:
        try:
            print(f"[NODE {PORT}] sending heartbeat")
            s = socket.socket()
            s.connect((MASTER_HOST, MASTER_PORT))

            msg = {
                "type": "heartbeat",
                "node": PORT
            }

            s.send(json.dumps(msg).encode())
            s.close()
        except:
            print(f"[NODE {PORT}] heartbeat failed")

        time.sleep(2)  # every 2 sec

if __name__ == "__main__":
    start_node()