import socket
import os
import threading 
import time
import json
import hashlib

MASTER_HOST = '127.0.0.1'
MASTER_PORT = 5000

HOST = '127.0.0.1'
PORT = int(input("Enter node port: "))
STORAGE_DIR = f"storage/node_{PORT}/"

os.makedirs(STORAGE_DIR, exist_ok=True)
print(f"📁 Storage initialized at {STORAGE_DIR}")

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

def background_scrubber():
    while True:
        try:
            files = os.listdir(STORAGE_DIR)
            if files:
                print(f"[NODE {PORT}] 🧹 Scrubbing {len(files)} chunks for bit-rot...")
                for filename in files:
                    filepath = os.path.join(STORAGE_DIR, filename)
                    # Just reading the file ensures the OS can still access the sectors
                    with open(filepath, "rb") as f:
                        data = f.read()
                        # Calculate hash to simulate integrity check
                        current_hash = hashlib.sha256(data).hexdigest()
                        # In a full implementation, you would compare this hash with the master's metadata
        except Exception as e:
            print(f"[NODE {PORT}] ⚠️ Scrubbing error: {e}")
            
        time.sleep(30) # Run every 30 seconds

def start_node():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"[NODE {PORT}] Running...")
    threading.Thread(target=send_heartbeat, daemon=True).start()
    threading.Thread(target=background_scrubber, daemon=True).start() # Add this line

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

        time.sleep(4)  # every 2 sec

if __name__ == "__main__":
    start_node()

#added this to test