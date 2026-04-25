import socket
import os
import threading 
import time
import json
import hashlib

MASTER_HOST = '127.0.0.1'
MASTER_PORTS = [5000, 5001]

HOST = '127.0.0.1'
PORT = int(input("Enter node port: "))
STORAGE_DIR = f"storage/node_{PORT}/"

os.makedirs(STORAGE_DIR, exist_ok=True)
print(f"📁 Storage initialized at {STORAGE_DIR}")

def handle_client(conn):
    full_data = b""
    while True:
        packet = conn.recv(4096)
        if not packet:
            break 
        full_data += packet

    command = full_data[:6].decode(errors="ignore")

    if command == "STORE:":
        filename = full_data[6:50].decode(errors="ignore").strip()
        content = full_data[50:] 

        with open(STORAGE_DIR + filename, "wb") as f:
            f.write(content)

        conn.sendall(b"STORED")

    elif command == "GET:::":
        filename = full_data[6:].decode(errors="ignore").strip()
        filepath = STORAGE_DIR + filename

        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                while True:
                   chunk = f.read(4096)
                   if not chunk:
                       break
                   conn.sendall(chunk)
        else:
            conn.sendall(b"NOTFOUND")

    conn.close()

def background_scrubber():
    while True:
        try:
            files = os.listdir(STORAGE_DIR)
            if files:
                print(f"[NODE {PORT}] 🧹 Scrubbing {len(files)} chunks for bit-rot...")
                for filename in files:
                    filepath = os.path.join(STORAGE_DIR, filename)
                    with open(filepath, "rb") as f:
                        data = f.read()
                        current_hash = hashlib.sha256(data).hexdigest()
        except Exception as e:
            print(f"[NODE {PORT}] ⚠️ Scrubbing error: {e}")
            
        time.sleep(30) 

def start_node():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"[NODE {PORT}] Running...")
    threading.Thread(target=send_heartbeat, daemon=True).start()
    threading.Thread(target=background_scrubber, daemon=True).start() 

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

def send_heartbeat():
    while True:
        success = False
        for port in MASTER_PORTS:
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect((MASTER_HOST, port))

                msg = {
                    "type": "heartbeat",
                    "node": PORT
                }

                s.sendall(json.dumps(msg).encode())
                s.shutdown(socket.SHUT_WR) # Safe close
                s.close()
                
                print(f"[NODE {PORT}] heartbeat sent to Port {port}")
                success = True
                break 
            except:
                continue

        if not success:
            print(f"[NODE {PORT}] ❌ heartbeat failed (Masters down)")

        time.sleep(4) 

if __name__ == "__main__":
    start_node()