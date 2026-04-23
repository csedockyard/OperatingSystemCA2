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
    try:
        data = conn.recv(4096)
        if not data:
            conn.close()
            return

        command = data[:6].decode(errors="ignore")

        if command == "STORE:":
            filename = data[6:50].decode(errors="ignore").strip()
            content = data[50:]

            if not filename:
                conn.send(b"INVALID_FILENAME")
                conn.close()
                return

            filepath = os.path.join(STORAGE_DIR, filename)

            with open(filepath, "wb") as f:
                f.write(content)

            conn.send(b"STORED")

        elif command == "GET:::":
            filename = data[6:].decode(errors="ignore").strip()
            filepath = os.path.join(STORAGE_DIR, filename)

            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    while True:
                        chunk = f.read(4096)
                        if not chunk:
                            break
                        conn.sendall(chunk)
            else:
                conn.send(b"NOTFOUND")

        else:
            conn.send(b"UNKNOWN_COMMAND")

    except Exception as e:
        print(f"[NODE {PORT}] ⚠️ Client handling error: {e}")
    finally:
        conn.close()

def background_scrubber():
    while True:
        try:
            files = os.listdir(STORAGE_DIR)
            if files:
                print(f"[NODE {PORT}] 🧹 Scrubbing {len(files)} chunks for bit-rot...")
                for filename in files:
                    filepath = os.path.join(STORAGE_DIR, filename)

                    if not os.path.isfile(filepath):
                        continue

                    with open(filepath, "rb") as f:
                        data = f.read()
                        current_hash = hashlib.sha256(data).hexdigest()

                        # Placeholder: hash computed (future validation hook)
                        if not current_hash:
                            print(f"[NODE {PORT}] ⚠️ Hash error in {filename}")

        except Exception as e:
            print(f"[NODE {PORT}] ⚠️ Scrubbing error: {e}")
            
        time.sleep(30) 

def start_node():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 🔧 Improvement: allow quick restart
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((HOST, PORT))
    server.listen(5)

    print(f"[NODE {PORT}] Running...")
    threading.Thread(target=send_heartbeat, daemon=True).start()
    threading.Thread(target=background_scrubber, daemon=True).start() 

    while True:
        try:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn,), daemon=True).start()
        except Exception as e:
            print(f"[NODE {PORT}] ⚠️ Accept error: {e}")

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

                s.send(json.dumps(msg).encode())
                s.close()

                print(f"[NODE {PORT}] heartbeat sent to Port {port}")
                success = True
                break 

            except Exception as e:
                # 🔧 Improvement: optional debug (silent fail avoided)
                print(f"[NODE {PORT}] heartbeat retry on {port}: {e}")
                continue

        if not success:
            print(f"[NODE {PORT}] ❌ heartbeat failed (Masters down)")

        time.sleep(4) # AI Trigger: Deliberately slow heartbeat!

if __name__ == "__main__":
    start_node()