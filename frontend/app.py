from flask import Flask, render_template, request, jsonify, send_file
import socket
import json
import os
import sys
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client.client import upload_file, download_file

app = Flask(__name__)

MASTER_PORTS = [5000, 5001] 

def send_to_master(payload):
    for port in MASTER_PORTS:
        try:
            s = socket.socket()
            s.settimeout(5)
            s.connect(("127.0.0.1", port))
            
            s.sendall(json.dumps(payload).encode())
            s.shutdown(socket.SHUT_WR)

            res = b""
            while True:
                packet = s.recv(4096)
                if not packet: break
                res += packet
                
            s.close()
            return res.decode('utf-8') if res else ""
        except (ConnectionRefusedError, socket.timeout):
            continue
            
    return "" 

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    file = request.files["file"]

    os.makedirs("temp", exist_ok=True)
    path = os.path.join("temp", file.filename)
    file.save(path)

    threading.Thread(target=upload_file, args=(path,), daemon=True).start()

    return jsonify({"msg": "Upload initiated in background. Check cluster logs."})

@app.route("/download", methods=["POST"])
@app.route("/download", methods=["POST"])
def download():
    filename = request.json["filename"]
    
    # Run the client download logic
    result = download_file(filename)
    
    # Catch backend logic errors
    if result and result.startswith("Error"):
        return jsonify({"error": result}), 400

    path = f"downloaded_{filename}"

    if os.path.exists(path):
        try:
            # FIX: Force OS Absolute Path to prevent Flask security crash
            absolute_path = os.path.abspath(path)
            return send_file(absolute_path, as_attachment=True)
        except Exception as e:
            return jsonify({"error": f"Flask File System Error: {str(e)}"}), 500

    return jsonify({"error": "Failed to locate assembled file on disk"}), 400

@app.route("/kill", methods=["POST"])
def kill():
    node = request.json["node"]
    send_to_master({
        "type": "kill_node",
        "node": node
    })
    return jsonify({"msg": f"Node {node} killed"})

@app.route("/logs")
def logs():
    res = send_to_master({"type": "get_logs"})
    return jsonify({"logs": json.loads(res)} if res else {"logs": []})

@app.route("/nodes")
def nodes():
    res = send_to_master({"type": "get_nodes"})
    return jsonify({"nodes": json.loads(res)} if res else {"nodes": {"active": [], "degraded": [], "all": []}})

@app.route("/stats")
def stats():
    res = send_to_master({"type": "get_stats"})
    return jsonify({"stats": json.loads(res)} if res else {"stats": []})

@app.route("/delete", methods=["POST"])
def delete_file():
    filename = request.json["filename"]
    send_to_master({
        "type": "delete",
        "filename": filename
    })
    return jsonify({"msg": f"Deleted {filename}"})

if __name__ == "__main__":
    app.run(port=3000, debug=True)