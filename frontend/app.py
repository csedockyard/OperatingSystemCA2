from flask import Flask, render_template, request, jsonify, send_file
import socket
import json
import os
import sys

# FIX PATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client.client import upload_file, download_file

app = Flask(__name__)


def send_to_master(payload):
    try:
        s = socket.socket()
        s.connect(("127.0.0.1", 5000))
        s.send(json.dumps(payload).encode())

        res = s.recv(4096)
        s.close()
        return res.decode() if res else ""
    except:
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

    upload_file(path)

    return jsonify({"msg": "Uploaded"})


@app.route("/download", methods=["POST"])
def download():
    filename = request.json["filename"]

    download_file(filename)

    path = f"downloaded_{filename}"

    if os.path.exists(path):
        return send_file(path, as_attachment=True)

    return jsonify({"error": "Failed"})


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
    return jsonify({"logs": json.loads(res)})


@app.route("/nodes")
def nodes():
    res = send_to_master({"type": "get_nodes"})
    return jsonify({"nodes": json.loads(res)})


if __name__ == "__main__":
    app.run(port=3000, debug=True)