from flask import Flask, request
app = Flask(__name__)

@app.route("/api/upload", methods=["POST"])
def upload():
    path = request.form.get("path", "unknown")
    file = request.files.get("file")
    print(f"  Received: {path} ({len(file.read())} bytes)")
    return "OK", 200

if __name__ == "__main__":
    app.run(port=5050)