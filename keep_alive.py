from flask import Flask
from threading import Thread
import os

app = Flask('')

@app.route('/')
def home():
    return """
    <body style="background-color: #1a1a1a; color: #00ff00; font-family: monospace; text-align: center; padding-top: 50px;">
        <h1>📡 Scanner Web Service</h1>
        <p style="color: #ffffff;">Status: <span style="color: #00ff00;">ACTIVE</span></p>
        <hr style="width: 50%; border: 1px solid #333;">
        <p>Monitoring Telegram Channel for new reports...</p>
    </body>
    """

def run():
    # رندر پورت را خودکار مدیریت می‌کند
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()
