from flask import Flask
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)

socketio = SocketIO(app, cors_allowed_origins="*")

consumer = KafkaConsumer(
    'cyber_war',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def stream_events():

    for message in consumer:

        event = message.value

        print("[WAR EVENT]", event)

        socketio.emit("war_event", event)

@app.route("/")
def home():
    return "Cyber Warfare Backend Running"

if __name__ == "__main__":

    threading.Thread(target=stream_events).start()

    socketio.run(app, host="0.0.0.0", port=5000)