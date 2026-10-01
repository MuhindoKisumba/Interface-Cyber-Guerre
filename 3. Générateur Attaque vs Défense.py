from kafka import KafkaProducer
import random
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

sources = [
    {"country":"Russia","lat":61.5240,"lon":105.3188},
    {"country":"China","lat":35.8617,"lon":104.1954},
    {"country":"Brazil","lat":-14.2350,"lon":-51.9253},
    {"country":"Germany","lat":51.1657,"lon":10.4515}
]

targets = [
    {"country":"DR Congo","lat":-4.0383,"lon":21.7587},
    {"country":"USA","lat":37.0902,"lon":-95.7129},
    {"country":"France","lat":46.2276,"lon":2.2137}
]

attacks = [
    "DDoS",
    "SQL Injection",
    "Malware Beacon",
    "TOR Access",
    "SSH Bruteforce"
]

while True:

    src = random.choice(sources)
    tgt = random.choice(targets)

    payload = {
        "source_country": src["country"],
        "source_lat": src["lat"],
        "source_lon": src["lon"],

        "target_country": tgt["country"],
        "target_lat": tgt["lat"],
        "target_lon": tgt["lon"],

        "attack": random.choice(attacks),

        "blocked": random.choice([True, False])
    }

    producer.send("cyber_war", payload)

    print("[EVENT]", payload)

    time.sleep(1)