from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # usa 127.0.0.1:9092 si estás en Windows sin WSL
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbols = [f"STK{i:03d}" for i in range(1, 6)]  # Solo 5 para pruebas

while True:
    for symbol in symbols:
        tick = {
            "symbol": symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "price": round(random.uniform(10, 1000), 2),
            "volume": random.randint(100, 10000)
        }

        # Send message and get metadata to confirm success
        future = producer.send("acciones-ticks", key=symbol, value=tick)
        metadata = future.get(timeout=10)
        print(f"✅ Sent {symbol} to partition {metadata.partition}, offset {metadata.offset}")
    time.sleep(1)
