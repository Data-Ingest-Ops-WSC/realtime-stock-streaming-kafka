from kafka import KafkaProducer
import json, random, time
from datetime import datetime

# ✅ Kafka Producer instance
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker address
    key_serializer=lambda k: k.encode('utf-8'),  # Encode the message key (symbol)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize message value as JSON
)

# ✅ Generate a list of 500 stock symbols (STK001 to STK500)
symbols = [f"STK{i:03d}" for i in range(1, 501)]

# ✅ Simulate real-time streaming of stock ticks
while True:
    for symbol in symbols:
        # Build a tick record (a Kafka message value)
        tick = {
            "symbol": symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "price": round(random.uniform(10, 1000), 2),
            "volume": random.randint(100, 10000)
        }

        # ✅ Send to topic 'acciones-ticks'
        # Kafka will:
        # - Use the symbol as the key
        # - Hash the key to determine the partition
        # - Always route the same key to the same partition (ensures message order per symbol)
        producer.send("acciones-ticks", key=symbol, value=tick)

        # Optional: print what was sent
        print(f"[{symbol}] Sent → ${tick['price']} @ {tick['volume']}")

    # Wait 1 second before the next batch of ticks
    time.sleep(1)
