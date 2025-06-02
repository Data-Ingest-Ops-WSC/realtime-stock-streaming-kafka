from kafka import KafkaConsumer
import json

# ✅ Kafka Consumer instance
consumer = KafkaConsumer(
    'acciones-ticks',  # Topic to subscribe to
    bootstrap_servers='localhost:9092',  # Kafka broker address
    group_id='lector-acciones',  # Consumer group ID (enables parallelism and offset management)
    auto_offset_reset='earliest',  # Start from the beginning if no committed offset exists
    enable_auto_commit=True,  # Auto-commit the offset after reading
    key_deserializer=lambda k: k.decode('utf-8'),  # Decode the message key
    value_deserializer=lambda v: json.loads(v)  # Parse the JSON message value
)

# ✅ Consume and display messages
for msg in consumer:
    data = msg.value

    # Access Kafka metadata
    partition = msg.partition
    offset = msg.offset
    key = msg.key

    # Print the message with its metadata
    print(f"[{key}] Partition {partition} | Offset {offset} → ${data['price']} @ {data['volume']}")
