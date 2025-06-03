from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'acciones-ticks',
    bootstrap_servers='localhost:9092',
    group_id='stock-tick-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    key_deserializer=lambda k: k.decode('utf-8'),
    value_deserializer=lambda v: json.loads(v)
)

for msg in consumer:
    data = msg.value
    print(f"[{msg.key}] Partition {msg.partition} | Offset {msg.offset} → ${data['price']} @ {data['volume']}")
