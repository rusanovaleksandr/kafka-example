import asyncio
from aiokafka import AIOKafkaConsumer
import os
import json
from dotenv import load_dotenv

import asyncio
from aiokafka import AIOKafkaConsumer
import os
import json
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")

TOPICS = ["f1_events", "nascar_events", "lemans_events"]

async def consume_consumer1():
    """Consumer 1: выводит события гонок в реальном времени"""
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{KAFKA_CONSUMER_GROUP}_consumer1",
        auto_offset_reset='earliest'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            topic = msg.topic
            event = json.loads(msg.value.decode('utf-8'))
            emoji_map = {"F1": "🏎️", "NASCAR": "🇺🇸", "LeMans": "🇫🇷"}
            emoji = emoji_map.get(event["race"], "📍")
            print(f"[CONSUMER1] {emoji} {event['race']} UPDATE: {event['driver']} "
                  f"at P{event['position']} on lap {event['lap']} @ {event['timestamp']}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    print("Starting Consumer 1...")
    asyncio.run(consume_consumer1())