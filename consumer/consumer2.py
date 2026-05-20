import asyncio
from aiokafka import AIOKafkaConsumer
import os
import json
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")

TOPICS = ["f1_events", "nascar_events", "lemans_events"]

leaders = defaultdict(lambda: {"driver": "Unknown", "position": 999})

async def consume_consumer2():
    """Consumer 2: отслеживает лидеров и выводит сводку"""
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{KAFKA_CONSUMER_GROUP}_consumer2",
        auto_offset_reset='earliest'
    )
    await consumer.start()
    
    message_count = defaultdict(int)
    
    try:
        async for msg in consumer:
            topic = msg.topic
            event = json.loads(msg.value.decode('utf-8'))
            race = event["race"]
            
            message_count[race] += 1
            if event["position"] == 1:
                leaders[race] = {
                    "driver": event["driver"],
                    "position": event["position"],
                    "lap": event["lap"]
                }
            
            if sum(message_count.values()) % 12 == 0:
                print("\n" + "="*60)
                print("[CONSUMER2] 🏆 CURRENT LEADERS:")
                print("="*60)
                for race in ["F1", "NASCAR", "LeMans"]:
                    leader_info = leaders[race]
                    print(f"  {race:8} → {leader_info['driver']:15} | Lap {leader_info.get('lap', 0):3} | "
                          f"Messages: {message_count[race]}")
                print("="*60 + "\n")
    
    finally:
        await consumer.stop()

if __name__ == "__main__":
    print("Starting Consumer 2 (Leaders Tracker)...")
    asyncio.run(consume_consumer2())
