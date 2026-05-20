from fastapi import APIRouter
from schema import RaceEvent
from aiokafka import AIOKafkaProducer
import json
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

route = APIRouter()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

races_data = {
    "F1": {
        "topic": "f1_events",
        "drivers": ["Hamilton", "Verstappen", "Sainz", "Leclerc"],
        "max_laps": 70
    },
    "NASCAR": {
        "topic": "nascar_events",
        "drivers": ["Johnson", "Elliott", "Logano", "Larson"],
        "max_laps": 400
    },
    "LeMans": {
        "topic": "lemans_events",
        "drivers": ["Porsche Team", "Ferrari Team", "Toyota Team", "BMW Team"],
        "max_laps": 350
    }
}

race_state = {
    "F1": {"current_lap": 0, "positions": [1, 2, 3, 4]},
    "NASCAR": {"current_lap": 0, "positions": [1, 2, 3, 4]},
    "LeMans": {"current_lap": 0, "positions": [1, 2, 3, 4]}
}

@route.post('/start_race_events')
async def start_race_events():
    """Запуск генерации событий гонок в фоне"""
    asyncio.create_task(generate_race_events_loop())
    return {"status": "Race events started"}

async def generate_race_events_loop():
    """Бесконечный цикл генерации и публикации событий"""
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    
    try:
        iteration = 0
        while True:
            iteration += 1
            
            for race_name, race_info in races_data.items():
                topic = race_info["topic"]
                drivers = race_info["drivers"]
                
                state = race_state[race_name]
                state["current_lap"] = (state["current_lap"] + 1) % race_info["max_laps"]
                
                for idx, driver in enumerate(drivers):
                    position = state["positions"][idx]
                    event = RaceEvent(
                        race=race_name,
                        driver=driver,
                        position=position,
                        lap=state["current_lap"],
                        timestamp=datetime.utcnow().isoformat()
                    )
                    
                    value_json = json.dumps(event.dict()).encode('utf-8')
                    await producer.send_and_wait(topic=topic, value=value_json)
                    print(f"[PRODUCER] {race_name}: {driver} at P{position} on lap {state['current_lap']}")
            
            import random
            for race_name in race_state:
                if iteration % 5 == 0:
                    positions = race_state[race_name]["positions"]
                    i, j = random.sample(range(len(positions)), 2)
                    positions[i], positions[j] = positions[j], positions[i]
            
            await asyncio.sleep(2)
    
    finally:
        await producer.stop()

@route.get('/race_status')
async def get_race_status():
    """Текущий статус гонок"""
    return race_state