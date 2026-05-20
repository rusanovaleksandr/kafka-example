from fastapi import FastAPI
import router
import asyncio

app = FastAPI()

@app.get('/')
async def home():
    return {"status": "Race Events Producer API is running", "available_endpoints": ["/start_race_events", "/race_status", "/docs"]}

app.include_router(router.route)

@app.on_event("startup")
async def startup_event():
    print("Starting race event generation...")
    asyncio.create_task(router.generate_race_events_loop())