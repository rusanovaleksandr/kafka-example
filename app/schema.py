from pydantic import BaseModel

# Модель события гонки
class RaceEvent(BaseModel):
    race: str  # "F1", "NASCAR", "LeMans"
    driver: str
    position: int
    lap: int
    timestamp: str