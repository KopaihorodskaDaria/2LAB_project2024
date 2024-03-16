import json
from sqlalchemy.orm import Session
from typing import Set, List
from fastapi import Depends
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

    class Config:
        orm_mode = True


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime


class ProcessedAgentDataRequest(BaseModel):
    road_state: str
    agent_data: AgentData

# FastAPI WebSocket endpoint
subscriptions: Set[WebSocket] = set()

@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)


async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


def create_db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# FastAPI CRUD endpoints

# Insert data to database
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentDataRequest], db: Session = Depends(create_db_session)):
    try:
        for element in data:
            road_state = element.road_state
            accelerometer = element.agent_data.accelerometer
            gps = element.agent_data.gps
            timestamp = element.agent_data.timestamp

            db.execute(processed_agent_data.insert().values(
                road_state=road_state,
                x=accelerometer.x,
                y=accelerometer.y,
                z=accelerometer.z,
                latitude=gps.latitude,
                longitude=gps.longitude,
                timestamp=timestamp
            ))
        db.commit()
        return {"status": "data added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="data create failed")

# Get data by id
@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(create_db_session)):
    try:
        query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        res = db.execute(query)
        data = res.fetchone()

        if data is None:
            raise HTTPException(status_code=404, detail="data not exist")

        return ProcessedAgentDataInDB(
            id=data.id,
            road_state=data.road_state,
            x=data.x,
            y=data.y,
            z=data.z,
            latitude=data.latitude,
            longitude=data.longitude,
            timestamp=data.timestamp
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# Get list of data
@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data(db: Session = Depends(create_db_session)):
    try:
        query = select(processed_agent_data)
        res = db.execute(query)
        getList = res.fetchall()

        list_processed_data = []

        for element in getList:
            processed_data = ProcessedAgentDataInDB(
                id=element.id,
                road_state=element.road_state,
                x=element.x,
                y=element.y,
                z=element.z,
                latitude=element.latitude,
                longitude=element.longitude,
                timestamp=element.timestamp
            )
            list_processed_data.append(processed_data)

        return list_processed_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# Update data
@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentDataRequest, db: Session = Depends(create_db_session)):
    try:
        query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        res = db.execute(query)
        existing_data = res.fetchone()

        if existing_data is None:
            raise HTTPException(status_code=404, detail="data not exist")

        road_state = data.road_state
        accelerometer = data.agent_data.accelerometer
        gps = data.agent_data.gps
        timestamp = data.agent_data.timestamp

        update_values = {
            "road_state": road_state,
            "x": accelerometer.x,
            "y": accelerometer.y,
            "z": accelerometer.z,
            "latitude": gps.latitude,
            "longitude": gps.longitude,
            "timestamp": timestamp
        }

        query = (
            processed_agent_data
            .update()
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values(**update_values)
            .returning(processed_agent_data)
        )
        updated_data = db.execute(query).fetchone()
        db.commit()

        return ProcessedAgentDataInDB(
            id=updated_data.id,
            road_state=updated_data.road_state,
            x=updated_data.x,
            y=updated_data.y,
            z=updated_data.z,
            latitude=updated_data.latitude,
            longitude=updated_data.longitude,
            timestamp=updated_data.timestamp
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# Delete by id
@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=None)
def delete_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(create_db_session)):
    try:
        query = processed_agent_data.delete().where(processed_agent_data.c.id == processed_agent_data_id)
        db.execute(query)
        db.commit()
        return {"status": "data deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
