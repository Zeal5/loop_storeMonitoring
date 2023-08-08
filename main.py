from fastapi import FastAPI
from database.connections import get_connection
from on_server_startup import on_startup

app = FastAPI()





@app.on_event("startup")
async def startup_event():
    await on_startup()
    