from fastapi import FastAPI, BackgroundTasks
from database.connections import get_connection
from on_server_startup import on_startup
from reports.generate_report import report
from reports.report_status import cache
import uvicorn

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    # await on_startup()
    await report()
    
"""
@app.get("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    background_tasks.add_task(report)
    cache["report_status"] = "Running"

    return {"message": "report generation triggered"}


@app.get("/get_report/{report_id}")
async def get_report(report_id: int):
    cached_value = cache.get("report_status")
    return cached_value
"""


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)