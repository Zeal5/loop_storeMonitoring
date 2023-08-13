from fastapi import FastAPI, BackgroundTasks
from database.connections import get_connection
from on_server_startup import on_startup
from reports.generate_report import report
from reports.report_status import cache

app = FastAPI()
 



@app.on_event("startup")
async def startup_event():
    await on_startup()
    print("startup complete")
    


@app.get("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    background_tasks.add_task(report)
    cache["report_status"] = "Running"
    return {"message": "report generation triggered"}


@app.get("/get_report/{report_id}")
async def get_report(report_id: int):
    cached_value = cache.get("report_status")
    async with get_connection() as conn:
        async with conn.transaction():
            hourly_result = await conn.fetch("SELECT * FROM last_hour_activity WHERE store_id = $1", report_id)
            if hourly_result:
                daily_result = await conn.fetch("SELECT * FROM last_day_activity WHERE store_id = $1", report_id)
                weekly_result = await conn.fetch("SELECT * FROM last_week_activity WHERE store_id = $1", report_id)
                result = f"""{report_id},{hourly_result[0]['active_time']},{daily_result[0]['active_time']},{weekly_result[0]['active_time']},{hourly_result[0]['inactive_time']},{daily_result[0]['inactive_time']},{weekly_result[0]['inactive_time']}"""                                                 
                return result
            else:
                return {"message": cached_value}



