import asyncio

from reports.report_status import cache


async def report():
    await asyncio.sleep(33)
    cache["report_status"] = "complete"
