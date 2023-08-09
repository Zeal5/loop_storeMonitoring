import asyncpg
from contextlib import asynccontextmanager


# Create the connection pool
pool = None


async def create_pool():
    hostname = "192.168.56.1"
    database_name = "loop"
    port = 5432
    owner = "postgres"
    password = "zeal"
    global pool
    pool = await asyncpg.create_pool(
        host=hostname,
        port=port,
        database=database_name,
        user=owner,
        password=password,
    )


async def close_pool():
    global pool
    if pool is not None:
        await pool.close()


@asynccontextmanager
async def get_connection():
    conn = None
    try:
        if pool is None:
            await create_pool()  # Create the pool if it doesn't exist

        conn = await pool.acquire()  # Acquire a connection from the pool
        yield conn

    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            await pool.release(conn)  # Release the connection back to the pool


# Make sure to close the pool when your application shuts down
async def on_shutdown():
    await close_pool()
