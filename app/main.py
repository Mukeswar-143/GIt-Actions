#Todo
#Add authentication

import asyncio
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Dict, Set
import select
import psycopg2
import psycopg2.extensions
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# recruiter_email -> set of WebSockets
connections: Dict[str, Set[WebSocket]] = {}

# thread pool for blocking tasks
executor = ThreadPoolExecutor(max_workers=10)

# PostgreSQL DSN (fill with your own credentials)
DSN_DEV= (
    "dbname=postgres "
    "user=postgres.heivjxaodqzygxutbbby "
    "password='Persimmon@12345' "
    "host=aws-0-ap-south-1.pooler.supabase.com "
    "port=5432 "
    "sslmode=require"
)


DSN_QA= (
    "dbname=postgres "
    "user=postgres.hvbyjegqzksqduwkhzcx "
    "password='Persimmon@12345' "
    "host=aws-0-ap-south-1.pooler.supabase.com "
    "port=5432 "
    "sslmode=require"
)

# this will hold the app event loop
app_loop: asyncio.AbstractEventLoop = None


async def run_blocking(func, *args, **kwargs):
    """
    Run a blocking function inside a thread pool executor
    without blocking the event loop.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


async def send_to_clients(recruiter_email: str, payload: dict):
    """Send message to all sockets for this recruiter."""
    print(f"Sending to clients of {recruiter_email}: {payload}")
    if recruiter_email not in connections:
        return
    dead = set()
    for ws in connections[recruiter_email]:
        try:
            print(f"Sending to {ws}")
            await ws.send_json(payload)
            print(f"Sent to {ws}")
        except Exception:
            dead.add(ws)
    # Clean up closed sockets
    for ws in dead:
        connections[recruiter_email].remove(ws)
    if not connections[recruiter_email]:
        del connections[recruiter_email]


def listen_postgres(loop: asyncio.AbstractEventLoop, dsn: str, channel: str = "recruiter_events"):
    """Runs in its own thread, listens for NOTIFY and dispatches."""
    while True:
        try:
            conn = psycopg2.connect(
                dsn,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
            )
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute(f"LISTEN {channel};")
            print(f"Listening on {channel} channel for DSN: {dsn}")

            while True:
                if select.select([conn], [], [], 10) == ([], [], []):
                    continue
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    try:
                        payload = json.loads(notify.payload)
                    except json.JSONDecodeError:
                        continue
                    recruiter_email = payload.get("recruiter_email")
                    if recruiter_email:
                        asyncio.run_coroutine_threadsafe(
                            send_to_clients(recruiter_email, payload), loop
                        )
                time.sleep(0.2)

        except psycopg2.OperationalError as e:
            print(f"Lost Postgres LISTEN connection for {dsn}: {e}, reconnecting in 5s")
            time.sleep(5)
            continue
        
async def heartbeat():
    while True:
        for email, websockets in list(connections.items()):
            dead = set()
            for ws in websockets:
                try:
                    await ws.send_json({"type": "ping"})
                except Exception:
                    dead.add(ws)
            for ws in dead:
                websockets.remove(ws)
            if not websockets:
                connections.pop(email, None)
        await asyncio.sleep(30)
        
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start Postgres listener thread when app starts."""
    global app_loop
    app_loop = asyncio.get_running_loop()
    threading.Thread(target=listen_postgres, args=(app_loop, DSN_DEV), daemon=True).start()
    threading.Thread(target=listen_postgres, args=(app_loop, DSN_QA), daemon=True).start()
    #threading.Thread(target=listen_postgres, args=(app_loop, DSN_PROD), daemon=True).start()
    asyncio.create_task(heartbeat())
    yield    

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws/{email}")
async def websocket_endpoint(ws: WebSocket, email: str):
    await ws.accept()

    if email not in connections:
        connections[email] = set()
    connections[email].add(ws)
    print(f"Client connected for {email}, total: {len(connections[email])}")

    try:
        while True:
            # receive data from client (if any)
            msg = await ws.receive_text()
            print(f"Received from {email}: {msg}")
           
    except WebSocketDisconnect:
        if email in connections and ws in connections[email]:
            connections[email].remove(ws)
            if not connections[email]:
                del connections[email]
        print(f"Client disconnected for {email}")
