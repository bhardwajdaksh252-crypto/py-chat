"""
Real-Time Chat Server
=====================
Usage:
  pip install websockets
  python chat_server.py
"""

import asyncio
import json
import logging
from datetime import datetime

try:
    import websockets
    from websockets.server import serve
except ImportError:
    raise SystemExit("Run:  pip install websockets")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("chat")

connected: dict = {}
history:   list = []
MAX_HISTORY = 50
import os
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8765))

def now_ts(): return datetime.now().strftime("%H:%M:%S")
def packet(kind, **kw): return json.dumps({"type": kind, "ts": now_ts(), **kw})
def user_list(): return sorted(connected.values())

async def broadcast(data, exclude=None):
    targets = [ws for ws in connected if ws is not exclude]
    if targets:
        await asyncio.gather(*[ws.send(data) for ws in targets])

async def send_to(ws, data):
    try: await ws.send(data)
    except: pass

async def handler(ws):
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        msg = json.loads(raw)
    except:
        await ws.close(1008, "Bad handshake"); return

    if msg.get("type") != "join" or not str(msg.get("username","")).strip():
        await ws.close(1008, "Expected join packet"); return

    username = str(msg["username"]).strip()[:20]
    base, suffix = username, 1
    while username in connected.values():
        username = f"{base}_{suffix}"; suffix += 1

    connected[ws] = username
    log.info("+ %s joined (%d online)", username, len(connected))

    await send_to(ws, packet("welcome", username=username, users=user_list(), history=history))

    join_msg = packet("system", text=f"{username} joined the chat", users=user_list())
    history.append(json.loads(join_msg))
    if len(history) > MAX_HISTORY: history.pop(0)
    await broadcast(join_msg, exclude=ws)

    try:
        async for raw in ws:
            try: msg = json.loads(raw)
            except: continue

            if msg.get("type") == "message":
                text = str(msg.get("text","")).strip()[:500]
                if not text: continue
                out = packet("message", username=username, text=text)
                history.append(json.loads(out))
                if len(history) > MAX_HISTORY: history.pop(0)
                log.info("[%s] %s", username, text)
                await broadcast(out)

            elif msg.get("type") == "typing":
                await broadcast(packet("typing", username=username, typing=bool(msg.get("typing"))), exclude=ws)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected.pop(ws, None)
        log.info("- %s left (%d online)", username, len(connected))
        leave_msg = packet("system", text=f"{username} left the chat", users=user_list())
        history.append(json.loads(leave_msg))
        if len(history) > MAX_HISTORY: history.pop(0)
        await broadcast(leave_msg)

async def main():
    log.info("Server on ws://%s:%d — open chat_client.html to connect", HOST, PORT)
    async with serve(handler, HOST, PORT):
        await asyncio.get_running_loop().create_future()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Stopped.")
