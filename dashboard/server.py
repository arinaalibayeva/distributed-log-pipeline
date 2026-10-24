"""The dashboard: a web page showing the cluster live, with kill/restart buttons.

    python -m dashboard.server        # then open http://127.0.0.1:8000

It starts the three brokers itself, so it can kill one (like a crash: the
process dies and its in-memory log is gone) and start it again. Stopping the
dashboard stops the brokers it started. It can also make brokers lose the
messages they send each other (see broker/chaos.py), setting every broker
so that, for example, a cut-off broker is cut off in both directions.

Endpoints:
    GET  /                      the page (dashboard/index.html)
    GET  /cluster               every broker's /state, or null for a broker that doesn't answer
    POST /brokers/{id}/kill     kill a broker's process
    POST /brokers/{id}/restart  start a broker again (it comes back empty)
    POST /chaos/cut-off/{id}    lose every message between that broker and the others
    POST /chaos/drop            lose a share of all messages between brokers: {"rate": 0.3}
    POST /chaos/heal            stop losing messages
"""

import asyncio
import signal
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from broker import config

PAGE = Path(__file__).with_name("index.html")
PORT = 8000


def broker_command(broker_id):
    return [sys.executable, "-m", "broker.server", str(broker_id)]


class BrokerProcesses:
    """Starts and kills broker processes, logging each to data/logs/."""

    def __init__(self, broker_ids=tuple(config.BROKERS), command=broker_command, log_dir="data/logs"):
        self.broker_ids = list(broker_ids)
        self.command = command
        self.log_dir = Path(log_dir)
        self.processes = {}

    def is_running(self, broker_id):
        process = self.processes.get(broker_id)
        return process is not None and process.poll() is None

    def start(self, broker_id):
        """Start a broker unless it is already running."""
        if self.is_running(broker_id):
            return
        self.log_dir.mkdir(parents=True, exist_ok=True)
        with (self.log_dir / f"broker{broker_id}.log").open("a") as log:
            self.processes[broker_id] = subprocess.Popen(
                self.command(broker_id), stdout=log, stderr=subprocess.STDOUT,
            )

    def kill(self, broker_id):
        """Kill a broker at once, without letting it shut down cleanly."""
        if self.is_running(broker_id):
            process = self.processes[broker_id]
            process.send_signal(signal.SIGKILL)
            process.wait()

    def start_all(self):
        for broker_id in self.broker_ids:
            self.start(broker_id)

    def kill_all(self):
        for broker_id in self.broker_ids:
            self.kill(broker_id)


class DropRate(BaseModel):
    rate: float = Field(ge=0, le=1)


def create_app(brokers=config.BROKERS, processes=None):
    """Build the dashboard app.

    `processes` is the BrokerProcesses the buttons control; without it the
    page only shows brokers started some other way.
    """

    @asynccontextmanager
    async def lifespan(app):
        app.state.http = httpx.AsyncClient(timeout=0.5)
        if processes is not None:
            processes.start_all()
        yield
        if processes is not None:
            processes.kill_all()
        await app.state.http.aclose()

    app = FastAPI(title="dashboard", lifespan=lifespan)

    async def fetch_state(broker_id):
        try:
            response = await app.state.http.get(f"{brokers[broker_id]}/state")
            return response.json()
        except (httpx.HTTPError, ValueError):
            return None

    async def admin(broker_id, path, body=None):
        """Send a chaos setting to one broker; return its settings, or None if it's down."""
        try:
            response = await app.state.http.post(f"{brokers[broker_id]}/admin/{path}", json=body)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError:
            return None

    async def admin_all(path, body_for):
        ids = list(brokers)
        replies = await asyncio.gather(*(admin(b, path, body_for(b)) for b in ids))
        return {"brokers": dict(zip(ids, replies))}

    def known_broker(broker_id):
        if broker_id not in brokers:
            raise HTTPException(status_code=404, detail=f"no broker {broker_id}")
        if processes is None:
            raise HTTPException(status_code=409, detail="this dashboard didn't start the brokers")

    @app.get("/")
    async def page():
        return FileResponse(PAGE)

    @app.get("/cluster")
    async def cluster():
        ids = list(brokers)
        states = await asyncio.gather(*(fetch_state(b) for b in ids))
        return {
            "partitions": config.PARTITIONS,
            "brokers": {b: state for b, state in zip(ids, states)},
            "controls": processes is not None,
        }

    @app.post("/chaos/cut-off/{broker_id}")
    async def cut_off(broker_id: int):
        if broker_id not in brokers:
            raise HTTPException(status_code=404, detail=f"no broker {broker_id}")
        states = dict(zip(brokers, await asyncio.gather(*(fetch_state(b) for b in brokers))))

        def blocked(b):
            if b == broker_id:
                return {"blocked": [o for o in brokers if o != b]}
            already = states[b]["chaos"]["blocked"] if states[b] else []
            return {"blocked": sorted(set(already) | {broker_id})}

        return await admin_all("chaos", blocked)

    @app.post("/chaos/drop")
    async def drop(setting: DropRate):
        return await admin_all("chaos", lambda b: {"drop_rate": setting.rate})

    @app.post("/chaos/heal")
    async def heal():
        return await admin_all("heal", lambda b: None)

    @app.post("/brokers/{broker_id}/kill")
    async def kill(broker_id: int):
        known_broker(broker_id)
        processes.kill(broker_id)
        return {"broker": broker_id, "running": False}

    @app.post("/brokers/{broker_id}/restart")
    async def restart(broker_id: int):
        known_broker(broker_id)
        processes.start(broker_id)
        return {"broker": broker_id, "running": True}

    return app


def main():
    print(f"dashboard on http://127.0.0.1:{PORT}", flush=True)
    uvicorn.run(create_app(processes=BrokerProcesses()), host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
