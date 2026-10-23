"""The dashboard: a web page showing the cluster live, with kill/restart buttons.

    python -m dashboard.server        # then open http://127.0.0.1:8000

It starts the three brokers itself, so it can kill one (like a crash: the
process dies and its in-memory log is gone) and start it again. Stopping the
dashboard stops the brokers it started.

Endpoints:
    GET  /                      the page (dashboard/index.html)
    GET  /cluster               every broker's /state, or null for a broker that doesn't answer
    POST /brokers/{id}/kill     kill a broker's process
    POST /brokers/{id}/restart  start a broker again (it comes back empty)
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
