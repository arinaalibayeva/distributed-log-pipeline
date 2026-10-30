"""The dashboard: a web page showing the cluster live, with kill/restart buttons.

    python -m dashboard.server        # then open http://127.0.0.1:8000

It starts the three brokers itself, so it can kill one (like a crash: the
process dies and its in-memory log is gone) and start it again. Stopping the
dashboard stops the brokers it started. It can also make brokers lose the
messages they send each other (see broker/chaos.py), setting every broker
so that, for example, a cut-off broker is cut off in both directions.

In the background it looks at the cluster every second to keep a timeline
of what happened (see events.py). It also shows what the producers report
through files in data/producers/ (see producer.ProducerFiles), and sends
them commands the same way.

Endpoints:
    GET  /                      the page (dashboard/index.html)
    GET  /cluster               everything the page shows: every broker's /state (null if it doesn't
                                answer), whether its process runs, the producers, recent events
    POST /brokers/{id}/kill     kill a broker's process
    POST /brokers/{id}/restart  start a broker again (it comes back empty)
    POST /chaos/cut-off/{id}    lose every message between that broker and the others
    POST /chaos/reconnect/{id}  undo cut-off for that broker only
    POST /chaos/drop            lose a share of all messages between brokers: {"rate": 0.3}
    POST /chaos/heal            stop losing messages
    POST /producers/{service}/pause   stop that producer making new lines (it still resends waiting ones)
    POST /producers/{service}/resume  start it again
"""

import asyncio
import json
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from broker import config
from dashboard.events import IN_SYNC, EventLog
from producer.producer import ProducerFiles

PAGE = Path(__file__).with_name("index.html")
PORT = 8000
STALE = 3.0  # seconds after which a producer's status counts as out of date


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

    def statuses(self):
        """{broker ID: "running" or "stopped"}."""
        return {b: "running" if self.is_running(b) else "stopped" for b in self.broker_ids}

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


def create_app(brokers=config.BROKERS, processes=None, data_dir="data", watch_every=1.0):
    """Build the dashboard app.

    `processes` is the BrokerProcesses the buttons control; without it the
    page only shows brokers started some other way. `data_dir` is where the
    producers leave their files. The timeline looks at the cluster every
    `watch_every` seconds.
    """
    producer_dir = Path(data_dir) / "producers"

    @asynccontextmanager
    async def lifespan(app):
        app.state.http = httpx.AsyncClient(timeout=0.5)
        if processes is not None:
            processes.start_all()
        watcher = asyncio.create_task(watch())
        yield
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        if processes is not None:
            processes.kill_all()
        await app.state.http.aclose()

    app = FastAPI(title="dashboard", lifespan=lifespan)
    app.state.cut_off = set()  # brokers cut off from the others, until reconnected or healed
    app.state.events = EventLog(config.PARTITIONS)
    events = app.state.events

    async def fetch_state(broker_id):
        try:
            response = await app.state.http.get(f"{brokers[broker_id]}/state")
            return response.json()
        except (httpx.HTTPError, ValueError):
            return None

    async def fetch_states():
        ids = list(brokers)
        return dict(zip(ids, await asyncio.gather(*(fetch_state(b) for b in ids))))

    async def watch():
        """Record what changed in the cluster since the last look, every `watch_every` seconds."""
        while True:
            try:
                events.observe(await fetch_states(), processes.statuses() if processes is not None else None)
            except Exception as exc:  # keep watching even if one look fails oddly
                print(f"dashboard watcher: {exc!r}", flush=True)
            await asyncio.sleep(watch_every)

    def read_producers():
        """What each producer last reported, or None if it hasn't."""
        now = time.time()
        found = {}
        for service in config.PARTITIONS:
            try:
                status = json.loads((producer_dir / f"{service}.json").read_text())
            except (OSError, ValueError):
                found[service] = None
                continue
            found[service] = {**status, "stale": now - status.get("updated", 0) > STALE}
        return found

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

    def check_broker(broker_id):
        if broker_id not in brokers:
            raise HTTPException(status_code=404, detail=f"no broker {broker_id}")

    def known_broker(broker_id):
        """A broker this dashboard started, so it can kill and restart it."""
        check_broker(broker_id)
        if processes is None:
            raise HTTPException(status_code=409, detail="this dashboard didn't start the brokers")

    @app.get("/")
    async def page():
        return FileResponse(PAGE)

    @app.get("/cluster")
    async def cluster():
        return {
            "partitions": config.PARTITIONS,
            "brokers": await fetch_states(),
            # A broker whose process runs but doesn't answer is unreachable
            # (starting up, or hung) rather than stopped.
            "processes": processes.statuses() if processes is not None else None,
            "cut_off": sorted(app.state.cut_off),
            "controls": processes is not None,
            "producers": read_producers(),
            "events": events.recent(30),
            "in_sync": IN_SYNC,
        }

    def check_service(service):
        if service not in config.PARTITIONS:
            raise HTTPException(status_code=404, detail=f"no service {service}")

    def control_producer(service, change):
        """Apply `change` to the producer's command file; return the new commands."""
        files = ProducerFiles(producer_dir, service)
        control = files.read_control()
        change(control)
        tmp = files.control_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(control))
        tmp.replace(files.control_path)
        return {"service": service, **control}

    @app.post("/producers/{service}/pause")
    async def pause(service: str):
        check_service(service)
        events.add("action", f"paused the {service} producer")
        return control_producer(service, lambda c: c.update(paused=True))

    @app.post("/producers/{service}/resume")
    async def resume(service: str):
        check_service(service)
        events.add("action", f"resumed the {service} producer")
        return control_producer(service, lambda c: c.update(paused=False))

    async def apply_cut_offs():
        """Set every broker's blocked list from the brokers that were cut off.

        The dashboard remembers which brokers it cut off: with two cut off,
        the third blocks everyone too, so the brokers' settings alone can't
        say which ones were meant to be cut off.
        """
        cut = app.state.cut_off

        def blocked(b):
            return {"blocked": [o for o in brokers if o != b and (b in cut or o in cut)]}

        return await admin_all("chaos", blocked)

    @app.post("/chaos/cut-off/{broker_id}")
    async def cut_off(broker_id: int):
        check_broker(broker_id)
        app.state.cut_off.add(broker_id)
        events.add("action", f"cut off broker {broker_id}")
        return await apply_cut_offs()

    @app.post("/chaos/reconnect/{broker_id}")
    async def reconnect(broker_id: int):
        check_broker(broker_id)
        app.state.cut_off.discard(broker_id)
        events.add("action", f"reconnected broker {broker_id}")
        return await apply_cut_offs()

    @app.post("/chaos/drop")
    async def drop(setting: DropRate):
        events.add("action", f"brokers now lose {setting.rate:.0%} of their messages")
        return await admin_all("chaos", lambda b: {"drop_rate": setting.rate})

    @app.post("/chaos/heal")
    async def heal():
        app.state.cut_off.clear()
        events.add("action", "healed the network")
        return await admin_all("heal", lambda b: None)

    @app.post("/brokers/{broker_id}/kill")
    async def kill(broker_id: int):
        known_broker(broker_id)
        processes.kill(broker_id)
        events.add("action", f"killed broker {broker_id}")
        return {"broker": broker_id, "running": False}

    @app.post("/brokers/{broker_id}/restart")
    async def restart(broker_id: int):
        known_broker(broker_id)
        processes.start(broker_id)
        events.add("action", f"restarted broker {broker_id}")
        return {"broker": broker_id, "running": True}

    return app


def main():
    print(f"dashboard on http://127.0.0.1:{PORT}", flush=True)
    uvicorn.run(create_app(processes=BrokerProcesses()), host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
