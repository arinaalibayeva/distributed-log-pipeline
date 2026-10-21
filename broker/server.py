"""The web server for one broker.

Run one per broker:

    python -m broker.server 1

Endpoints:
    POST /partitions/{name}/append   store a log line (leader only)
    GET  /partitions/{name}/read     committed lines from an offset (leader only)
    GET  /state                      summary of every partition, for the dashboard

A request for a partition this broker doesn't lead gets a 421 response with
the leader's ID, so the client can retry there.
"""

import sys
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from broker import config
from broker.node import BrokerNode, NotLeaderError, UnknownPartitionError


class LogLine(BaseModel):
    msg_id: str
    service: str
    level: Literal["INFO", "WARN", "ERROR"]
    text: str
    ts: float


def create_app(broker_id):
    node = BrokerNode(broker_id)
    app = FastAPI(title=f"broker {broker_id}")
    app.state.node = node

    # Handlers are `async def` with no awaits inside, so they run one at a
    # time on the event loop and never touch a partition concurrently.

    @app.exception_handler(NotLeaderError)
    async def not_leader(request, exc):
        return JSONResponse(
            status_code=421,
            content={"error": "not_leader", "partition": exc.partition, "leader": exc.leader},
        )

    @app.exception_handler(UnknownPartitionError)
    async def unknown_partition(request, exc):
        return JSONResponse(status_code=404, content={"error": "unknown_partition", "partition": str(exc)})

    @app.post("/partitions/{name}/append")
    async def append(name: str, line: LogLine):
        if line.service != name:
            raise HTTPException(status_code=400, detail=f"line is for {line.service}, not {name}")
        offset = node.append(name, line.model_dump())
        return {"offset": offset}

    @app.get("/partitions/{name}/read")
    async def read(name: str, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
        entries = node.read(name, offset, limit)
        return {"entries": entries, "commit_index": node.partitions[name].commit_index}

    @app.get("/state")
    async def state():
        return node.state()

    return app


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit() or int(sys.argv[1]) not in config.BROKERS:
        sys.exit(f"usage: python -m broker.server <broker id: {', '.join(map(str, config.BROKERS))}>")
    broker_id = int(sys.argv[1])
    port = config.port_of(broker_id)
    print(f"broker {broker_id} listening on port {port}", flush=True)
    uvicorn.run(create_app(broker_id), host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
