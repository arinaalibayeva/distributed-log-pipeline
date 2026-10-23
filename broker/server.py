"""The web server for one broker.

Run one per broker:

    python -m broker.server 1

Endpoints:
    POST /partitions/{name}/append      store a log line; replies once it is committed (leader only)
    GET  /partitions/{name}/read        committed lines from an offset (leader only)
    POST /partitions/{name}/replicate   entries copied from the leader (followers only)
    GET  /state                         summary of every partition, for the dashboard

A request for a partition this broker doesn't lead gets a 421 response with
the leader's ID, so the client can retry there. An append that can't be
committed in time (a majority of brokers can't be reached) gets a 503, and the
producer keeps the line and retries.
"""

import sys
from contextlib import asynccontextmanager
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from broker import config
from broker.node import BrokerNode, NotFollowerError, NotLeaderError, UnknownPartitionError
from broker.partition import ConflictError, StaleEpochError
from broker.replication import Replicator


class LogLine(BaseModel):
    msg_id: str
    service: str
    level: Literal["INFO", "WARN", "ERROR"]
    text: str
    ts: float


class Entry(BaseModel):
    offset: int = Field(ge=0)
    epoch: int = Field(ge=0)
    line: LogLine


class ReplicateRequest(BaseModel):
    leader: int
    epoch: int = Field(ge=0)
    start: int = Field(ge=0)
    prev_msg_id: str | None = None
    prev_epoch: int | None = None
    entries: list[Entry]
    commit_index: int = Field(ge=-1)


def create_app(broker_id, brokers=config.BROKERS, interval=0.2, batch=500, ack_timeout=1.0):
    """Build the app for one broker.

    `brokers` maps every broker ID to its URL. `interval` and `batch` tune
    replication (see Replicator), and `ack_timeout` is how long an append
    waits for its line to be committed before giving up with a 503.
    """
    node = BrokerNode(broker_id, list(brokers))
    replicator = Replicator(node, brokers, interval=interval, batch=batch)

    @asynccontextmanager
    async def lifespan(app):
        replicator.start()
        yield
        await replicator.stop()

    app = FastAPI(title=f"broker {broker_id}", lifespan=lifespan)
    app.state.node = node
    app.state.replicator = replicator

    # Handlers are `async def` and only await outside the code that changes a
    # partition, so changes happen one at a time on the event loop.

    @app.exception_handler(NotLeaderError)
    async def not_leader(request, exc):
        return JSONResponse(
            status_code=421,
            content={"error": "not_leader", "partition": exc.partition, "leader": exc.leader},
        )

    @app.exception_handler(UnknownPartitionError)
    async def unknown_partition(request, exc):
        return JSONResponse(status_code=404, content={"error": "unknown_partition", "partition": str(exc)})

    @app.exception_handler(NotFollowerError)
    async def not_follower(request, exc):
        return JSONResponse(
            status_code=409,
            content={"error": "not_follower", "partition": exc.partition, "leader": exc.leader},
        )

    @app.exception_handler(StaleEpochError)
    async def stale_epoch(request, exc):
        return JSONResponse(
            status_code=409, content={"error": "stale_epoch", "epoch": exc.current, "detail": str(exc)},
        )

    @app.exception_handler(ConflictError)
    async def conflict(request, exc):
        return JSONResponse(status_code=409, content={"error": "conflict", "detail": str(exc)})

    @app.post("/partitions/{name}/append")
    async def append(name: str, line: LogLine):
        if line.service != name:
            raise HTTPException(status_code=400, detail=f"line is for {line.service}, not {name}")
        offset = node.append(name, line.model_dump())
        replicator.wake(name)
        if not await replicator.wait_committed(name, offset, ack_timeout):
            return JSONResponse(
                status_code=503,
                content={"error": "not_committed", "partition": name, "offset": offset},
            )
        return {"offset": offset}

    @app.get("/partitions/{name}/read")
    async def read(name: str, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
        entries = node.read(name, offset, limit)
        return {"entries": entries, "commit_index": node.partitions[name].commit_index}

    @app.post("/partitions/{name}/replicate")
    async def replicate(name: str, request: ReplicateRequest):
        entries = [entry.model_dump() for entry in request.entries]
        try:
            accepted, length = node.replicate(
                name, request.leader, request.epoch, request.start, entries,
                request.commit_index, request.prev_msg_id, request.prev_epoch,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"accepted": accepted, "length": length}

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
