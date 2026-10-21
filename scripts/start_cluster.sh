#!/usr/bin/env bash
# Start three brokers, one producer per service, and the archive and alert
# consumers. Alerts print here; everything else logs to data/logs/.
# Press Ctrl-C to stop everything.
set -euo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python
[ -x "$PY" ] || PY=python3

# A broker already answering means another cluster is running; the new
# brokers couldn't take their ports, and the checks below would be fooled.
for port in 8001 8002 8003; do
    if curl -s -o /dev/null "http://127.0.0.1:$port/state"; then
        echo "port $port is already in use; is another cluster running?" >&2
        exit 1
    fi
done

# Brokers start empty, so archive files and logs from an earlier run no
# longer match.
rm -rf data/archive data/logs
mkdir -p data/logs

pids=()
cleanup() {
    if [ ${#pids[@]} -gt 0 ]; then
        kill "${pids[@]}" 2>/dev/null || true
        wait "${pids[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for id in 1 2 3; do
    "$PY" -m broker.server "$id" > "data/logs/broker$id.log" 2>&1 &
    pids+=($!)
done

echo "starting brokers..."
for port in 8001 8002 8003; do
    ready=0
    for _ in $(seq 600); do  # up to 60 seconds; the first start can be slow
        if curl -sf "http://127.0.0.1:$port/state" > /dev/null; then
            ready=1
            break
        fi
        sleep 0.1
    done
    if [ "$ready" -eq 0 ]; then
        echo "broker on port $port did not start; see data/logs/" >&2
        exit 1
    fi
done
echo "brokers up on ports 8001-8003"

for service in login search upload; do
    "$PY" -m producer.producer "$service" > "data/logs/producer-$service.log" 2>&1 &
    pids+=($!)
done
"$PY" -m consumers.archive > data/logs/archive.log 2>&1 &
pids+=($!)
echo "producers and archive running; logs in data/logs/, archive in data/archive/"

"$PY" -m consumers.alert_consumer &
pids+=($!)
wait
