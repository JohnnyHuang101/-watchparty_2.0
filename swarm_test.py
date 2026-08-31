import asyncio
import websockets
import aiohttp
import json
import time
import uuid
import statistics
import random


def build_message(
    msg_type: str,
    username: str,
    content: str = "",
    video_time: float = 0.0,
    source_server: str = "loadtest",
):
    """Matches the Go struct exactly:
    Type, Username, Content, VideoTime, SourceServer
    """
    return json.dumps(
        {
            "type": msg_type,
            "username": username,
            "content": content,
            "video_time": video_time,
            "source_server": source_server,
        }
    )


class LoadClient:
    def __init__(self, room_id, server_url):
        self.room_id = room_id
        self.server_url = server_url
        self.username = f"bot-{uuid.uuid4().hex[:8]}"
        self.pending = {}  # msg_id -> sent_at timestamp
        self.latencies = []  # list of floats (seconds)
        self.errors = 0

    async def run(self, duration_s):
        try:
            async with websockets.connect(self.server_url) as ws:
                await ws.send(
                    build_message(
                        msg_type="identity",
                        username=self.username,
                    )
                )

                listen_task = asyncio.create_task(self._listen(ws))
                send_task = asyncio.create_task(self._send_loop(ws, duration_s))

                await asyncio.wait(
                    [listen_task, send_task],
                    timeout=duration_s + 2,
                    return_when=asyncio.ALL_COMPLETED,
                )

        except Exception as e:
            print(f"[{self.username}] error: {e}")
            self.errors += 1

    async def _send_loop(self, ws, duration_s):
        end_time = time.monotonic() + duration_s

        while time.monotonic() < end_time:
            msg_id = str(uuid.uuid4())
            self.pending[msg_id] = time.monotonic()

            # Encode msg_id inside content since the struct has no dedicated ID field.
            content_payload = json.dumps({"msg_id": msg_id})

            await ws.send(
                build_message(
                    msg_type="seek",
                    username=self.username,
                    content=content_payload,
                    video_time=round(random.uniform(0, 3600), 2),
                )
            )
            await asyncio.sleep(2)

        await ws.send(
            build_message(
                msg_type="exiting",
                username=self.username,
            )
        )

    async def _listen(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            content = msg.get("content", "")
            try:
                inner = json.loads(content)
            except (json.JSONDecodeError, TypeError):
                continue

            msg_id = inner.get("msg_id")
            if msg_id and msg_id in self.pending:
                latency = time.monotonic() - self.pending.pop(msg_id)
                self.latencies.append(latency)


def normalize_state_entry(entry: dict) -> dict:
    """Strip fields that are *expected* to differ per-instance (wall-clock
    timestamps) so we're comparing semantic state, not arrival jitter.
    Keeps last_action / last_updated_by / detail only.
    """
    return {
        "last_action": entry.get("last_action"),
        "last_updated_by": entry.get("last_updated_by"),
        "detail": entry.get("detail"),
    }


async def fetch_debug_state(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def check_instance_sync(debug_urls: list[str]):
    """Hits each server instance's /debug/state DIRECTLY (must bypass the LB
    — round-robin means you can't validate cluster-wide sync through it).
    Compares the most recent RoomState entry from each instance, ignoring
    updated_at, and reports which instances (if any) disagree.
    """
    if not debug_urls:
        return {"checked": False, "reason": "no debug_urls provided"}

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[fetch_debug_state(session, u) for u in debug_urls],
            return_exceptions=True,
        )

    per_instance = {}
    for url, result in zip(debug_urls, results):
        if isinstance(result, Exception):
            per_instance[url] = {"error": str(result)}
            continue
        if not result:
            per_instance[url] = {"error": "empty history"}
            continue
        per_instance[url] = {
            "history_len": len(result),
            "latest_normalized": normalize_state_entry(result[-1]),
        }

    reachable = {url: v for url, v in per_instance.items() if "error" not in v}

    if len(reachable) < 2:
        return {
            "checked": True,
            "in_sync": None,
            "note": "fewer than 2 reachable instances — nothing to compare",
            "per_instance": per_instance,
        }

    latest_states = [v["latest_normalized"] for v in reachable.values()]
    baseline = latest_states[0]
    in_sync = all(state == baseline for state in latest_states)

    return {
        "checked": True,
        "in_sync": in_sync,
        "per_instance": per_instance,
    }


async def swarm_test(
    server_urls, steps, debug_urls=None, hold_seconds=60, settle_seconds=3
):
    """
    server_urls: list of ws:// URLs — one per backend instance. With no
                 Nginx/LB in front, there's nothing to round-robin through,
                 so the script spreads clients across these URLs itself
                 (client i connects to server_urls[i % len(server_urls)])
                 to approximate load-balanced traffic.
                 If you add an LB back later, just pass a single-item list
                 pointed at it instead.
    debug_urls:  list of http:// URLs pointing DIRECTLY at each backend
                 instance's /debug/state. Required to validate cross-
                 instance sync.
    settle_seconds: pause after load stops, before polling /debug/state,
                 to let NATS propagation/broadcast catch up. Without this
                 you'll see false "out of sync" results from in-flight
                 messages, not real bugs.
    """
    results = {}
    debug_urls = debug_urls or []

    for n in steps:
        print(f"\nRamping to {n} connections, holding for {hold_seconds}s...")

        clients = [
            LoadClient(
                room_id=f"room-{i % 10}",
                server_url=server_urls[i % len(server_urls)],
            )
            for i in range(n)
        ]

        await asyncio.gather(*[c.run(hold_seconds) for c in clients])

        all_latencies = [lat for c in clients for lat in c.latencies]
        total_errors = sum(c.errors for c in clients)

        step_result = {
            "samples": len(all_latencies),
            "errors": total_errors,
            "error_rate": total_errors / n,
        }

        if all_latencies:
            step_result.update(
                {
                    "p50": statistics.median(all_latencies),
                    "p95": statistics.quantiles(all_latencies, n=100)[94],
                    "p99": statistics.quantiles(all_latencies, n=100)[98],
                }
            )
        else:
            step_result["note"] = (
                "no latency samples recorded — check message schema/round-trip logic"
            )

        # Let async broadcast/NATS propagation settle before checking sync,
        # otherwise you're measuring propagation lag, not real drift.
        await asyncio.sleep(settle_seconds)
        step_result["sync"] = await check_instance_sync(debug_urls)

        results[n] = step_result
        print(f"Results for {n} clients: {step_result}")

    return results


if __name__ == "__main__":
    # NOTE: there is currently no Nginx/LB service in the compose file, so
    # clients connect straight to each app node. This script (running as
    # the `loadtester` service on `watchparty-network`) reaches them via
    # Docker's embedded DNS using their container_name, on the container-
    # internal port 8080 — nothing needs to be published to the host.
    # If you add the Nginx LB back, replace server_urls with a single-item
    # list pointed at it (e.g. ["ws://load-balancer/ws"]).
    asyncio.run(
        swarm_test(
            # Real traffic goes through Nginx, same as production would —
            # this is a single-item list so every client connects here and
            # gets round-robined by Nginx itself, not by this script.
            server_urls=[
                "ws://load-balancer/ws",  # internal service name, Nginx's container-internal port 80
            ],
            # Debug checks bypass Nginx entirely and hit each node directly,
            # since the whole point is comparing raw per-instance state.
            debug_urls=[
                "http://watchparty-app-1:8080/debug/state",
                "http://watchparty-app-2:8080/debug/state",
            ],
            steps=[100, 500, 1000, 2500, 5000],
            hold_seconds=60,
            settle_seconds=3,
        )
    )
