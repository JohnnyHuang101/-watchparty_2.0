import asyncio
import websockets
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
                # Adjust "type" here to whatever your server expects for joining.
                # If there's no explicit join message, this line can be removed.
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


async def swarm_test(server_url, steps, hold_seconds=60):
    results = {}

    for n in steps:
        print(f"\nRamping to {n} connections, holding for {hold_seconds}s...")

        clients = [
            LoadClient(room_id=f"room-{i % 10}", server_url=server_url)
            for i in range(n)
        ]

        await asyncio.gather(*[c.run(hold_seconds) for c in clients])

        all_latencies = [lat for c in clients for lat in c.latencies]
        all_states = [c.state for c in clients]
        total_errors = sum(c.errors for c in clients)

        if all_latencies:
            results[n] = {
                "samples": len(all_latencies),
                "p50": statistics.median(all_latencies),
                "p95": statistics.quantiles(all_latencies, n=100)[94],
                "p99": statistics.quantiles(all_latencies, n=100)[98],
                "errors": total_errors,
                "error_rate": total_errors / n,
                f"total_requests_for_{n}_clients_chatting": len(all_latencies),
                "final_state_achieved_sync": True if equals(all_state) else False,
                "final_state": all_states[0],
            }
        else:
            results[n] = {
                "samples": 0,
                "errors": total_errors,
                "error_rate": total_errors / n,
                "note": "no latency samples recorded — check message schema/round-trip logic",
            }

        print(f"Results for {n} clients: {results[n]}")

    return results


if __name__ == "__main__":
    asyncio.run(
        swarm_test(
            "ws://localhost:8080/ws",  # <-- replace with your actual server URL
            steps=[10],  # [100, 500, 1000, 2500, 5000],
            hold_seconds=60,
        )
    )
