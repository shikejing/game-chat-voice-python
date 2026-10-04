"""Online connection registry with bounded outbound queues (back-pressure).

A slow client must never stall a room. Every Connection has a bounded
queue and ``offer`` never awaits room-side callers:

* ``drop_oldest`` (default): if the queue is full the oldest pending event
  is discarded to make room for the newest one;
* ``disconnect``: the connection is marked dead (the reader task will then
  tear down the socket) and further events are refused.

All registry operations are O(1) and safe without a lock because the
asyncio server runs on one thread; callers mutate sets only at
synchronous (non-await) points.
"""

from __future__ import annotations

import asyncio
from typing import Any

DROP_OLDEST = "drop_oldest"
DISCONNECT = "disconnect"
VALID_SLOW_BEHAVIORS = (DROP_OLDEST, DISCONNECT)


class Connection:
    """A single live WebSocket session for a player in a room."""

    def __init__(self, player_id: str, room_id: str, *, queue_size: int,
                 slow_behavior: str) -> None:
        if slow_behavior not in VALID_SLOW_BEHAVIORS:
            raise ValueError(f"unknown slow_consumer_behavior: {slow_behavior}")
        self.player_id = player_id
        self.room_id = room_id
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=queue_size)
        self._slow_behavior = slow_behavior
        self.closed = False
        self.dropped_events = 0

    async def offer(self, event: dict[str, Any]) -> bool:
        """Enqueue an outbound event without blocking on the network.

        On a saturated queue the call yields the event loop exactly once so
        this session's writer can drain; if the queue is still full
        afterwards the configured policy applies (drop oldest event or
        disconnect the slow session). Returns False when rejected.
        """
        if self.closed:
            return False
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            await asyncio.sleep(0)  # let the writer task make room
            if self.closed:
                return False
            try:
                self._queue.put_nowait(event)
                return True
            except asyncio.QueueFull:
                pass
            if self._slow_behavior == DROP_OLDEST:
                try:
                    self._queue.get_nowait()
                    self.dropped_events += 1
                except asyncio.QueueEmpty:
                    pass
                try:
                    self._queue.put_nowait(event)
                except asyncio.QueueFull:
                    return False
                return True
            # disconnect policy: kill this slow session
            self.closed = True
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                self._drain_one_then_none()
            return False
        return True

    def _drain_one_then_none(self) -> None:
        try:
            self._queue.get_nowait()
            self._queue.put_nowait(None)
        except (asyncio.QueueEmpty, asyncio.QueueFull):
            pass

    async def get(self) -> dict[str, Any] | None:
        """Await the next event; None means the connection is shutting down."""
        event = await self._queue.get()
        if event is None:
            self.closed = True
        return event

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:
            self._drain_one_then_none()


class ConnectionHub:
    """All live connections in the service.

    A player may hold more than one live session (multi-device reconnect);
    events are offered to every session keyed by (room_id, player_id).
    """

    def __init__(self, *, queue_size: int, slow_behavior: str) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        self._connections: dict[tuple[str, str], set[Connection]] = {}
        self._queue_size = queue_size
        self._slow_behavior = slow_behavior

    def register(self, room_id: str, player_id: str) -> Connection:
        conn = Connection(
            player_id, room_id,
            queue_size=self._queue_size,
            slow_behavior=self._slow_behavior,
        )
        self._connections.setdefault((room_id, player_id), set()).add(conn)
        return conn

    def unregister(self, conn: Connection) -> None:
        key = (conn.room_id, conn.player_id)
        conns = self._connections.get(key)
        if not conns:
            return
        conns.discard(conn)
        conn.close()
        if not conns:
            self._connections.pop(key, None)

    async def send_to(self, room_id: str, player_id: str,
                      event: dict[str, Any]) -> bool:
        """Deliver an event to all live sessions of one player."""
        conns = list(self._connections.get((room_id, player_id), ()))
        delivered = False
        for conn in conns:
            if await conn.offer(event):
                delivered = True
        return delivered

    async def broadcast(self, room_id: str, event: dict[str, Any],
                        *, exclude: frozenset[str] | set[str] | None = None) -> int:
        """Deliver an event to every connected player in the room."""
        exclude = exclude or frozenset()
        delivered = 0
        targets = [
            (pid, list(conns))
            for (rid, pid), conns in self._connections.items()
            if rid == room_id and pid not in exclude and conns
        ]
        for _pid, conns in targets:
            for conn in conns:
                if await conn.offer(event):
                    delivered += 1
        return delivered

    async def send_to_many(self, room_id: str, player_ids: frozenset[str] | set[str],
                           event: dict[str, Any]) -> int:
        """Deliver an event to all sessions of the given players only."""
        delivered = 0
        targets = [
            list(conns)
            for (rid, pid), conns in self._connections.items()
            if rid == room_id and pid in player_ids and conns
        ]
        for conns in targets:
            for conn in conns:
                if await conn.offer(event):
                    delivered += 1
        return delivered

    def online_players(self, room_id: str) -> set[str]:
        return {
            pid
            for (rid, pid), conns in self._connections.items()
            if rid == room_id and conns
        }

    def session_count(self, room_id: str, player_id: str) -> int:
        return len(self._connections.get((room_id, player_id), ()))
