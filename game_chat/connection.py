"""Per-connection send queue with token-bucket rate limiting and backpressure."""

from __future__ import annotations

import asyncio

from .config import RoomSettings
from .errors import RateLimitedError
from .models import now_ts


class ClientConnection:
    """One live WebSocket session belonging to a player.

    Server -> client events pass through a bounded queue. A full queue means
    the consumer is too slow; the caller decides whether to drop or close the
    connection so that one slow player cannot stall a whole room.
    """

    def __init__(self, player_id: str, settings: RoomSettings) -> None:
        self.player_id = player_id
        self.settings = settings
        self.send_queue: asyncio.Queue[dict | None] = asyncio.Queue(
            maxsize=settings.client_send_queue_max
        )
        # Token bucket for inbound (client -> server) requests.
        self._send_tokens: float = float(settings.client_send_quota)
        self._last_refill: float = now_ts()
        self.closed: bool = False
        self.close_reason: str = ""

    async def offer(self, event: dict) -> bool:
        """Enqueue a server event without blocking.

        Returns True when accepted, False when the bounded queue is full
        (slow consumer) or the connection is already closed.
        """
        if self.closed:
            return False
        try:
            self.send_queue.put_nowait(event)
        except asyncio.QueueFull:
            return False
        return True

    def check_send_quota(self, ts: float | None = None) -> None:
        """Raise :class:`RateLimitedError` when the inbound quota is exceeded.

        A token bucket refills continuously at ``quota / window`` per second,
        which permits short bursts while bounding the long-term rate.
        """
        ts = now_ts() if ts is None else ts
        elapsed = max(0.0, ts - self._last_refill)
        self._last_refill = ts
        quota = float(self.settings.client_send_quota)
        window = max(1e-6, self.settings.client_send_window_seconds)
        self._send_tokens = min(
            quota, self._send_tokens + elapsed * (quota / window)
        )
        if self._send_tokens < 1.0:
            retry_after = (1.0 - self._send_tokens) * window / quota
            raise RateLimitedError(
                f"send rate limit exceeded; retry_after={retry_after:.3f}"
            )
        self._send_tokens -= 1.0

    def mark_closed(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        self.close_reason = reason
        # Sentinel unblocks a waiting writer/reader loop.
        try:
            self.send_queue.put_nowait(None)
        except asyncio.QueueFull:
            # Drain one slot to guarantee the sentinel fits.
            try:
                self.send_queue.get_nowait()
                self.send_queue.put_nowait(None)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass


class ConnectionRegistry:
    """Tracks the at-most-one live connection of every player in a room."""

    def __init__(self) -> None:
        self._connections: dict[str, ClientConnection] = {}

    def bind(self, player_id: str, conn: ClientConnection) -> ClientConnection | None:
        """Register *conn* as the player's live connection.

        If the player already had a live connection it is returned (already
        marked closed by the caller) so exactly one session stays live --
        duplicate joins never create duplicate member state.
        """
        previous = self._connections.get(player_id)
        self._connections[player_id] = conn
        return previous

    def unbind(self, player_id: str, conn: ClientConnection | None = None) -> None:
        current = self._connections.get(player_id)
        if conn is None or current is conn:
            self._connections.pop(player_id, None)

    def get(self, player_id: str) -> ClientConnection | None:
        return self._connections.get(player_id)

    def online_players(self) -> list[str]:
        return [p for p, c in self._connections.items() if not c.closed]
