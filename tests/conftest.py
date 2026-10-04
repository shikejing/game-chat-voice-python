"""Shared pytest helpers: fast settings, in-memory event collectors."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from game_chat.config import RoomSettings
from game_chat.connection import ClientConnection
from game_chat.service import RoomService
from game_chat.state import RoomRegistry


@pytest.fixture
def settings() -> RoomSettings:
    return RoomSettings(
        offline_grace_seconds=0.15,
        channel_message_retention=50,
        replay_max_messages=50,
        client_send_queue_max=512,
        client_send_quota=1000,
        client_send_window_seconds=1.0,
        dedup_cache_size=64,
    )


@pytest.fixture
def service(settings: RoomSettings) -> RoomService:
    return RoomService(registry=RoomRegistry(), settings=settings)


@dataclass
class Session:
    """A fake client session wrapping a real bounded ClientConnection."""

    room_id: str
    player_id: str
    service: RoomService
    conn: ClientConnection
    received: list[dict] = field(default_factory=list)

    async def connect(self, name: str = "") -> dict:
        return await self.service.connect(
            self.room_id, self.player_id, name or self.player_id, self.conn
        )

    async def drain(self) -> list[dict]:
        await asyncio.sleep(0)
        events: list[dict] = []
        while True:
            try:
                item = self.conn.send_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is None:
                break
            events.append(item)
        self.received.extend(events)
        return events

    async def drain_all(self, rounds: int = 5) -> list[dict]:
        for _ in range(rounds):
            await self.drain()
        return list(self.received)

    def events_of(self, event_type: str) -> list[dict]:
        return [e for e in self.received if e.get("type") == event_type]

    def latest(self, event_type: str) -> dict:
        matches = self.events_of(event_type)
        assert matches, f"no event of type {event_type!r}"
        return matches[-1]

    async def close(self) -> None:
        await self.service.session_closed(self.room_id, self.player_id, self.conn)


def make_session(service: RoomService, room_id: str, player_id: str) -> Session:
    return Session(
        room_id=room_id,
        player_id=player_id,
        service=service,
        conn=service._new_connection(player_id),
    )


async def settle() -> None:
    """Yield to the loop a few times so scheduled callbacks can run."""
    for _ in range(3):
        await asyncio.sleep(0)


@pytest.fixture
def make(service):
    def _make(room_id: str, player_id: str) -> Session:
        return make_session(service, room_id, player_id)

    return _make


def pytest_pyfunc_call(pyfuncitem):
    """Run ``async def`` test functions via a fresh event loop."""
    import inspect

    testfunction = pyfuncitem.obj
    if inspect.iscoroutinefunction(testfunction):
        argnames = pyfuncitem._fixtureinfo.argnames
        kwargs = {name: pyfuncitem.funcargs[name] for name in argnames}
        sig = inspect.signature(testfunction)
        kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        asyncio.run(testfunction(**kwargs))
        return True
    return None
