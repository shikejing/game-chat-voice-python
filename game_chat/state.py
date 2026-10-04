"""In-memory room registry and per-room locking."""

from __future__ import annotations

import asyncio

from .errors import AlreadyExistsError, NotFoundError
from .models import Room


class RoomRegistry:
    """Process-local registry of all active rooms.

    Every room owns an :class:`asyncio.Lock`. All state mutations for a room
    happen while holding that lock, which makes membership/message/permission
    transitions atomic even under concurrent requests.
    """

    def __init__(self) -> None:
        self._rooms: dict[str, Room] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._registry_lock = asyncio.Lock()

    def get(self, room_id: str) -> Room | None:
        return self._rooms.get(room_id)

    async def create(self, room_id: str, owner_id: str | None = None) -> Room:
        async with self._registry_lock:
            if room_id in self._rooms:
                raise AlreadyExistsError(f"room {room_id!r} already exists")
            room = Room(room_id=room_id, owner_id=owner_id)
            self._rooms[room_id] = room
            self._locks[room_id] = asyncio.Lock()
            return room

    async def get_or_create(self, room_id: str, owner_id: str | None) -> Room:
        room = self._rooms.get(room_id)
        if room is not None:
            return room
        async with self._registry_lock:
            room = self._rooms.get(room_id)
            if room is None:
                room = Room(room_id=room_id, owner_id=owner_id)
                self._rooms[room_id] = room
                self._locks[room_id] = asyncio.Lock()
            return room

    def require(self, room_id: str) -> Room:
        room = self._rooms.get(room_id)
        if room is None:
            raise NotFoundError(f"room {room_id!r} not found")
        return room

    def lock_for(self, room_id: str) -> asyncio.Lock:
        try:
            return self._locks[room_id]
        except KeyError:
            raise NotFoundError(f"room {room_id!r} not found") from None

    async def delete(self, room_id: str) -> None:
        async with self._registry_lock:
            self._rooms.pop(room_id, None)
            self._locks.pop(room_id, None)

    def all_rooms(self) -> list[Room]:
        return list(self._rooms.values())
