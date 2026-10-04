"""Requirement 6: high concurrency correctness, rate limiting and backpressure."""

import asyncio
import uuid

import pytest

from game_chat.config import RoomSettings
from game_chat.errors import RateLimitedError
from game_chat.models import ChannelType, now_ts
from game_chat.service import RoomService
from game_chat.state import RoomRegistry
from tests.conftest import make_session


def _room() -> str:
    return f"conc-{uuid.uuid4().hex[:8]}"


async def test_mass_concurrent_speech_no_gaps_or_duplicates(make):
    room = _room()
    service = RoomService(
        RoomRegistry(),
        RoomSettings(
            offline_grace_seconds=30,
            channel_message_retention=5000,
            replay_max_messages=5000,
            client_send_queue_max=10000,
            client_send_quota=100000,
        ),
    )
    alice = make_session(service, room, "alice")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]

    listeners = []
    for i in range(10):
        s = make_session(service, room, f"p{i}")
        await s.connect()
        await service.join_channel(room, f"p{i}", cid)
        listeners.append(s)
    await alice.drain_all()

    async def speaker(pid, n):
        for j in range(n):
            await service.send_message(room, pid, cid, f"{pid}:{j}")

    await asyncio.gather(*(speaker(f"p{i}", 20) for i in range(10)))

    for s in listeners:
        await s.drain_all(10)

    state = service.registry.require(room).get_text_channel(cid)
    assert [m.seq for m in state.messages] == list(range(1, 201))
    for s in listeners:
        seqs = [m["seq"] for m in s.events_of("message")]
        assert seqs == list(range(1, 201))
        ids = [m["message_id"] for m in s.events_of("message")]
        assert len(ids) == len(set(ids))


async def test_concurrent_join_leave_churn_converges(service, make):
    room = _room()
    alice = make(room, "alice")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await alice.drain_all()

    players = [make(room, f"u{i}") for i in range(8)]
    for p in players:
        await p.connect()
        await p.drain_all()

    async def churn(p):
        for _ in range(10):
            await service.join_channel(room, p.player_id, cid)
            await service.leave_channel(room, p.player_id, cid)

    await asyncio.gather(*(churn(p) for p in players))
    for p in players:
        await p.drain_all()

    state = service.registry.require(room).channels[cid]
    # Every leave matched its join; final set is exactly the creator.
    assert state.members == {"alice"}
    # The last event seen by everyone agrees with the authoritative state.
    await alice.drain_all()
    assert alice.latest("channel_members")["members"] == ["alice"]


async def test_token_bucket_rate_limit(service, make, settings):
    room = _room()
    conn = service._new_connection("flood")
    quota = settings.client_send_quota
    accepted = 0
    for _ in range(quota):
        conn.check_send_quota()
        accepted += 1
    with pytest.raises(RateLimitedError):
        conn.check_send_quota()
    assert accepted == quota


async def test_slow_consumer_dropped_but_room_survives(make):
    room = _room()
    settings = RoomSettings(
        offline_grace_seconds=30,
        client_send_queue_max=512,
        client_send_quota=100000,
        channel_message_retention=1000,
        replay_max_messages=1000,
    )
    service = RoomService(RoomRegistry(), settings)
    alice = make_session(service, room, "alice")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]

    slow = make_session(service, room, "slow")
    fast = make_session(service, room, "fast")
    await slow.connect()
    await fast.connect()
    await service.join_channel(room, "slow", cid)
    await service.join_channel(room, "fast", cid)
    # Only the slow client has a tiny buffer; healthy clients must not suffer.
    slow.conn.send_queue = asyncio.Queue(maxsize=4)
    await slow.drain_all()
    await fast.drain_all()

    # Never drain the slow client; flood enough to overflow its queue.
    for i in range(50):
        await service.send_message(room, "alice", cid, f"f{i}")
    await fast.drain_all(10)

    # Fast consumer receives every message; room kept working.
    assert [m["seq"] for m in fast.events_of("message")] == list(range(1, 51))
    # Slow consumer was marked closed and removed from presence.
    state = service.registry.require(room)
    assert "slow" not in state.members or state.members["slow"].connected is False
