"""Rate limiting, back-pressure and out-of-order semantics."""

from __future__ import annotations

import pytest

from game_chat.config import BackpressureConfig, RateLimitConfig, Settings
from game_chat.errors import RateLimited
from game_chat.models import ChannelType
from game_chat.rooms import Room, _TokenBucket


class FakeHub:
    def __init__(self):
        self.online = set()

    async def send_to(self, *a, **k):
        return True

    async def send_to_many(self, *a, **k):
        return 1

    async def broadcast(self, *a, **k):
        return 1

    def online_players(self, room_id):
        return set()


def _room(messages_per_second=1.0, burst=2, **bp):
    settings = Settings(
        rate_limits=RateLimitConfig(
            messages_per_second=messages_per_second,
            signaling_per_second=1.0,
            actions_per_second=1.0, burst=burst,
        ),
        backpressure=BackpressureConfig(
            outbound_queue_size=bp.get("queue", 8),
            slow_consumer_behavior=bp.get("slow", "drop_oldest"),
            channel_history=50,
        ),
    )
    return Room("r", settings, FakeHub())


def test_token_bucket_refills_over_time():
    bucket = _TokenBucket(rate=10.0, burst=2)
    assert bucket.allow(now=0.0)
    assert bucket.allow(now=0.0)
    assert bucket.allow(now=0.0) is False
    # 100ms later one token has accrued
    assert bucket.allow(now=0.1) is True


async def test_message_rate_limit_raises_distinguishable_error():
    room = _room()
    ch = await room.create_channel("p1", ChannelType.TEXT)
    await room.send_message("p1", ch.id, "a", "c1")
    await room.send_message("p1", ch.id, "b", "c2")  # burst 2 consumed
    with pytest.raises(RateLimited) as exc:
        await room.send_message("p1", ch.id, "c", "c3")
    assert exc.value.code == "rate_limited.messages"
    # rejection must not consume a sequence number
    missed = await room.replay_messages("p1", ch.id, 0)
    assert [m.seq for m in missed] == [1, 2]


async def test_out_of_order_arrival_still_total_order():
    """Two coroutines interleave before the lock; the log stays consistent."""
    import asyncio
    room = _room(messages_per_second=10000, burst=10000)
    ch = await room.create_channel("host", ChannelType.TEXT)

    order = []
    await room.join("p1", ch.id)
    await room.join("p2", ch.id)
    await room.join("p3", ch.id)

    async def sender(pid, seq_ids, delay_each):
        for cid in seq_ids:
            order.append(cid)
            await asyncio.sleep(delay_each)
            await room.send_message(pid, ch.id, cid, cid)

    await asyncio.gather(
        sender("p1", ["a1", "a2", "a3"], 0.001),
        sender("p2", ["b1", "b2"], 0.003),
        sender("p3", ["c1", "c2", "c3", "c4"], 0.002),
    )
    missed = await room.replay_messages("host", ch.id, 0)
    # Whatever the arrival interleaving, observers see one dense total order
    # and the same sender client ids map to the same messages.
    assert [m.seq for m in missed] == list(range(1, 10))
    by_client = {m.client_msg_id: m for m in missed}
    assert by_client["a2"].sender_id == "p1"
    assert by_client["c4"].sender_id == "p3"
    # seqs unique and stable
    assert len({m.id for m in missed}) == 9


async def test_voice_state_converges_after_disconnect():
    room = _room(messages_per_second=10000, burst=10000)
    ch = await room.create_channel("a", ChannelType.VOICE)
    await room.join("b", ch.id)
    await room.join("c", ch.id)
    await room.join_call("a", ch.id)
    await room.join_call("b", ch.id)
    await room.join_call("c", ch.id)

    await room.disconnect_player("b")  # network drop
    call = room._calls[ch.id]
    assert call.participants == frozenset({"a", "c"})

    # b reconnects: membership intact, voice roster shows without b until
    # it rejoins; rejoining returns b to exactly one participant slot
    snapshot = room.snapshot("b")
    view = next(c for c in snapshot["channels"] if c["id"] == ch.id)
    assert "b" in view["members"]
    assert "b" not in view["voice_participants"]
    await room.join_call("b", ch.id)
    assert room._calls[ch.id].participants == frozenset({"a", "b", "c"})
