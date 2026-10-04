import asyncio
from collections import defaultdict

import pytest

from game_chat.config import BackpressureConfig, RateLimitConfig, Settings
from game_chat.errors import (
    NotCallParticipant,
    PermissionDenied,
    VoiceCallNotFound,
)
from game_chat.models import ChannelType
from game_chat.rooms import Room, RoomRegistry


class FakeHub:
    """Records delivered events in-memory instead of touching sockets."""

    def __init__(self):
        self.sent = defaultdict(list)          # player -> [payload]
        self.broadcasts = []                   # (payload, exclude)
        self.online = set()

    async def send_to(self, room_id, player_id, event):
        self.sent[player_id].append(event)
        return True

    async def broadcast(self, room_id, event, *, exclude=None):
        self.broadcasts.append((event, frozenset(exclude or ())))
        return 1

    async def send_to_many(self, room_id, player_ids, event):
        self.broadcasts.append((event, frozenset()))
        return len(player_ids)

    def online_players(self, room_id):
        return set(self.online)


@pytest.fixture
def room():
    settings = Settings(
        rate_limits=RateLimitConfig(messages_per_second=1000,
                                    signaling_per_second=1000,
                                    actions_per_second=1000, burst=10000),
        backpressure=BackpressureConfig(outbound_queue_size=128,
                                        channel_history=100),
    )
    counter = {"n": 0}

    def ids():
        counter["n"] += 1
        return f"id-{counter['n']}"

    return Room("r1", settings, FakeHub(), id_factory=ids)


async def _data_events(room, event_name):
    out = []
    for payload, _ in room.hub.broadcasts:
        if payload["event"] == event_name:
            out.append(payload["data"])
    for player, items in room.hub.sent.items():
        for payload in items:
            if payload["event"] == event_name:
                out.append({**payload["data"], "_to": player})
    return out


async def test_create_channel_owner_is_member_admin(room):
    ch = await room.create_channel("p1", ChannelType.TEXT)
    assert ch.owner_id == "p1"
    assert ch.members == {"p1"}
    created = await _data_events(room, "channel.created")
    assert created and created[0]["channel"]["id"] == ch.id


async def test_join_is_idempotent_and_emits_once(room):
    ch = await room.create_channel("p1", ChannelType.TEXT)
    await room.join("p2", ch.id)
    ch2 = await room.join("p2", ch.id)  # repeat join
    assert ch2.members == {"p1", "p2"}
    joins = [d for d in await _data_events(room, "member.joined")
             if d.get("player_id") == "p2"]
    assert len(joins) == 1


async def test_leave_is_idempotent_and_converges(room):
    ch = await room.create_channel("p1", ChannelType.TEXT)
    await room.join("p2", ch.id)
    await room.leave("p2", ch.id)
    await room.leave("p2", ch.id)  # repeat leave
    assert "p2" not in room.get_channel(ch.id).members
    leaves = [d for d in await _data_events(room, "member.left")
              if d.get("player_id") == "p2"]
    assert len(leaves) == 1


async def test_close_requires_owner(room):
    ch = await room.create_channel("p1", ChannelType.TEXT)
    await room.join("p2", ch.id)
    with pytest.raises(PermissionDenied) as exc:
        await room.close_channel("p2", ch.id)
    assert exc.value.reason == "not_owner"
    await room.close_channel("p1", ch.id)
    from game_chat.errors import ChannelNotFound
    with pytest.raises(ChannelNotFound):
        room.get_channel(ch.id)


async def test_kick_permissions_and_reasons(room):
    ch = await room.create_channel("owner", ChannelType.TEXT)
    await room.join("admin", ch.id)
    ch.admins.add("admin")
    await room.join("member", ch.id)

    # plain member cannot kick
    with pytest.raises(PermissionDenied) as exc:
        await room.kick("member", ch.id, "admin")
    assert exc.value.reason == "not_admin"

    # admin cannot kick another admin
    await room.join("admin2", ch.id)
    ch.admins.add("admin2")
    with pytest.raises(PermissionDenied) as exc:
        await room.kick("admin", ch.id, "admin2")
    assert exc.value.reason == "target_higher_role"

    # owner can kick an admin
    await room.kick("owner", ch.id, "admin2")
    assert "admin2" not in ch.members

    # admin can kick a normal member
    await room.kick("admin", ch.id, "member")
    assert "member" not in ch.members


async def test_message_order_dedupe_and_replay(room):
    ch = await room.create_channel("p1", ChannelType.TEXT)
    await room.join("p2", ch.id)
    room.hub.broadcasts.clear()
    room.hub.sent.clear()

    m1 = await room.send_message("p1", ch.id, "hello", "c-1")
    m2 = await room.send_message("p2", ch.id, "world", "c-2")
    m1_retry = await room.send_message("p1", ch.id, "hello", "c-1")
    assert m1_retry.id == m1.id
    assert [m1.seq, m2.seq] == [1, 2]

    # p2 reconnects fresh (watermark 0): gets both exactly once
    missed = await room.replay_messages("p2", ch.id, 0)
    assert [m.seq for m in missed] == [1, 2]
    # a second replay after ack 2 delivers nothing new
    assert await room.replay_messages("p2", ch.id, 2) == []


async def test_non_member_cannot_speak(room):
    ch = await room.create_channel("p1", ChannelType.TEXT)
    with pytest.raises(PermissionDenied) as exc:
        await room.send_message("outsider", ch.id, "hi", "c-x")
    assert exc.value.reason == "not_member"


async def test_voice_join_leave_and_targeted_signaling(room):
    ch = await room.create_channel("p1", ChannelType.VOICE)
    await room.join("p2", ch.id)
    await room.join_call("p1", ch.id)
    await room.join_call("p2", ch.id)
    room.hub.sent.clear()

    await room.signal("p1", ch.id, "p2", {"sdp": "offer"})
    assert room.hub.sent["p2"][-1]["event"] == "voice.signal"
    assert room.hub.sent["p2"][-1]["data"]["from"] == "p1"
    assert "p1" not in room.hub.sent  # sender never gets its own signal

    # after p2 leaves, signaling to p2 is rejected and p2 receives nothing more
    await room.leave_call("p2", ch.id)
    room.hub.sent.clear()
    with pytest.raises(NotCallParticipant):
        await room.signal("p1", ch.id, "p2", {"sdp": "again"})
    assert "p2" not in room.hub.sent


async def test_signal_requires_existing_call(room):
    ch = await room.create_channel("p1", ChannelType.VOICE)
    with pytest.raises(VoiceCallNotFound):
        await room.signal("p1", ch.id, "p2", {})


async def test_disconnect_releases_voice_but_keeps_membership(room):
    ch = await room.create_channel("p1", ChannelType.VOICE)
    await room.join("p2", ch.id)
    await room.join_call("p2", ch.id)
    await room.disconnect_player("p2")
    # membership retained for reconnect...
    assert "p2" in room.get_channel(ch.id).members
    # ...but no longer a voice participant
    call = room._calls.get(ch.id)
    assert call is None or not call.is_participant("p2")


async def test_concurrent_joins_and_messages_converge(room):
    ch = await room.create_channel("host", ChannelType.TEXT)

    async def join_and_speak(i):
        pid = f"p{i}"
        await room.join(pid, ch.id)
        for j in range(5):
            await room.send_message(pid, ch.id, f"m{i}-{j}", f"c-{i}-{j}")

    await asyncio.gather(*(join_and_speak(i) for i in range(10)))
    channel = room.get_channel(ch.id)
    assert len(channel.members) == 11  # host + 10 players, no dupes
    missed = await room.replay_messages("host", ch.id, 0)
    seqs = [m.seq for m in missed]
    assert seqs == list(range(1, len(seqs) + 1))  # dense, no loss
    assert len(seqs) == 50


async def test_permission_check_under_concurrent_close(room):
    """A kick racing with channel close must never corrupt state."""
    ch = await room.create_channel("owner", ChannelType.TEXT)
    for i in range(5):
        await room.join(f"p{i}", ch.id)

    async def kick():
        for i in range(5):
            try:
                await room.kick("owner", ch.id, f"p{i}")
            except Exception as exc:  # noqa: BLE001 - race with close is expected
                assert hasattr(exc, "code")

    async def close():
        await asyncio.sleep(0)
        await room.close_channel("owner", ch.id)

    await asyncio.gather(kick(), close())
    from game_chat.errors import ChannelNotFound
    with pytest.raises(ChannelNotFound):
        room.get_channel(ch.id)


async def test_registry_get_or_create_same_instance():
    settings = Settings()
    hub = FakeHub()
    reg = RoomRegistry(settings, hub)
    r1 = reg.get_or_create("x")
    r2 = reg.get_or_create("x")
    assert r1 is r2
