"""Requirement 4: reconnect replays missed messages and keeps state coherent."""

import asyncio

from game_chat.models import ChannelType

import uuid as _uuid


def _room() -> str:
    return f"room-{_uuid.uuid4().hex[:8]}"


ROOM = _room()


async def test_missed_messages_replayed_in_seq_order(service, make):
    ROOM = _room()
    alice, bob = make(ROOM, "alice"), make(ROOM, "bob")
    await alice.connect()
    ch = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()
    await service.join_channel(ROOM, "bob", cid)
    await bob.drain_all()
    await alice.drain_all()

    await bob.close()  # bob goes offline

    for i in range(5):
        await service.send_message(ROOM, "alice", cid, f"offline-{i}")

    # Bob reconnects: replay must arrive before the snapshot.
    bob2 = make(ROOM, "bob")
    await bob2.connect()
    raw = await bob2.drain_all()

    type_order = [e["type"] for e in raw]
    assert type_order.index("message") < type_order.index("snapshot")
    messages = bob2.events_of("message")
    assert [m["seq"] for m in messages] == [1, 2, 3, 4, 5]
    assert [m["body"] for m in messages] == [f"offline-{i}" for i in range(5)]


async def test_acked_messages_are_not_redelivered(service, make):
    ROOM = _room()
    alice, bob = make(ROOM, "alice"), make(ROOM, "bob")
    await alice.connect()
    ch = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()
    await service.join_channel(ROOM, "bob", cid)
    await bob.drain_all()

    for i in range(3):
        res = await service.send_message(ROOM, "alice", cid, f"m{i}")
    await bob.drain_all()
    # Bob confirms seq 1 and 2 while online.
    await service.ack_message(ROOM, "bob", cid, 2)
    await bob.close()
    await service.send_message(ROOM, "alice", cid, "m3")

    bob2 = make(ROOM, "bob")
    await bob2.connect()
    await bob2.drain_all()
    replayed = bob2.events_of("message")
    assert [m["seq"] for m in replayed] == [3, 4]

    # Bob now confirms everything delivered on the new session.
    await service.ack_message(ROOM, "bob", cid, 4)

    # Stale/late ACK never moves the cursor backwards; still no redelivery.
    await service.ack_message(ROOM, "bob", cid, 1)
    bob3 = make(ROOM, "bob")
    await bob3.connect()
    await bob3.drain_all()
    assert bob3.events_of("message") == []


async def test_state_continuous_across_reconnect(service, make):
    ROOM = _room()
    alice, bob = make(ROOM, "alice"), make(ROOM, "bob")
    await alice.connect()
    text = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    voice = await service.create_channel(ROOM, "alice", "v", ChannelType.VOICE)
    await bob.connect()
    await service.join_channel(ROOM, "bob", voice["channel_id"])
    await bob.drain_all()
    await alice.drain_all()

    voice_state = service.registry.require(ROOM).channels[voice["channel_id"]]
    assert voice_state.voice_participants == {"alice", "bob"}

    await bob.close()
    await alice.drain_all()
    # Voice participation drops immediately on disconnect...
    voice_state = service.registry.require(ROOM).channels[voice["channel_id"]]
    assert voice_state.voice_participants == {"alice"}
    # ...but room/channel membership is retained for the grace window.
    assert voice_state.members == {"alice", "bob"}

    bob2 = make(ROOM, "bob")
    await bob2.connect()
    await bob2.drain_all()
    snap = bob2.latest("snapshot")
    voice_snap = next(c for c in snap["channels"] if c["kind"] == "voice")
    assert "bob" in voice_snap["members"]
    # Rejoin of the call is explicit; snapshot is explainable post-reconnect.
    await service.join_channel(ROOM, "bob", voice["channel_id"])
    await bob2.drain_all()
    assert bob2.latest("voice_participants")["participants"] == ["alice", "bob"]


async def test_offline_grace_evicts_and_notifies(service, make):
    ROOM = _room()
    alice, bob = make(ROOM, "alice"), make(ROOM, "bob")
    await alice.connect()
    ch = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    await bob.connect()
    await service.join_channel(ROOM, "bob", ch["channel_id"])
    await alice.drain_all()
    await bob.close()

    await asyncio.sleep(0.3)  # beyond offline_grace_seconds
    await alice.drain_all()
    left = alice.events_of("room_member_left")
    assert left and left[-1]["reason"] == "offline_grace_expired"
    state = service.registry.require(ROOM)
    assert "bob" not in state.members
    assert "bob" not in state.channels[ch["channel_id"]].members
