"""Requirement 1: multiple channels, real-time membership sync, idempotency."""

import pytest

from game_chat.errors import InvalidStateError
from game_chat.models import ChannelType



async def test_create_text_and_voice_channels(service, make):
    room = "r1"
    alice = make(room, "alice")
    await alice.connect()
    await alice.drain()

    text = await service.create_channel(room, "alice", "general", ChannelType.TEXT)
    voice = await service.create_channel(room, "alice", "talk", ChannelType.VOICE)
    await alice.drain()

    assert text["kind"] == "text"
    assert voice["kind"] == "voice"

    state = service.registry.require(room)
    assert set(state.channels) == {text["channel_id"], voice["channel_id"]}


async def test_join_and_leave_is_idempotent_and_broadcast(service, make):
    room = "r2"
    alice, bob, carol = make(room, "alice"), make(room, "bob"), make(room, "carol")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()
    await carol.connect()
    await alice.drain_all()
    await bob.drain_all()
    await carol.drain_all()

    # Duplicate joins: membership set must not change, events converge.
    await service.join_channel(room, "bob", cid)
    await service.join_channel(room, "bob", cid)
    for s in (alice, bob, carol):
        await s.drain_all()
    assert alice.latest("channel_members")["members"] == ["alice", "bob"]

    # Duplicate leaves are a no-op and keep the member set consistent.
    await service.leave_channel(room, "bob", cid)
    await service.leave_channel(room, "bob", cid)
    for s in (alice, bob, carol):
        await s.drain_all()
    assert alice.latest("channel_members")["members"] == ["alice"]


async def test_close_channel_twice_rejected(service, make):
    room = "r3"
    alice = make(room, "alice")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "g", ChannelType.TEXT)
    await service.close_channel(room, "alice", ch["channel_id"])
    with pytest.raises(InvalidStateError):
        await service.close_channel(room, "alice", ch["channel_id"])


async def test_membership_survives_reconnect_within_grace(service, make):
    room = "r4"
    alice, bob = make(room, "alice"), make(room, "bob")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()
    await service.join_channel(room, "bob", cid)
    await bob.close()
    await alice.drain_all()
    assert alice.latest("presence")["connected"] is False

    # Reconnect within grace: membership retained, presence flips back once.
    bob2 = make(room, "bob")
    await bob2.connect()
    await alice.drain_all()
    presence_true = [e for e in alice.events_of("presence") if e["connected"]]
    assert presence_true[-1]["rejoined"] is True
    state = service.registry.require(room)
    assert state.channels[cid].members == {"alice", "bob"}
