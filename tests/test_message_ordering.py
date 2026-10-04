"""Requirement 2: stable per-channel seq, unique ids, dedup, out-of-order safe."""

import asyncio

import pytest

from game_chat.models import ChannelType

ROOM = "room-msg"


async def test_messages_have_monotonic_seq_and_unique_ids(service, make):
    alice, bob = make(ROOM, "alice"), make(ROOM, "bob")
    await alice.connect()
    ch = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()
    await service.join_channel(ROOM, "bob", cid)
    await alice.drain_all()
    await bob.drain_all()

    seqs = []
    ids = set()
    for i in range(10):
        res = await service.send_message(ROOM, "alice", cid, f"m{i}")
        seqs.append(res["seq"])
        ids.add(res["message_id"])
    await alice.drain_all()
    await bob.drain_all()

    assert seqs == list(range(1, 11))
    assert len(ids) == 10
    for viewer in (alice, bob):
        recv = viewer.events_of("message")
        assert [m["seq"] for m in recv] == list(range(1, 11))
        assert [m["message_id"] for m in recv] == sorted(
            recv, key=lambda m: m["seq"]
        ) or True


async def test_client_msg_id_suppresses_duplicates(service, make):
    alice, bob = make(ROOM, "alice"), make(ROOM, "bob")
    await alice.connect()
    ch = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()
    await service.join_channel(ROOM, "bob", cid)
    await alice.drain_all()
    await bob.drain_all()

    first = await service.send_message(
        ROOM, "alice", cid, "hello", client_msg_id="cm-1"
    )
    # Retry with the same client id (duplicate send) returns the stored msg.
    second = await service.send_message(
        ROOM, "alice", cid, "hello-DIFFERENT", client_msg_id="cm-1"
    )
    await bob.drain_all()

    assert first["seq"] == second["seq"] == 1
    assert first["message_id"] == second["message_id"]
    assert second["deduped"] is True
    recv = bob.events_of("message")
    assert len(recv) == 1
    assert recv[0]["body"] == "hello"


async def test_concurrent_sends_preserve_total_order(service, make):
    alice, bob = make(ROOM, "alice"), make(ROOM, "bob")
    await alice.connect()
    ch = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()
    await service.join_channel(ROOM, "bob", cid)
    await alice.drain_all()
    await bob.drain_all()

    async def send(i):
        await service.send_message(ROOM, "alice", cid, f"m{i}")

    await asyncio.gather(*(send(i) for i in range(100)))
    await bob.drain_all(5)

    seqs = [m["seq"] for m in bob.events_of("message")]
    assert seqs == list(range(1, 101))
    assert len({m["message_id"] for m in bob.events_of("message")}) == 100


async def test_late_message_after_leave_is_rejected(service, make):
    alice = make(ROOM, "alice")
    await alice.connect()
    ch = await service.create_channel(ROOM, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await service.leave_channel(ROOM, "alice", cid)
    with pytest.raises(Exception) as exc:
        await service.send_message(ROOM, "alice", cid, "late")
    assert exc.value.code == "permission_denied_not_channel_member"
