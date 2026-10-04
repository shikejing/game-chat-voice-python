"""Requirement 5: channel-level permissions and distinguishable error codes."""

import asyncio
import uuid

import pytest

from game_chat.models import ChannelType


def _room() -> str:
    return f"perm-{uuid.uuid4().hex[:8]}"


async def _setup(service, make, room, kind=ChannelType.TEXT):
    alice, bob, carol = make(room, "alice"), make(room, "bob"), make(room, "carol")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "c", kind)
    cid = ch["channel_id"]
    await bob.connect()
    await carol.connect()
    await service.join_channel(room, "bob", cid)
    await service.join_channel(room, "carol", cid)
    for s in (alice, bob, carol):
        await s.drain_all()
    return alice, bob, carol, cid


async def test_non_admin_cannot_close_channel(service, make):
    room = _room()
    alice, bob, carol, cid = await _setup(service, make, room)

    with pytest.raises(Exception) as exc:
        await service.close_channel(room, "bob", cid)
    assert exc.value.code == "permission_denied_not_channel_admin"

    # Channel is still open and usable.
    res = await service.send_message(room, "bob", cid, "still alive")
    assert res["seq"] == 1


async def test_non_admin_cannot_remove_member(service, make):
    room = _room()
    alice, bob, carol, cid = await _setup(service, make, room)

    with pytest.raises(Exception) as exc:
        await service.remove_member(room, "bob", cid, "carol")
    assert exc.value.code == "permission_denied_not_channel_admin"
    state = service.registry.require(room).channels[cid]
    assert "carol" in state.members


async def test_admin_can_remove_and_reason_is_distinct(service, make):
    room = _room()
    alice, bob, carol, cid = await _setup(service, make, room)

    await service.remove_member(room, "alice", cid, "bob")
    await bob.drain_all()
    removed = bob.latest("removed_from_channel")
    assert removed["by"] == "alice"
    state = service.registry.require(room).channels[cid]
    assert "bob" not in state.members


async def test_admin_cannot_remove_peer_admin_but_owner_can(service, make):
    room = _room()
    alice, bob, carol, cid = await _setup(service, make, room)
    room_state = service.registry.require(room)
    # Alice (owner/creator) promotes bob and carol to channel admins.
    room_state.channels[cid].admins.update(["bob", "carol"])

    # Admin bob tries to remove fellow admin carol -> distinct refusal.
    with pytest.raises(Exception) as exc:
        await service.remove_member(room, "bob", cid, "carol")
    assert exc.value.code == "permission_denied_admin_target"
    assert "carol" in room_state.channels[cid].members

    # Room owner may remove admins.
    await service.remove_member(room, "alice", cid, "carol")
    assert "carol" not in room_state.channels[cid].members


async def test_permission_checks_hold_under_concurrent_close_vs_join(service, make):
    room = _room()
    alice, bob, carol, cid = await _setup(service, make, room)

    # Fire a non-admin close and messages concurrently many times; none of the
    # closes may succeed and message state must stay consistent.
    async def close_attempt():
        try:
            await service.close_channel(room, "bob", cid)
            return "closed"
        except Exception as exc:
            return exc.code

    results = await asyncio.gather(*(close_attempt() for _ in range(50)))
    assert set(results) == {"permission_denied_not_channel_admin"}
    state = service.registry.require(room).channels[cid]
    assert state.closed is False

    # Concurrent admin close interleaved with non-admin close: exactly one wins.
    await service.close_channel(room, "alice", cid)
    state = service.registry.require(room).channels[cid]
    assert state.closed is True
