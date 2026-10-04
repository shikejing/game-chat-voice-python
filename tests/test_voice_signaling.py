"""Requirement 3: voice signaling only reaches live call participants."""

import uuid

import pytest

from game_chat.models import ChannelType
from game_chat.service import RoomService
from game_chat.state import RoomRegistry


def _room() -> str:
    return f"voice-{uuid.uuid4().hex[:8]}"


async def _three_in_call(service, make, room):
    alice, bob, carol = make(room, "alice"), make(room, "bob"), make(room, "carol")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "talk", ChannelType.VOICE)
    cid = ch["channel_id"]
    await bob.connect()
    await carol.connect()
    await service.join_channel(room, "bob", cid)
    await service.join_channel(room, "carol", cid)
    for s in (alice, bob, carol):
        await s.drain_all()
    return alice, bob, carol, cid


async def test_signal_delivered_only_to_target(service, make):
    room = _room()
    alice, bob, carol, cid = await _three_in_call(service, make, room)

    await service.voice_relay(
        room, "alice", cid, "offer", {"sdp": "x"}, target_id="bob"
    )
    await bob.drain_all()
    await carol.drain_all()

    signals = bob.events_of("voice_signal")
    assert len(signals) == 1
    sig = signals[0]
    assert sig["from"] == "alice"
    assert sig["target_id"] == "bob"
    assert sig["signal_type"] == "offer"
    assert carol.events_of("voice_signal") == []


async def test_departed_participant_cannot_receive_or_send(service, make):
    room = _room()
    alice, bob, carol, cid = await _three_in_call(service, make, room)

    # Bob leaves the voice call.
    await service.leave_channel(room, "bob", cid)
    for s in (alice, bob, carol):
        await s.drain_all()
    state = service.registry.require(room).channels[cid]
    assert state.voice_participants == {"alice", "carol"}

    # Alice -> Bob must now fail (target not a participant).
    with pytest.raises(Exception) as exc:
        await service.voice_relay(
            room, "alice", cid, "offer", {}, target_id="bob"
        )
    assert exc.value.code == "not_found"

    # Bob -> Alice must fail too (sender no longer a participant).
    with pytest.raises(Exception) as exc:
        await service.voice_relay(
            room, "bob", cid, "answer", {}, target_id="alice"
        )
    assert exc.value.code == "permission_denied_not_voice_participant"

    await carol.drain_all()
    assert carol.events_of("voice_signal") == []


async def test_participants_converge_after_disconnect(service, make):
    room = _room()
    alice, bob, carol, cid = await _three_in_call(service, make, room)

    await bob.close()
    await alice.drain_all()
    await carol.drain_all()

    event = alice.latest("voice_participants")
    assert event["participants"] == ["alice", "carol"]
    assert carol.latest("voice_participants")["participants"] == ["alice", "carol"]

    # Signaling to disconnected bob is impossible.
    with pytest.raises(Exception) as exc:
        await service.voice_relay(room, "alice", cid, "ice_candidate", {}, "bob")
    assert exc.value.code == "not_found"


async def test_reject_unknown_signal_type(service, make):
    room = _room()
    alice = make(room, "alice")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "talk", ChannelType.VOICE)
    with pytest.raises(Exception) as exc:
        await service.voice_relay(
            room, "alice", ch["channel_id"], "media_packet", {}, "alice"
        )
    assert exc.value.code == "invalid_request"


async def test_voice_participant_limit_enforced(settings):
    import dataclasses

    from tests.conftest import make_session

    room = f"voice-lim-{uuid.uuid4().hex[:6]}"
    tight = dataclasses.replace(settings, voice_participant_limit=2)
    svc = RoomService(RoomRegistry(), tight)
    try:
        a = make_session(svc, room, "a")
        await a.connect()
        ch = await svc.create_channel(room, "a", "v", ChannelType.VOICE)
        cid = ch["channel_id"]
        b = make_session(svc, room, "b")
        await b.connect()
        await svc.join_channel(room, "b", cid)

        c = make_session(svc, room, "c")
        await c.connect()
        with pytest.raises(Exception) as exc:
            await svc.join_channel(room, "c", cid)
        assert exc.value.code == "conflict"
    finally:
        await svc.stop()
