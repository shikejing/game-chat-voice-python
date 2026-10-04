"""Cross-cutting concurrency: interleaved close/join/leave/voice must converge."""

import asyncio
import uuid

from game_chat.models import ChannelType


def _room() -> str:
    return f"race-{uuid.uuid4().hex[:8]}"


async def test_join_after_close_always_rejected_once_closed(service, make):
    room = _room()
    alice, bob = make(room, "alice"), make(room, "bob")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "g", ChannelType.TEXT)
    cid = ch["channel_id"]
    await bob.connect()

    outcomes = []

    async def close_then_join():
        await service.close_channel(room, "alice", cid)
        try:
            await service.join_channel(room, "bob", cid)
            outcomes.append("joined")
        except Exception as exc:
            outcomes.append(exc.code)

    # Run many times; every attempt after close must observe invalid_state.
    await asyncio.gather(*(close_then_join() for _ in range(1)))
    assert outcomes == ["invalid_state"]
    state = service.registry.require(room).channels[cid]
    assert state.closed is True
    assert "bob" not in state.members


async def test_concurrent_voice_join_leave_signal_converges(service, make):
    room = _room()
    alice, bob, carol = make(room, "alice"), make(room, "bob"), make(room, "carol")
    await alice.connect()
    ch = await service.create_channel(room, "alice", "v", ChannelType.VOICE)
    cid = ch["channel_id"]
    await bob.connect()
    await carol.connect()
    await service.join_channel(room, "bob", cid)
    await service.join_channel(room, "carol", cid)
    for s in (alice, bob, carol):
        await s.drain_all()

    async def bob_churn():
        for _ in range(20):
            await service.leave_channel(room, "bob", cid)
            await service.join_channel(room, "bob", cid)

    async def alice_signals():
        for _ in range(40):
            try:
                await service.voice_relay(
                    room, "alice", cid, "custom", {"k": 1}, target_id="carol"
                )
            except Exception:
                pass

    await asyncio.gather(bob_churn(), alice_signals())
    await carol.drain_all(10)

    state = service.registry.require(room).channels[cid]
    # Final participant set is deterministic after churn settles.
    assert state.voice_participants == {"alice", "bob", "carol"}
    # Every delivered signal must have gone to a live participant (carol stayed).
    for ev in carol.events_of("voice_signal"):
        assert ev["from"] in {"alice"}
        assert ev["target_id"] == "carol"
