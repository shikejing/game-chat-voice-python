"""FastAPI application wiring: HTTP bootstrap + WebSocket session loop.

Session model
-------------
Every WebSocket runs two tasks:

* **reader** parses frames via :mod:`game_chat.protocol` and dispatches
  them to the room. Mutating calls go through the room's serialized lock;
* **writer** drains the connection's bounded outbound queue onto the
  socket. It is the *only* task that writes the socket, and blocking on a
  slow client only stalls this one session (the room never awaits it).

On disconnect the connection is unregistered. When a player loses its
last session, voice participation is released for convergence while
channel membership is retained so a reconnect can resume.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from game_chat.config import Settings, default_settings
from game_chat.connections import Connection, ConnectionHub
from game_chat.errors import GameChatError
from game_chat.models import ChannelType
from game_chat.protocol import (
    C2S_ACK,
    C2S_CALL_JOIN,
    C2S_CALL_LEAVE,
    C2S_CLOSE_CHANNEL,
    C2S_CREATE_CHANNEL,
    C2S_JOIN,
    C2S_KICK,
    C2S_LEAVE,
    C2S_MESSAGE,
    C2S_RESUME,
    C2S_SIGNAL,
    C2S_SYNC,
    S2C_READY,
    S2C_SNAPSHOT,
    ProtocolError,
    decode_event,
    encode_event,
)
from game_chat.rooms import Room, RoomRegistry


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or default_settings()
    app = FastAPI(title="game-chat-voice")
    hub = ConnectionHub(
        queue_size=settings.backpressure.outbound_queue_size,
        slow_behavior=settings.backpressure.slow_consumer_behavior,
    )
    registry = RoomRegistry(settings, hub)
    app.state.hub = hub
    app.state.registry = registry

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/ws/rooms/{room_id}/players/{player_id}")
    async def ws_endpoint(websocket: WebSocket, room_id: str,
                          player_id: str) -> None:
        await _run_session(
            websocket, room_id, player_id,
            registry.get_or_create(room_id), hub,
        )

    return app


app = create_app()


async def _run_session(websocket: WebSocket, room_id: str, player_id: str,
                       room: Room, hub: ConnectionHub) -> None:
    await websocket.accept()
    conn = hub.register(room_id, player_id)

    async def writer() -> None:
        while True:
            event = await conn.get()
            if event is None:
                return
            try:
                await websocket.send_json(event)
            except (WebSocketDisconnect, RuntimeError, ConnectionResetError):
                return

    writer_task = asyncio.create_task(writer())
    try:
        # Initial bootstrap snapshot so a (re)connecting player converges.
        await conn.offer(encode_event(S2C_READY, {"player_id": player_id,
                                                  "room_id": room_id}))
        await conn.offer(encode_event(S2C_SNAPSHOT, room.snapshot(player_id)))

        while True:
            raw = await websocket.receive_json()
            try:
                event, data, req_id = decode_event(raw)
            except ProtocolError as exc:
                await conn.offer(encode_event(
                    "error", {"code": exc.code, "message": str(exc)},
                ))
                continue
            await _dispatch(room, conn, event, data, req_id)
    except WebSocketDisconnect:
        pass
    except Exception:  # pragma: no cover - defensive: never kill the loop
        import logging
        logging.getLogger(__name__).exception("session error")
    finally:
        hub.unregister(conn)
        writer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await writer_task
        if hub.session_count(room_id, player_id) == 0:
            # Last session gone: release voice state, keep membership.
            await room.disconnect_player(player_id)


async def _error(conn: Connection, error: GameChatError | Exception,
                 req_id: str | None) -> None:
    code = getattr(error, "code", "internal_error")
    frame = encode_event(
        "error",
        {"code": code, "message": str(error)},
        req_id=req_id,
    )
    await conn.offer(frame)


async def _dispatch(room: Room, conn: Connection, event: str,
                    data: dict[str, Any], req_id: str | None) -> None:
    player = conn.player_id
    try:
        if event == C2S_CREATE_CHANNEL:
            channel = await room.create_channel(
                player, ChannelType(data.get("type", "text")),
            )
            await conn.offer(encode_event(
                "channel.created",
                {"channel": _channel_public(room, channel.id)},
                req_id=req_id,
            ))
        elif event == C2S_CLOSE_CHANNEL:
            await room.close_channel(player, data["channel_id"])
            await conn.offer(encode_event("ok", {"closed": data["channel_id"]},
                                          req_id=req_id))
        elif event == C2S_JOIN:
            await room.join(player, data["channel_id"])
            await conn.offer(encode_event("ok", {"joined": data["channel_id"]},
                                          req_id=req_id))
        elif event == C2S_LEAVE:
            await room.leave(player, data["channel_id"])
            await conn.offer(encode_event("ok", {"left": data["channel_id"]},
                                          req_id=req_id))
        elif event == C2S_KICK:
            await room.kick(player, data["channel_id"], data["target_id"])
            await conn.offer(encode_event(
                "ok", {"kicked": data["target_id"],
                       "channel_id": data["channel_id"]},
                req_id=req_id,
            ))
        elif event == C2S_MESSAGE:
            client_msg_id = data.get("client_msg_id")
            if not isinstance(client_msg_id, str) or not client_msg_id:
                from game_chat.protocol import ProtocolError as PErr
                raise PErr("message.send requires non-empty 'client_msg_id'")
            message = await room.send_message(
                player,
                data["channel_id"],
                str(data.get("body", "")),
                client_msg_id,
            )
            await conn.offer(encode_event(
                "message.ack",
                {"channel_id": message.channel_id,
                 "client_msg_id": message.client_msg_id,
                 "id": message.id, "seq": message.seq},
                req_id=req_id,
            ))
        elif event == C2S_ACK:
            await room.acknowledge(
                player, data["channel_id"], int(data["seq"]),
            )
            await conn.offer(encode_event("ok", {"acked": data["seq"]},
                                          req_id=req_id))
        elif event == C2S_CALL_JOIN:
            await room.join_call(player, data["channel_id"])
            await conn.offer(encode_event(
                "ok", {"voice": "joined",
                       "channel_id": data["channel_id"]},
                req_id=req_id,
            ))
        elif event == C2S_CALL_LEAVE:
            await room.leave_call(player, data["channel_id"])
            await conn.offer(encode_event("ok", {"voice": "left"},
                                          req_id=req_id))
        elif event == C2S_SIGNAL:
            await room.signal(
                player, data["channel_id"], data["target_id"],
                data.get("payload", {}),
            )
            await conn.offer(encode_event("ok", {"signal": "delivered"},
                                          req_id=req_id))
        elif event == C2S_SYNC:
            await conn.offer(encode_event(
                S2C_SNAPSHOT, room.snapshot(player), req_id=req_id,
            ))
        elif event == C2S_RESUME:
            await _resume(room, conn, data, req_id)
    except (GameChatError, KeyError, ValueError) as exc:
        if isinstance(exc, KeyError):
            from game_chat.protocol import ProtocolError as PErr
            exc = PErr(f"missing field: {exc}")
        await _error(conn, exc, req_id)


def _channel_public(room: Room, channel_id: str) -> dict[str, Any]:
    channel = room.get_channel(channel_id)
    return {
        "id": channel.id,
        "type": channel.type.value,
        "owner_id": channel.owner_id,
        "members": sorted(channel.members),
        "admins": sorted(channel.admins),
    }


async def _resume(room: Room, conn: Connection, data: dict[str, Any],
                  req_id: str | None) -> None:
    """Replay missed text messages after reconnect, then current state."""
    player = conn.player_id
    channels = data.get("channels", [])
    delivered: list[dict[str, Any]] = []
    for entry in channels:
        channel_id = entry["channel_id"]
        after_seq = int(entry.get("last_seq", 0))
        messages = await room.replay_messages(player, channel_id, after_seq)
        for message in messages:
            await conn.offer(encode_event("message", {
                "message": {
                    "id": message.id,
                    "channel_id": message.channel_id,
                    "sender_id": message.sender_id,
                    "seq": message.seq,
                    "body": message.body,
                    "client_msg_id": message.client_msg_id,
                },
                "replayed": True,
            }))
        delivered.append({"channel_id": channel_id,
                          "replayed": len(messages)})
    # State snapshot after replay makes membership/call state explainable.
    await conn.offer(encode_event(
        S2C_SNAPSHOT, room.snapshot(player), req_id=req_id,
    ))
    await conn.offer(encode_event("resume.complete",
                                  {"channels": delivered}, req_id=req_id))
