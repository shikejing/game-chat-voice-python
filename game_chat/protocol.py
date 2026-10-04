"""WebSocket JSON protocol: request parsing/dispatch and response envelopes."""

from __future__ import annotations

from typing import Any

from .errors import GameChatError
from .models import ChannelType
from .service import RoomService


def error_event(code: str, message: str, ref: str | None = None) -> dict[str, Any]:
    return {"type": "error", "code": code, "message": message, "ref": ref}


def ok_event(ref: str | None, data: dict | None = None) -> dict[str, Any]:
    return {"type": "ack", "ref": ref, "data": data or {}}


def _require_str(message: dict[str, Any], key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str) or not value:
        raise GameChatError(f"{key!r} must be a non-empty string", code="invalid_request")
    return value


def _opt_str(message: dict[str, Any], key: str) -> str | None:
    value = message.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise GameChatError(f"{key!r} must be a string", code="invalid_request")
    return value


async def dispatch(
    service: RoomService,
    room_id: str,
    player_id: str,
    message: dict[str, Any],
) -> dict[str, Any] | list[dict[str, Any]] | None:
    """Translate one decoded client request into a service call.

    Returns an event (or list of events) to send back to the caller,
    or None when the request is purely notification-style.
    """
    if not isinstance(message, dict):
        raise GameChatError("request must be a JSON object", code="invalid_request")

    action = message.get("type")
    ref = message.get("ref")
    if not isinstance(action, str):
        raise GameChatError(
            "missing or invalid 'type'", code="invalid_request"
        )

    data: dict[str, Any] | None = None

    if action == "ping":
        return {"type": "pong", "ref": ref}

    if action == "create_channel":
        name = _require_str(message, "name")
        kind_raw = _require_str(message, "kind")
        try:
            kind = ChannelType(kind_raw)
        except ValueError:
            raise GameChatError(
                f"unknown channel kind {kind_raw!r}", code="invalid_request"
            ) from None
        data = await service.create_channel(
            room_id, player_id, name, kind, _opt_str(message, "channel_id")
        )

    elif action == "close_channel":
        channel_id = _require_str(message, "channel_id")
        await service.close_channel(room_id, player_id, channel_id)
        data = {"channel_id": channel_id, "closed": True}

    elif action == "join_channel":
        channel_id = _require_str(message, "channel_id")
        data = await service.join_channel(room_id, player_id, channel_id)

    elif action == "leave_channel":
        channel_id = _require_str(message, "channel_id")
        data = await service.leave_channel(room_id, player_id, channel_id)

    elif action == "remove_member":
        channel_id = _require_str(message, "channel_id")
        target_id = _require_str(message, "target_id")
        await service.remove_member(room_id, player_id, channel_id, target_id)
        data = {"channel_id": channel_id, "target_id": target_id, "removed": True}

    elif action == "message":
        channel_id = _require_str(message, "channel_id")
        body = message.get("body")
        if not isinstance(body, str):
            raise GameChatError("'body' must be a string", code="invalid_request")
        data = await service.send_message(
            room_id,
            player_id,
            channel_id,
            body,
            _opt_str(message, "client_msg_id"),
        )

    elif action == "ack":
        channel_id = _require_str(message, "channel_id")
        seq = message.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise GameChatError("'seq' must be an integer", code="invalid_request")
        await service.ack_message(room_id, player_id, channel_id, seq)
        data = {"channel_id": channel_id, "acked_seq": seq}

    elif action == "fetch_history":
        channel_id = _require_str(message, "channel_id")
        after_seq = message.get("after_seq", 0)
        limit = message.get("limit")
        if not isinstance(after_seq, int) or isinstance(after_seq, bool) or after_seq < 0:
            raise GameChatError(
                "'after_seq' must be a non-negative integer", code="invalid_request"
            )
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
        ):
            raise GameChatError("'limit' must be a positive integer", code="invalid_request")
        data = await service.fetch_history(
            room_id, player_id, channel_id, after_seq, limit
        )

    elif action == "voice_signal":
        channel_id = _require_str(message, "channel_id")
        signal_type = _require_str(message, "signal_type")
        target_id = _require_str(message, "target_id")
        payload = message.get("payload")
        if payload is not None and not isinstance(payload, dict):
            raise GameChatError(
                "'payload' must be an object", code="invalid_request"
            )
        await service.voice_relay(
            room_id, player_id, channel_id, signal_type, payload, target_id
        )
        data = {"delivered": True}

    elif action == "leave_room":
        await service.leave_room(room_id, player_id)
        data = {"left": True}

    else:
        raise GameChatError(
            f"unknown request type {action!r}", code="unknown_request"
        )

    return ok_event(ref, data)
