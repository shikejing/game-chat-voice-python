"""WebSocket JSON wire protocol event types and (de)serialization.

Frame shape (both directions)::

    {"event": "<event_name>", "data": { ... }, "req_id": "<optional>"}

``req_id`` is echoed back on the response/error frame of a client request,
so clients can correlate retries (important for idempotent message sends).
"""

from __future__ import annotations

from typing import Any

# Client -> server event types
C2S_CREATE_CHANNEL = "channel.create"
C2S_CLOSE_CHANNEL = "channel.close"
C2S_JOIN = "channel.join"
C2S_LEAVE = "channel.leave"
C2S_MESSAGE = "message.send"
C2S_ACK = "message.ack"
C2S_CALL_JOIN = "voice.join"
C2S_CALL_LEAVE = "voice.leave"
C2S_SIGNAL = "voice.signal"
C2S_KICK = "channel.kick"
C2S_RESUME = "session.resume"
C2S_SYNC = "state.snapshot"

CLIENT_EVENTS = frozenset({
    C2S_CREATE_CHANNEL, C2S_CLOSE_CHANNEL, C2S_JOIN, C2S_LEAVE,
    C2S_MESSAGE, C2S_ACK, C2S_CALL_JOIN, C2S_CALL_LEAVE,
    C2S_SIGNAL, C2S_KICK, C2S_RESUME, C2S_SYNC,
})

# Server -> client event types
S2C_READY = "ready"
S2C_SNAPSHOT = "state.snapshot"
S2C_CHANNEL_CREATED = "channel.created"
S2C_CHANNEL_CLOSED = "channel.closed"
S2C_MEMBER_JOINED = "member.joined"
S2C_MEMBER_LEFT = "member.left"
S2C_MESSAGE = "message"
S2C_VOICE_JOINED = "voice.participant_joined"
S2C_VOICE_LEFT = "voice.participant_left"
S2C_VOICE_SIGNAL = "voice.signal"
S2C_ERROR = "error"
S2C_ACK = "message.ack"


class ProtocolError(ValueError):
    """A frame did not conform to the wire protocol."""

    code = "protocol_error"


def encode_event(event: str, data: dict[str, Any] | None = None, /,
                 **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"event": event, "data": data or {}}
    payload.update(extra)
    return payload


def decode_event(raw: Any) -> tuple[str, dict[str, Any], str | None]:
    """Validate an inbound frame and return (event, data, req_id)."""
    if not isinstance(raw, dict):
        raise ProtocolError("frame must be a JSON object")
    event = raw.get("event")
    if not isinstance(event, str) or not event:
        raise ProtocolError("missing 'event' string")
    if event not in CLIENT_EVENTS:
        raise ProtocolError(f"unknown event: {event}")
    data = raw.get("data", {})
    if not isinstance(data, dict):
        raise ProtocolError("'data' must be an object")
    req_id = raw.get("req_id")
    if req_id is not None and not isinstance(req_id, str):
        raise ProtocolError("'req_id' must be a string when present")
    return event, data, req_id
