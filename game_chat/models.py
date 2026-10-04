"""Core domain data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ChannelType(str, Enum):
    TEXT = "text"
    VOICE = "voice"


@dataclass(frozen=True)
class Player:
    id: str
    name: str = ""


@dataclass
class Channel:
    id: str
    room_id: str
    type: ChannelType
    owner_id: str
    closed: bool = False
    # member player ids; the owner is always an admin
    members: set[str] = field(default_factory=set)
    admins: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class ChatMessage:
    """A chat message with a globally stable unique identity.

    seq is a per-channel monotonically increasing sequence number assigned by
    the server; client_id is the client-supplied dedupe id.
    """

    id: str
    channel_id: str
    sender_id: str
    seq: int
    body: str
    client_msg_id: str


@dataclass(frozen=True)
class StoredEnvelope:
    """Persisted (in-memory) message record."""

    message: ChatMessage
    # set of player ids that have acknowledged this message
    acks: frozenset[str] = frozenset()
