"""Core domain entities: rooms, channels, members and stored messages."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import RoomSettings


def now_ts() -> float:
    """Monotonic clock; used for ordering and offline grace timers."""
    return time.monotonic()


def wall_ts() -> float:
    """Wall clock; exposed to clients for human-readable timestamps."""
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class ChannelType(str, Enum):
    TEXT = "text"
    VOICE = "voice"


@dataclass
class Member:
    player_id: str
    name: str = ""
    connected: bool = False
    offline_at: float | None = None
    # Channel ids for which this member is a channel admin.
    channel_admin: set[str] = field(default_factory=set)
    # Highest message seq this member has acknowledged per text channel.
    acked_seq: dict[str, int] = field(default_factory=dict)

    def reset(self) -> None:
        self.connected = False
        self.offline_at = now_ts()


@dataclass
class Channel:
    channel_id: str
    name: str
    kind: ChannelType
    owner_id: str
    closed: bool = False
    members: set[str] = field(default_factory=set)
    admins: set[str] = field(default_factory=set)
    # Only meaningful for voice channels: live call participants.
    voice_participants: set[str] = field(default_factory=set)
    created_at: float = field(default_factory=now_ts)

    def is_member(self, player_id: str) -> bool:
        return player_id in self.members

    def is_admin(self, player_id: str) -> bool:
        return player_id in self.admins


@dataclass
class StoredMessage:
    message_id: str
    channel_id: str
    sender_id: str
    seq: int
    body: str
    client_msg_id: str | None
    created_at: float = field(default_factory=wall_ts)

    def to_event(self) -> dict:
        return {
            "type": "message",
            "channel_id": self.channel_id,
            "message_id": self.message_id,
            "client_msg_id": self.client_msg_id,
            "sender_id": self.sender_id,
            "seq": self.seq,
            "body": self.body,
            "created_at": self.created_at,
        }


@dataclass
class TextChannelState(Channel):
    next_seq: int = 1
    messages: list[StoredMessage] = field(default_factory=list)
    # Bounded client_msg_id -> message index for duplicate suppression.
    client_id_index: dict[str, int] = field(default_factory=dict)

    def append_message(
        self,
        sender_id: str,
        body: str,
        retention: int,
        client_msg_id: str | None = None,
        dedup_size: int = 4096,
    ) -> tuple[StoredMessage, bool]:
        """Append a message, assigning the next stable seq.

        Returns ``(message, deduped)``. When *client_msg_id* was already
        seen, the previously stored message is returned and ``deduped`` is
        True -- no new seq is consumed and no duplicate is stored.
        """
        if client_msg_id is not None and client_msg_id in self.client_id_index:
            idx = self.client_id_index[client_msg_id]
            return self.messages[idx], True

        msg = StoredMessage(
            message_id=new_id("msg"),
            channel_id=self.channel_id,
            sender_id=sender_id,
            seq=self.next_seq,
            body=body,
            client_msg_id=client_msg_id,
        )
        self.next_seq += 1
        self.messages.append(msg)
        if client_msg_id is not None:
            self.client_id_index[client_msg_id] = len(self.messages) - 1
            if len(self.client_id_index) > dedup_size:
                # Evict oldest client ids that may have fallen out of retention.
                for stored in self.messages[: max(0, len(self.messages) - retention)]:
                    if stored.client_msg_id:
                        self.client_id_index.pop(stored.client_msg_id, None)
        if len(self.messages) > retention:
            dropped = len(self.messages) - retention
            del self.messages[:dropped]
            # Rebase client-id indices and drop stale entries.
            kept_ids: set[str] = set()
            for i, stored in enumerate(self.messages):
                if stored.client_msg_id is not None:
                    self.client_id_index[stored.client_msg_id] = i
                    kept_ids.add(stored.client_msg_id)
            for dead in [k for k in self.client_id_index if k not in kept_ids]:
                del self.client_id_index[dead]
        return msg, False

    def messages_after(self, after_seq: int, limit: int) -> list[StoredMessage]:
        out = [m for m in self.messages if m.seq > after_seq]
        return out[:limit]


@dataclass
class VoiceChannelState(Channel):
    pass


@dataclass
class Room:
    room_id: str
    owner_id: str | None = None
    members: dict[str, Member] = field(default_factory=dict)
    channels: dict[str, Channel] = field(default_factory=dict)
    created_at: float = field(default_factory=now_ts)

    def get_text_channel(self, channel_id: str) -> TextChannelState | None:
        ch = self.channels.get(channel_id)
        if isinstance(ch, TextChannelState):
            return ch
        return None

    def get_voice_channel(self, channel_id: str) -> VoiceChannelState | None:
        ch = self.channels.get(channel_id)
        if isinstance(ch, VoiceChannelState):
            return ch
        return None

    def is_owner(self, player_id: str) -> bool:
        return self.owner_id is not None and player_id == self.owner_id
