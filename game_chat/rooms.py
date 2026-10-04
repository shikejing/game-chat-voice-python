"""Room / channel / membership / permission / voice coordination core.

Concurrency model
-----------------
Each room owns a single asyncio lock. Every mutating operation performs
*permission check -> state transition -> collect outbound events* inside
that critical section, so concurrent operations can never interleave
between an authorization check and its effect. Events are delivered
through the ConnectionHub *after* the lock is released, therefore a slow
consumer cannot block other players in the room.

Permission model
----------------
Roles per channel: ``owner`` (creator) > ``admin`` > ``member``.
* close channel ........ owner only
* kick member .......... owner or admin (cannot kick an equal/higher role)
* join/leave/speak ..... any current member of the channel
Each denial carries a distinguishable reason code, e.g.
``permission_denied.not_owner`` or ``permission_denied.not_member``.

Voice
-----
The server only tracks the participant set and forwards opaque signaling
payloads to a single, currently-participating target. Media is never
handled or stored.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from game_chat.config import Settings
from game_chat.connections import ConnectionHub
from game_chat.errors import (
    ChannelNotFound,
    NotCallParticipant,
    NotMember,
    PermissionDenied,
    RateLimited,
    RoomNotFound,
    StaleOperation,
    VoiceCallNotFound,
)
from game_chat.messages import ChannelMessageLog, DuplicateMessage
from game_chat.models import Channel, ChannelType, ChatMessage
from game_chat.protocol import (
    S2C_CHANNEL_CLOSED,
    S2C_CHANNEL_CREATED,
    S2C_MEMBER_JOINED,
    S2C_MEMBER_LEFT,
    S2C_VOICE_JOINED,
    S2C_VOICE_LEFT,
    S2C_VOICE_SIGNAL,
)
from game_chat.voice import VoiceCall

IdFactory = Callable[[], str]


def _uuid_hex() -> str:
    return uuid.uuid4().hex


@dataclass(frozen=True)
class PendingEvent:
    """An event captured inside the lock and delivered outside it.

    Delivery modes (mutually exclusive):

    * ``targets`` set: deliver only to those players (channel-scoped);
    * ``target`` string: deliver to a single player;
    * neither: room-wide broadcast, optionally filtered by ``exclude``.
    """

    event: str
    data: dict[str, Any]
    target: str | None = None
    exclude: frozenset[str] = frozenset()
    targets: frozenset[str] | None = None


@dataclass
class _TokenBucket:
    """Simple token bucket; refill is computed lazily on check."""

    rate: float
    burst: int
    tokens: float = field(init=False)
    last: float = field(init=False)

    def __post_init__(self) -> None:
        self.tokens = float(self.burst)
        self.last = time.monotonic()

    def allow(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        elapsed = max(0.0, now - self.last)
        self.last = now
        self.tokens = min(float(self.burst), self.tokens + elapsed * self.rate)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class _RateLimiters:
    """Per (player, bucket-name) token buckets for a room."""

    def __init__(self, rate: float, signal_rate: float, action_rate: float,
                 burst: int) -> None:
        self._rate = rate
        self._signal_rate = signal_rate
        self._action_rate = action_rate
        self._burst = burst
        self._buckets: dict[tuple[str, str], _TokenBucket] = {}

    def _bucket(self, player: str, kind: str, rate: float) -> _TokenBucket:
        key = (player, kind)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _TokenBucket(rate=rate, burst=self._burst)
            self._buckets[key] = bucket
        return bucket

    def check_message(self, player: str) -> None:
        if not self._bucket(player, "msg", self._rate).allow():
            raise RateLimited("too many messages", code="rate_limited.messages")

    def check_signal(self, player: str) -> None:
        if not self._bucket(player, "signal", self._signal_rate).allow():
            raise RateLimited("too many signaling frames",
                              code="rate_limited.signaling")

    def check_action(self, player: str) -> None:
        if not self._bucket(player, "action", self._action_rate).allow():
            raise RateLimited("too many channel actions",
                              code="rate_limited.actions")


class Room:
    """A game room holding text and voice channels."""

    def __init__(self, room_id: str, settings: Settings, hub: ConnectionHub,
                 *, id_factory: IdFactory = _uuid_hex) -> None:
        self.id = room_id
        self.settings = settings
        self.hub = hub
        self._id_factory = id_factory
        self._lock = asyncio.Lock()
        self._channels: dict[str, Channel] = {}
        self._messages: dict[str, ChannelMessageLog] = {}
        self._calls: dict[str, VoiceCall] = {}
        # (channel_id, player_id) -> highest seq acknowledged/delivered
        self._ack_seq: dict[tuple[str, str], int] = {}
        rl = settings.rate_limits
        self._limits = _RateLimiters(
            rl.messages_per_second, rl.signaling_per_second,
            rl.actions_per_second, rl.burst,
        )

    # ------------------------------------------------------------------ utils
    def _channel_locked(self, channel_id: str) -> Channel:
        channel = self._channels.get(channel_id)
        if channel is None or channel.closed:
            raise ChannelNotFound()
        return channel

    @staticmethod
    def _require_member(channel: Channel, player_id: str) -> None:
        if player_id not in channel.members:
            raise PermissionDenied(
                "only channel members may perform this action",
                reason="not_member",
            )

    @staticmethod
    def _require_admin(channel: Channel, player_id: str) -> None:
        if player_id not in channel.admins:
            raise PermissionDenied(
                "channel admin or owner required",
                reason="not_admin",
            )

    @staticmethod
    def _require_owner(channel: Channel, player_id: str) -> None:
        if player_id != channel.owner_id:
            raise PermissionDenied(
                "only the channel owner may close the channel",
                reason="not_owner",
            )

    @staticmethod
    def _channel_view(channel: Channel,
                      call: VoiceCall | None = None) -> dict[str, Any]:
        return {
            "id": channel.id,
            "type": channel.type.value,
            "owner_id": channel.owner_id,
            "closed": channel.closed,
            "members": sorted(channel.members),
            "admins": sorted(channel.admins),
            "voice_participants": sorted(call.participants) if call else [],
        }

    async def _flush(self, events: list[PendingEvent]) -> None:
        """Deliver queued events; called outside the room lock."""
        for evt in events:
            from game_chat.protocol import encode_event
            payload = encode_event(evt.event, evt.data)
            if evt.targets is not None:
                await self.hub.send_to_many(self.id, evt.targets, payload)
            elif evt.target is not None:
                await self.hub.send_to(self.id, evt.target, payload)
            else:
                await self.hub.broadcast(self.id, payload, exclude=evt.exclude)

    # -------------------------------------------------------------- channels
    async def create_channel(self, creator_id: str,
                             channel_type: ChannelType) -> Channel:
        self._limits.check_action(creator_id)
        async with self._lock:
            channel = Channel(
                id=self._id_factory(),
                room_id=self.id,
                type=channel_type,
                owner_id=creator_id,
                members={creator_id},
                admins={creator_id},
            )
            self._channels[channel.id] = channel
            self._messages[channel.id] = ChannelMessageLog(
                channel.id, history_limit=self.settings.backpressure.channel_history,
            )
            snapshot = self._channel_view(channel)
        # Creator gets its own channel.created response from the dispatch
        # layer (with req_id); notify the rest of the room for discovery.
        await self._flush([
            PendingEvent(S2C_CHANNEL_CREATED, {"channel": snapshot},
                         exclude=frozenset({creator_id})),
        ])
        return channel

    async def close_channel(self, actor_id: str, channel_id: str) -> None:
        self._limits.check_action(actor_id)
        async with self._lock:
            channel = self._channel_locked(channel_id)
            self._require_owner(channel, actor_id)
            channel.closed = True
            removed = set(channel.members)
            channel.members.clear()
            channel.admins.clear()
            self._calls.pop(channel_id, None)
            for cid, pid in list(self._ack_seq):
                if cid == channel_id:
                    self._ack_seq.pop((cid, pid), None)
            data = {"channel_id": channel_id, "by": actor_id}
            events = [PendingEvent(S2C_CHANNEL_CLOSED, data,
                                   exclude=frozenset({actor_id}))]
            if removed - {actor_id}:
                events.append(PendingEvent(
                    S2C_MEMBER_LEFT,
                    {"channel_id": channel_id, "player_id": actor_id,
                     "reason": "channel_closed", "removed": sorted(removed)},
                    targets=frozenset(removed - {actor_id}),
                ))
        await self._flush(events)

    async def join(self, actor_id: str, channel_id: str) -> Channel:
        self._limits.check_action(actor_id)
        async with self._lock:
            channel = self._channel_locked(channel_id)
            if actor_id in channel.members:
                # Idempotent join: no duplicate member state, no event.
                return channel
            channel.members.add(actor_id)
            evt = PendingEvent(
                S2C_MEMBER_JOINED,
                {"channel_id": channel_id, "player_id": actor_id},
                targets=frozenset(channel.members - {actor_id}),
            )
        await self._flush([evt])
        return channel

    async def leave(self, actor_id: str, channel_id: str) -> None:
        self._limits.check_action(actor_id)
        async with self._lock:
            events = self._leave_locked(channel_id, actor_id,
                                        reason="left")
        await self._flush(events)

    def _leave_locked(self, channel_id: str, player_id: str, *,
                      reason: str) -> list[PendingEvent]:
        """Remove a member (and its voice participation) under the lock."""
        channel = self._channels.get(channel_id)
        if channel is None or channel.closed:
            return []
        if player_id not in channel.members:
            return []  # idempotent leave: no state churn, no event
        # Recipients computed before removal so the departing player itself
        # is informed (important when reason == "kicked").
        recipients = frozenset(channel.members)
        channel.members.discard(player_id)
        channel.admins.discard(player_id)
        self._ack_seq.pop((channel_id, player_id), None)
        events: list[PendingEvent] = [
            PendingEvent(
                S2C_MEMBER_LEFT,
                {"channel_id": channel_id, "player_id": player_id,
                 "reason": reason},
                targets=recipients,
            )
        ]
        call = self._calls.get(channel_id)
        if call is not None and call.leave(player_id):
            voice_recipients = call.participants | {player_id}
            events.append(PendingEvent(
                S2C_VOICE_LEFT,
                {"channel_id": channel_id, "player_id": player_id,
                 "reason": reason},
                targets=voice_recipients,
            ))
            if call.empty():
                self._calls.pop(channel_id, None)
        return events

    async def kick(self, actor_id: str, channel_id: str,
                   target_id: str) -> None:
        self._limits.check_action(actor_id)
        async with self._lock:
            channel = self._channel_locked(channel_id)
            # Membership is checked first so outsiders get not_member and
            # members-without-role get not_admin (distinguishable reasons).
            self._require_member(channel, actor_id)
            self._require_admin(channel, actor_id)
            if target_id not in channel.members:
                raise NotMember("target is not a channel member")
            if target_id == channel.owner_id:
                raise PermissionDenied("the owner cannot be kicked",
                                       reason="target_is_owner")
            if target_id in channel.admins and actor_id != channel.owner_id:
                raise PermissionDenied(
                    "an admin cannot kick another admin",
                    reason="target_higher_role",
                )
            events = self._leave_locked(channel_id, target_id, reason="kicked")
        await self._flush(events)

    # ------------------------------------------------------------- text chat
    async def send_message(self, sender_id: str, channel_id: str, body: str,
                           client_msg_id: str) -> ChatMessage:
        if not isinstance(client_msg_id, str) or not client_msg_id:
            raise StaleOperation("client_msg_id is required",
                                 code="invalid_client_msg_id")
        self._limits.check_message(sender_id)
        async with self._lock:
            channel = self._channel_locked(channel_id)
            self._require_member(channel, sender_id)
            log = self._messages[channel_id]
            try:
                message = log.append(
                    sender_id=sender_id,
                    body=body,
                    client_msg_id=client_msg_id,
                    message_id=self._id_factory(),
                )
            except DuplicateMessage as dup:
                # Idempotent retry: replay the original, no new seq.
                return dup.message
            recipients = frozenset(channel.members - {sender_id})
            data = {
                "message": {
                    "id": message.id,
                    "channel_id": message.channel_id,
                    "sender_id": message.sender_id,
                    "seq": message.seq,
                    "body": message.body,
                    "client_msg_id": message.client_msg_id,
                }
            }
            # Deliver only to current members of THIS channel.
            evt = PendingEvent("message", data, targets=recipients)
            # Sender gets its authoritative copy (id/seq) too.
            self_evt = PendingEvent(
                "message", data, target=sender_id,
            )
            # Advance the sender watermark so reconnect replay skips it.
            self._ack_seq[(channel_id, sender_id)] = message.seq
        await self._flush([evt, self_evt])
        return message

    async def acknowledge(self, player_id: str, channel_id: str,
                          seq: int) -> None:
        async with self._lock:
            channel = self._channel_locked(channel_id)
            self._require_member(channel, player_id)
            log = self._messages[channel_id]
            if seq < 0 or seq > log.last_seq():
                raise StaleOperation(
                    f"ack seq {seq} is outside the channel log",
                )
            key = (channel_id, player_id)
            prev = self._ack_seq.get(key, 0)
            self._ack_seq[key] = max(prev, seq)

    async def replay_messages(self, player_id: str, channel_id: str,
                              after_seq: int) -> list[ChatMessage]:
        async with self._lock:
            channel = self._channel_locked(channel_id)
            self._require_member(channel, player_id)
            log = self._messages[channel_id]
            if after_seq < 0:
                raise StaleOperation("after_seq must be >= 0")
            messages = log.replay(after_seq)
            # Replay advances the watermark: confirmed msgs are never re-sent.
            if messages:
                self._ack_seq[(channel_id, player_id)] = messages[-1].seq
            return messages

    # ----------------------------------------------------------------- voice
    async def join_call(self, actor_id: str, channel_id: str) -> VoiceCall:
        self._limits.check_action(actor_id)
        async with self._lock:
            channel = self._channel_locked(channel_id)
            if channel.type is not ChannelType.VOICE:
                raise PermissionDenied(
                    "voice calls only exist in voice channels",
                    reason="not_voice_channel",
                )
            self._require_member(channel, actor_id)
            call = self._calls.get(channel_id)
            if call is None:
                call = VoiceCall(channel_id)
                self._calls[channel_id] = call
            is_new = call.join(actor_id)  # idempotent
            current = call.participants
            data = {"channel_id": channel_id, "player_id": actor_id,
                    "participants": sorted(current)}
            events: list[PendingEvent] = []
            if is_new:
                events.append(PendingEvent(
                    S2C_VOICE_JOINED, data,
                    targets=current - {actor_id},
                ))
            # The joiner always receives the current roster so its view
            # converges even on a repeated join / reconnect.
            roster = PendingEvent(
                S2C_VOICE_JOINED, data, target=actor_id,
            )
            events.append(roster)
        await self._flush(events)
        return call

    async def leave_call(self, actor_id: str, channel_id: str) -> None:
        self._limits.check_action(actor_id)
        async with self._lock:
            events = self._leave_call_locked(channel_id, actor_id, "left")
        await self._flush(events)

    def _leave_call_locked(self, channel_id: str, player_id: str,
                           reason: str) -> list[PendingEvent]:
        channel = self._channels.get(channel_id)
        if channel is None or channel.closed:
            return []
        call = self._calls.get(channel_id)
        if call is None or not call.leave(player_id):
            return []  # idempotent
        data = {"channel_id": channel_id, "player_id": player_id,
                "reason": reason, "participants": sorted(call.participants)}
        events = [PendingEvent(
            S2C_VOICE_LEFT, data,
            targets=call.participants | {player_id},
        )]
        if call.empty():
            self._calls.pop(channel_id, None)
        return events

    async def signal(self, sender_id: str, channel_id: str, target_id: str,
                     payload: dict[str, Any]) -> None:
        self._limits.check_signal(sender_id)
        async with self._lock:
            channel = self._channel_locked(channel_id)
            if channel.type is not ChannelType.VOICE:
                raise PermissionDenied(
                    "signaling is only valid in voice channels",
                    reason="not_voice_channel",
                )
            self._require_member(channel, sender_id)
            call = self._calls.get(channel_id)
            if call is None:
                raise VoiceCallNotFound()
            if not call.is_participant(sender_id):
                raise NotCallParticipant("sender is not in the call")
            if not call.is_participant(target_id):
                # A left participant can never be a signaling target.
                raise NotCallParticipant("target is not in the call")
            if target_id == sender_id:
                raise PermissionDenied("cannot signal yourself",
                                       reason="self_target")
            data = {"channel_id": channel_id, "from": sender_id,
                    "payload": payload}
            evt = PendingEvent(S2C_VOICE_SIGNAL, data, target=target_id)
        await self._flush([evt])

    async def disconnect_player(self, player_id: str) -> None:
        """Cleanup when *all* sessions of a player drop.

        Channel membership is retained for reconnect, but voice
        participation is released so the remaining parties converge.
        """
        async with self._lock:
            events: list[PendingEvent] = []
            for channel_id in list(self._channels):
                if player_id in self._channels[channel_id].members:
                    events.extend(self._leave_call_locked(
                        channel_id, player_id, "disconnected"))
        await self._flush(events)

    # ----------------------------------------------------------------- reads
    def get_channel(self, channel_id: str) -> Channel:
        channel = self._channels.get(channel_id)
        if channel is None or channel.closed:
            raise ChannelNotFound()
        return channel

    def snapshot(self, player_id: str) -> dict[str, Any]:
        """Serializable room state for a freshly (re)connected player."""
        channels = []
        for channel in self._channels.values():
            if channel.closed:
                continue
            channels.append(self._channel_view(
                channel, self._calls.get(channel.id),
            ))
        return {
            "room_id": self.id,
            "player_id": player_id,
            "channels": channels,
            "online": sorted(self.hub.online_players(self.id)),
        }

    def watermark(self, player_id: str, channel_id: str) -> int:
        return self._ack_seq.get((channel_id, player_id), 0)


class RoomRegistry:
    """Process-wide map of room ids to Room instances."""

    def __init__(self, settings: Settings, hub: ConnectionHub) -> None:
        self._rooms: dict[str, Room] = {}
        self._settings = settings
        self._hub = hub

    def get_or_create(self, room_id: str) -> Room:
        room = self._rooms.get(room_id)
        if room is None:
            room = Room(room_id, self._settings, self._hub)
            self._rooms[room_id] = room
        return room

    def get(self, room_id: str) -> Room | None:
        return self._rooms.get(room_id)

    def require(self, room_id: str) -> Room:
        room = self.get(room_id)
        if room is None:
            raise RoomNotFound()
        return room
