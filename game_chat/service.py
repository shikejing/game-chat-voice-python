"""Room service: orchestrates state mutations, fan-out, replay and voice signaling.

Concurrency model
-----------------
Each room has an :class:`asyncio.Lock` (see :class:`~game_chat.state.RoomRegistry`).
Every public mutating operation acquires that lock, inspects state and performs
the mutation atomically. Outbound events are *enqueued* while holding the lock
in a deterministic order, so reconnect replays always precede live events and
permission checks can never be interleaved/TOCTOU-bypassed.

Slow consumers never block the room: :meth:`ClientConnection.offer` is a
non-blocking bounded enqueue; a full queue causes that single connection to be
marked closed and cleaned up on the next sweep.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .config import RoomSettings, default_settings
from .connection import ClientConnection, ConnectionRegistry
from .errors import (
    ConflictError,
    InvalidRequestError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
)
from .models import (
    Channel,
    ChannelType,
    Member,
    Room,
    TextChannelState,
    VoiceChannelState,
    new_id,
    now_ts,
)
from .state import RoomRegistry

# Signal types clients are allowed to relay through the server.
_ALLOWED_VOICE_SIGNALS = frozenset(
    {"offer", "answer", "ice_candidate", "custom"}
)


class RoomService:
    def __init__(
        self,
        registry: RoomRegistry | None = None,
        settings: RoomSettings | None = None,
    ) -> None:
        self.registry = registry or RoomRegistry()
        self.settings = settings or default_settings()
        self.connections = ConnectionRegistry()
        # (room_id, player_id) -> scheduled offline eviction timer
        self._offline_tasks: dict[tuple[str, str], asyncio.TimerHandle] = {}

    # ------------------------------------------------------------------ utils

    def _new_connection(self, player_id: str) -> ClientConnection:
        return ClientConnection(player_id, self.settings)

    def _channel_snapshot(self, ch: Channel) -> dict[str, Any]:
        snap: dict[str, Any] = {
            "channel_id": ch.channel_id,
            "name": ch.name,
            "kind": ch.kind.value,
            "owner_id": ch.owner_id,
            "closed": ch.closed,
            "members": sorted(ch.members),
            "admins": sorted(ch.admins),
        }
        if isinstance(ch, TextChannelState):
            snap["next_seq"] = ch.next_seq - 1
        if isinstance(ch, VoiceChannelState):
            snap["voice_participants"] = sorted(ch.voice_participants)
        return snap

    def _room_snapshot(self, room: Room) -> dict[str, Any]:
        return {
            "type": "snapshot",
            "room_id": room.room_id,
            "owner_id": room.owner_id,
            "members": [
                {
                    "player_id": m.player_id,
                    "name": m.name,
                    "connected": m.connected,
                }
                for m in room.members.values()
            ],
            "channels": [self._channel_snapshot(ch) for ch in room.channels.values()],
        }

    def _require_room_member(self, room: Room, player_id: str) -> Member:
        member = room.members.get(player_id)
        if member is None:
            raise NotFoundError("player is not a member of this room")
        return member

    def _require_channel(self, room: Room, channel_id: str) -> Channel:
        ch = room.channels.get(channel_id)
        if ch is None:
            raise NotFoundError(f"channel {channel_id!r} not found")
        return ch

    def _require_active_channel(self, room: Room, channel_id: str) -> Channel:
        ch = self._require_channel(room, channel_id)
        if ch.closed:
            raise InvalidStateError(f"channel {channel_id!r} is closed")
        return ch

    def _require_channel_admin(self, room: Room, ch: Channel, player_id: str) -> None:
        """Channel admin or room owner only. Raises a distinguishable error."""
        if ch.is_admin(player_id) or room.is_owner(player_id):
            return
        raise PermissionDeniedError(
            "channel_admin or room_owner required",
            code="permission_denied_not_channel_admin",
        )

    async def _fanout(
        self,
        room: Room,
        event: dict[str, Any],
        recipients,
        *,
        exclude: str | None = None,
    ) -> list[str]:
        """Offer *event* to every online recipient. Returns slow/closed players.

        Players whose bounded send queue is full are reported so the caller can
        close their session; the room itself is never blocked on them.
        """
        stalled: list[str] = []
        for pid in recipients:
            if exclude is not None and pid == exclude:
                continue
            conn = self.connections.get(pid)
            if conn is None or conn.closed:
                continue
            if not await conn.offer(event):
                stalled.append(pid)
        return stalled

    async def _close_stalled(self, room_id: str, stalled: list[str]) -> None:
        for pid in stalled:
            conn = self.connections.get(pid)
            if conn is not None:
                conn.mark_closed("slow_consumer_send_queue_full")
            await self._handle_disconnect(room_id, pid, is_stall=True)

    # ------------------------------------------------ room/connection lifecycle

    async def ensure_room(self, room_id: str, creator_id: str) -> Room:
        return await self.registry.get_or_create(room_id, creator_id)

    async def connect(
        self, room_id: str, player_id: str, name: str, conn: ClientConnection
    ) -> dict[str, Any]:
        """Bind a live WebSocket session and converge room membership.

        Idempotent with respect to reconnects: a returning player keeps their
        room/channel membership and admin grants; only ``connected`` flips.
        Any previous live session for the same player is replaced.
        """
        room = await self.ensure_room(room_id, player_id)
        async with self.registry.lock_for(room_id):
            if len(room.members) >= self.settings.room_member_limit and (
                player_id not in room.members
            ):
                raise ConflictError("room member limit reached")

            previous = self.connections.bind(player_id, conn)
            if previous is not None and previous is not conn:
                previous.mark_closed("replaced_by_new_session")

            self._cancel_offline_timer(room_id, player_id)

            member = room.members.get(player_id)
            is_new_member = member is None
            if member is None:
                if room.owner_id is None:
                    room.owner_id = player_id
                member = Member(player_id=player_id, name=name or player_id)
                room.members[player_id] = member
            member.name = name or member.name
            was_connected = member.connected
            member.connected = True
            member.offline_at = None

            # 1) Replay missed text messages first so seq order is preserved.
            replay_events: list[dict[str, Any]] = []
            for ch in room.channels.values():
                if isinstance(ch, TextChannelState) and ch.is_member(player_id):
                    after = member.acked_seq.get(ch.channel_id, 0)
                    missed = ch.messages_after(after, self.settings.replay_max_messages)
                    replay_events.extend(m.to_event() for m in missed)
            replay_events.sort(key=lambda e: (e["channel_id"], e["seq"]))

            # 2) Then the authoritative full snapshot.
            snapshot = self._room_snapshot(room)

            for event in replay_events:
                await conn.offer(event)
            await conn.offer(snapshot)

            # 3) Finally notify other members of presence change.
            stalled: list[str] = []
            if not was_connected:
                presence = {
                    "type": "presence",
                    "room_id": room_id,
                    "player_id": player_id,
                    "name": member.name,
                    "connected": True,
                    "rejoined": not is_new_member,
                }
                stalled = await self._fanout(
                    room, presence, room.members, exclude=player_id
                )
        await self._close_stalled(room_id, stalled)
        return {"replayed": len(replay_events), "new_member": is_new_member}

    async def disconnect(self, room_id: str, player_id: str) -> None:
        await self._handle_disconnect(room_id, player_id)

    async def session_closed(
        self, room_id: str, player_id: str, conn: ClientConnection
    ) -> None:
        """Notify that one specific WebSocket session has ended.

        Only performs offline cleanup when *conn* is still the player's
        registered live session; an older session that was replaced by a
        reconnect must not tear the new session's presence down.
        """
        if self.connections.get(player_id) is not conn:
            return
        await self._handle_disconnect(room_id, player_id)

    async def _handle_disconnect(
        self, room_id: str, player_id: str, *, is_stall: bool = False
    ) -> None:
        room = self.registry.get(room_id)
        if room is None:
            return
        async with self.registry.lock_for(room_id):
            member = room.members.get(player_id)
            if member is None or not member.connected:
                # Idempotent: duplicate disconnect leaves state untouched.
                return
            conn = self.connections.get(player_id)
            if is_stall and conn is not None and not conn.closed:
                # Only the actually stalled session triggers this cleanup.
                return
            self.connections.unbind(player_id)
            member.connected = False
            member.offline_at = now_ts()

            # Leaving a live voice call is immediate even while grace runs.
            voice_events: list[tuple[Channel, dict[str, Any]]] = []
            for ch in room.channels.values():
                if isinstance(ch, VoiceChannelState) and player_id in ch.voice_participants:
                    ch.voice_participants.discard(player_id)
                    voice_events.append((ch, self._voice_participants_event(ch)))

            stalled: list[str] = []
            if is_stall:
                # A persistently slow consumer is evicted immediately from the
                # room and every channel so it can never poison later fan-out.
                room.members.pop(player_id, None)
                self._cancel_offline_timer(room_id, player_id)
                channel_events = self._withdraw_from_all_channels(room, player_id)
                stalled += await self._fanout(
                    room,
                    {
                        "type": "room_member_left",
                        "room_id": room_id,
                        "player_id": player_id,
                        "reason": "slow_consumer",
                    },
                    room.members,
                )
                for ch, event in channel_events:
                    stalled += await self._fanout(room, event, ch.members)
            else:
                presence = {
                    "type": "presence",
                    "room_id": room_id,
                    "player_id": player_id,
                    "connected": False,
                }
                stalled = await self._fanout(
                    room, presence, room.members, exclude=player_id
                )
                for ch, event in voice_events:
                    stalled += await self._fanout(room, event, ch.members)

                self._schedule_offline_eviction(room_id, player_id)
        await self._close_stalled(room_id, stalled)

    def _withdraw_from_all_channels(
        self, room: Room, player_id: str
    ) -> list[tuple[Channel, dict[str, Any]]]:
        """Remove a player from every channel and return events to broadcast.

        State-only mutation; the caller (already holding the room lock)
        performs the actual fan-out. Returned events describe the converged
        member/participant sets remaining members must observe.
        """
        events: list[tuple[Channel, dict[str, Any]]] = []
        for ch in room.channels.values():
            if not ch.is_member(player_id):
                continue
            ch.members.discard(player_id)
            ch.admins.discard(player_id)
            if player_id in ch.voice_participants:
                ch.voice_participants.discard(player_id)
                events.append((ch, self._voice_participants_event(ch)))
            events.append((ch, self._channel_members_event(ch)))
        return events

    async def leave_room(self, room_id: str, player_id: str) -> None:
        """Explicit leave: remove membership immediately (no offline grace)."""
        room = self.registry.get(room_id)
        if room is None:
            return
        async with self.registry.lock_for(room_id):
            member = room.members.pop(player_id, None)
            if member is None:
                # Idempotent: repeated leave must not corrupt member state.
                return
            conn = self.connections.get(player_id)
            if conn is not None:
                conn.mark_closed("left_room")
                self.connections.unbind(player_id, conn)
            self._cancel_offline_timer(room_id, player_id)

            events: list[tuple[Channel | None, dict[str, Any], Any]] = []
            for ch in list(room.channels.values()):
                if ch.is_member(player_id):
                    ch.members.discard(player_id)
                    ch.admins.discard(player_id)
                    if isinstance(ch, VoiceChannelState):
                        ch.voice_participants.discard(player_id)
                        events.append((ch, self._voice_participants_event(ch), ch.members))
                    events.append((ch, self._channel_members_event(ch), ch.members))

            events.append(
                (
                    None,
                    {
                        "type": "room_member_left",
                        "room_id": room_id,
                        "player_id": player_id,
                    },
                    room.members,
                )
            )
            stalled: list[str] = []
            for ch, event, recipients in events:
                stalled += await self._fanout(room, event, recipients)
        await self._close_stalled(room_id, stalled)

    def _schedule_offline_eviction(self, room_id: str, player_id: str) -> None:
        self._cancel_offline_timer(room_id, player_id)
        grace = self.settings.offline_grace_seconds
        if grace <= 0:
            return
        loop = asyncio.get_running_loop()

        def _fire() -> None:
            self._offline_tasks.pop((room_id, player_id), None)
            asyncio.ensure_future(self._evict_if_still_offline(room_id, player_id))

        self._offline_tasks[(room_id, player_id)] = loop.call_later(grace, _fire)

    def _cancel_offline_timer(self, room_id: str, player_id: str) -> None:
        handle = self._offline_tasks.pop((room_id, player_id), None)
        if handle is not None:
            handle.cancel()

    async def _evict_if_still_offline(self, room_id: str, player_id: str) -> None:
        room = self.registry.get(room_id)
        if room is None:
            return
        async with self.registry.lock_for(room_id):
            member = room.members.get(player_id)
            if member is None or member.connected:
                return
            room.members.pop(player_id, None)
            events: list[tuple[dict[str, Any], Any]] = []
            for ch in room.channels.values():
                if ch.is_member(player_id):
                    ch.members.discard(player_id)
                    ch.admins.discard(player_id)
                    if isinstance(ch, VoiceChannelState):
                        ch.voice_participants.discard(player_id)
                        events.append(
                            (self._voice_participants_event(ch), ch.members)
                        )
                    events.append((self._channel_members_event(ch), ch.members))
            events.append(
                (
                    {
                        "type": "room_member_left",
                        "room_id": room_id,
                        "player_id": player_id,
                        "reason": "offline_grace_expired",
                    },
                    room.members,
                )
            )
            stalled: list[str] = []
            for event, recipients in events:
                stalled += await self._fanout(room, event, recipients)
        await self._close_stalled(room_id, stalled)

    # ------------------------------------------------------------- channels

    async def create_channel(
        self,
        room_id: str,
        player_id: str,
        name: str,
        kind: ChannelType,
        channel_id: str | None = None,
    ) -> dict[str, Any]:
        if not name or not name.strip():
            raise InvalidRequestError("channel name must not be empty")
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            self._require_room_member(room, player_id)
            cid = channel_id or new_id("ch")
            if cid in room.channels:
                raise ConflictError(f"channel {cid!r} already exists")
            cls = TextChannelState if kind is ChannelType.TEXT else VoiceChannelState
            ch = cls(
                channel_id=cid,
                name=name.strip(),
                kind=ChannelType(kind),
                owner_id=player_id,
            )
            # Creator joins automatically and becomes the channel admin.
            ch.members.add(player_id)
            ch.admins.add(player_id)
            if isinstance(ch, VoiceChannelState):
                # Opening a voice channel puts the opener in the live call.
                ch.voice_participants.add(player_id)
            room.members[player_id].channel_admin.add(cid)
            room.channels[cid] = ch
            event = {
                "type": "channel_created",
                "channel": self._channel_snapshot(ch),
            }
            stalled = await self._fanout(room, event, room.members)
        await self._close_stalled(room_id, stalled)
        return self._channel_snapshot(ch)

    async def close_channel(
        self, room_id: str, player_id: str, channel_id: str
    ) -> None:
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            ch = self._require_channel(room, channel_id)
            self._require_room_member(room, player_id)
            self._require_channel_admin(room, ch, player_id)
            if ch.closed:
                raise InvalidStateError("channel already closed")
            ch.closed = True
            ch.voice_participants.clear()
            event = {"type": "channel_closed", "channel_id": channel_id}
            stalled = await self._fanout(room, event, ch.members)
        await self._close_stalled(room_id, stalled)

    def _channel_members_event(self, ch: Channel) -> dict[str, Any]:
        event: dict[str, Any] = {
            "type": "channel_members",
            "channel_id": ch.channel_id,
            "members": sorted(ch.members),
        }
        if isinstance(ch, VoiceChannelState):
            event["voice_participants"] = sorted(ch.voice_participants)
        return event

    def _voice_participants_event(self, ch: VoiceChannelState) -> dict[str, Any]:
        return {
            "type": "voice_participants",
            "channel_id": ch.channel_id,
            "participants": sorted(ch.voice_participants),
        }

    async def join_channel(
        self, room_id: str, player_id: str, channel_id: str
    ) -> dict[str, Any]:
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            self._require_room_member(room, player_id)
            ch = self._require_active_channel(room, channel_id)
            already = ch.is_member(player_id)
            if (
                isinstance(ch, VoiceChannelState)
                and player_id not in ch.voice_participants
                and len(ch.voice_participants) >= self.settings.voice_participant_limit
            ):
                raise ConflictError(
                    "voice participant limit reached",
                )
            ch.members.add(player_id)  # set add: duplicate joins are idempotent
            if isinstance(ch, VoiceChannelState):
                ch.voice_participants.add(player_id)
            event = self._channel_members_event(ch)
            # Newcomer gets a snapshot first, then the broadcast ordering keeps
            # every recipient converging on the same member set.
            conn = self.connections.get(player_id)
            if conn is not None and not already:
                await conn.offer(self._channel_snapshot_event(ch))
            stalled = await self._fanout(room, event, ch.members)
            if isinstance(ch, VoiceChannelState):
                voice_event = self._voice_participants_event(ch)
                stalled += await self._fanout(room, voice_event, ch.members)
        await self._close_stalled(room_id, stalled)
        return self._channel_snapshot(ch)

    def _channel_snapshot_event(self, ch: Channel) -> dict[str, Any]:
        return {"type": "channel_snapshot", "channel": self._channel_snapshot(ch)}

    async def leave_channel(
        self, room_id: str, player_id: str, channel_id: str
    ) -> dict[str, Any]:
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            ch = self._require_channel(room, channel_id)
            was_member = ch.is_member(player_id)
            ch.members.discard(player_id)  # set discard: idempotent leave
            if isinstance(ch, VoiceChannelState):
                ch.voice_participants.discard(player_id)
            if not was_member:
                # Still return the converged state; no event for a no-op leave.
                return self._channel_snapshot(ch)
            stalled = await self._fanout(
                room, self._channel_members_event(ch), ch.members
            )
            if isinstance(ch, VoiceChannelState):
                stalled += await self._fanout(
                    room, self._voice_participants_event(ch), ch.members
                )
            # Tell the leaver too so their UI converges.
            conn = self.connections.get(player_id)
            if conn is not None:
                await conn.offer(self._channel_snapshot_event(ch))
        await self._close_stalled(room_id, stalled)
        return self._channel_snapshot(ch)

    async def remove_member(
        self, room_id: str, player_id: str, channel_id: str, target_id: str
    ) -> None:
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            ch = self._require_active_channel(room, channel_id)
            self._require_room_member(room, player_id)
            self._require_channel_admin(room, ch, player_id)
            if player_id != target_id and ch.is_admin(target_id) and not room.is_owner(
                player_id
            ):
                raise PermissionDeniedError(
                    "cannot remove another channel admin",
                    code="permission_denied_admin_target",
                )
            if not ch.is_member(target_id):
                raise NotFoundError("target player is not a channel member")
            ch.members.discard(target_id)
            ch.voice_participants.discard(target_id)
            stalled = await self._fanout(
                room, self._channel_members_event(ch), ch.members
            )
            if isinstance(ch, VoiceChannelState):
                stalled += await self._fanout(
                    room, self._voice_participants_event(ch), ch.members
                )
            target_conn = self.connections.get(target_id)
            if target_conn is not None:
                await target_conn.offer(
                    {
                        "type": "removed_from_channel",
                        "channel_id": channel_id,
                        "by": player_id,
                    }
                )
        await self._close_stalled(room_id, stalled)

    # ------------------------------------------------------------------ text

    async def send_message(
        self,
        room_id: str,
        player_id: str,
        channel_id: str,
        body: str,
        client_msg_id: str | None = None,
    ) -> dict[str, Any]:
        if body is None or not isinstance(body, str) or not body.strip():
            raise InvalidRequestError("message body must not be empty")
        if client_msg_id is not None and not isinstance(client_msg_id, str):
            raise InvalidRequestError("client_msg_id must be a string")
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            self._require_room_member(room, player_id)
            ch = self._require_active_channel(room, channel_id)
            text_ch = room.get_text_channel(channel_id)
            if text_ch is None:
                raise InvalidRequestError("cannot send text to a voice channel")
            if not ch.is_member(player_id):
                raise PermissionDeniedError(
                    "must join the channel before sending",
                    code="permission_denied_not_channel_member",
                )
            msg, deduped = ch.append_message(
                sender_id=player_id,
                body=body,
                retention=self.settings.channel_message_retention,
                client_msg_id=client_msg_id,
                dedup_size=self.settings.dedup_cache_size,
            )
            event = msg.to_event()
            event["deduped"] = deduped
            # A deduplicated resend is not rebroadcast: live recipients already
            # saw the original, and anyone who missed it gets it via replay by
            # seq. Only genuinely new messages are fanned out exactly once.
            stalled: list[str] = []
            if not deduped:
                # Fan-out under the lock in one deterministic step: every
                # online member observes the same seq order. Members who
                # reconnect later replay by seq from acked state.
                stalled = await self._fanout(room, event, ch.members)
        await self._close_stalled(room_id, stalled)
        return {
            "message_id": msg.message_id,
            "channel_id": msg.channel_id,
            "seq": msg.seq,
            "deduped": deduped,
        }

    async def ack_message(
        self, room_id: str, player_id: str, channel_id: str, seq: int
    ) -> None:
        if not isinstance(seq, int) or seq < 0:
            raise InvalidRequestError("seq must be a non-negative integer")
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            self._require_room_member(room, player_id)
            self._require_channel(room, channel_id)
            member = room.members[player_id]
            # ACKs are monotonic; stale/late acks never move the cursor back,
            # which guarantees already-confirmed messages are not redelivered.
            if seq > member.acked_seq.get(channel_id, 0):
                member.acked_seq[channel_id] = seq

    async def fetch_history(
        self,
        room_id: str,
        player_id: str,
        channel_id: str,
        after_seq: int = 0,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Explicit on-demand history fetch; bounded and idempotent by seq."""
        if after_seq < 0:
            raise InvalidRequestError("after_seq must be >= 0")
        limit = (
            self.settings.replay_max_messages
            if limit is None
            else min(limit, self.settings.replay_max_messages)
        )
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            self._require_room_member(room, player_id)
            ch = self._require_channel(room, channel_id)
            text_ch = room.get_text_channel(channel_id)
            if text_ch is None:
                raise InvalidRequestError("voice channels have no text history")
            if not ch.is_member(player_id):
                raise PermissionDeniedError(
                    "must join the channel to read history",
                    code="permission_denied_not_channel_member",
                )
            messages = [
                m.to_event() for m in text_ch.messages_after(after_seq, limit)
            ]
        return {"channel_id": channel_id, "messages": messages}

    # ----------------------------------------------------------------- voice

    async def voice_relay(
        self,
        room_id: str,
        player_id: str,
        channel_id: str,
        signal_type: str,
        payload: dict[str, Any] | None,
        target_id: str | None,
    ) -> None:
        if signal_type not in _ALLOWED_VOICE_SIGNALS:
            raise InvalidRequestError(
                f"unsupported voice signal type {signal_type!r}"
            )
        if payload is not None and not isinstance(payload, dict):
            raise InvalidRequestError("signal payload must be an object")
        if target_id is None or not isinstance(target_id, str):
            raise InvalidRequestError("voice signals require a target_id")
        room = self.registry.require(room_id)
        async with self.registry.lock_for(room_id):
            self._require_room_member(room, player_id)
            ch = self._require_active_channel(room, channel_id)
            voice_ch = room.get_voice_channel(channel_id)
            if voice_ch is None:
                raise InvalidRequestError("channel is not a voice channel")
            # Signaling is strictly participant-to-participant. Membership of
            # the channel is not enough: only live call participants may
            # exchange signals, so a departed participant can never receive.
            if player_id not in voice_ch.voice_participants:
                raise PermissionDeniedError(
                    "sender is not a participant of the voice call",
                    code="permission_denied_not_voice_participant",
                )
            if target_id not in voice_ch.voice_participants:
                raise NotFoundError(
                    "target is not a participant of the voice call"
                )
            event = {
                "type": "voice_signal",
                "channel_id": channel_id,
                "signal_type": signal_type,
                "from": player_id,
                "target_id": target_id,
                "payload": payload or {},
            }
            target_conn = self.connections.get(target_id)
            stalled: list[str] = []
            if target_conn is None or target_conn.closed or not await target_conn.offer(
                event
            ):
                stalled.append(target_id)
        await self._close_stalled(room_id, stalled)

    # --------------------------------------------------------------- shutdown

    async def stop(self) -> None:
        """Cancel all pending offline-eviction timers.

        Already-scheduled ``_evict_if_still_offline`` task wrappers are no-ops
        once the timer is gone; they additionally re-check membership under the
        room lock before changing anything.
        """
        for handle in list(self._offline_tasks.values()):
            handle.cancel()
        self._offline_tasks.clear()
        # Let just-fired timer wrappers observe cancellation before shutdown.
        await asyncio.sleep(0)
