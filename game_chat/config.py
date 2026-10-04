"""Runtime configuration for rooms, channels and transport limits."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RoomSettings:
    """Tunable limits for a room and its channels."""

    offline_grace_seconds: float = 30.0
    channel_message_retention: int = 500
    replay_max_messages: int = 500
    client_send_queue_max: int = 1024
    client_send_quota: int = 40
    client_send_window_seconds: float = 1.0
    slow_consumer_drain_seconds: float = 5.0
    dedup_cache_size: int = 4096
    voice_participant_limit: int = 16
    room_member_limit: int = 1000


def default_settings() -> RoomSettings:
    return RoomSettings()
