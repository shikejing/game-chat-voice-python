"""Tunable configuration for rate limiting and back-pressure.

Values may be supplied programmatically (``Settings(...)``) or overridden
through environment variables using the ``GAME_CHAT_`` prefix, e.g.::

    GAME_CHAT_RATE_MESSAGES_PER_SECOND=20
    GAME_CHAT_BACKPRESSURE_QUEUE_SIZE=1024
    GAME_CHAT_BACKPRESSURE_SLOW_CONSUMER_BEHAVIOR=disconnect
"""

from __future__ import annotations

from dataclasses import dataclass, field

DROP_OLDEST = "drop_oldest"
DISCONNECT = "disconnect"
VALID_SLOW_BEHAVIORS = (DROP_OLDEST, DISCONNECT)


@dataclass(frozen=True)
class RateLimitConfig:
    """Per-player token-bucket style limits."""

    messages_per_second: float = 5.0
    signaling_per_second: float = 10.0
    actions_per_second: float = 5.0
    burst: int = 10


@dataclass(frozen=True)
class BackpressureConfig:
    """Settings protecting the room from slow consumers."""

    outbound_queue_size: int = 256
    slow_consumer_behavior: str = DROP_OLDEST  # drop_oldest | disconnect
    channel_history: int = 500


@dataclass(frozen=True)
class Settings:
    """Top-level service configuration."""

    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    backpressure: BackpressureConfig = field(default_factory=BackpressureConfig)


def _env_float(name: str, default: float) -> float:
    import os
    raw = os.environ.get(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    import os
    raw = os.environ.get(name)
    return int(raw) if raw is not None else default


def default_settings() -> Settings:
    """Build settings, applying GAME_CHAT_* environment overrides."""
    import os
    rate = RateLimitConfig(
        messages_per_second=_env_float(
            "GAME_CHAT_RATE_MESSAGES_PER_SECOND",
            RateLimitConfig.messages_per_second),
        signaling_per_second=_env_float(
            "GAME_CHAT_RATE_SIGNALING_PER_SECOND",
            RateLimitConfig.signaling_per_second),
        actions_per_second=_env_float(
            "GAME_CHAT_RATE_ACTIONS_PER_SECOND",
            RateLimitConfig.actions_per_second),
        burst=_env_int("GAME_CHAT_RATE_BURST", RateLimitConfig.burst),
    )
    slow = os.environ.get(
        "GAME_CHAT_BACKPRESSURE_SLOW_CONSUMER_BEHAVIOR",
        BackpressureConfig.slow_consumer_behavior,
    )
    if slow not in VALID_SLOW_BEHAVIORS:
        raise ValueError(
            f"invalid GAME_CHAT_BACKPRESSURE_SLOW_CONSUMER_BEHAVIOR={slow}; "
            f"expected one of {VALID_SLOW_BEHAVIORS}",
        )
    backpressure = BackpressureConfig(
        outbound_queue_size=_env_int(
            "GAME_CHAT_BACKPRESSURE_QUEUE_SIZE",
            BackpressureConfig.outbound_queue_size),
        slow_consumer_behavior=slow,
        channel_history=_env_int(
            "GAME_CHAT_CHANNEL_HISTORY",
            BackpressureConfig.channel_history),
    )
    if rate.messages_per_second <= 0 or rate.signaling_per_second <= 0 \
            or rate.actions_per_second <= 0 or rate.burst < 1:
        raise ValueError("rate limits must be positive with burst >= 1")
    if backpressure.outbound_queue_size < 1 or backpressure.channel_history < 1:
        raise ValueError("queue size and history must be >= 1")
    return Settings(rate_limits=rate, backpressure=backpressure)
