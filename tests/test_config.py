
import pytest

from game_chat.config import default_settings


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("GAME_CHAT_RATE_MESSAGES_PER_SECOND", "42")
    monkeypatch.setenv("GAME_CHAT_BACKPRESSURE_QUEUE_SIZE", "99")
    monkeypatch.setenv("GAME_CHAT_BACKPRESSURE_SLOW_CONSUMER_BEHAVIOR",
                       "disconnect")
    settings = default_settings()
    assert settings.rate_limits.messages_per_second == 42
    assert settings.backpressure.outbound_queue_size == 99
    assert settings.backpressure.slow_consumer_behavior == "disconnect"


def test_invalid_slow_behavior(monkeypatch):
    monkeypatch.setenv("GAME_CHAT_BACKPRESSURE_SLOW_CONSUMER_BEHAVIOR", "nope")
    with pytest.raises(ValueError):
        default_settings()


def test_defaults_present():
    s = default_settings()
    assert s.rate_limits.burst == 10
    assert s.backpressure.channel_history == 500
