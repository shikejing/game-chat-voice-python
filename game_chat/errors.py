"""Domain errors with stable, distinguishable reason codes."""

from __future__ import annotations


class GameChatError(Exception):
    """Base class for all game-chat domain errors."""

    code: str = "internal_error"
    http_status: int = 400

    def __init__(self, message: str = "", *, code: str | None = None) -> None:
        super().__init__(message or self.__class__.__doc__ or self.code)
        if code is not None:
            self.code = code


class RoomNotFound(GameChatError):
    """The room does not exist."""

    code = "room_not_found"
    http_status = 404


class ChannelNotFound(GameChatError):
    """The channel does not exist."""

    code = "channel_not_found"
    http_status = 404


class AlreadyMember(GameChatError):
    """The player is already a member of the channel."""

    code = "already_member"
    http_status = 409


class NotMember(GameChatError):
    """The player is not a member of the channel."""

    code = "not_member"
    http_status = 409


class PermissionDenied(GameChatError):
    """The player lacks permission for the action."""

    code = "permission_denied"
    http_status = 403

    def __init__(self, message: str = "", *, reason: str = "insufficient_role") -> None:
        super().__init__(message, code=f"permission_denied.{reason}")
        self.reason = reason


class RateLimited(GameChatError):
    """The player exceeded the configured message/action rate limit."""

    code = "rate_limited"
    http_status = 429


class StaleOperation(GameChatError):
    """The operation is stale and was rejected to preserve ordering."""

    code = "stale_operation"
    http_status = 409


class VoiceCallNotFound(GameChatError):
    """There is no active voice call in that channel."""

    code = "voice_call_not_found"
    http_status = 404


class NotCallParticipant(GameChatError):
    """The target or sender is not a participant of the voice call."""

    code = "not_call_participant"
    http_status = 409
