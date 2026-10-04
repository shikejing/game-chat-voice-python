"""Domain error types with stable, client-distinguishable error codes."""

from __future__ import annotations


class GameChatError(Exception):
    code: str = "internal_error"

    def __init__(self, message: str = "", *, code: str | None = None) -> None:
        super().__init__(message or self.code)
        if code is not None:
            self.code = code


class NotFoundError(GameChatError):
    code = "not_found"


class AlreadyExistsError(GameChatError):
    code = "already_exists"


class PermissionDeniedError(GameChatError):
    code = "permission_denied"


class InvalidStateError(GameChatError):
    code = "invalid_state"


class RateLimitedError(GameChatError):
    code = "rate_limited"


class InvalidRequestError(GameChatError):
    code = "invalid_request"


class ConflictError(GameChatError):
    code = "conflict"
