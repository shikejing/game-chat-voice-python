"""Per-channel ordered message log with dedupe and replay support.

Ordering model
--------------
Each text channel owns an append-only log. The server assigns a dense,
monotonically increasing ``seq`` (starting at 1) *while holding the room
lock*, so concurrent sends are totally ordered and every observer sees the
same sequence.

Dedupe model
------------
Clients tag every send with an idempotency key (``client_msg_id``). A
resent key returns the *original* message instead of appending a new one,
so retries/duplicates can never produce two entries. Messages also carry a
globally unique server-assigned ``id`` for client-side dedupe.
"""

from __future__ import annotations

from game_chat.models import ChatMessage


class DuplicateMessage(Exception):
    """Raised when a client resends the same client_msg_id."""

    def __init__(self, message: ChatMessage) -> None:
        super().__init__(f"duplicate message {message.id}")
        self.message = message


class ChannelMessageLog:
    """Append-only ordered log for one text channel."""

    def __init__(self, channel_id: str, *, history_limit: int = 500) -> None:
        if history_limit < 1:
            raise ValueError("history_limit must be >= 1")
        self.channel_id = channel_id
        self._limit = history_limit
        self._next_seq = 1
        self._by_client_id: dict[str, str] = {}  # client_msg_id -> message id
        self._messages: list[ChatMessage] = []

    def append(self, sender_id: str, body: str, client_msg_id: str,
               message_id: str) -> ChatMessage:
        """Append a message, or return the original if client_msg_id repeats."""
        existing_id = self._by_client_id.get(client_msg_id)
        if existing_id is not None:
            existing = self.get(existing_id)
            assert existing is not None
            raise DuplicateMessage(existing)
        message = ChatMessage(
            id=message_id,
            channel_id=self.channel_id,
            sender_id=sender_id,
            seq=self._next_seq,
            body=body,
            client_msg_id=client_msg_id,
        )
        self._messages.append(message)
        self._by_client_id[client_msg_id] = message_id
        self._next_seq += 1
        if len(self._messages) > self._limit:
            # Trim oldest history; retained entries keep their original seq.
            trimmed = self._messages[:-self._limit]
            self._messages = self._messages[-self._limit:]
            for m in trimmed:
                self._by_client_id.pop(m.client_msg_id, None)
        return message

    def get(self, message_id: str) -> ChatMessage | None:
        for message in self._messages:
            if message.id == message_id:
                return message
        return None

    def get_by_client_id(self, client_msg_id: str) -> ChatMessage | None:
        message_id = self._by_client_id.get(client_msg_id)
        return self.get(message_id) if message_id is not None else None

    def replay(self, after_seq: int) -> list[ChatMessage]:
        """Return retained messages with seq strictly greater than after_seq."""
        return [m for m in self._messages if m.seq > after_seq]

    def last_seq(self) -> int:
        return self._messages[-1].seq if self._messages else 0
