"""Voice call participant state and targeted signaling within a channel.

The server never sees media: a VoiceCall is just the converging set of
participant ids plus helpers that guarantee signaling targets are computed
from the *current* set. The room lock wraps every mutation, so a departed
participant can never receive a signal sent after its leave.
"""

from __future__ import annotations


class VoiceCall:
    """Live voice-call state associated with one voice channel."""

    def __init__(self, channel_id: str) -> None:
        self.channel_id = channel_id
        self._participants: set[str] = set()

    @property
    def participants(self) -> frozenset[str]:
        return frozenset(self._participants)

    def join(self, player_id: str) -> bool:
        """Add a participant.

        Returns False if the player was already present, so duplicate joins
        never create duplicate membership state.
        """
        if player_id in self._participants:
            return False
        self._participants.add(player_id)
        return True

    def leave(self, player_id: str) -> bool:
        """Remove a participant.

        Returns False if the player was not present, so duplicate leaves
        never corrupt the set.
        """
        if player_id not in self._participants:
            return False
        self._participants.remove(player_id)
        return True

    def is_participant(self, player_id: str) -> bool:
        return player_id in self._participants

    def targets_for(self, sender_id: str) -> frozenset[str]:
        """Return the other current participants; never the sender."""
        return frozenset(p for p in self._participants if p != sender_id)

    def empty(self) -> bool:
        return not self._participants
