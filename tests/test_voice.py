from game_chat.voice import VoiceCall


def test_join_leave_idempotent():
    call = VoiceCall("v1")
    assert call.join("a") is True
    assert call.join("a") is False  # duplicate join
    assert call.participants == frozenset({"a"})
    assert call.leave("a") is True
    assert call.leave("a") is False  # duplicate leave
    assert call.empty()


def test_targets_never_include_sender_or_departed():
    call = VoiceCall("v1")
    call.join("a")
    call.join("b")
    call.join("c")
    assert call.targets_for("a") == frozenset({"b", "c"})
    call.leave("b")
    assert call.targets_for("a") == frozenset({"c"})
    assert call.is_participant("b") is False
