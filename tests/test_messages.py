from game_chat.messages import ChannelMessageLog, DuplicateMessage


def test_seq_is_dense_and_ordered():
    log = ChannelMessageLog("c1")
    m1 = log.append("p1", "a", "cm-1", "id-1")
    m2 = log.append("p2", "b", "cm-2", "id-2")
    m3 = log.append("p1", "c", "cm-3", "id-3")
    assert [m.seq for m in (m1, m2, m3)] == [1, 2, 3]


def test_duplicate_client_id_returns_original():
    log = ChannelMessageLog("c1")
    first = log.append("p1", "a", "cm-1", "id-1")
    try:
        log.append("p1", "a", "cm-1", "id-2")
        assert False, "expected DuplicateMessage"
    except DuplicateMessage as exc:
        assert exc.message is first
    assert log.last_seq() == 1


def test_replay_after_seq():
    log = ChannelMessageLog("c1")
    log.append("p1", "a", "cm-1", "id-1")
    log.append("p1", "b", "cm-2", "id-2")
    log.append("p1", "c", "cm-3", "id-3")
    missed = [m.seq for m in log.replay(1)]
    assert missed == [2, 3]
    assert log.replay(3) == []


def test_history_trim_keeps_latest():
    log = ChannelMessageLog("c1", history_limit=2)
    log.append("p1", "a", "cm-1", "id-1")
    m2 = log.append("p1", "b", "cm-2", "id-2")
    m3 = log.append("p1", "c", "cm-3", "id-3")
    assert [m.seq for m in log.replay(0)] == [2, 3]
    assert m2.seq == 2 and m3.seq == 3
    # trimmed client id may be reused and starts a new message
    assert log.get_by_client_id("cm-1") is None
