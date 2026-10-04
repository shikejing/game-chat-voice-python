

from game_chat.connections import DISCONNECT, DROP_OLDEST, Connection, ConnectionHub


async def test_offer_and_get_preserves_order():
    conn = Connection("p1", "r1", queue_size=4, slow_behavior=DROP_OLDEST)
    for i in range(3):
        assert await conn.offer({"i": i}) is True
    got = [await conn.get() for _ in range(3)]
    assert got == [{"i": 0}, {"i": 1}, {"i": 2}]


async def test_drop_oldest_on_full_queue():
    conn = Connection("p1", "r1", queue_size=2, slow_behavior=DROP_OLDEST)
    assert await conn.offer({"i": 1}) is True
    assert await conn.offer({"i": 2}) is True
    assert await conn.offer({"i": 3}) is True  # evicts i=1
    assert await conn.get() == {"i": 2}
    assert await conn.get() == {"i": 3}
    assert conn.dropped_events == 1


async def test_disconnect_on_full_queue():
    conn = Connection("p1", "r1", queue_size=2, slow_behavior=DISCONNECT)
    await conn.offer({"i": 1})
    await conn.offer({"i": 2})
    assert await conn.offer({"i": 3}) is False
    assert conn.closed is True
    assert await conn.get() == {"i": 2}
    assert await conn.get() is None


async def test_hub_send_and_broadcast():
    hub = ConnectionHub(queue_size=8, slow_behavior=DROP_OLDEST)
    a = hub.register("r1", "a")
    b = hub.register("r1", "b")
    hub.register("r2", "c")  # other room, must not receive
    assert await hub.send_to("r1", "a", {"e": "x"}) is True
    assert await a.get() == {"e": "x"}
    count = await hub.broadcast("r1", {"e": "room"}, exclude=frozenset({"a"}))
    assert count == 1
    assert await b.get() == {"e": "room"}
    assert hub.online_players("r1") == {"a", "b"}
    hub.unregister(b)
    assert hub.online_players("r1") == {"a"}
    # duplicate unregister is harmless
    hub.unregister(b)


async def test_hub_multiple_sessions_same_player():
    hub = ConnectionHub(queue_size=4, slow_behavior=DROP_OLDEST)
    s1 = hub.register("r1", "a")
    s2 = hub.register("r1", "a")
    assert hub.session_count("r1", "a") == 2
    assert await hub.send_to("r1", "a", {"e": 1}) is True
    assert await s1.get() == {"e": 1}
    assert await s2.get() == {"e": 1}
    hub.unregister(s1)
    assert hub.session_count("r1", "a") == 1
