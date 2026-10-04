
from game_chat.connections import DISCONNECT, DROP_OLDEST, Connection


async def test_disconnect_policy_marks_slow_connection_dead():
    conn = Connection("p", "r", queue_size=3, slow_behavior=DISCONNECT)
    # fill queue and exhaust it; every further offer is refused
    results = []
    for i in range(6):
        results.append(await conn.offer({"i": i}))
    assert any(ok is False for ok in results)
    assert conn.closed is True


async def test_drop_oldest_never_blocks_room_loop():
    conn = Connection("p", "r", queue_size=2, slow_behavior=DROP_OLDEST)
    # Hundreds of offers on a queue of size 2 complete immediately and the
    # newest state is always observable (latest-wins for presence).
    for i in range(200):
        await conn.offer({"i": i})
    last = await conn.get()
    assert last["i"] in (198, 199)


async def test_closed_connection_rejects_offers():
    conn = Connection("p", "r", queue_size=4, slow_behavior=DROP_OLDEST)
    conn.close()
    assert await conn.offer({"x": 1}) is False
    assert await conn.get() is None
