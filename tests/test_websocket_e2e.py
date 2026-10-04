"""End-to-end tests over real WebSockets through FastAPI."""

import contextlib
import uuid

import pytest
from starlette.testclient import TestClient

from game_chat.app import create_app
from game_chat.config import RoomSettings
from game_chat.service import RoomService
from game_chat.state import RoomRegistry


@pytest.fixture
def client():
    settings = RoomSettings(
        offline_grace_seconds=30,
        client_send_queue_max=512,
        client_send_quota=5,
        client_send_window_seconds=1.0,
    )
    service = RoomService(RoomRegistry(), settings)
    app = create_app(service=service, settings=settings)
    with TestClient(app) as c:
        yield c


def _connect(client: TestClient, room: str, player: str):
    return client.websocket_connect(f"/ws/rooms/{room}?player_id={player}")


def _drain(sock, max_events=200):
    """Read events already queued in the in-memory transport, never blocking.

    The starlette test transport is an anyio memory stream; we poll its buffer
    statistics instead of doing a blocking receive, so tests can assert
    "no further messages arrived" without hanging.
    """
    import json

    stream = sock._send_rx
    events = []
    for _ in range(max_events):
        if stream.statistics().current_buffer_used == 0:
            break
        message = sock.receive()
        if message.get("type") == "websocket.close":
            break
        text = message.get("text")
        if text is not None:
            events.append(json.loads(text))
    return events


def _wait(sock, predicate, max_events=50):
    """Read events until predicate(event) is true; returns matched event."""
    for _ in range(max_events):
        event = sock.receive_json()
        if predicate(event):
            return event
    raise AssertionError("expected event not observed")


def _wait_ack(sock, ref):
    return _wait(
        sock, lambda e: e.get("type") == "ack" and e.get("ref") == ref
    )


def test_end_to_end_text_chat_and_permission_error(client):
    room = f"ws-{uuid.uuid4().hex[:6]}"
    with _connect(client, room, "alice") as alice, _connect(client, room, "bob") as bob:
        _wait(alice, lambda e: e.get("type") == "snapshot")
        _wait(bob, lambda e: e.get("type") == "snapshot")

        alice.send_json({"type": "create_channel", "ref": "1", "name": "g", "kind": "text"})
        ack = _wait_ack(alice, "1")
        cid = ack["data"]["channel_id"]

        bob.send_json({"type": "join_channel", "ref": "2", "channel_id": cid})
        _wait_ack(bob, "2")

        # Bob cannot close alice's channel: distinguishable error over the wire.
        bob.send_json({"type": "close_channel", "ref": "3", "channel_id": cid})
        err = _wait(
            bob, lambda e: e.get("type") == "error" and e.get("ref") == "3"
        )
        assert err["code"] == "permission_denied_not_channel_admin"

        alice.send_json(
            {"type": "message", "ref": "4", "channel_id": cid, "body": "hello",
             "client_msg_id": "c-1"}
        )
        _wait_ack(alice, "4")

        delivered = _wait(bob, lambda e: e.get("type") == "message")
        assert delivered["body"] == "hello"
        assert delivered["seq"] == 1
        assert delivered["message_id"]


def test_duplicate_client_msg_id_idempotent_over_ws(client):
    room = f"ws-{uuid.uuid4().hex[:6]}"
    with _connect(client, room, "alice") as a, _connect(client, room, "bob") as b:
        _wait(a, lambda e: e.get("type") == "snapshot")
        _wait(b, lambda e: e.get("type") == "snapshot")
        a.send_json({"type": "create_channel", "name": "g", "kind": "text"})
        cid = _wait(a, lambda e: e.get("type") == "channel_created")["channel"]["channel_id"]
        b.send_json({"type": "join_channel", "channel_id": cid})
        _wait(b, lambda e: e.get("type") == "ack")

        for _ in range(2):
            a.send_json(
                {"type": "message", "channel_id": cid, "body": "x",
                 "client_msg_id": "dup-1"}
            )
        # Bob sees the message exactly once even though it was sent twice.
        first = _wait(b, lambda e: e.get("type") == "message")
        assert first["client_msg_id"] == "dup-1"
        rest = _drain(b, max_events=10)
        assert [e for e in rest if e.get("type") == "message"] == []


def test_rate_limit_enforced(client):
    room = f"ws-{uuid.uuid4().hex[:6]}"
    with _connect(client, room, "alice") as alice:
        _wait(alice, lambda e: e.get("type") == "snapshot")
        saw_limit = False
        for i in range(30):
            alice.send_json({"type": "ping", "ref": str(i)})
            event = alice.receive_json()
            if event.get("type") == "error" and event.get("code") == "rate_limited":
                saw_limit = True
                break
        assert saw_limit


def test_reconnect_replays_missed_messages(client):
    room = f"ws-{uuid.uuid4().hex[:6]}"
    with contextlib.ExitStack() as stack:
        alice = stack.enter_context(_connect(client, room, "alice"))
        _wait(alice, lambda e: e.get("type") == "snapshot")
        alice.send_json({"type": "create_channel", "name": "g", "kind": "text"})
        cid = _wait(alice, lambda e: e.get("type") == "channel_created")[
            "channel"
        ]["channel_id"]

        bob = stack.enter_context(_connect(client, room, "bob"))
        _wait(bob, lambda e: e.get("type") == "snapshot")
        bob.send_json({"type": "join_channel", "channel_id": cid})
        _wait(bob, lambda e: e.get("type") == "ack")
        bob.close()  # first session goes offline

        alice.send_json({"type": "message", "channel_id": cid, "body": "m1"})
        _wait(alice, lambda e: e.get("type") == "ack")

        # Same player reconnects on a fresh socket; missed message is replayed.
        with _connect(client, room, "bob") as bob2:
            events = []
            for _ in range(20):
                ev = bob2.receive_json()
                events.append(ev)
                if ev.get("type") == "snapshot":
                    break

    messages = [e for e in events if e.get("type") == "message"]
    assert len(messages) == 1
    assert messages[0]["body"] == "m1"
    assert messages[0]["seq"] == 1
