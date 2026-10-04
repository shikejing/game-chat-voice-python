"""End-to-end tests over a real WebSocket using FastAPI TestClient."""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from game_chat.app import create_app
from game_chat.config import BackpressureConfig, RateLimitConfig, Settings


@pytest.fixture
def client():
    settings = Settings(
        rate_limits=RateLimitConfig(messages_per_second=1000,
                                    signaling_per_second=1000,
                                    actions_per_second=1000, burst=100000),
        backpressure=BackpressureConfig(outbound_queue_size=64,
                                        slow_consumer_behavior="drop_oldest",
                                        channel_history=200),
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


@contextmanager
def _connect(client, room, player):
    with client.websocket_connect(f"/ws/rooms/{room}/players/{player}") as ws:
        ready = ws.receive_json()
        snap = ws.receive_json()
        assert ready["event"] == "ready"
        assert snap["event"] == "state.snapshot"
        yield ws, snap


def _send(ws, event, data, req_id=None):
    frame = {"event": event, "data": data}
    if req_id is not None:
        frame["req_id"] = req_id
    ws.send_json(frame)


def _find(ws, event, max_frames=100):
    for _ in range(max_frames):
        frame = ws.receive_json()
        if frame["event"] == event:
            return frame
    raise AssertionError(f"event {event} not observed")



def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_full_text_chat_flow_with_dedup_and_order(client):
    with _connect(client, "room1", "alice") as (ws_a, _), \
         _connect(client, "room1", "bob") as (ws_b, _):
        _send(ws_a, "channel.create", {"type": "text"}, req_id="c")
        created = ws_a.receive_json()
        assert created["event"] == "channel.created"
        channel_id = created["data"]["channel"]["id"]

        _send(ws_b, "channel.join", {"channel_id": channel_id}, req_id="j")
        join_ok = _find(ws_b, "ok")
        assert join_ok["data"]["joined"] == channel_id
        note = _find(ws_a, "member.joined")
        assert note["data"]["player_id"] == "bob"

        _send(ws_a, "message.send",
              {"channel_id": channel_id, "body": "one", "client_msg_id": "m1"})
        _send(ws_a, "message.send",
              {"channel_id": channel_id, "body": "two", "client_msg_id": "m2"})
        first = _find(ws_b, "message")
        second = _find(ws_b, "message")
        assert first["data"]["message"]["body"] == "one"
        assert first["data"]["message"]["seq"] == 1
        assert second["data"]["message"]["seq"] == 2

        _send(ws_a, "message.send",
              {"channel_id": channel_id, "body": "one", "client_msg_id": "m1"},
              req_id="dup")
        dup_ack = _find(ws_a, "message.ack")
        assert dup_ack["data"]["seq"] == 1
        assert dup_ack["data"]["id"] == first["data"]["message"]["id"]


def test_reconnect_replays_missed_without_duplicates(client):
    with _connect(client, "room2", "alice") as (ws_a, _):
        _send(ws_a, "channel.create", {"type": "text"})
        channel_id = ws_a.receive_json()["data"]["channel"]["id"]

        with _connect(client, "room2", "bob") as (ws_b, _):
            _send(ws_b, "channel.join", {"channel_id": channel_id})
            _find(ws_b, "ok")
            _find(ws_a, "member.joined")
        # bob disconnected here

        for i in range(3):
            _send(ws_a, "message.send", {
                "channel_id": channel_id, "body": f"n{i}",
                "client_msg_id": f"off-{i}",
            })
            ws_a.receive_json()  # message.ack response

        # bob reconnects with watermark 1
        with _connect(client, "room2", "bob") as (ws_b2, _):
            _send(ws_b2, "session.resume", {"channels": [
                {"channel_id": channel_id, "last_seq": 1},
            ]})
            m2 = _find(ws_b2, "message")
            m3 = _find(ws_b2, "message")
            assert [m2["data"]["message"]["seq"],
                    m3["data"]["message"]["seq"]] == [2, 3]
            complete = _find(ws_b2, "resume.complete")
            assert complete["data"]["channels"][0]["replayed"] == 2

            _send(ws_b2, "session.resume", {"channels": [
                {"channel_id": channel_id, "last_seq": 3},
            ]})
            complete2 = _find(ws_b2, "resume.complete")
            assert complete2["data"]["channels"][0]["replayed"] == 0


def test_unauthorized_close_and_kick_are_rejected(client):
    with _connect(client, "room3", "owner") as (ws_owner, _), \
         _connect(client, "room3", "bob") as (ws_b, _):
        _send(ws_owner, "channel.create", {"type": "text"})
        channel_id = ws_owner.receive_json()["data"]["channel"]["id"]

        _send(ws_b, "channel.join", {"channel_id": channel_id})
        _find(ws_b, "ok")
        _find(ws_owner, "member.joined")

        _send(ws_b, "channel.close", {"channel_id": channel_id}, req_id="x")
        err = _find(ws_b, "error")
        assert err["data"]["code"] == "permission_denied.not_owner"
        assert err["req_id"] == "x"

        with _connect(client, "room3", "carol") as (ws_c, _):
            _send(ws_c, "channel.kick",
                  {"channel_id": channel_id, "target_id": "bob"}, req_id="k")
            err2 = _find(ws_c, "error")
            assert err2["data"]["code"] == "permission_denied.not_member"

        _send(ws_owner, "message.send", {
            "channel_id": channel_id, "body": "still alive",
            "client_msg_id": "alive",
        })
        ack = _find(ws_owner, "message.ack")
        assert ack["data"]["client_msg_id"] == "alive"


def test_voice_signaling_only_to_target_and_converges_on_leave(client):
    with _connect(client, "room4", "alice") as (ws_a, _), \
         _connect(client, "room4", "bob") as (ws_b, _), \
         _connect(client, "room4", "carol") as (ws_c, _):
        _send(ws_a, "channel.create", {"type": "voice"})
        channel_id = ws_a.receive_json()["data"]["channel"]["id"]
        for ws in (ws_b, ws_c):
            _send(ws, "channel.join", {"channel_id": channel_id})
            _find(ws, "ok")
        _find(ws_a, "member.joined")
        _find(ws_a, "member.joined")

        for ws in (ws_a, ws_b, ws_c):
            _send(ws, "voice.join", {"channel_id": channel_id})
            ws.receive_json()
        _find(ws_a, "voice.participant_joined")
        _find(ws_a, "voice.participant_joined")

        _send(ws_a, "voice.signal", {
            "channel_id": channel_id, "target_id": "bob",
            "payload": {"ice": "candidate"},
        })
        sig = _find(ws_b, "voice.signal")
        assert sig["data"]["payload"] == {"ice": "candidate"}
        assert sig["data"]["from"] == "alice"

        _send(ws_b, "voice.leave", {"channel_id": channel_id})
        ws_b.receive_json()
        _find(ws_a, "voice.participant_left")
        _send(ws_a, "voice.signal", {
            "channel_id": channel_id, "target_id": "bob", "payload": {},
        })
        err = _find(ws_a, "error")
        assert err["data"]["code"] == "not_call_participant"


def test_concurrent_members_no_duplicate_state(client):
    import threading

    with _connect(client, "room5", "host") as (host, _):
        _send(host, "channel.create", {"type": "text"})
        channel_id = host.receive_json()["data"]["channel"]["id"]

        errors = []

        def worker(i):
            try:
                with client.websocket_connect(
                    f"/ws/rooms/room5/players/p{i}"
                ) as ws:
                    ws.receive_json()
                    ws.receive_json()
                    ws.send_json({"event": "channel.join",
                                  "data": {"channel_id": channel_id}})
                    f = ws.receive_json()
                    for _ in range(20):
                        if f["event"] == "ok":
                            break
                        f = ws.receive_json()
                    assert f["event"] == "ok"
                    for j in range(10):
                        ws.send_json({"event": "message.send", "data": {
                            "channel_id": channel_id,
                            "body": f"{i}-{j}",
                            "client_msg_id": f"p{i}-{j}",
                        }})
                        f = ws.receive_json()
                        for _ in range(20):
                            if f["event"] == "message.ack":
                                break
                            f = ws.receive_json()
                        assert f["event"] == "message.ack"
            except Exception as exc:  # noqa: BLE001 pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors

        _send(host, "state.snapshot", {})
        snap = _find(host, "state.snapshot")
        target = next(c for c in snap["data"]["channels"]
                      if c["id"] == channel_id)
        assert len(target["members"]) == 9

        with client.websocket_connect(
            "/ws/rooms/room5/players/lurker"
        ) as ws:
            ws.receive_json()
            ws.receive_json()
            ws.send_json({"event": "channel.join",
                          "data": {"channel_id": channel_id}})
            f = ws.receive_json()
            for _ in range(5):
                if f["event"] == "ok":
                    break
                f = ws.receive_json()
            assert f["event"] == "ok"
            ws.send_json({"event": "session.resume", "data": {"channels": [
                {"channel_id": channel_id, "last_seq": 0},
            ]}})
            seqs = []
            for _ in range(120):
                f = ws.receive_json()
                if f["event"] == "message":
                    seqs.append(f["data"]["message"]["seq"])
                if f["event"] == "resume.complete":
                    break
        assert seqs == list(range(1, 81))


def test_kicked_player_is_notified_and_channel_created_once(client):
    with _connect(client, "room6", "owner") as (ws_owner, _), \
         _connect(client, "room6", "bob") as (ws_b, _):
        _send(ws_owner, "channel.create", {"type": "text"})
        # owner receives exactly one channel.created (its own response)
        created = ws_owner.receive_json()
        channel_id = created["data"]["channel"]["id"]
        # bob received the discovery broadcast
        bob_created = _find(ws_b, "channel.created")
        assert bob_created["data"]["channel"]["id"] == channel_id

        _send(ws_b, "channel.join", {"channel_id": channel_id})
        _find(ws_b, "ok")
        _find(ws_owner, "member.joined")

        _send(ws_owner, "channel.kick",
              {"channel_id": channel_id, "target_id": "bob"})
        _find(ws_owner, "ok")
        left = _find(ws_b, "member.left")
        assert left["data"]["player_id"] == "bob"
        assert left["data"]["reason"] == "kicked"

        # bob can no longer speak
        _send(ws_b, "message.send", {
            "channel_id": channel_id, "body": "ghost", "client_msg_id": "g1",
        })
        err = _find(ws_b, "error")
        assert err["data"]["code"] == "permission_denied.not_member"
