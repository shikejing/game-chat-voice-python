import pytest

from game_chat.protocol import C2S_MESSAGE, ProtocolError, decode_event, encode_event


def test_encode_shape():
    frame = encode_event("message", {"x": 1}, req_id="r1")
    assert frame == {"event": "message", "data": {"x": 1}, "req_id": "r1"}


def test_decode_valid():
    event, data, req_id = decode_event(
        {"event": C2S_MESSAGE, "data": {"body": "hi"}, "req_id": "a"})
    assert event == C2S_MESSAGE and data == {"body": "hi"} and req_id == "a"


@pytest.mark.parametrize("raw", [
    "string",
    42,
    [],
    {},
    {"event": ""},
    {"event": "nope"},
    {"event": C2S_MESSAGE, "data": []},
    {"event": C2S_MESSAGE, "req_id": 9},
])
def test_decode_invalid(raw):
    with pytest.raises(ProtocolError):
        decode_event(raw)


def test_decode_defaults():
    _event, data, req_id = decode_event({"event": C2S_MESSAGE})
    assert data == {} and req_id is None
