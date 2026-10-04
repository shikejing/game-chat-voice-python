"""FastAPI application exposing the WebSocket endpoint and health check."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .config import RoomSettings, default_settings
from .connection import ClientConnection
from .errors import GameChatError
from .protocol import dispatch, error_event
from .service import RoomService

logger = logging.getLogger("game_chat")


def create_app(
    service: RoomService | None = None,
    settings: RoomSettings | None = None,
) -> FastAPI:
    settings = settings or default_settings()
    svc = service or RoomService(settings=settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await svc.stop()

    app = FastAPI(title="game-chat-voice", lifespan=lifespan)
    app.state.service = svc
    app.state.settings = settings

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/ws/rooms/{room_id}")
    async def room_socket(websocket: WebSocket, room_id: str) -> None:
        await _socket_loop(svc, settings, websocket, room_id)

    return app


def _query_param(websocket: WebSocket, key: str) -> str | None:
    value = websocket.query_params.get(key)
    return value if value else None


async def _socket_loop(
    svc: RoomService,
    settings: RoomSettings,
    websocket: WebSocket,
    room_id: str,
) -> None:
    player_id = _query_param(websocket, "player_id")
    name = _query_param(websocket, "name") or ""
    if not player_id:
        await websocket.close(code=4401)
        return

    await websocket.accept()
    conn = svc._new_connection(player_id)
    writer_task = asyncio.create_task(_write_loop(websocket, conn))

    try:
        try:
            result = await svc.connect(room_id, player_id, name, conn)
        except GameChatError as exc:
            await websocket.send_json(error_event(exc.code, str(exc)))
            await websocket.close(code=4403)
            writer_task.cancel()
            return
        except Exception:  # pragma: no cover - defensive
            logger.exception("connect failed")
            await websocket.close(code=4500)
            writer_task.cancel()
            return

        await websocket.send_json({"type": "connected", **result})

        while True:
            # A receive failure (peer closed, cancelled, transport error)
            # terminates the loop for good -- it must never be swallowed by the
            # per-request handling below, otherwise the coroutine would spin.
            raw = await websocket.receive_text()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                if not await _safe_send(
                    websocket,
                    error_event("invalid_request", "message must be valid JSON"),
                ):
                    break
                continue

            # Inbound token bucket: protects the room from a flooding client.
            try:
                conn.check_send_quota()
            except GameChatError as exc:
                if not await _safe_send(websocket, error_event(exc.code, str(exc))):
                    break
                continue

            try:
                response = await dispatch(svc, room_id, player_id, message)
            except GameChatError as exc:
                ref = message.get("ref") if isinstance(message, dict) else None
                if not await _safe_send(
                    websocket, error_event(exc.code, str(exc), ref)
                ):
                    break
                continue

            if response is not None:
                events = response if isinstance(response, list) else [response]
                for event in events:
                    if not await conn.offer(event):
                        # Rare: caller's own queue full. Close as slow consumer.
                        conn.mark_closed("slow_consumer_send_queue_full")
                        break
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        raise
    except Exception:  # pragma: no cover - defensive
        logger.exception("socket loop error")
    finally:
        await svc.session_closed(room_id, player_id, conn)
        conn.mark_closed("socket_closed")
        # Cancel the pump but do not await it: a send already in flight on a
        # half-closed transport may not observe cancellation promptly. The
        # task is detached and reaps itself when that send settles; closing the
        # websocket aborts any pending write.
        writer_task.cancel()
        try:
            await websocket.close()
        except Exception:
            pass


async def _safe_send(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    """Send directly for control/error frames; False when the socket is gone."""
    try:
        await websocket.send_json(payload)
        return True
    except Exception:
        return False


async def _write_loop(websocket: WebSocket, conn: ClientConnection) -> None:
    """Pump the bounded per-connection queue to the wire."""
    while True:
        event = await conn.send_queue.get()
        if event is None:
            return
        try:
            await websocket.send_json(event)
        except Exception:
            conn.mark_closed("write_failed")
            return


app = create_app()
