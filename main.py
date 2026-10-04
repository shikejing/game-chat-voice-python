import uvicorn

from game_chat.app import app

__all__ = ["app"]


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
