from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from config.settings import settings


class _BodyTooLarge(Exception):
    pass


class RequestSizeLimitMiddleware:
    """Pure ASGI middleware that enforces a maximum request body size.

    Checks the declared Content-Length header for a fast-path rejection, then
    counts actual bytes as they stream in to cover chunked transfer encoding.
    """

    def __init__(self, app: ASGIApp, max_bytes: int = settings.max_request_body_bytes):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Fast path: reject on a valid, declared Content-Length
        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        raw_cl = headers.get(b"content-length")
        if raw_cl is not None:
            try:
                if int(raw_cl) > self.max_bytes:
                    await self._send_413(scope, receive, send)
                    return
            except ValueError:
                pass  # malformed header; fall through to stream enforcement

        # Stream enforcement: count bytes as they arrive
        total = 0

        async def checked_receive() -> Message:
            nonlocal total
            message = await receive()
            if message["type"] == "http.request":
                total += len(message.get("body", b""))
                if total > self.max_bytes:
                    raise _BodyTooLarge()
            return message

        try:
            await self.app(scope, checked_receive, send)
        except _BodyTooLarge:
            await self._send_413(scope, receive, send)

    @staticmethod
    async def _send_413(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response(
            content='{"detail": "Request body too large"}',
            status_code=413,
            media_type="application/json",
        )
        await response(scope, receive, send)
