from secrets import compare_digest

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

_HEALTH_PATH = "/api/v1/health"
_BEARER_PREFIX = "Bearer "


class ApiKeyMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, api_key: str) -> None:
        super().__init__(app)
        self._api_key = api_key

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.method == "GET" and request.url.path.rstrip("/") == _HEALTH_PATH:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith(_BEARER_PREFIX):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

        token = auth_header[len(_BEARER_PREFIX) :]
        if not compare_digest(token, self._api_key):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

        return await call_next(request)
