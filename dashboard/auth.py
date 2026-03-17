"""
Auth helpers for the dashboard.

Token format: "{expires_ts}:{hmac_hex}"
  - expires_ts: unix timestamp when the token expires
  - hmac_hex:   HMAC-SHA256(expires_ts, AUTH_SECRET)

Stored as a session cookie (no max_age → deleted when browser closes).
The embedded expiry means the token is invalid after 1 hour regardless.
"""

import hmac
import hashlib
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse

from dashboard.config import AUTH_SECRET, AUTH_TOKEN_TTL, DASHBOARD_PASSWORD

# Paths that don't require authentication
_PUBLIC_PATHS = {"/login", "/auth/login", "/auth/logout"}


def _sign(expires_ts: int) -> str:
    payload = str(expires_ts).encode()
    return hmac.new(AUTH_SECRET.encode(), payload, hashlib.sha256).hexdigest()


def make_token() -> str:
    expires = int(time.time()) + AUTH_TOKEN_TTL
    return f"{expires}:{_sign(expires)}"


def validate_token(token: str | None) -> bool:
    if not token:
        return False
    try:
        expires_str, sig = token.split(":", 1)
        expires = int(expires_str)
    except (ValueError, AttributeError):
        return False
    if time.time() > expires:
        return False
    return hmac.compare_digest(sig, _sign(expires))


def check_password(password: str) -> bool:
    if not DASHBOARD_PASSWORD:
        return True  # no password configured → open access
    return hmac.compare_digest(password, DASHBOARD_PASSWORD)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Always allow public paths and static assets
        if path in _PUBLIC_PATHS or path.startswith("/static"):
            return await call_next(request)

        # If no password is configured, skip auth entirely
        if not DASHBOARD_PASSWORD:
            return await call_next(request)

        token = request.cookies.get("ds_token")
        if validate_token(token):
            return await call_next(request)

        # API routes return 401 instead of redirect
        if path.startswith("/api/"):
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)

        return RedirectResponse(url="/login", status_code=302)
