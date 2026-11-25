"""cTrader Open API session, JSON over WebSocket, on asyncio: the transport under the adapter.

One session is one connection authorised for one trading account. A request carries a
``clientMsgId`` and its answer is matched on it; every other message (spot quotes, execution
events, disconnect notices) goes to ``on_event(payload_type, payload)``. Payload types and field
names are Spotware's (OpenApiMessages.proto, OpenApiModelMessages.proto), sent as JSON on port
5036, so no protobuf; the ones used here were checked against a Fusion Markets demo account.

Credentials: a JSON file with the Open API application's ``client_id`` and ``client_secret``, the
``access_token`` and ``refresh_token`` granted for the account, ``account`` (the login cTrader
shows) and ``host`` (``demo.ctraderapi.com`` when absent). An expired access token is renewed with
the refresh token and the file is rewritten, readable by its owner only. A live host
(``live.ctraderapi.com``) is refused unless the caller allows it.

Reconnect. When the connection ends without ``close()`` (the network, the server, or one of its
disconnect notices: token invalidated, client or account disconnected), the session opens a new
one after 1, 2, 4, 8, 15, 30 and then every 60 seconds, authorises it again (refreshing an expired
token) and awaits ``on_reconnect()``: there a client restores what the server forgot with the old
connection (spot subscriptions) and learns what it missed (its orders' fills, cancels, expiries).
A restore that fails drops the new connection, so the next one restores again. A request made
while the session is down waits for it up to its timeout; one in flight when the connection drops
fails with ``ConnectionError``. websockets' keepalive (a ping every 20 s, 20 s for the pong) ends
a connection that died without closing.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path

from nautilus_trader.common import Logger
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

JSON_PORT = 5036
HEARTBEAT_S = 10                # Spotware: a heartbeat every 10 seconds keeps the connection
HISTORY_PER_S = 5               # Spotware: historical requests per second per connection
BACKOFF_S = (1, 2, 4, 8, 15, 30, 60)

ERROR_RES, HEARTBEAT = 50, 51
APP_AUTH, ACCOUNT_AUTH = 2100, 2102
NEW_ORDER, CANCEL_ORDER, AMEND_ORDER = 2106, 2108, 2109
ASSET_LIST, SYMBOLS, SYMBOL_BY_ID = 2112, 2114, 2116
TRADER, TRADER_UPDATE, RECONCILE, EXECUTION_EVENT = 2121, 2123, 2124, 2126
SUBSCRIBE_SPOTS, UNSUBSCRIBE_SPOTS, SPOT_EVENT, ORDER_ERROR_EVENT = 2127, 2129, 2131, 2132
DEAL_LIST, GET_TRENDBARS, OA_ERROR = 2133, 2137, 2142
TOKEN_INVALIDATED, CLIENT_DISCONNECT, ACCOUNTS, ACCOUNT_DISCONNECT = 2147, 2148, 2149, 2164
REFRESH_TOKEN, ORDER_LIST, ORDER_DETAILS = 2173, 2175, 2181

TOKEN_EXPIRED = {"CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"}
RATE_LIMITED = {"BLOCKED_PAYLOAD_TYPE", "REQUEST_FREQUENCY_EXCEEDED"}
HISTORICAL = {GET_TRENDBARS, 2145}
DISCONNECTS = {TOKEN_INVALIDATED, CLIENT_DISCONNECT, ACCOUNT_DISCONNECT}

__all__ = ["BACKOFF_S", "OpenApiError", "OpenApiSession"]


class OpenApiError(RuntimeError):
    def __init__(self, code: str | None, description: str | None, retry_after=None):
        super().__init__(f"{code}: {description}")
        self.code, self.description, self.retry_after = code, description, retry_after


class OpenApiSession:
    """``on_event(payload_type, payload)`` runs on the session's loop for every message that is
    not the answer to a request, and also for an answer whose payload type is in ``tap_answers``,
    in the order the messages arrived and before the request sees its answer: an order's
    acceptance is the answer to the order request and its fill a later event, and a handler fed
    by both sees them in that order. ``on_close(exc)`` runs when a connection ends unasked,
    ``on_reconnect()`` is awaited once a new one is authorised. ``backoff_s`` empty: no reconnect."""

    def __init__(self, credentials_path: Path | str, *, allow_live: bool = False,
                 on_event: Callable[[int, dict], None] | None = None,
                 on_close: Callable[[BaseException | None], None] | None = None,
                 on_reconnect: Callable[[], Awaitable[None]] | None = None, url: str | None = None,
                 tap_answers: frozenset[int] = frozenset(), backoff_s=BACKOFF_S):
        self.credentials_path = Path(credentials_path)
        self.allow_live = allow_live
        self.on_event = on_event
        self.on_close = on_close
        self.on_reconnect = on_reconnect
        self.tap_answers = tap_answers
        self.backoff_s = tuple(backoff_s)
        self._url = url
        self.account_id: int | None = None
        self.is_live: bool | None = None
        self.reconnects = 0
        self._ws = None
        self._pending: dict[str, asyncio.Future] = {}
        self._conn_tasks: list[asyncio.Task] = []
        self._supervisor: asyncio.Task | None = None
        self._connected = asyncio.Event()
        self._dropped = asyncio.Event()
        self._closing = False
        self._last_send = 0.0
        self._history_lock = asyncio.Lock()
        self._last_history = 0.0
        self._log = Logger(type(self).__name__)

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    # ── connection ───────────────────────────────────────────────────────

    async def connect(self) -> int:
        """Open the connection, authorise the application and the account; returns
        ``ctidTraderAccountId``. From here on the session reconnects by itself."""
        await self._open()
        if self.backoff_s and self._supervisor is None:
            self._supervisor = asyncio.ensure_future(self._supervise())
        return self.account_id

    async def _open(self) -> None:
        cred = json.loads(self.credentials_path.read_text())
        host = cred.get("host", "demo.ctraderapi.com")
        live_host = host.startswith("live")
        if live_host and not self.allow_live:
            raise PermissionError(f"{host} is a live server and this session does not allow live")
        for task in self._conn_tasks:
            task.cancel()
        self._fail_pending(ConnectionError("cTrader connection replaced"))   # only the old one's can be pending
        ws = await connect(self._url or f"wss://{host}:{JSON_PORT}", max_size=None, open_timeout=30)
        self._ws = ws
        self._dropped.clear()
        self._conn_tasks = [asyncio.ensure_future(self._read(ws)), asyncio.ensure_future(self._heartbeat(ws))]
        try:
            await self._request_now(APP_AUTH, {"clientId": cred["client_id"], "clientSecret": cred["client_secret"]})
            try:
                _, accounts = await self._request_now(ACCOUNTS, {"accessToken": cred["access_token"]})
            except OpenApiError as e:
                if e.code not in TOKEN_EXPIRED:
                    raise
                cred = await self._refresh(cred)
                _, accounts = await self._request_now(ACCOUNTS, {"accessToken": cred["access_token"]})
            listed = accounts.get("ctidTraderAccount", [])
            match = [a for a in listed if int(a.get("traderLogin", -1)) == int(cred["account"])]
            if not match:
                raise PermissionError(f"account {cred['account']} is not among this token's: "
                                      f"{[a.get('traderLogin') for a in listed]}")
            self.is_live = bool(match[0].get("isLive"))
            if self.is_live != live_host:
                raise PermissionError(f"account {cred['account']} has isLive={self.is_live}, "
                                      f"host {host} is the other environment")
            self.account_id = int(match[0]["ctidTraderAccountId"])
            await self._request_now(ACCOUNT_AUTH, {"ctidTraderAccountId": self.account_id,
                                                   "accessToken": cred["access_token"]})
        except BaseException:
            await ws.close()
            raise
        self._connected.set()

    async def _refresh(self, cred: dict) -> dict:
        _, fresh = await self._request_now(REFRESH_TOKEN, {"refreshToken": cred["refresh_token"]})
        cred = cred | {"access_token": fresh["accessToken"], "refresh_token": fresh["refreshToken"]}
        tmp = self.credentials_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(cred, indent=2))
        tmp.chmod(0o600)
        os.replace(tmp, self.credentials_path)
        return cred

    async def close(self) -> None:
        self._closing = True
        self._connected.clear()
        if self._supervisor is not None:
            self._supervisor.cancel()
        for task in self._conn_tasks:
            task.cancel()
        if self._ws is not None:
            await self._ws.close()
        self._fail_pending(ConnectionError("session closed"))

    async def _supervise(self) -> None:
        delays = itertools.chain(self.backoff_s, itertools.repeat(self.backoff_s[-1]))
        while True:
            await self._dropped.wait()
            if self._closing:
                return
            for attempt in itertools.count(1):
                await asyncio.sleep(next(delays))
                if self._closing:
                    return
                try:
                    await self._open()
                except Exception as e:  # noqa: BLE001 - every failure to reconnect is retried
                    self._log.warning(f"cTrader reconnect attempt {attempt} failed: {e!r}")
                    continue
                self.reconnects += 1
                self._log.warning(f"cTrader reconnected (attempt {attempt})")
                try:
                    if self.on_reconnect is not None:
                        await self.on_reconnect()
                except Exception as e:  # noqa: BLE001 - a failed restore is retried on a new connection
                    self._log.error(f"cTrader restore after the reconnect failed, reconnecting again: {e!r}")
                    await self._ws.close()
                    continue
                delays = itertools.chain(self.backoff_s, itertools.repeat(self.backoff_s[-1]))
                break

    # ── requests ─────────────────────────────────────────────────────────

    async def request(self, payload_type: int, payload: dict, timeout: float = 30) -> tuple[int, dict]:
        """``(payload_type, payload)`` of the answer: an execution event or an order error event
        answers an order request. ``OpenApiError`` for an error response, ``ConnectionError`` when
        the session is down for ``timeout`` or the connection drops before the answer."""
        if not self._connected.is_set():
            try:
                await asyncio.wait_for(self._connected.wait(), timeout)
            except TimeoutError:
                raise ConnectionError(f"cTrader has not been connected for {timeout} s") from None
        if payload_type in HISTORICAL:
            return await self._paced(payload_type, payload, timeout)
        return await self._request_now(payload_type, payload, timeout)

    async def account_request(self, payload_type: int, payload: dict | None = None, timeout: float = 30):
        return await self.request(payload_type, {"ctidTraderAccountId": self.account_id, **(payload or {})}, timeout)

    async def _request_now(self, payload_type: int, payload: dict, timeout: float = 30) -> tuple[int, dict]:
        msg_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = future
        try:
            await self._send(payload_type, payload, msg_id)
            return await asyncio.wait_for(future, timeout)
        finally:
            self._pending.pop(msg_id, None)

    async def _paced(self, payload_type: int, payload: dict, timeout: float, attempts: int = 20):
        """Historical requests, ``HISTORY_PER_S`` a second; a rate-limit answer waits as told."""
        for _ in range(attempts):
            async with self._history_lock:
                await asyncio.sleep(max(0.0, self._last_history + 1 / HISTORY_PER_S - time.monotonic()))
                self._last_history = time.monotonic()
            try:
                return await self._request_now(payload_type, payload, timeout)
            except OpenApiError as e:
                if e.code not in RATE_LIMITED:
                    raise
                await asyncio.sleep(float(e.retry_after or 1))
        raise OpenApiError("REQUEST_FREQUENCY_EXCEEDED", f"still rate-limited after {attempts} attempts")

    async def _send(self, payload_type: int, payload: dict, msg_id: str | None = None, ws=None) -> None:
        msg = {"payloadType": payload_type, "payload": payload}
        if msg_id:
            msg["clientMsgId"] = msg_id
        try:
            await (ws or self._ws).send(json.dumps(msg))
        except ConnectionClosed as e:
            raise ConnectionError(f"cTrader connection closed: {e}") from e
        self._last_send = time.monotonic()

    # ── per connection ───────────────────────────────────────────────────

    async def _read(self, ws) -> None:
        error: BaseException | None = None
        try:
            async for raw in ws:
                msg = json.loads(raw)
                kind, body, msg_id = msg.get("payloadType"), msg.get("payload") or {}, msg.get("clientMsgId")
                future = self._pending.get(msg_id) if msg_id else None
                answer = future is not None and not future.done()
                if kind != HEARTBEAT and self.on_event is not None and (not answer or kind in self.tap_answers):
                    try:
                        self.on_event(kind, body)
                    except Exception as e:  # noqa: BLE001 - a handler's bug must not end the connection
                        self._log.error(f"cTrader on_event({kind}) failed: {e!r}")
                if answer:
                    if kind in (ERROR_RES, OA_ERROR):
                        future.set_exception(OpenApiError(body.get("errorCode"), body.get("description"),
                                                          body.get("retryAfter")))
                    else:
                        future.set_result((kind, body))
                if kind in DISCONNECTS:
                    self._log.warning(f"cTrader disconnect notice {kind}: {body}; reconnecting")
                    asyncio.ensure_future(ws.close())
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - any end of the connection fails the pending requests
            error = e
        if ws is not self._ws:              # an older connection: nothing waits on it any more
            return
        self._connected.clear()
        self._fail_pending(ConnectionError(f"cTrader connection lost: {error!r}" if error else "cTrader connection lost"))
        self._dropped.set()
        if self.on_close is not None and not self._closing:
            self.on_close(error)

    async def _heartbeat(self, ws) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_S / 2)
            if time.monotonic() - self._last_send >= HEARTBEAT_S / 2:
                try:
                    await self._send(HEARTBEAT, {}, ws=ws)
                except ConnectionError:
                    return

    def _fail_pending(self, exc: BaseException) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)
