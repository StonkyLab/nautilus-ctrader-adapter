"""cTrader execution for Nautilus: limit orders, their amendment and cancellation, fills, the
account's balance, and the reports Nautilus reconciles with at start.

What a Fusion Markets demo account showed (2026-11-24) and this client is built on:

- The account is HEDGED: an order opens a position of its own unless it names an open one with
  ``positionId``. A limit order naming the position reduces it, closes it, or (larger than the
  position) flips it in place under the same id. So the client keeps ONE cTrader position per
  instrument and names it in every order: the account behaves as netting, ``OmsType.NETTING``.
  An instrument with more than one open position (a manual hedge) is refused, and so is a second
  order while the first one that opens the position is still working.
- Only LIMIT orders, GTC or GTD. cTrader has no post-only; an order
  with ``post_only`` is checked here against the last quote in the cache (a buy at or above the
  ask, a sell at or below the bid is rejected ``due_post_only``). The quote can move before the
  order arrives, so that check narrows the race, it does not close it.
- Volumes are hundredths of a unit; an order below the minimum or off the step is refused here
  (denied), not sent.
- The answer to an order request is its acceptance (or an ``ORDER_ERROR_EVENT``); the fill is a
  later event. Both reach ``_on_execution`` in the order they arrived (``tap_answers``).
- A deal carries its commission in the deposit currency's minor units, negative for a cost; the
  account's balance is read back after every fill. cTrader does not say whether a fill made or
  took liquidity: a fill within a second of the order's placement is called TAKER, a later one
  MAKER.

After a reconnect (``OpenApiSession``) the client reads the balance, the positions and the
working orders again, then settles every order Nautilus holds open or in flight: found by venue
order id or client order id among the working orders or the order history, its details
(``ORDER_DETAILS``: the final status and the deals) tell Nautilus what it missed, in order:
the acceptance, the fills, an amendment, the cancel or expiry (``catch_up``). An order cTrader
never heard of did not reach it and is rejected; one whose request is still under way is left to
that request. An order request without an answer gets the same settling a few seconds later.
Startup is Nautilus's own reconciliation from the reports, which know open orders and positions
and the deals of the lookback, not closed orders.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from nautilus_trader.common import Logger
from nautilus_trader.live import ExecutionClientConfig, InstrumentProviderConfig
from nautilus_trader.live.clients import ExecutionClient, ExecutionClientFactory
from nautilus_trader.model import (
    AccountBalance,
    AccountId,
    AccountType,
    ClientOrderId,
    Currency,
    ExecutionMassStatus,
    FillReport,
    InstrumentId,
    LiquiditySide,
    Money,
    OmsType,
    OrderSide,
    OrderStatus,
    OrderStatusReport,
    OrderType,
    PositionSide,
    PositionStatusReport,
    Price,
    Quantity,
    TimeInForce,
    TradeId,
    Venue,
    VenueOrderId,
)

from nautilus_ctrader.instruments import CTraderInstrumentProvider, units
from nautilus_ctrader.openapi import (
    ACCOUNT_DISCONNECT,
    AMEND_ORDER,
    CANCEL_ORDER,
    CLIENT_DISCONNECT,
    DEAL_LIST,
    EXECUTION_EVENT,
    NEW_ORDER,
    ORDER_DETAILS,
    ORDER_ERROR_EVENT,
    ORDER_LIST,
    RECONCILE,
    TOKEN_INVALIDATED,
    TRADER,
    TRADER_UPDATE,
    OpenApiError,
    OpenApiSession,
)

ACCEPTED, FILLED, REPLACED, CANCELLED, EXPIRED, REJECTED, CANCEL_REJECTED = 2, 3, 4, 5, 6, 7, 8
SWAP, DEPOSIT_WITHDRAW, PARTIAL_FILL = 9, 10, 11
BUY, SELL, LIMIT = 1, 2, 2
GTD, GTC = 1, 2
POSITION_OPEN, POSITION_CLOSED, POSITION_CREATED = 1, 2, 3
DEAL_FILLED, DEAL_PARTIAL = 2, 3
ORDER_WORKING, ORDER_FILLED, ORDER_REJECTED, ORDER_EXPIRED, ORDER_CANCELLED = 1, 2, 3, 4, 5
SETTLE_AFTER_S = 5                  # an order request without an answer is settled this much later
HISTORY_MARGIN_MS = 60_000          # the order history is read from this much before the oldest order
TAKER_WITHIN_MS = 1_000
FILL_LOOKBACK_NS = 24 * 3600 * 1_000_000_000
DEAL_PAGE = 1_000
MS = 1_000_000

__all__ = ["CTraderExecClientConfig", "CTraderExecutionClient", "CTraderExecutionClientFactory", "catch_up",
           "fill_report", "order_payload", "order_status_report", "position_reports", "post_only_refusal",
           "volume_refusal"]


class CTraderExecClientConfig(ExecutionClientConfig):
    """``account_currency``: the deposit currency (the Fusion demo's is EUR), checked at connect. A
    cTrader account holds one currency, and the Nautilus account must be told it: a multi-currency
    account prices EUR/USD's margin in USD, finds no USD and denies the order. ``label``: put on
    every order and so on the positions the orders open. Without an instrument provider config
    every FX pair is loaded, so the reports can name any position the account holds."""

    def __init__(self, *, account_currency: str, credentials_path: str | Path, venue: str = "CTRADER",
                 allow_live: bool = False, label: str = "nautilus", **base):
        self.credentials_path = str(credentials_path)
        self.venue = venue
        self.allow_live = allow_live
        self.account_currency = account_currency
        self.label = label


# ── pure pieces ──────────────────────────────────────────────────────────


def side_code(side) -> int:
    return BUY if side == OrderSide.BUY else SELL


def order_side(code) -> OrderSide:
    return OrderSide.BUY if int(code) == BUY else OrderSide.SELL


def ns(ms) -> int:
    return int(ms or 0) * MS


def volume_refusal(qty: int, inst) -> str | None:
    """Why the venue would refuse this quantity, or None."""
    step, low, high = int(inst.size_increment), int(inst.min_quantity), int(inst.max_quantity)
    if qty < low:
        return f"{qty} units is below the minimum of {low}"
    if qty % step:
        return f"{qty} units is not a multiple of the step of {step}"
    if qty > high:
        return f"{qty} units is above the maximum of {high}"
    return None


def post_only_refusal(side, price: float, quote) -> str | None:
    """Why a post-only order would take liquidity against ``quote``, or None."""
    if quote is None:
        return "no quote in the cache to check a post-only order against"
    if side == OrderSide.BUY and price >= float(quote.ask_price):
        return f"post-only buy at {price} would take the ask {quote.ask_price}"
    if side == OrderSide.SELL and price <= float(quote.bid_price):
        return f"post-only sell at {price} would take the bid {quote.bid_price}"
    return None


def order_payload(*, symbol_id: int, side, qty: int, price: float, digits: int, time_in_force, expire_ns: int | None,
                  client_order_id: str, label: str, position_id: int | None) -> dict:
    payload = {"symbolId": symbol_id, "orderType": LIMIT, "tradeSide": side_code(side), "volume": qty * 100,
               "limitPrice": round(price, digits), "timeInForce": GTD if time_in_force == TimeInForce.GTD else GTC,
               "label": label, "clientOrderId": client_order_id}
    if time_in_force == TimeInForce.GTD:
        payload["expirationTimestamp"] = int(expire_ns) // MS
    if position_id is not None:
        payload["positionId"] = position_id
    return payload


def order_status_report(o: dict, instrument_id: InstrumentId, account_id: AccountId, digits: int,
                        ts_init: int) -> OrderStatusReport:
    """An open cTrader order (from RECONCILE) as Nautilus sees it."""
    td = o.get("tradeData") or {}
    filled = units(o.get("executedVolume", 0))
    return OrderStatusReport(
        account_id=account_id, instrument_id=instrument_id, venue_order_id=VenueOrderId(str(o["orderId"])),
        order_side=order_side(td["tradeSide"]),
        order_type=OrderType.LIMIT if int(o.get("orderType", LIMIT)) == LIMIT else OrderType.MARKET,
        time_in_force=TimeInForce.GTD if int(o.get("timeInForce", GTC)) == GTD else TimeInForce.GTC,
        order_status=OrderStatus.PARTIALLY_FILLED if filled else OrderStatus.ACCEPTED,
        quantity=Quantity(units(td["volume"]), 0), filled_qty=Quantity(filled, 0),
        ts_accepted=ns(td.get("openTimestamp")), ts_last=ns(o.get("utcLastUpdateTimestamp") or td.get("openTimestamp")),
        ts_init=ts_init,
        client_order_id=ClientOrderId(o["clientOrderId"]) if o.get("clientOrderId") else None,
        price=Price(float(o["limitPrice"]), digits) if o.get("limitPrice") else None,
        expire_time=ns(o["expirationTimestamp"]) if o.get("expirationTimestamp") else None,
    )


def liquidity(deal: dict, opened_ms) -> LiquiditySide:
    executed = int(deal.get("executionTimestamp") or 0)
    return LiquiditySide.TAKER if opened_ms and executed - int(opened_ms) < TAKER_WITHIN_MS else LiquiditySide.MAKER


def commission(deal: dict, currency: Currency, inst) -> Money:
    """cTrader charges the commission in the deposit currency, negative for a cost; Nautilus's is
    positive. When the deposit currency is the pair's base currency (EUR/USD on a EUR account) it is
    handed on in the quote currency at the fill's price: Nautilus takes a base-currency commission
    of a ``CurrencyPair`` out of the position, as from a spot purchase (EUR/USD LONG 999.98 after
    buying 1,000 at 0.02 EUR), while a margin account pays it from the balance."""
    cost = -int(deal.get("commission", 0)) / 10 ** int(deal.get("moneyDigits", 2))
    if currency == inst.base_currency:
        return Money(cost * float(deal["executionPrice"]), inst.quote_currency)
    return Money(cost, currency)


def fill_report(deal: dict, inst, account_id: AccountId, currency: Currency, ts_init: int,
                client_order_id: ClientOrderId | None = None) -> FillReport:
    return FillReport(
        account_id=account_id, instrument_id=inst.id, venue_order_id=VenueOrderId(str(deal["orderId"])),
        trade_id=TradeId(str(deal["dealId"])), order_side=order_side(deal["tradeSide"]),
        last_qty=Quantity(units(deal["filledVolume"]), 0),
        last_px=Price(float(deal["executionPrice"]), inst.price_precision),
        commission=commission(deal, currency, inst), liquidity_side=liquidity(deal, deal.get("createTimestamp")),
        ts_event=ns(deal.get("executionTimestamp")), ts_init=ts_init, client_order_id=client_order_id,
    )


def position_reports(positions: list[dict], instrument_ids: dict[int, InstrumentId], account_id: AccountId,
                     ts_init: int) -> list[PositionStatusReport]:
    """Open cTrader positions netted per instrument (the client keeps one each anyway)."""
    net: dict[InstrumentId, list] = {}
    for p in positions:
        td = p.get("tradeData") or {}
        iid = instrument_ids.get(int(td.get("symbolId", -1)))
        if iid is None or int(p.get("positionStatus", POSITION_OPEN)) != POSITION_OPEN:
            continue
        signed = units(td.get("volume", 0)) * (1 if int(td.get("tradeSide", BUY)) == BUY else -1)
        acc = net.setdefault(iid, [0, 0.0, 0])
        acc[0] += signed
        acc[1] += abs(signed) * float(p.get("price") or 0)
        acc[2] = max(acc[2], int(p.get("utcLastUpdateTimestamp") or td.get("openTimestamp") or 0))
    out = []
    for iid, (signed, notional, last_ms) in net.items():
        side = PositionSide.FLAT if signed == 0 else PositionSide.LONG if signed > 0 else PositionSide.SHORT
        out.append(PositionStatusReport(
            account_id=account_id, instrument_id=iid, position_side=side, quantity=Quantity(abs(signed), 0),
            ts_last=ns(last_ms), ts_init=ts_init,
            avg_px_open=None if signed == 0 else notional / abs(signed)))
    return out


def catch_up(accepted: bool, price: float, qty: int, venue_order: dict, deals: list[dict],
             filled_deals: set[int]) -> list[tuple]:
    """What Nautilus missed of one order, in the order to tell it: ``("accepted",)``,
    ``("filled", deal)`` for each deal not told yet, ``("updated", price, qty)`` when the working
    order differs from Nautilus's, then ``("canceled",)``, ``("expired",)`` or ``("rejected",)``.
    ``venue_order`` and ``deals`` as ``ORDER_DETAILS`` answers them."""
    status = int(venue_order.get("orderStatus", 0))
    if status == ORDER_REJECTED:
        return [] if accepted else [("rejected",)]
    acts = [] if accepted else [("accepted",)]
    for d in sorted(deals, key=lambda d: int(d.get("executionTimestamp") or 0)):
        if int(d.get("dealStatus", 0)) in (DEAL_FILLED, DEAL_PARTIAL) and int(d["dealId"]) not in filled_deals:
            acts.append(("filled", d))
    if status == ORDER_WORKING:
        venue_price = float(venue_order.get("limitPrice") or price)
        venue_qty = units((venue_order.get("tradeData") or {}).get("volume", qty * 100))
        if abs(venue_price - price) > 1e-9 or venue_qty != qty:
            acts.append(("updated", venue_price, venue_qty))
    elif status == ORDER_CANCELLED:
        acts.append(("canceled",))
    elif status == ORDER_EXPIRED:
        acts.append(("expired",))
    return acts


def account_id_for(config: CTraderExecClientConfig) -> AccountId:
    """``<venue>-<login>``, the login read from the credentials file (no network at construction)."""
    login = json.loads(Path(config.credentials_path).read_text())["account"]
    return AccountId(f"{config.venue}-{login}")


# ── the client ───────────────────────────────────────────────────────────


class CTraderExecutionClient(ExecutionClient):
    def __init__(self, *, name: str, config: CTraderExecClientConfig, cache, clock, trader_id):
        venue = Venue(config.venue)
        self.session = OpenApiSession(config.credentials_path, allow_live=config.allow_live, on_event=self._on_event,
                                      on_close=self._on_close, on_reconnect=self._restore,
                                      tap_answers=frozenset({EXECUTION_EVENT}))
        provider_config = config.instrument_provider
        if not provider_config.load_all and not provider_config.load_ids:   # the base default loads nothing
            provider_config = InstrumentProviderConfig(load_all=True)
        super().__init__(name=name, config=config, cache=cache, clock=clock, trader_id=trader_id, venue=venue,
                         account_id=account_id_for(config), account_type=AccountType.MARGIN,
                         oms_type=OmsType.NETTING,
                         base_currency=Currency.from_str(config.account_currency),
                         instrument_provider=CTraderInstrumentProvider(self.session, venue, provider_config))
        self._label = config.label
        self._want_currency = config.account_currency
        self._log = Logger(type(self).__name__)
        self._currency: Currency | None = None
        self._money_digits = 2
        self._balance = 0
        self._positions: dict[int, dict[int, dict]] = {}     # symbolId -> positionId -> open position
        self._opening: dict[int, int] = {}                    # symbolId -> position CREATED by a working order
        self._accepted: set[str] = set()                      # client order ids seen accepted
        self._filled_deals: set[int] = set()                  # deal ids already turned into fills
        self._submitting: set[str] = set()                    # client order ids whose request is under way

    # ── lifecycle ────────────────────────────────────────────────────────

    async def _connect(self) -> None:
        await self.session.connect()
        await self.instrument_provider.initialize()
        for inst in self.instrument_provider.list_all():
            self._handle_instrument(inst)
        await self._refresh_account()
        self._load_book(*await self._reconcile())

    def _load_book(self, positions: list[dict], orders: list[dict]) -> None:
        self._positions.clear()
        self._opening.clear()
        for p in positions:
            self._track_position(p)
        for o in orders:                                       # a working order whose position is not open yet
            sid, pid = int((o.get("tradeData") or {}).get("symbolId", -1)), o.get("positionId")
            if pid and pid not in self._positions.get(sid, {}):
                self._opening[sid] = int(pid)

    async def _disconnect(self) -> None:
        await self.session.close()

    def _on_close(self, exc) -> None:
        self._log.warning(f"cTrader execution connection lost ({exc!r}); reconnecting")

    async def _restore(self) -> None:
        """After a reconnect: the balance, the positions and every order Nautilus holds open."""
        await self._refresh_account()
        positions, orders = await self._reconcile()
        self._load_book(positions, orders)
        settled = await self._settle_orders(orders)
        self._log.warning(f"cTrader execution restored after a reconnect: {len(positions)} open positions, "
                          f"{len(orders)} working orders, {settled} of Nautilus's orders settled")

    async def _settle_later(self, order) -> None:
        await asyncio.sleep(SETTLE_AFTER_S)
        _, orders = await self._reconcile()
        await self._settle_orders(orders, only=order.client_order_id)

    async def _settle_orders(self, working: list[dict], only: ClientOrderId | None = None) -> int:
        """Tell Nautilus what it missed of each order it holds open or in flight (``catch_up``)."""
        mine = {o.client_order_id: o for o in [*self.cache.orders_open(venue=self.venue),
                                               *self.cache.orders_inflight(venue=self.venue)]
                if (only is None or o.client_order_id == only) and str(o.client_order_id) not in self._submitting}
        if not mine:
            return 0
        by_id = {str(o["orderId"]): o for o in working}
        by_coid = {o["clientOrderId"]: o for o in working if o.get("clientOrderId")}
        history = None
        for order in mine.values():
            voi = order.venue_order_id.value if order.venue_order_id is not None else None
            found = by_id.get(voi) or by_coid.get(str(order.client_order_id))
            if found is None:
                if history is None:
                    history = await self._order_history(min(o.ts_init for o in mine.values()) // MS - HISTORY_MARGIN_MS)
                found = history[0].get(voi) or history[1].get(str(order.client_order_id))
            if found is None:
                if voi is None:
                    self.generate_order_rejected(order, "cTrader has no order of this client order id: the request "
                                                        "never reached it", self.clock.timestamp_ns(), False)
                else:
                    self._log.error(f"{order.client_order_id} (venue {voi}) is not in cTrader's order history")
                continue
            _, details = await self.session.account_request(ORDER_DETAILS, {"orderId": int(found["orderId"])})
            venue_order = details.get("order") or found
            acts = catch_up(str(order.client_order_id) in self._accepted, float(order.price), int(order.quantity),
                            venue_order, details.get("deal", []), self._filled_deals)
            self._apply(order, venue_order, acts)
        return len(mine)

    async def _order_history(self, from_ms: int) -> tuple[dict, dict]:
        """cTrader's orders since ``from_ms`` (any status), by order id and by client order id."""
        by_id, by_coid, to = {}, {}, self.clock.timestamp_ns() // MS
        while True:
            _, res = await self.session.account_request(ORDER_LIST, {"fromTimestamp": from_ms, "toTimestamp": to})
            page = [o for o in res.get("order", []) if str(o["orderId"]) not in by_id]
            for o in page:
                by_id[str(o["orderId"])] = o
                if o.get("clientOrderId"):
                    by_coid[o["clientOrderId"]] = o
            if not res.get("hasMore") or not page:
                return by_id, by_coid
            to = min(int(o.get("utcLastUpdateTimestamp") or (o.get("tradeData") or {}).get("openTimestamp") or to)
                     for o in page)

    def _apply(self, order, venue_order: dict, acts: list[tuple]) -> None:
        inst = self.instrument_provider.find(order.instrument_id)
        voi = VenueOrderId(str(venue_order["orderId"]))
        ts = ns(venue_order.get("utcLastUpdateTimestamp")) or self.clock.timestamp_ns()
        for act in acts:
            if act[0] == "accepted":
                self._accepted.add(str(order.client_order_id))
                self.generate_order_accepted(order, voi, ns((venue_order.get("tradeData") or {}).get("openTimestamp")) or ts)
            elif act[0] == "filled":
                self._emit_fill(order, voi, venue_order, act[1], inst)
            elif act[0] == "updated":
                self.generate_order_updated(order, voi, Quantity(act[2], 0), Price(act[1], inst.price_precision),
                                            None, None, ts)
            elif act[0] == "canceled":
                self.generate_order_canceled(order, voi, ts)
            elif act[0] == "expired":
                self.generate_order_expired(order, voi, ts)
            elif act[0] == "rejected":
                self.generate_order_rejected(order, "rejected by cTrader (learned after a reconnect)", ts, False)
        if acts:
            self._log.warning(f"{order.client_order_id}: caught up {[a[0] for a in acts]}")

    # ── account ──────────────────────────────────────────────────────────

    async def _refresh_account(self) -> None:
        _, res = await self.session.account_request(TRADER)
        trader = res["trader"]
        code = self.instrument_provider.asset_names.get(trader.get("depositAssetId"))
        if code is None:
            await self.instrument_provider.load_ids_async([])            # the asset list comes with a load
            code = self.instrument_provider.asset_names.get(trader.get("depositAssetId"))
        if code != self._want_currency:
            raise RuntimeError(f"the account's deposit currency is {code}, the config says {self._want_currency}")
        self._currency = Currency.from_str(code)
        self._money_digits = int(trader.get("moneyDigits", 2))
        self._balance = int(trader.get("balance", 0))
        self._emit_account()

    def _emit_account(self) -> None:
        scale = 10 ** self._money_digits
        used = sum(int(p.get("usedMargin", 0)) for ps in self._positions.values() for p in ps.values())
        cur = self._currency
        self.generate_account_state(
            [AccountBalance(Money(self._balance / scale, cur), Money(used / scale, cur),
                            Money((self._balance - used) / scale, cur))],
            [], True, self.clock.timestamp_ns())

    def _refresh_account_soon(self) -> None:
        self.create_task(self._refresh_account(), "ctrader:account")

    # ── events ───────────────────────────────────────────────────────────

    def _on_event(self, kind: int, body: dict) -> None:
        if kind == EXECUTION_EVENT:
            self._on_execution(body)
        elif kind == TRADER_UPDATE and body.get("trader"):
            self._balance = int(body["trader"].get("balance", self._balance))
            self._emit_account()
        elif kind == ORDER_ERROR_EVENT:
            self._log.warning(f"cTrader order error outside a request: {body}")
        elif kind in (TOKEN_INVALIDATED, CLIENT_DISCONNECT, ACCOUNT_DISCONNECT):
            self._log.error(f"cTrader session event {kind}: {body}")

    def _track_position(self, p: dict) -> None:
        td = p.get("tradeData") or {}
        sid, pid, status = int(td.get("symbolId", -1)), int(p["positionId"]), int(p.get("positionStatus", 0))
        held = self._positions.setdefault(sid, {})
        if status == POSITION_OPEN and int(td.get("volume", 0)) > 0:
            held[pid] = p
            if self._opening.get(sid) == pid:
                del self._opening[sid]
        elif status == POSITION_CLOSED:
            held.pop(pid, None)
            if self._opening.get(sid) == pid:
                del self._opening[sid]
        elif status == POSITION_CREATED and pid not in held:
            self._opening[sid] = pid

    def _order_for(self, o: dict):
        if o.get("clientOrderId"):
            order = self.cache.order(ClientOrderId(o["clientOrderId"]))
            if order is not None:
                return order
        coid = self.cache.client_order_id(VenueOrderId(str(o.get("orderId"))))
        return self.cache.order(coid) if coid is not None else None

    def _on_execution(self, e: dict) -> None:
        etype, o, deal = int(e.get("executionType", 0)), e.get("order") or {}, e.get("deal")
        if e.get("position"):
            self._track_position(e["position"])
        if etype in (SWAP, DEPOSIT_WITHDRAW):
            self._refresh_account_soon()
            return
        order = self._order_for(o) if o else None
        if order is None:
            if o:
                self._log.info(f"cTrader event {etype} for an order this node did not place: {o.get('orderId')}")
            if deal:
                self._refresh_account_soon()
            return
        inst = self.instrument_provider.find(order.instrument_id)
        voi = VenueOrderId(str(o["orderId"]))
        ts = ns(o.get("utcLastUpdateTimestamp")) or self.clock.timestamp_ns()
        coid = str(order.client_order_id)
        if etype in (ACCEPTED, FILLED, PARTIAL_FILL) and coid not in self._accepted:
            self._accepted.add(coid)
            self.generate_order_accepted(order, voi, ns((o.get("tradeData") or {}).get("openTimestamp")) or ts)
        if etype == REPLACED:
            self.generate_order_updated(order, voi, Quantity(units((o.get("tradeData") or {})["volume"]), 0),
                                        Price(float(o["limitPrice"]), inst.price_precision), None, None, ts)
        elif etype == CANCELLED:
            self.generate_order_canceled(order, voi, ts)
        elif etype == EXPIRED:
            self.generate_order_expired(order, voi, ts)
        elif etype == REJECTED:
            self.generate_order_rejected(order, str(e.get("errorCode") or "rejected by cTrader"), ts, False)
        elif etype == CANCEL_REJECTED:
            self.generate_order_cancel_rejected(order, voi, str(e.get("errorCode") or "cancel rejected"), ts)
        elif etype in (FILLED, PARTIAL_FILL) and deal:
            self._emit_fill(order, voi, o, deal, inst)

    def _emit_fill(self, order, voi: VenueOrderId, o: dict, deal: dict, inst) -> None:
        if int(deal["dealId"]) in self._filled_deals:
            return
        self._filled_deals.add(int(deal["dealId"]))
        self.generate_order_filled(
            order, voi, None, TradeId(str(deal["dealId"])), Quantity(units(deal["filledVolume"]), 0),
            Price(float(deal["executionPrice"]), inst.price_precision), inst.quote_currency,
            commission(deal, self._currency, inst), liquidity(deal, (o.get("tradeData") or {}).get("openTimestamp")),
            ns(deal.get("executionTimestamp")) or self.clock.timestamp_ns())
        self._refresh_account_soon()

    # ── commands ─────────────────────────────────────────────────────────

    async def _submit_order(self, command) -> None:
        order = command.order
        inst = self.instrument_provider.find(order.instrument_id)
        if inst is None:
            self.generate_order_denied(order, f"{order.instrument_id} is not a loaded cTrader instrument")
            return
        if order.order_type != OrderType.LIMIT or order.time_in_force not in (TimeInForce.GTC, TimeInForce.GTD):
            self.generate_order_denied(order, "the cTrader adapter sends GTC or GTD LIMIT orders only")
            return
        qty = int(order.quantity)
        if reason := volume_refusal(qty, inst):
            self.generate_order_denied(order, reason)
            return
        sid = self.instrument_provider.symbol_ids[order.instrument_id]
        held = self._positions.get(sid, {})
        if len(held) > 1:
            self.generate_order_denied(order, f"{order.instrument_id} has {len(held)} open positions on the hedged "
                                              "account; this adapter keeps one per instrument")
            return
        if not held and sid in self._opening:
            self.generate_order_denied(order, f"an order opening {order.instrument_id} is still working; a second "
                                              "one would open a second position on the hedged account")
            return
        position = next(iter(held.values()), None)
        if order.is_reduce_only and (position is None or order.side == order_side(position["tradeData"]["tradeSide"])
                                     or qty > units(position["tradeData"]["volume"])):
            self.generate_order_denied(order, "a reduce-only order that would not reduce the position")
            return
        self.generate_order_submitted(order)
        if order.is_post_only and (reason := post_only_refusal(order.side, float(order.price),
                                                               self.cache.quote(order.instrument_id))):
            self.generate_order_rejected(order, reason, self.clock.timestamp_ns(), True)
            return
        payload = order_payload(symbol_id=sid, side=order.side, qty=qty, price=float(order.price),
                                digits=inst.price_precision, time_in_force=order.time_in_force,
                                expire_ns=order.expire_time_ns, client_order_id=str(order.client_order_id),
                                label=self._label, position_id=int(position["positionId"]) if position else None)
        coid = str(order.client_order_id)
        self._submitting.add(coid)
        try:
            kind, body = await self.session.account_request(NEW_ORDER, payload)
        except OpenApiError as e:
            self.generate_order_rejected(order, f"{e.code}: {e.description}", self.clock.timestamp_ns(), False)
            return
        except ConnectionError as e:         # the reconnect's restore settles it
            self._log.warning(f"{coid}: {e}; settled after the reconnect")
            return
        except TimeoutError:
            self._log.error(f"no answer to {coid}; settling it in {SETTLE_AFTER_S} s")
            self.create_task(self._settle_later(order), "ctrader:settle")
            return
        finally:
            self._submitting.discard(coid)
        if kind == ORDER_ERROR_EVENT:
            self.generate_order_rejected(order, f"{body.get('errorCode')}: {body.get('description')}",
                                         self.clock.timestamp_ns(), False)

    async def _modify_order(self, command) -> None:
        order = self.cache.order(command.client_order_id)
        voi = command.venue_order_id or (order.venue_order_id if order is not None else None)
        now = self.clock.timestamp_ns()
        if order is None or voi is None:
            if order is not None:
                self.generate_order_modify_rejected(order, None, "no venue order id yet", now, command.command_id)
            return
        inst = self.instrument_provider.find(order.instrument_id)
        price = float(command.price if command.price is not None else order.price)
        qty = int(command.quantity if command.quantity is not None else order.quantity)
        reason = volume_refusal(qty, inst)
        if reason is None and order.is_post_only:
            reason = post_only_refusal(order.side, price, self.cache.quote(order.instrument_id))
        if reason:
            self.generate_order_modify_rejected(order, voi, reason, now, command.command_id)
            return
        payload = {"orderId": int(voi.value), "limitPrice": round(price, inst.price_precision), "volume": qty * 100}
        try:
            kind, body = await self.session.account_request(AMEND_ORDER, payload)
        except (OpenApiError, ConnectionError, TimeoutError) as e:     # an amend that did land: the restore tells
            self.generate_order_modify_rejected(order, voi, repr(e), self.clock.timestamp_ns(), command.command_id)
            return
        if kind == ORDER_ERROR_EVENT:
            self.generate_order_modify_rejected(order, voi, f"{body.get('errorCode')}: {body.get('description')}",
                                                self.clock.timestamp_ns(), command.command_id)

    async def _cancel_order(self, command) -> None:
        order = self.cache.order(command.client_order_id)
        if order is not None:
            await self._cancel(order, command.venue_order_id or order.venue_order_id, command.command_id)

    async def _cancel_all_orders(self, command) -> None:
        for order in self.cache.orders_open(venue=self.venue, instrument_id=command.instrument_id):
            if command.order_side in (None, OrderSide.NO_ORDER_SIDE) or order.side == command.order_side:
                await self._cancel(order, order.venue_order_id, command.command_id)

    async def _cancel(self, order, voi, causation_id) -> None:
        if voi is None:
            self.generate_order_cancel_rejected(order, None, "no venue order id yet", self.clock.timestamp_ns(),
                                                causation_id)
            return
        try:
            kind, body = await self.session.account_request(CANCEL_ORDER, {"orderId": int(voi.value)})
        except (OpenApiError, ConnectionError, TimeoutError) as e:     # a cancel that did land: the restore tells
            self.generate_order_cancel_rejected(order, voi, repr(e), self.clock.timestamp_ns(), causation_id)
            return
        if kind == ORDER_ERROR_EVENT:
            self.generate_order_cancel_rejected(order, voi, f"{body.get('errorCode')}: {body.get('description')}",
                                                self.clock.timestamp_ns(), causation_id)

    async def _query_account(self, command) -> None:
        await self._refresh_account()

    async def _query_order(self, command) -> None:
        if (report := await self._open_order_report(command.client_order_id, command.venue_order_id)) is not None:
            self._handle_report(report)

    # ── reports ──────────────────────────────────────────────────────────

    async def _reconcile(self) -> tuple[list[dict], list[dict]]:
        _, res = await self.session.account_request(RECONCILE)
        return res.get("position", []), res.get("order", [])

    async def _deals(self, from_ms: int, to_ms: int) -> list[dict]:
        """Filled deals executed in ``[from_ms, to_ms]``, newest first, paged backwards."""
        out, seen, to = [], set(), to_ms
        while True:
            _, res = await self.session.account_request(DEAL_LIST, {"fromTimestamp": from_ms, "toTimestamp": to,
                                                                    "maxRows": DEAL_PAGE})
            page = [d for d in res.get("deal", []) if int(d["dealId"]) not in seen]
            seen |= {int(d["dealId"]) for d in page}
            out += [d for d in page if int(d.get("dealStatus", 0)) in (DEAL_FILLED, DEAL_PARTIAL)]
            if not res.get("hasMore") or not page:
                return out
            to = min(int(d.get("executionTimestamp") or d["createTimestamp"]) for d in page)

    def _instrument(self, symbol_id):
        iid = self.instrument_provider.instrument_ids.get(int(symbol_id))
        if iid is None:
            self._log.warning(f"cTrader symbol {symbol_id} is not loaded; its order, deal or position is not reported")
            return None
        return self.instrument_provider.find(iid)

    def _order_reports(self, orders: list[dict], instrument_id=None) -> list[OrderStatusReport]:
        out, now = [], self.clock.timestamp_ns()
        for o in orders:
            inst = self._instrument((o.get("tradeData") or {}).get("symbolId", -1))
            if inst is not None and instrument_id in (None, inst.id):
                out.append(order_status_report(o, inst.id, self.account_id, inst.price_precision, now))
        return out

    async def _open_order_report(self, client_order_id=None, venue_order_id=None) -> OrderStatusReport | None:
        _, orders = await self._reconcile()
        for r in self._order_reports(orders):
            if (venue_order_id is not None and r.venue_order_id == venue_order_id) or \
                    (client_order_id is not None and r.client_order_id == client_order_id):
                return r
        return None

    async def _fill_reports(self, start_ns=None, end_ns=None, instrument_id=None, venue_order_id=None) -> list[FillReport]:
        now = self.clock.timestamp_ns()
        deals = await self._deals((start_ns or now - FILL_LOOKBACK_NS) // MS, (end_ns or now) // MS)
        out = []
        for d in deals:
            inst = self._instrument(d.get("symbolId", -1))
            if inst is None or instrument_id not in (None, inst.id) or \
                    (venue_order_id is not None and str(d["orderId"]) != venue_order_id.value):
                continue
            out.append(fill_report(d, inst, self.account_id, self._currency, now,
                                   self.cache.client_order_id(VenueOrderId(str(d["orderId"])))))
        return out

    async def _generate_order_status_report(self, command) -> OrderStatusReport | None:
        return await self._open_order_report(command.client_order_id, command.venue_order_id)

    async def _generate_order_status_reports(self, command) -> list[OrderStatusReport]:
        _, orders = await self._reconcile()
        return self._order_reports(orders, command.instrument_id)

    async def _generate_fill_reports(self, command) -> list[FillReport]:
        return await self._fill_reports(command.start, command.end, command.instrument_id, command.venue_order_id)

    async def _generate_position_status_reports(self, command) -> list[PositionStatusReport]:
        positions, _ = await self._reconcile()
        reports = position_reports(positions, self.instrument_provider.instrument_ids, self.account_id,
                                   self.clock.timestamp_ns())
        if command.instrument_id is not None:
            reports = [r for r in reports if r.instrument_id == command.instrument_id] or [PositionStatusReport(
                account_id=self.account_id, instrument_id=command.instrument_id, position_side=PositionSide.FLAT,
                quantity=Quantity(0, 0), ts_last=self.clock.timestamp_ns(), ts_init=self.clock.timestamp_ns())]
        return reports

    async def _generate_mass_status(self, lookback_mins: int | None) -> ExecutionMassStatus:
        now = self.clock.timestamp_ns()
        positions, orders = await self._reconcile()
        status = ExecutionMassStatus(self.client_id, self.account_id, self.venue, now)
        status.add_order_reports(self._order_reports(orders))
        start = now - lookback_mins * 60 * 1_000_000_000 if lookback_mins else None
        status.add_fill_reports(await self._fill_reports(start, now))
        status.add_position_reports(position_reports(positions, self.instrument_provider.instrument_ids,
                                                     self.account_id, now))
        return status


class CTraderExecutionClientFactory(ExecutionClientFactory):
    @staticmethod
    def create(*, name: str, config: CTraderExecClientConfig, cache, clock, trader_id) -> CTraderExecutionClient:
        return CTraderExecutionClient(name=name, config=config, cache=cache, clock=clock, trader_id=trader_id)
