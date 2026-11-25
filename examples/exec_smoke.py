"""Smoke test of the cTrader execution client inside a real ``LiveNode``, on a DEMO account, EUR/USD
at the minimum size (1,000 units). Five steps, each started by the last one's event:

1. a limit buy 20 pips under the bid: accepted, amended 5 pips lower, cancelled;
2. a post-only buy at the ask: refused by the adapter (it would take liquidity);
3. a limit buy at the ask: a long position of 1,000;
4. a limit sell of 2,000 at the bid: the position flipped to a short of 1,000;
5. a limit buy of 1,000 at the ask: flat.

Steps 3-5 trade at the touch, so they fill at once. It trades: run it on a demo account that no
other node trades (the client keeps one position per instrument and would trade that one). The
session refuses a live server.

    python examples/exec_smoke.py --credentials ~/.config/ctrader/demo.json --account-currency EUR
"""
from __future__ import annotations

import argparse
import asyncio
import threading
from dataclasses import dataclass

from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.live import InstrumentProviderConfig, LiveExecutionEngineConfig, LiveNode
from nautilus_trader.model import InstrumentId, OrderSide, Price, Quantity, TimeInForce, TraderId
from nautilus_trader.trading import Strategy, StrategyConfig

from nautilus_ctrader.data import CTraderDataClientConfig, CTraderDataClientFactory
from nautilus_ctrader.execution import CTraderExecClientConfig, CTraderExecutionClientFactory
from nautilus_ctrader.openapi import CANCEL_ORDER, RECONCILE, OpenApiSession

QTY, PIP, LABEL = 1_000, 0.0001, "exec-smoke"
TIMEOUT_S = 120
HANDLE = []


@dataclass
class ExecSmokeConfig(StrategyConfig):
    pair: str = "EUR/USD.CTRADER"


class ExecSmoke(Strategy):
    def __init__(self, config: ExecSmokeConfig):
        super().__init__(config)
        self.step = None
        self.order = None
        self.quote = None
        self.amended = False
        self.passed: list[str] = []

    def on_start(self) -> None:
        self.iid = InstrumentId.from_str(self.config.pair)
        self.subscribe_quotes(self.iid)
        self.clock.set_time_alert_ns("smoke-timeout", self.clock.timestamp_ns() + TIMEOUT_S * 1_000_000_000,
                                     self._timeout)

    def on_quote(self, quote) -> None:
        self.quote = quote
        if self.step is None:
            self._go("passive")

    def _px(self, x: float) -> Price:
        return Price(round(x, 5), 5)

    def _limit(self, side, qty: int, px: float, post_only: bool = False) -> None:
        self.order = self.order_factory.limit(self.iid, side, Quantity.from_int(qty), self._px(px),
                                              time_in_force=TimeInForce.GTC, post_only=post_only)
        self.submit_order(self.order)

    def _go(self, step: str) -> None:
        if self.step is not None:
            self.passed.append(self.step)
        self.step = step
        bid, ask = float(self.quote.bid_price), float(self.quote.ask_price)
        print(f"STEP {step}: bid {bid} ask {ask}, position {self._position()}", flush=True)
        if step == "passive":
            self._limit(OrderSide.BUY, QTY, bid - 20 * PIP)
        elif step == "post_only":
            self._limit(OrderSide.BUY, QTY, ask, post_only=True)
        elif step == "open":
            self._limit(OrderSide.BUY, QTY, ask)
        elif step == "flip":
            self._limit(OrderSide.SELL, 2 * QTY, bid)
        elif step == "close":
            self._limit(OrderSide.BUY, QTY, ask)
        elif step == "done":
            self._finish("all steps passed")

    def _position(self) -> str:
        ps = self.cache.positions_open(instrument_id=self.iid)
        return ", ".join(f"{p.side.name} {p.quantity}" for p in ps) or "flat"

    def _finish(self, why: str) -> None:
        account = self.cache.account_for_venue(self.iid.venue)
        print(f"RESULT {why}; passed {self.passed}; position {self._position()}; "
              f"balances {account.balances() if account else None}", flush=True)
        HANDLE[0].stop()

    def _timeout(self, event) -> None:
        if self.step != "done":
            for o in self.cache.orders_open(instrument_id=self.iid):
                self.cancel_order(o.client_order_id)
            self._finish(f"TIMEOUT in step {self.step}")

    # ── events ───────────────────────────────────────────────────────────

    def on_order_accepted(self, event) -> None:
        print(f"  accepted {event.client_order_id} venue {event.venue_order_id}", flush=True)
        if self.step == "passive" and not self.amended:
            self.amended = True
            self.modify_order(self.order.client_order_id, price=self._px(float(self.order.price) - 5 * PIP))

    def on_order_updated(self, event) -> None:
        print(f"  updated {event.client_order_id} price {event.price} qty {event.quantity}", flush=True)
        if self.step == "passive":
            self.cancel_order(self.order.client_order_id)

    def on_order_canceled(self, event) -> None:
        print(f"  canceled {event.client_order_id}", flush=True)
        if self.step == "passive":
            self._go("post_only")

    def on_order_rejected(self, event) -> None:
        print(f"  rejected {event.client_order_id}: {event.reason} (post-only {event.due_post_only})", flush=True)
        if self.step == "post_only" and event.due_post_only:
            self._go("open")
        else:
            self._finish(f"unexpected rejection in step {self.step}")

    def on_order_denied(self, event) -> None:
        print(f"  denied {event.client_order_id}: {event.reason}", flush=True)
        self._finish(f"denied in step {self.step}")

    def on_order_modify_rejected(self, event) -> None:
        print(f"  modify rejected: {event.reason}", flush=True)
        self._finish("modify rejected")

    def on_order_cancel_rejected(self, event) -> None:
        print(f"  cancel rejected: {event.reason}", flush=True)
        self._finish("cancel rejected")

    def on_order_filled(self, event) -> None:
        print(f"  filled {event.client_order_id} {event.order_side.name} {event.last_qty} @ {event.last_px} "
              f"commission {event.commission} {event.liquidity_side.name} trade {event.trade_id}", flush=True)
        if self.order is not None and self.cache.order(self.order.client_order_id).is_closed:
            nxt = {"open": "flip", "flip": "close", "close": "done"}.get(self.step)
            if nxt:
                self._go(nxt)

    def on_stop(self) -> None:
        account = self.cache.account_for_venue(self.iid.venue)
        print(f"AT STOP position {self._position()}; balances {account.balances() if account else None}", flush=True)

    def on_position_opened(self, event) -> None:
        print(f"  position opened {event.side.name} {event.quantity} @ {event.avg_px_open}", flush=True)

    def on_position_changed(self, event) -> None:
        print(f"  position changed {event.side.name} {event.quantity}", flush=True)

    def on_position_closed(self, event) -> None:
        print(f"  position closed, realized {event.realized_pnl}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--credentials", required=True, help="the Open API credentials file of a DEMO account")
    ap.add_argument("--account-currency", required=True, help="the account's deposit currency, e.g. EUR")
    ap.add_argument("--venue", default="CTRADER")
    args = ap.parse_args()
    pair = f"EUR/USD.{args.venue}"
    node = (LiveNode.builder("ctrader-exec-smoke", TraderId("SMOKE-002"), Environment.LIVE)
            .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
            .with_exec_engine_config(LiveExecutionEngineConfig(reconciliation=True))
            .add_data_client(args.venue, CTraderDataClientFactory(), CTraderDataClientConfig(
                credentials_path=args.credentials, venue=args.venue,
                instrument_provider=InstrumentProviderConfig(load_ids=[pair])))
            .add_exec_client(args.venue, CTraderExecutionClientFactory(), CTraderExecClientConfig(
                credentials_path=args.credentials, venue=args.venue, account_currency=args.account_currency,
                label=LABEL))
            .build())
    HANDLE.append(node.handle())
    node.add_strategy(ExecSmoke(ExecSmokeConfig(pair=pair)))
    timer = threading.Timer(TIMEOUT_S + 30, HANDLE[0].stop)      # the last resort if the strategy hangs
    timer.daemon = True
    timer.start()
    node.run()
    asyncio.run(leftovers(args.credentials))


async def leftovers(credentials: str) -> None:
    """The safety net: cancel any order of this smoke still on the account, report its positions."""
    s = OpenApiSession(credentials, backoff_s=())
    await s.connect()
    _, rec = await s.account_request(RECONCILE)
    for o in rec.get("order", []):
        if (o.get("tradeData") or {}).get("label") == LABEL:
            await s.account_request(CANCEL_ORDER, {"orderId": o["orderId"]})
            print(f"LEFTOVER order {o['orderId']} cancelled")
    held = [(p["positionId"], p["tradeData"]["tradeSide"], p["tradeData"]["volume"]) for p in rec.get("position", [])
            if (p.get("tradeData") or {}).get("label") == LABEL]
    print(f"LEFTOVER positions of this smoke: {held or 'none'}")
    await s.close()


if __name__ == "__main__":
    main()
