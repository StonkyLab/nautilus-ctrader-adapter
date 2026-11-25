"""Reconnect smoke of the cTrader execution client inside a real ``LiveNode``, on a DEMO account,
EUR/USD at 1,000 units. A passive limit buy goes in and is accepted; then the node's own connection
is dropped and, while it is down, the order is filled through a second connection (amended to the
ask). After the reconnect the client must catch the fill up from cTrader's order history; the
strategy then closes the position and the account ends flat. It trades: run it on a demo account
that no other node trades.

    python examples/reconnect_smoke.py --credentials ~/.config/ctrader/demo.json --account-currency EUR
"""
from __future__ import annotations

import argparse
import asyncio
import threading
from dataclasses import dataclass

from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.live import InstrumentProviderConfig, LiveExecutionEngineConfig, LiveNode
from nautilus_trader.live.clients import ExecutionClientFactory
from nautilus_trader.model import InstrumentId, OrderSide, Price, Quantity, TimeInForce, TraderId
from nautilus_trader.trading import Strategy, StrategyConfig

from nautilus_ctrader.data import CTraderDataClientConfig, CTraderDataClientFactory
from nautilus_ctrader.execution import ACCEPTED, CTraderExecClientConfig, CTraderExecutionClient
from nautilus_ctrader.openapi import AMEND_ORDER, CANCEL_ORDER, RECONCILE, OpenApiSession

QTY, PIP, LABEL = 1_000, 0.0001, "reconnect-smoke"
TIMEOUT_S = 90
HANDLE = []


class DroppingClient(CTraderExecutionClient):
    """Drops its own connection once the first order is accepted and fills that order meanwhile
    through a second session."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session.backoff_s = (3, 5, 10)        # time enough to fill the order while the node is away
        self.dropped = False
        self.other = None

    async def _connect(self) -> None:
        await super()._connect()
        self.other = OpenApiSession(self.session.credentials_path, backoff_s=())
        await self.other.connect()

    async def _disconnect(self) -> None:
        await self.other.close()
        await super()._disconnect()

    def _on_execution(self, e: dict) -> None:
        super()._on_execution(e)
        o = e.get("order") or {}
        if not self.dropped and int(e.get("executionType", 0)) == ACCEPTED and o.get("clientOrderId", "").startswith("O-"):
            self.dropped = True
            self.create_task(self._drop_and_fill(int(o["orderId"])), "smoke:drop")

    async def _drop_and_fill(self, order_id: int) -> None:
        await self.session._ws.close()
        ask = float(self.cache.quote(InstrumentId.from_str(f"EUR/USD.{self.venue}")).ask_price)
        kind, body = await self.other.account_request(AMEND_ORDER, {"orderId": order_id, "limitPrice": ask,
                                                                    "volume": QTY * 100})
        print(f"HARNESS dropped the node's connection, then amended order {order_id} to the ask {ask} through "
              f"another one: answer {kind}, execution type {body.get('executionType')}", flush=True)

    async def _restore(self) -> None:
        print(f"HARNESS reconnected ({self.session.reconnects}); restoring", flush=True)
        await super()._restore()


class DroppingFactory(ExecutionClientFactory):
    @staticmethod
    def create(*, name, config, cache, clock, trader_id):
        return DroppingClient(name=name, config=config, cache=cache, clock=clock, trader_id=trader_id)


@dataclass
class ReconnectSmokeConfig(StrategyConfig):
    pair: str = "EUR/USD.CTRADER"


class ReconnectSmoke(Strategy):
    def __init__(self, config: ReconnectSmokeConfig):
        super().__init__(config)
        self.entry = None
        self.exit = None
        self.quote = None

    def on_start(self) -> None:
        self.iid = InstrumentId.from_str(self.config.pair)
        self.subscribe_quotes(self.iid)
        self.clock.set_time_alert_ns("timeout", self.clock.timestamp_ns() + TIMEOUT_S * 1_000_000_000,
                                     lambda e: self._finish(f"TIMEOUT, entry {self.entry and self.entry.status}"))

    def on_quote(self, quote) -> None:
        self.quote = quote
        if self.entry is None:
            px = Price(round(float(quote.bid_price) - 20 * PIP, 5), 5)
            self.entry = self.order_factory.limit(self.iid, OrderSide.BUY, Quantity.from_int(QTY), px,
                                                  time_in_force=TimeInForce.GTC)
            self.submit_order(self.entry)
            print(f"STRATEGY entry: buy {QTY} @ {px}, 20 pips under the bid", flush=True)

    def on_order_accepted(self, event) -> None:
        print(f"STRATEGY accepted {event.client_order_id}", flush=True)

    def on_order_filled(self, event) -> None:
        print(f"STRATEGY filled {event.client_order_id} {event.order_side.name} {event.last_qty} @ {event.last_px} "
              f"(trade {event.trade_id}); position {self._position()}", flush=True)
        if event.client_order_id == self.entry.client_order_id and self.exit is None:
            self.exit = self.order_factory.limit(self.iid, OrderSide.SELL, Quantity.from_int(QTY),
                                                 self.quote.bid_price, time_in_force=TimeInForce.GTC)
            self.submit_order(self.exit)
        elif self.exit is not None and event.client_order_id == self.exit.client_order_id:
            self._finish("PASSED: the fill missed while away was caught up and the position closed")

    def on_order_rejected(self, event) -> None:
        self._finish(f"rejected {event.client_order_id}: {event.reason}")

    def on_order_denied(self, event) -> None:
        self._finish(f"denied {event.client_order_id}: {event.reason}")

    def _position(self) -> str:
        return ", ".join(f"{p.side.name} {p.quantity}" for p in self.cache.positions_open(instrument_id=self.iid)) or "flat"

    def _finish(self, why: str) -> None:
        account = self.cache.account_for_venue(self.iid.venue)
        print(f"RESULT {why}; position {self._position()}; balances {account.balances() if account else None}",
              flush=True)
        HANDLE[0].stop()


async def leftovers(credentials: str) -> None:
    """The safety net: cancel this smoke's orders still on the account, report its positions."""
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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--credentials", required=True, help="the Open API credentials file of a DEMO account")
    ap.add_argument("--account-currency", required=True, help="the account's deposit currency, e.g. EUR")
    ap.add_argument("--venue", default="CTRADER")
    args = ap.parse_args()
    pair = f"EUR/USD.{args.venue}"
    node = (LiveNode.builder("ctrader-reconnect-smoke", TraderId("SMOKE-003"), Environment.LIVE)
            .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
            .with_exec_engine_config(LiveExecutionEngineConfig(reconciliation=True))
            .add_data_client(args.venue, CTraderDataClientFactory(), CTraderDataClientConfig(
                credentials_path=args.credentials, venue=args.venue,
                instrument_provider=InstrumentProviderConfig(load_ids=[pair])))
            .add_exec_client(args.venue, DroppingFactory(), CTraderExecClientConfig(
                credentials_path=args.credentials, venue=args.venue, account_currency=args.account_currency,
                label=LABEL))
            .build())
    HANDLE.append(node.handle())
    node.add_strategy(ReconnectSmoke(ReconnectSmokeConfig(pair=pair)))
    timer = threading.Timer(TIMEOUT_S + 30, HANDLE[0].stop)
    timer.daemon = True
    timer.start()
    node.run()
    asyncio.run(leftovers(args.credentials))


if __name__ == "__main__":
    main()
