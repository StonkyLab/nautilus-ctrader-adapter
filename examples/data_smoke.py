"""Smoke test of the cTrader data client inside a real ``LiveNode``: instruments at connect, quotes
for a while, historical BID bars, live one-minute bars. Reads only; sends no order.

    python examples/data_smoke.py --credentials ~/.config/ctrader/demo.json [--venue CTRADER] [--seconds 30]
"""
from __future__ import annotations

import argparse
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta

from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.live import InstrumentProviderConfig, LiveNode
from nautilus_trader.model import BarType, InstrumentId, TraderId
from nautilus_trader.trading import Strategy, StrategyConfig

from nautilus_ctrader.data import CTraderDataClientConfig, CTraderDataClientFactory

PAIRS = ["EUR/USD", "USD/JPY", "USD/CHF"]


@dataclass
class SmokeConfig(StrategyConfig):
    venue: str = "CTRADER"


class Smoke(Strategy):
    def __init__(self, config: SmokeConfig):
        super().__init__(config)
        self.quotes = Counter()
        self.live_bars = []
        self.bars: dict[str, list] = {}

    def on_start(self) -> None:
        venue = self.config.venue
        for pair in PAIRS:
            iid = InstrumentId.from_str(f"{pair}.{venue}")
            print(f"instrument {iid}: {self.cache.instrument(iid)}")
            self.subscribe_quotes(iid)
        self.subscribe_bars(BarType.from_str(f"EUR/USD.{venue}-1-MINUTE-BID-EXTERNAL"))   # live, one a minute
        self.request_bars(BarType.from_str(f"EUR/USD.{venue}-1-DAY-BID-EXTERNAL"),
                          start=self.clock.utc_now() - timedelta(days=14))
        self.request_bars(BarType.from_str(f"USD/JPY.{venue}-1-HOUR-BID-EXTERNAL"), limit=24)

    # Nautilus 2.0 names: on_quote, and requested bars as one on_historical_bars call
    def on_quote(self, quote) -> None:
        self.quotes[str(quote.instrument_id)] += 1
        if self.quotes[str(quote.instrument_id)] <= 2:
            print(f"quote {quote}")

    def on_bar(self, bar) -> None:
        self.live_bars.append(bar)
        print(f"live bar {bar} (closed {bar.ts_init})", flush=True)

    def on_historical_bars(self, bars) -> None:
        for bar in bars:
            self.bars.setdefault(str(bar.bar_type), []).append(bar)

    def on_stop(self) -> None:
        print("quotes received:", dict(self.quotes))
        for k, v in self.bars.items():
            print(f"bars {k}: {len(v)}, first {v[0]}, last {v[-1]}")
        if not self.bars:
            print("no bars arrived")
        print(f"live one-minute bars: {len(self.live_bars)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--credentials", required=True, help="the Open API credentials file")
    ap.add_argument("--venue", default="CTRADER")
    ap.add_argument("--seconds", type=int, default=30)
    args = ap.parse_args()
    data_cfg = CTraderDataClientConfig(credentials_path=args.credentials, venue=args.venue,
                                       instrument_provider=InstrumentProviderConfig(
                                           load_ids=[f"{p}.{args.venue}" for p in PAIRS]))
    node = (LiveNode.builder("ctrader-smoke", TraderId("SMOKE-001"), Environment.LIVE)
            .with_logging(LoggerConfig(stdout_level=LogLevel.INFO))
            .add_data_client(args.venue, CTraderDataClientFactory(), data_cfg)
            .build())
    node.add_strategy(Smoke(SmokeConfig(venue=args.venue)))
    timer = threading.Timer(args.seconds, node.handle().stop)
    timer.daemon = True
    timer.start()
    node.run()


if __name__ == "__main__":
    main()
