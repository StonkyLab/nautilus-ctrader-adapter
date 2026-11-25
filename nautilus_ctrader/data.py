"""cTrader market data for Nautilus: live quotes and historical bars.

Quotes: a spot subscription sends ``SPOT_EVENT`` with the bid, the ask or both (prices x 10^5,
whatever the digits; the timestamp only when asked for). A ``QuoteTick`` goes out once both sides
are known, carrying the last of each. cTrader sends no sizes with a spot, so both are 0. A quote with
bid = ask is ordinary at Fusion: its raw account quotes EURUSD at a zero spread in every minute
of 13:00-20:00 UTC (stored ticks 2026-11-17 to 19, and the live spots of 2026-11-24).

Bars: ``GET_TRENDBARS`` answers BID bars, oldest first, at most ~14,000 per answer; with more in
the window it answers the newest and ``hasMore``, so a window is paged backwards. A bar comes as
its low and deltas to open, high and close, and the minute it opens. Daily bars open at 17:00 New
York (the symbols' schedule zone). The bar in progress is never answered. A bar's ``ts_init`` is
its close and ``ts_event`` the close minus the bar's nominal length, so ``ts_event + length`` is
the close on a day New York changes its clock too. Only ``BID-EXTERNAL`` bar types are served:
that is what cTrader has.

Live bars: a subscription reads each bar back from ``GET_TRENDBARS`` once it has closed (1.5 s
after its close, again at 5, 15 and 30 s for hours and days, 2 and 5 s for minutes, while the
closed bar is not there yet), and asks for everything since the last bar it handed on, so a bar
missed while the connection was down follows at the next close. Streamed: minutes dividing the
hour and hours on UTC multiples, days closing at 17:00 New York. A subscription whose ``params``
carry ``since_ns`` (the close of the subscriber's last bar) first hands on at once every bar
closed after it: a strategy warmed up on history misses none closed while the node was starting.

``history`` reads the closed bars after a given close from a session of its own, before a node
runs: what a strategy warmed up on another data source needs between that source's end and now.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from nautilus_trader.common import Logger
from nautilus_trader.live import (
    BarsResponse,
    DataClientConfig,
    InstrumentProviderConfig,
)
from nautilus_trader.live.clients import DataClientFactory, MarketDataClient
from nautilus_trader.model import (
    AggregationSource,
    Bar,
    BarAggregation,
    InstrumentId,
    Price,
    PriceType,
    Quantity,
    QuoteTick,
    Venue,
)

from nautilus_ctrader.instruments import CTraderInstrumentProvider
from nautilus_ctrader.openapi import (
    GET_TRENDBARS,
    SPOT_EVENT,
    SUBSCRIBE_SPOTS,
    UNSUBSCRIBE_SPOTS,
    OpenApiError,
    OpenApiSession,
)

NY = ZoneInfo("America/New_York")
PRICE_SCALE = 100_000
MS, MINUTE_NS = 1_000_000, 60_000_000_000
# (step, aggregation) -> ProtoOATrendbarPeriod
PERIODS = {
    (1, BarAggregation.MINUTE): 1, (2, BarAggregation.MINUTE): 2, (3, BarAggregation.MINUTE): 3,
    (4, BarAggregation.MINUTE): 4, (5, BarAggregation.MINUTE): 5, (10, BarAggregation.MINUTE): 6,
    (15, BarAggregation.MINUTE): 7, (30, BarAggregation.MINUTE): 8, (1, BarAggregation.HOUR): 9,
    (4, BarAggregation.HOUR): 10, (12, BarAggregation.HOUR): 11, (1, BarAggregation.DAY): 12,
    (1, BarAggregation.WEEK): 13,
}
NOMINAL_NS = {BarAggregation.MINUTE: MINUTE_NS, BarAggregation.HOUR: 60 * MINUTE_NS,
              BarAggregation.DAY: 1440 * MINUTE_NS, BarAggregation.WEEK: 7 * 1440 * MINUTE_NS}

STREAMED = {(s, BarAggregation.MINUTE) for s in (1, 2, 3, 4, 5, 10, 15, 30)} | {(1, BarAggregation.HOUR),
                                                                                (1, BarAggregation.DAY)}
FETCH_AFTER_S = 1.5
RETRY_S = {BarAggregation.MINUTE: (2, 5), BarAggregation.HOUR: (5, 15, 30), BarAggregation.DAY: (5, 15, 30)}

__all__ = ["CTraderDataClient", "CTraderDataClientConfig", "CTraderDataClientFactory", "SpotBook",
           "bar_close_ns", "fetch_trendbars", "history", "next_close_ns", "trendbar_bars"]


class CTraderDataClientConfig(DataClientConfig):
    """``credentials_path``: the Open API credentials file; ``venue``: the Nautilus venue of the
    account's instruments; ``allow_live``: refuse a live server unless set."""

    def __init__(self, *, credentials_path: str | Path, venue: str = "CTRADER", allow_live: bool = False, **base):
        self.credentials_path = str(credentials_path)
        self.venue = venue
        self.allow_live = allow_live


class SpotBook:
    """The last bid and ask per symbol; a spot event that leaves both known yields them."""

    def __init__(self):
        self._sides: dict[int, list] = {}

    def update(self, spot: dict) -> tuple[int, int] | None:
        sides = self._sides.setdefault(int(spot["symbolId"]), [None, None])
        if spot.get("bid"):
            sides[0] = int(spot["bid"])
        if spot.get("ask"):
            sides[1] = int(spot["ask"])
        return (sides[0], sides[1]) if None not in sides else None

    def forget(self, symbol_id: int) -> None:
        self._sides.pop(symbol_id, None)


def bar_close_ns(open_ns: int, step: int, aggregation) -> int:
    """A bar's close: a fixed length for minutes and hours, New York calendar days for days and
    weeks (a daily bar across a clock change lasts 23 or 25 hours)."""
    if aggregation in (BarAggregation.MINUTE, BarAggregation.HOUR):
        return open_ns + step * NOMINAL_NS[aggregation]
    days = step * (7 if aggregation == BarAggregation.WEEK else 1)
    opened = datetime.fromtimestamp(open_ns / 1e9, tz=NY)
    local = datetime(opened.year, opened.month, opened.day, opened.hour, opened.minute, tzinfo=NY) + timedelta(days=days)
    return int(local.timestamp()) * 1_000_000_000


def next_close_ns(now_ns: int, step: int, aggregation) -> int:
    """The first close strictly after ``now_ns`` of a streamed bar: minutes and hours on UTC
    multiples of their length, days at 17:00 New York."""
    if (step, aggregation) not in STREAMED:
        raise ValueError(f"no live stream of {step}-{aggregation.name} bars")
    if aggregation == BarAggregation.DAY:
        now = datetime.fromtimestamp(now_ns / 1e9, tz=NY)
        close = datetime(now.year, now.month, now.day, 17, tzinfo=NY)
        if int(close.timestamp()) * 1_000_000_000 <= now_ns:
            nxt = now.date() + timedelta(days=1)
            close = datetime(nxt.year, nxt.month, nxt.day, 17, tzinfo=NY)
        return int(close.timestamp()) * 1_000_000_000
    length = step * NOMINAL_NS[aggregation]
    return (now_ns // length + 1) * length


def trendbar_bars(trendbars: list[dict], bar_type, price_precision: int, now_ns: int) -> list[Bar]:
    """cTrader trendbars as Nautilus bars (``ts_init`` the close), the one still open dropped."""
    spec = bar_type.spec
    nominal = spec.step * NOMINAL_NS[spec.aggregation]
    out = []
    for tb in trendbars:
        open_ns = int(tb["utcTimestampInMinutes"]) * MINUTE_NS
        close_ns = bar_close_ns(open_ns, spec.step, spec.aggregation)
        if close_ns > now_ns:
            continue
        low = int(tb["low"])

        def px(v):
            return Price(v / PRICE_SCALE, price_precision)

        out.append(Bar(bar_type, px(low + int(tb.get("deltaOpen", 0))), px(low + int(tb.get("deltaHigh", 0))), px(low),
                       px(low + int(tb.get("deltaClose", 0))), Quantity(int(tb.get("volume", 0)), 0),
                       close_ns - nominal, close_ns))
    return out


async def fetch_trendbars(session: OpenApiSession, symbol_id: int, period: int, from_ms: int, to_ms: int) -> list[dict]:
    """Every trendbar opening in ``[from_ms, to_ms]``, oldest first, paged backwards."""
    pages, to = [], to_ms
    while True:
        _, res = await session.account_request(GET_TRENDBARS, {"symbolId": symbol_id, "period": period,
                                                               "fromTimestamp": from_ms, "toTimestamp": to})
        page = res.get("trendbar", [])
        pages.append([tb for tb in page if int(tb["utcTimestampInMinutes"]) * 60_000 < to] if pages else page)
        if not res.get("hasMore") or not page:
            break
        oldest = int(page[0]["utcTimestampInMinutes"]) * 60_000
        if oldest >= to:
            raise RuntimeError(f"cTrader trendbars do not page back from {to}")
        to = oldest
    return [tb for page in reversed(pages) for tb in page]


async def history(after_ns: dict, credentials_path: str | Path, venue: str = "CTRADER",
                  allow_live: bool = False) -> list[Bar]:
    """The closed bars of each bar type in ``after_ns`` (bar type -> a close) that closed after it."""
    session = OpenApiSession(credentials_path, allow_live=allow_live)
    await session.connect()
    try:
        provider = CTraderInstrumentProvider(session, Venue(venue), InstrumentProviderConfig(
            load_ids=sorted({str(bt.instrument_id) for bt in after_ns})))
        await provider.initialize()
        out = []
        for bar_type, after in after_ns.items():
            spec = bar_type.spec
            nominal = spec.step * NOMINAL_NS[spec.aggregation]
            sid = await provider.symbol_id(bar_type.instrument_id)
            now = time.time_ns()
            raw = await fetch_trendbars(session, sid, PERIODS[(spec.step, spec.aggregation)], (after - nominal) // MS,
                                        now // MS)
            digits = provider.find(bar_type.instrument_id).price_precision
            out += [b for b in trendbar_bars(raw, bar_type, digits, now) if b.ts_init > after]
        return out
    finally:
        await session.close()


class CTraderDataClient(MarketDataClient):
    def __init__(self, *, name: str, config: CTraderDataClientConfig, cache, clock):
        venue = Venue(config.venue)
        self.session = OpenApiSession(config.credentials_path, allow_live=config.allow_live,
                                      on_event=self._on_event, on_close=self._on_close,
                                      on_reconnect=self._resubscribe)
        provider_config = config.instrument_provider or InstrumentProviderConfig()
        super().__init__(name=name, config=config, cache=cache, clock=clock, venue=venue,
                         instrument_provider=CTraderInstrumentProvider(self.session, venue, provider_config))
        self._spots = SpotBook()
        self._quoted: dict[int, InstrumentId] = {}
        self._streams: dict = {}                              # bar type -> its streaming task
        self._log = Logger(type(self).__name__)

    # ── lifecycle ────────────────────────────────────────────────────────

    async def _connect(self) -> None:
        await self.session.connect()
        await self.instrument_provider.initialize()
        for inst in self.instrument_provider.list_all():
            self._handle_instrument(inst)

    async def _disconnect(self) -> None:
        for task in self._streams.values():
            task.cancel()
        self._streams.clear()
        if self._quoted:
            await self.session.account_request(UNSUBSCRIBE_SPOTS, {"symbolId": sorted(self._quoted)})
            self._quoted.clear()
        await self.session.close()

    def _on_close(self, exc) -> None:
        self._log.warning(f"cTrader data connection lost ({exc!r}); reconnecting")

    async def _resubscribe(self) -> None:
        """A new connection knows no subscriptions: the quotes are asked for again."""
        if self._quoted:
            await self.session.account_request(SUBSCRIBE_SPOTS, {"symbolId": sorted(self._quoted),
                                                                 "subscribeToSpotTimestamp": True})
        self._log.warning(f"cTrader data restored after a reconnect: quotes of {len(self._quoted)} symbols")

    # ── quotes ───────────────────────────────────────────────────────────

    async def _subscribe_quotes(self, command) -> None:
        ids = {}
        for instrument_id in command.instrument_ids:
            sid = await self.instrument_provider.symbol_id(instrument_id)
            if sid not in self._quoted:
                ids[sid] = instrument_id
        if ids:
            await self.session.account_request(SUBSCRIBE_SPOTS, {"symbolId": sorted(ids), "subscribeToSpotTimestamp": True})
            self._quoted |= ids

    async def _unsubscribe_quotes(self, command) -> None:
        sids = [sid for sid, i in self._quoted.items() if i in set(command.instrument_ids)]
        if sids:
            await self.session.account_request(UNSUBSCRIBE_SPOTS, {"symbolId": sids})
            for sid in sids:
                self._quoted.pop(sid, None)
                self._spots.forget(sid)

    def _on_event(self, kind: int, body: dict) -> None:
        if kind != SPOT_EVENT:
            return
        sid = int(body["symbolId"])
        instrument_id = self._quoted.get(sid)
        quote = self._spots.update(body)
        if instrument_id is None or quote is None:
            return
        inst = self.instrument_provider.find(instrument_id)
        now = self.clock.timestamp_ns()
        ts = int(body["timestamp"]) * MS if body.get("timestamp") else now
        self._handle_data(QuoteTick(instrument_id, Price(quote[0] / PRICE_SCALE, inst.price_precision),
                                    Price(quote[1] / PRICE_SCALE, inst.price_precision),
                                    Quantity(0, 0), Quantity(0, 0), ts, now))

    # ── bars ─────────────────────────────────────────────────────────────

    def _period(self, bar_type) -> int | None:
        """cTrader's trendbar period for a bar type it serves, else None (logged)."""
        spec = bar_type.spec
        period = PERIODS.get((spec.step, spec.aggregation))
        if period is None or spec.price_type != PriceType.BID or bar_type.aggregation_source != AggregationSource.EXTERNAL:
            self._log.error(f"cTrader serves BID-EXTERNAL bars of {sorted({f'{s}-{a.name}' for s, a in PERIODS})} "
                            f"only, not {bar_type}")
            return None
        return period

    async def _subscribe_bars(self, command) -> None:
        since = (command.params or {}).get("since_ns")
        for bar_type in command.bar_types:
            spec = bar_type.spec
            if bar_type in self._streams or self._period(bar_type) is None:
                continue
            if (spec.step, spec.aggregation) not in STREAMED:
                self._log.error(f"cTrader bars are streamed for {sorted(f'{s}-{a.name}' for s, a in STREAMED)} "
                                f"only, not {bar_type}")
                continue
            self._streams[bar_type] = self.create_task(self._stream_bars(bar_type, since), f"ctrader:bars:{bar_type}")

    async def _unsubscribe_bars(self, command) -> None:
        for bar_type in command.bar_types:
            if (task := self._streams.pop(bar_type, None)) is not None:
                task.cancel()

    async def _stream_bars(self, bar_type, since_ns=None) -> None:
        """Each bar of ``bar_type`` once it has closed, and any it missed before; with ``since_ns``,
        at once every bar closed after it."""
        spec = bar_type.spec
        period, nominal = PERIODS[(spec.step, spec.aggregation)], spec.step * NOMINAL_NS[spec.aggregation]
        sid = await self.instrument_provider.symbol_id(bar_type.instrument_id)
        digits = self.instrument_provider.find(bar_type.instrument_id).price_precision

        async def since(last: int) -> int:
            """Hands on the bars closed after ``last``; the new last, unchanged when the fetch failed."""
            now = self.clock.timestamp_ns()
            try:
                raw = await self._trendbars(sid, period, (last - nominal) // MS, now // MS)
            except (ConnectionError, OpenApiError, TimeoutError) as e:
                self._log.warning(f"cTrader bars {bar_type}: {e!r}")
                return last
            fresh = [b for b in trendbar_bars(raw, bar_type, digits, now) if b.ts_init > last]
            for bar in fresh:
                self._handle_data(bar)
            return fresh[-1].ts_init if fresh else last

        if since_ns is None:
            last = self.clock.timestamp_ns()                  # bars closed before the subscription are history
        else:
            last = await since(int(since_ns))
            self._log.info(f"cTrader bars {bar_type}: caught up from {int(since_ns)} to {last}")
        while True:
            close = next_close_ns(self.clock.timestamp_ns(), spec.step, spec.aggregation)
            await asyncio.sleep(max(0.0, (close - self.clock.timestamp_ns()) / 1e9) + FETCH_AFTER_S)
            for wait in (0, *RETRY_S[spec.aggregation]):
                await asyncio.sleep(wait)
                last = await since(last)
                if last >= close:
                    break

    async def _request_bars(self, request) -> None:
        # ponytail: the request's first bar type only; Strategy.request_bars with a list would need one answer each
        bar_type = request.bar_type
        spec = bar_type.spec
        period = self._period(bar_type)
        now = self.clock.timestamp_ns()
        bars = []
        if period is not None:
            sid = await self.instrument_provider.symbol_id(bar_type.instrument_id)
            inst = self.instrument_provider.find(bar_type.instrument_id)
            end_ns = request.end_ns or now
            nominal = spec.step * NOMINAL_NS[spec.aggregation]
            # without a start: enough calendar for the limit, weekends included
            start_ns = request.start_ns or end_ns - 2 * (request.limit or 1000) * nominal
            try:
                raw = await self._trendbars(sid, period, start_ns // MS, end_ns // MS)
            except (ConnectionError, OpenApiError, TimeoutError) as e:     # the requester gets an answer anyway
                self._log.error(f"cTrader bars for {bar_type} failed: {e!r}; answering none")
                raw = []
            bars = [b for b in trendbar_bars(raw, bar_type, inst.price_precision, now)
                    if (request.start_ns is None or b.ts_init >= request.start_ns) and b.ts_init <= end_ns]
            if request.limit:
                bars = bars[-request.limit:]
        self._handle_response(BarsResponse(self.client_id, bar_type, bars, request.request_id, now,
                                           request.start_ns, request.end_ns, request.params))

    async def _trendbars(self, symbol_id: int, period: int, from_ms: int, to_ms: int) -> list[dict]:
        return await fetch_trendbars(self.session, symbol_id, period, from_ms, to_ms)


class CTraderDataClientFactory(DataClientFactory):
    @staticmethod
    def create(*, name: str, config: CTraderDataClientConfig, cache, clock) -> CTraderDataClient:
        return CTraderDataClient(name=name, config=config, cache=cache, clock=clock)
