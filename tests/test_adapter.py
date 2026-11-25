"""The cTrader adapter's pieces: instruments from Fusion's symbol records, trendbars as bars, the
spot book, and the Open API session against a local stand-in for the server."""
import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nautilus_trader.model import BarType, Venue

from nautilus_ctrader.data import SpotBook, bar_close_ns, trendbar_bars
from nautilus_ctrader.instruments import currency_pair, is_fiat
from nautilus_ctrader.openapi import OpenApiError, OpenApiSession

# USDJPY as the Fusion demo server answered SYMBOL_BY_ID on 2026-11-24 (abridged)
USDJPY = {"symbolId": 4, "digits": 3, "pipPosition": 2, "maxVolume": 1000000000, "minVolume": 100000,
          "stepVolume": 100000, "lotSize": 10000000, "commission": 2250, "commissionType": 1,
          "scheduleTimeZone": "America/New_York"}


def ns(ts: str) -> int:
    return int(datetime.fromisoformat(ts).replace(tzinfo=UTC).timestamp()) * 1_000_000_000


def test_a_fusion_symbol_is_a_currency_pair_in_units():
    inst = currency_pair(USDJPY, "USDJPY", "USD", "JPY", Venue("FUSION"), Decimal("0.002"))
    assert str(inst.id) == "USD/JPY.FUSION" and str(inst.raw_symbol) == "USDJPY"
    assert inst.price_precision == 3 and str(inst.price_increment) == "0.001"
    assert (int(inst.size_increment), int(inst.min_quantity), int(inst.max_quantity), int(inst.lot_size)) == \
        (1_000, 1_000, 10_000_000, 100_000)                   # cTrader volumes are hundredths of a unit
    assert inst.margin_init == Decimal("0.002") and inst.info["symbol_id"] == 4
    assert is_fiat("JPY") and not is_fiat("XAU") and not is_fiat("GER40") and not is_fiat(None)


def test_trendbars_decode_to_bars_stamped_at_their_close_and_the_open_one_is_dropped():
    hourly = BarType.from_str("EUR/USD.FUSION-1-HOUR-BID-EXTERNAL")
    opened = ns("2026-11-24 10:00") // 60_000_000_000
    tbs = [{"utcTimestampInMinutes": opened, "low": 109623, "deltaOpen": 86, "deltaHigh": 122, "deltaClose": 56,
            "volume": 6966},
           {"utcTimestampInMinutes": opened + 60, "low": 109700, "deltaHigh": 5, "volume": 3}]
    bars = trendbar_bars(tbs, hourly, 5, now_ns=ns("2026-11-24 11:30"))
    assert len(bars) == 1                                     # 11:00-12:00 has not closed at 11:30
    b = bars[0]
    assert (float(b.open), float(b.high), float(b.low), float(b.close)) == (1.09709, 1.09745, 1.09623, 1.09679)
    assert (b.ts_event, b.ts_init, int(b.volume)) == (ns("2026-11-24 10:00"), ns("2026-11-24 11:00"), 6966)


def test_a_daily_bar_closes_at_17_00_new_york_whatever_the_clock_does():
    from nautilus_trader.model import BarAggregation
    day = BarAggregation.DAY
    # the session of Monday 9 March 2026 opens on Sunday at 17:00 EDT (21:00 UTC), after the change
    assert bar_close_ns(ns("2026-03-08 21:00"), 1, day) == ns("2026-03-09 21:00")
    # one opening at 17:00 EST the day before the change lasts 23 hours
    assert bar_close_ns(ns("2026-03-07 22:00"), 1, day) == ns("2026-03-08 21:00")
    daily = BarType.from_str("EUR/USD.FUSION-1-DAY-BID-EXTERNAL")
    tb = {"utcTimestampInMinutes": ns("2026-03-07 22:00") // 60_000_000_000, "low": 108000, "volume": 1}
    (b,) = trendbar_bars([tb], daily, 5, now_ns=ns("2026-03-10"))
    assert b.ts_init == ns("2026-03-08 21:00") and b.ts_event == b.ts_init - 86_400_000_000_000


def test_the_spot_book_quotes_once_both_sides_are_known_and_keeps_the_last_of_each():
    book = SpotBook()
    assert book.update({"symbolId": 1, "bid": 109700}) is None
    assert book.update({"symbolId": 1, "ask": 109702}) == (109700, 109702)
    assert book.update({"symbolId": 1, "ask": 109705}) == (109700, 109705)
    assert book.update({"symbolId": 2, "ask": 1}) is None


async def _fake_server(port_box, log):
    """Enough of the Open API: an expired token, its refresh, account auth, a request answered
    after an unrelated event, and an error answer."""
    from websockets.asyncio.server import serve

    async def handler(ws):
        async for raw in ws:
            msg = json.loads(raw)
            kind, body, mid = msg["payloadType"], msg.get("payload", {}), msg.get("clientMsgId")
            log.append(kind)

            async def reply(k, b, msg_id=mid):
                await ws.send(json.dumps({"payloadType": k, "payload": b, **({"clientMsgId": msg_id} if msg_id else {})}))

            if kind == 2100:
                await reply(2101, {})
            elif kind == 2149 and body["accessToken"] == "old":
                await reply(2142, {"errorCode": "CH_ACCESS_TOKEN_INVALID", "description": "expired"})
            elif kind == 2149:
                await reply(2150, {"ctidTraderAccount": [{"ctidTraderAccountId": 42, "traderLogin": 777, "isLive": False}]})
            elif kind == 2173:
                await reply(2174, {"accessToken": "new", "refreshToken": "r2"})
            elif kind == 2102:
                await reply(2131, {"symbolId": 1, "bid": 109700}, msg_id=None)       # a spot, no request's answer
                await reply(2103, {})
            elif kind == 2121:
                await reply(2126, {"executionType": 3}, msg_id=None)
                await reply(2122, {"trader": {"accountType": 0}})
            elif kind == 2124:
                await reply(2142, {"errorCode": "SOMETHING", "description": "refused"})

    server = await serve(handler, "127.0.0.1", 0)
    port_box.append(server.sockets[0].getsockname()[1])
    return server


def test_the_session_refreshes_an_expired_token_matches_answers_and_hands_on_events(tmp_path):
    cred = tmp_path / "cred.json"
    cred.write_text(json.dumps({"client_id": "c", "client_secret": "s", "access_token": "old",
                                "refresh_token": "r1", "account": 777, "host": "demo.example"}))

    async def run():
        port, log, events = [], [], []
        server = await _fake_server(port, log)
        s = OpenApiSession(cred, url=f"ws://127.0.0.1:{port[0]}", on_event=lambda k, b: events.append((k, b)))
        try:
            assert await s.connect() == 42 and s.is_live is False
            kind, body = await s.account_request(2121)
            assert (kind, body["trader"]["accountType"]) == (2122, 0)
            with pytest.raises(OpenApiError, match="SOMETHING"):
                await s.account_request(2124)
        finally:
            await s.close()
            server.close()
        return log, events

    log, events = asyncio.run(run())
    assert log[:5] == [2100, 2149, 2173, 2149, 2102]
    assert [k for k, _ in events] == [2131, 2126]               # events reach on_event, answers do not
    saved = json.loads(cred.read_text())
    assert (saved["access_token"], saved["refresh_token"]) == ("new", "r2")
    assert cred.stat().st_mode & 0o777 == 0o600


def test_a_live_host_is_refused_unless_allowed(tmp_path):
    cred = tmp_path / "cred.json"
    cred.write_text(json.dumps({"client_id": "c", "client_secret": "s", "access_token": "a",
                                "refresh_token": "r", "account": 1, "host": "live.ctraderapi.com"}))
    with pytest.raises(PermissionError, match="live"):
        asyncio.run(OpenApiSession(cred).connect())


# ── execution: messages as the Fusion demo server sent them on 2026-11-24 (abridged) ─────────────

EURUSD = {"symbolId": 1, "digits": 5, "pipPosition": 4, "maxVolume": 1000000000, "minVolume": 100000,
          "stepVolume": 100000, "lotSize": 10000000}
WORKING_ORDER = {"orderId": 34780776, "tradeData": {"symbolId": 1, "volume": 100000, "tradeSide": 1,
                 "openTimestamp": 1795546014544, "label": "capture"}, "orderType": 2, "orderStatus": 1,
                 "executedVolume": 0, "utcLastUpdateTimestamp": 1795546014776, "limitPrice": 1.09434,
                 "clientOrderId": "CAP-1", "timeInForce": 2, "positionId": 51198668}
OPENING_DEAL = {"dealId": 29346491, "orderId": 34780779, "positionId": 51198675, "volume": 100000,
                "filledVolume": 100000, "symbolId": 1, "createTimestamp": 1795546015477,
                "executionTimestamp": 1795546015477, "executionPrice": 1.09685, "tradeSide": 1, "dealStatus": 2,
                "commission": -2, "moneyDigits": 2}


def position(pid, status, volume, side, used=0):
    return {"positionId": pid, "positionStatus": status, "usedMargin": used, "price": 1.09685 if volume else 0.0,
            "utcLastUpdateTimestamp": 1795546015477,
            "tradeData": {"symbolId": 1, "volume": volume, "tradeSide": side, "openTimestamp": 1795546015477}}


def test_an_order_names_the_open_position_and_carries_its_volume_in_hundredths():
    from nautilus_trader.model import OrderSide, TimeInForce

    from nautilus_ctrader.execution import order_payload
    p = order_payload(symbol_id=1, side=OrderSide.SELL, qty=2_000, price=1.096784, digits=5,
                      time_in_force=TimeInForce.GTC, expire_ns=None, client_order_id="O-1", label="x",
                      position_id=51198675)
    assert p == {"symbolId": 1, "orderType": 2, "tradeSide": 2, "volume": 200_000, "limitPrice": 1.09678,
                 "timeInForce": 2, "label": "x", "clientOrderId": "O-1", "positionId": 51198675}
    g = order_payload(symbol_id=1, side=OrderSide.BUY, qty=1_000, price=1.0, digits=5, time_in_force=TimeInForce.GTD,
                      expire_ns=1_795_546_015_477_000_000, client_order_id="O-2", label="x", position_id=None)
    assert (g["timeInForce"], g["expirationTimestamp"], "positionId" in g) == (1, 1_795_546_015_477, False)


def test_what_the_venue_would_refuse_is_refused_before_it_is_sent():
    from nautilus_trader.model import (
        InstrumentId,
        OrderSide,
        Price,
        Quantity,
        QuoteTick,
    )

    from nautilus_ctrader.execution import post_only_refusal, volume_refusal
    inst = currency_pair(EURUSD, "EURUSD", "EUR", "USD", Venue("FUSION"), Decimal("0.002"))
    assert volume_refusal(1_000, inst) is None and volume_refusal(2_000, inst) is None
    assert "below the minimum" in volume_refusal(999, inst)
    assert "not a multiple" in volume_refusal(1_500, inst)
    assert "above the maximum" in volume_refusal(10_001_000, inst)
    q = QuoteTick(InstrumentId.from_str("EUR/USD.FUSION"), Price(1.09668, 5), Price(1.09668, 5),
                  Quantity(0, 0), Quantity(0, 0), 0, 0)                  # Fusion's zero spread
    assert "would take the ask" in post_only_refusal(OrderSide.BUY, 1.09668, q)
    assert post_only_refusal(OrderSide.BUY, 1.09667, q) is None
    assert "would take the bid" in post_only_refusal(OrderSide.SELL, 1.09668, q)
    assert "no quote" in post_only_refusal(OrderSide.BUY, 1.0, None)


def test_a_commission_in_the_pairs_base_currency_is_handed_on_in_its_quote_currency():
    from nautilus_trader.model import Currency

    from nautilus_ctrader.execution import commission
    eur = Currency.from_str("EUR")
    eurusd = currency_pair(EURUSD, "EURUSD", "EUR", "USD", Venue("FUSION"), Decimal("0.002"))
    usdjpy = currency_pair(USDJPY, "USDJPY", "USD", "JPY", Venue("FUSION"), Decimal("0.002"))
    c = commission(OPENING_DEAL, eur, eurusd)                  # 0.02 EUR would come out of a EUR/USD position
    assert (str(c.currency), round(float(c), 4)) == ("USD", 0.02)
    c = commission(OPENING_DEAL, eur, usdjpy)                  # EUR is no leg of USD/JPY: stays EUR
    assert (str(c.currency), float(c)) == ("EUR", 0.02)


def test_reports_from_the_servers_messages():
    from nautilus_trader.model import (
        AccountId,
        Currency,
        InstrumentId,
        LiquiditySide,
        OrderStatus,
        PositionSide,
    )

    from nautilus_ctrader.execution import (
        fill_report,
        order_status_report,
        position_reports,
    )
    acct, iid = AccountId("FUSION-1"), InstrumentId.from_str("EUR/USD.FUSION")
    r = order_status_report(WORKING_ORDER, iid, acct, 5, 7)
    assert (r.order_status, int(r.quantity), int(r.filled_qty), float(r.price), str(r.client_order_id)) == \
        (OrderStatus.ACCEPTED, 1_000, 0, 1.09434, "CAP-1")
    eurusd = currency_pair(EURUSD, "EURUSD", "EUR", "USD", Venue("FUSION"), Decimal("0.002"))
    f = fill_report(OPENING_DEAL, eurusd, acct, Currency.from_str("EUR"), 7)
    assert (str(f.trade_id), int(f.last_qty), float(f.last_px), f.liquidity_side) == \
        ("29346491", 1_000, 1.09685, LiquiditySide.TAKER)         # filled the millisecond it was placed
    short = position(51198675, 1, 100000, 2)
    (p,) = position_reports([short, position(9, 2, 0, 1)], {1: iid}, acct, 7)   # a closed one is not reported
    assert (p.position_side, int(p.quantity), float(p.avg_px_open)) == (PositionSide.SHORT, 1_000, 1.09685)


def test_one_position_per_instrument_through_an_order_its_fill_a_flip_and_the_close():
    from types import SimpleNamespace

    from nautilus_ctrader.execution import CTraderExecutionClient
    me = SimpleNamespace(_positions={}, _opening={})
    track = CTraderExecutionClient._track_position
    track(me, position(5, 3, 0, 1))                            # an opening order is working: CREATED
    assert me._opening == {1: 5} and me._positions == {1: {}}
    track(me, position(5, 1, 100000, 1, used=200))             # filled: OPEN
    assert me._opening == {} and me._positions[1][5]["usedMargin"] == 200
    track(me, position(5, 1, 100000, 2, used=200))             # flipped in place: the same id, now short
    assert me._positions[1][5]["tradeData"]["tradeSide"] == 2
    track(me, position(5, 2, 0, 2))                            # closed
    assert me._positions == {1: {}} and me._opening == {}


# ── reconnect ────────────────────────────────────────────────────────────────────────────────────

def test_the_session_reconnects_after_a_drop_and_after_a_disconnect_notice_and_not_after_close(tmp_path):
    cred = tmp_path / "cred.json"
    cred.write_text(json.dumps({"client_id": "c", "client_secret": "s", "access_token": "a", "refresh_token": "r",
                                "account": 777, "host": "demo.example"}))

    async def run():
        from websockets.asyncio.server import serve
        conns, restored = [], []

        async def handler(ws):
            conns.append(ws)
            n = len(conns)
            async for raw in ws:
                msg = json.loads(raw)
                kind, mid = msg["payloadType"], msg.get("clientMsgId")

                async def reply(k, b, msg_id=mid):
                    await ws.send(json.dumps({"payloadType": k, "payload": b,
                                              **({"clientMsgId": msg_id} if msg_id else {})}))

                if kind == 2100:
                    await reply(2101, {})
                elif kind == 2149:
                    await reply(2150, {"ctidTraderAccount": [{"ctidTraderAccountId": 42, "traderLogin": 777}]})
                elif kind == 2102:
                    await reply(2103, {})
                elif kind == 2121 and n == 1:
                    await ws.close()                          # the first connection dies under a request
                    return
                elif kind == 2121:
                    await reply(2122, {"trader": {"connection": n}})
                    if n == 2:                                # then the server says the account is disconnected
                        await reply(2164, {"reason": "test"}, msg_id=None)

        async def on_reconnect():
            restored.append(len(conns))

        server = await serve(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        s = OpenApiSession(cred, url=f"ws://127.0.0.1:{port}", backoff_s=(0.05,), on_reconnect=on_reconnect)
        try:
            await s.connect()
            with pytest.raises(ConnectionError):
                await s.account_request(2121)
            _, body = await s.account_request(2121)            # waits for the reconnect
            assert body["trader"]["connection"] == 2
            for _ in range(200):
                if len(restored) == 2:
                    break
                await asyncio.sleep(0.01)
            _, body = await s.account_request(2121)
            assert body["trader"]["connection"] == 3
        finally:
            await s.close()
        await asyncio.sleep(0.3)                               # no reconnect after close()
        server.close()
        return len(conns), restored, s.reconnects

    assert asyncio.run(run()) == (3, [2, 3], 2)


def _deal(deal_id, volume, ts, status=2):
    return {"dealId": deal_id, "orderId": 7, "filledVolume": volume, "executionTimestamp": ts, "dealStatus": status,
            "executionPrice": 1.1, "tradeSide": 1, "commission": -2, "moneyDigits": 2}


def _venue_order(status, price=1.09434, volume=100000):
    return {"orderId": 7, "orderStatus": status, "limitPrice": price,
            "tradeData": {"symbolId": 1, "volume": volume, "tradeSide": 1, "openTimestamp": 1}}


def test_catch_up_tells_what_was_missed_in_order():
    from nautilus_ctrader.execution import catch_up
    working, filled, cancelled, expired, rejected = (_venue_order(s) for s in (1, 2, 5, 4, 3))
    d1, d2 = _deal(11, 50000, 20), _deal(12, 50000, 30)
    # acceptance never seen, the order is still working as Nautilus knows it
    assert catch_up(False, 1.09434, 1_000, working, [], set()) == [("accepted",)]
    # filled while away, in two deals, the first one already told
    assert catch_up(True, 1.09434, 1_000, filled, [d2, d1], {11}) == [("filled", d2)]
    assert catch_up(False, 1.09434, 1_000, filled, [d2, d1], set()) == [("accepted",), ("filled", d1), ("filled", d2)]
    # half filled, then cancelled; expired; refused outright
    assert catch_up(True, 1.09434, 1_000, cancelled, [d1], set()) == [("filled", d1), ("canceled",)]
    assert catch_up(True, 1.09434, 1_000, expired, [], set()) == [("expired",)]
    assert catch_up(False, 1.09434, 1_000, rejected, [], set()) == [("rejected",)]
    # an amendment whose answer was lost: the working order differs from Nautilus's
    assert catch_up(True, 1.09484, 1_000, working, [], set()) == [("updated", 1.09434, 1_000)]
    assert catch_up(True, 1.09434, 2_000, working, [], set()) == [("updated", 1.09434, 1_000)]
    # a deal that did not execute is not a fill
    assert catch_up(True, 1.09434, 1_000, working, [_deal(13, 0, 40, status=4)], set()) == []


def test_a_streamed_bar_closes_on_utc_multiples_or_at_17_00_new_york():
    from nautilus_trader.model import BarAggregation

    from nautilus_ctrader.data import next_close_ns
    minute, hour, day = BarAggregation.MINUTE, BarAggregation.HOUR, BarAggregation.DAY
    assert next_close_ns(ns("2026-11-24 10:00:30"), 1, minute) == ns("2026-11-24 10:01")
    assert next_close_ns(ns("2026-11-24 10:01"), 1, minute) == ns("2026-11-24 10:02")      # strictly after
    assert next_close_ns(ns("2026-11-24 10:07"), 5, minute) == ns("2026-11-24 10:10")
    assert next_close_ns(ns("2026-11-24 10:00"), 1, hour) == ns("2026-11-24 11:00")
    assert next_close_ns(ns("2026-11-24 20:00"), 1, day) == ns("2026-11-24 22:00")          # 17:00 EST
    assert next_close_ns(ns("2026-11-24 22:00"), 1, day) == ns("2026-11-25 22:00")
    assert next_close_ns(ns("2026-03-07 23:00"), 1, day) == ns("2026-03-08 21:00")          # 17:00 EDT
    with pytest.raises(ValueError):
        next_close_ns(0, 4, hour)
