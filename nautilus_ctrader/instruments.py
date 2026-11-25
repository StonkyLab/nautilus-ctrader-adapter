"""cTrader symbols as Nautilus ``CurrencyPair``s.

A symbol comes in two halves: the light list (``SYMBOLS``: name, id, base and quote asset ids) and
the full record (``SYMBOL_BY_ID``: digits, volumes, commission, schedule). Volumes are in
hundredths of a unit (Fusion's EURUSD: minVolume = stepVolume = 100000, i.e. 1,000 units), prices
have ``digits`` decimals. Only FX pairs are loaded: both assets fiat currencies. The instrument id
is ``BASE/QUOTE.<venue>`` (``EUR/USD.CTRADER``); ``info["symbol_id"]`` is cTrader's id.

Margin is the account's leverage (``TRADER.leverageInCents``, 1:500 on the Fusion demo), the same
for every pair: Nautilus's own pre-trade margin check then mirrors the venue's.
"""
from __future__ import annotations

from decimal import Decimal

from nautilus_trader.live.providers import InstrumentProvider
from nautilus_trader.model import (
    Currency,
    CurrencyPair,
    CurrencyType,
    InstrumentId,
    Price,
    Quantity,
    Symbol,
    Venue,
)

from nautilus_ctrader.openapi import (
    ASSET_LIST,
    SYMBOL_BY_ID,
    SYMBOLS,
    TRADER,
    OpenApiSession,
)

SYMBOL_BY_ID_CHUNK = 50

__all__ = ["CTraderInstrumentProvider", "currency_pair", "is_fiat"]


def is_fiat(code: str | None) -> bool:
    try:
        return code is not None and Currency.from_str(code).currency_type == CurrencyType.FIAT
    except (TypeError, ValueError):
        return False


def units(volume) -> int:
    """cTrader volume (hundredths of a unit) in units."""
    return int(volume) // 100


def currency_pair(symbol: dict, name: str, base: str, quote: str, venue: Venue, margin: Decimal,
                  ts: int = 0) -> CurrencyPair:
    """One pair from the full symbol record and the light list's name and assets."""
    digits = int(symbol["digits"])
    return CurrencyPair(
        instrument_id=InstrumentId(Symbol(f"{base}/{quote}"), venue),
        raw_symbol=Symbol(name),
        base_currency=Currency.from_str(base),
        quote_currency=Currency.from_str(quote),
        price_precision=digits,
        size_precision=0,
        price_increment=Price.from_str(f"{10 ** -digits:.{digits}f}"),
        size_increment=Quantity.from_int(units(symbol["stepVolume"])),
        lot_size=Quantity.from_int(units(symbol["lotSize"])),
        min_quantity=Quantity.from_int(units(symbol["minVolume"])),
        max_quantity=Quantity.from_int(units(symbol["maxVolume"])),
        margin_init=margin,
        margin_maint=margin,
        info={"symbol_id": int(symbol["symbolId"]), "pip_position": int(symbol["pipPosition"]),
              "commission": symbol.get("commission"), "commission_type": symbol.get("commissionType"),
              "schedule_time_zone": symbol.get("scheduleTimeZone")},
        ts_event=ts,
        ts_init=ts,
    )


class CTraderInstrumentProvider(InstrumentProvider):
    """Loads FX pairs over a connected ``OpenApiSession`` and maps instrument ids to cTrader's
    symbol ids both ways."""

    def __init__(self, session: OpenApiSession, venue: Venue, config=None):
        super().__init__(config)
        self.session = session
        self.venue = venue
        self.symbol_ids: dict[InstrumentId, int] = {}
        self.instrument_ids: dict[int, InstrumentId] = {}
        self.asset_names: dict[int, str] = {}

    async def load_all_async(self, filters: dict | None = None) -> None:
        await self._load(None)

    async def load_ids_async(self, instrument_ids: list[InstrumentId], filters: dict | None = None) -> None:
        await self._load({str(i.symbol).replace("/", "") for i in instrument_ids if i.venue == self.venue})

    async def symbol_id(self, instrument_id: InstrumentId) -> int:
        """cTrader's id for the pair, loading it first when it is not loaded yet."""
        if instrument_id not in self.symbol_ids:
            await self.load_ids_async([instrument_id])
        if instrument_id not in self.symbol_ids:
            raise KeyError(f"{instrument_id} is not a cTrader FX symbol of this account")
        return self.symbol_ids[instrument_id]

    async def _load(self, names: set[str] | None) -> None:
        _, assets = await self.session.account_request(ASSET_LIST)
        asset = {a["assetId"]: a["name"] for a in assets.get("asset", [])}
        self.asset_names |= asset
        _, light = await self.session.account_request(SYMBOLS)
        _, trader = await self.session.account_request(TRADER)
        leverage = int((trader.get("trader") or {}).get("leverageInCents", 100)) / 100
        margin = Decimal(1) / Decimal(str(leverage))
        chosen = {}
        for s in light.get("symbol", []):
            base, quote = asset.get(s.get("baseAssetId")), asset.get(s.get("quoteAssetId"))
            if (s.get("enabled", True) and (names is None or s["symbolName"] in names)
                    and is_fiat(base) and is_fiat(quote)):
                chosen[int(s["symbolId"])] = (s["symbolName"], base, quote)
        if names is not None and (missing := names - {n for n, _, _ in chosen.values()}):
            self._log.warning(f"not cTrader FX symbols of this account: {sorted(missing)}")
        ids = sorted(chosen)
        for i in range(0, len(ids), SYMBOL_BY_ID_CHUNK):
            _, full = await self.session.account_request(SYMBOL_BY_ID, {"symbolId": ids[i:i + SYMBOL_BY_ID_CHUNK]})
            for sym in full.get("symbol", []):
                name, base, quote = chosen[int(sym["symbolId"])]
                inst = currency_pair(sym, name, base, quote, self.venue, margin)
                self.add(inst)
                self.symbol_ids[inst.id] = int(sym["symbolId"])
                self.instrument_ids[int(sym["symbolId"])] = inst.id
