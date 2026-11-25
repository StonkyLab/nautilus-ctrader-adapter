# Nautilus cTrader Adapter
Nautilus Trader adapter for the cTrader Open API.

A [Nautilus Trader](https://github.com/nautechsystems/nautilus_trader) data and execution client for
the [cTrader Open API](https://help.ctrader.com/open-api/): FX instruments, live quotes and BID bars,
GTC and GTD limit orders, fills, the account balance, the reports Nautilus reconciles with at start,
and reconnects that catch up whatever happened meanwhile. Written in Python against Nautilus Trader
2.0.0rc9 and tested on a Fusion Markets demo account.

# Instructions

Install with pip or uv. Nautilus 2.0 release candidates come from Nautech's package index:

```bash
pip install "nautilus-ctrader-adapter @ git+https://github.com/StonkyLab/nautilus-ctrader-adapter.git" \
    --extra-index-url https://packages.nautechsystems.io/simple
```

The credentials are a JSON file. An Open API application gives the client id and secret, and its
OAuth flow gives the tokens for the trading account:

```json
{
  "client_id": "...",
  "client_secret": "...",
  "access_token": "...",
  "refresh_token": "...",
  "account": 1234567,
  "host": "demo.ctraderapi.com"
}
```

`account` is the login cTrader shows for the account. When the access token expires, the adapter
renews it with the refresh token and rewrites the file, readable by its owner only. A live host
(`live.ctraderapi.com`) is refused unless the client configs set `allow_live=True`.

A node with both clients:

```python
from nautilus_trader.common import Environment
from nautilus_trader.live import InstrumentProviderConfig, LiveExecutionEngineConfig, LiveNode
from nautilus_trader.model import TraderId

from nautilus_ctrader.data import CTraderDataClientConfig, CTraderDataClientFactory
from nautilus_ctrader.execution import CTraderExecClientConfig, CTraderExecutionClientFactory

CREDENTIALS = "/path/to/ctrader-demo.json"
PAIRS = ["EUR/USD.CTRADER", "USD/JPY.CTRADER"]

node = (
    LiveNode.builder("my-node", TraderId("TRADER-001"), Environment.LIVE)
    .with_exec_engine_config(LiveExecutionEngineConfig(reconciliation=True))
    .add_data_client("CTRADER", CTraderDataClientFactory(), CTraderDataClientConfig(
        credentials_path=CREDENTIALS, instrument_provider=InstrumentProviderConfig(load_ids=PAIRS)))
    .add_exec_client("CTRADER", CTraderExecutionClientFactory(), CTraderExecClientConfig(
        credentials_path=CREDENTIALS, account_currency="EUR"))
    .build()
)
node.add_strategy(...)
node.run()
```

The configs' `venue` (default `CTRADER`) names the instruments: `EUR/USD.CTRADER`. The execution
client must be told the account's deposit currency (`account_currency`) and checks it at connect.

`pytest` runs the tests. They need no network: the session is tested against a local stand-in for
the server. `examples/` runs each client inside a real node against a demo account:

- `data_smoke.py` reads instruments, quotes, historical bars and live one-minute bars, and sends no
  order.
- `exec_smoke.py` places, amends, cancels and fills orders of 1,000 EUR/USD, flips the position and
  closes it.
- `reconnect_smoke.py` drops the node's connection while an order fills through another one, and
  checks that the fill is caught up.

The last two trade: run them on a demo account that no other node trades.

# Notes and limitations

- **FX pairs only.** The instrument provider loads the symbols whose base and quote assets are both
  fiat currencies, as `CurrencyPair`s in units (cTrader's volumes are hundredths of a unit). Margin
  is the account's leverage, the same for every pair, so Nautilus's own pre-trade margin check
  mirrors the venue's.
- **Quotes carry no sizes**: cTrader's spot events have none. A quote goes out once both sides are
  known. A bid equal to the ask is ordinary at some brokers (Fusion Markets quotes EUR/USD at a
  zero spread for much of the day).
- **BID bars only** (`BID-EXTERNAL`), from cTrader's trendbars: 1, 2, 3, 4, 5, 10, 15 and 30
  minutes, 1, 4 and 12 hours, a day and a week. A bar's `ts_init` is its close and `ts_event` the
  close minus its nominal length; daily bars close at 17:00 New York.
- **Live bars are read back, not built from quotes.** A subscription asks for the trendbars 1.5 s
  after each close, and again for up to 50 s while the closed bar is not there, always for
  everything since the last bar it handed on: a bar missed while disconnected follows at the next
  close. Minute, hourly and daily bars are streamed; 4-hour, 12-hour and weekly bars can only be
  requested. A subscription with `params={"since_ns": <a close>}` first hands on every bar closed
  after that close; `nautilus_ctrader.data.history` fetches the same before a node starts.
- **GTC and GTD limit orders only.** cTrader has no post-only flag: a post-only order is checked
  against the last quote in the cache and rejected if it would take liquidity. The quote can move
  before the order arrives, so the check narrows the race; it does not close it.
- **A hedged account is run as a netting one.** On cTrader an order opens a position of its own
  unless it names an open one. The client keeps one position per instrument and names it in every
  order, so the account behaves as `OmsType.NETTING`. Orders on an instrument with more than one
  open position (a manual hedge) are denied, and so is a second order while the one opening the
  position is still working.
- **The liquidity side is a guess.** cTrader does not say whether a fill made or took liquidity: a
  fill within a second of the order's placement is called TAKER, a later one MAKER.
- **Commissions** come with the deals, in the deposit currency. When that is the pair's base
  currency (EUR/USD on a EUR account), the commission is handed on in the quote currency at the
  fill's price, because Nautilus would take a base-currency commission out of the position.
- **Reconnects.** A connection that ends unasked is reopened after 1, 2, 4, 8, 15, 30 and then
  every 60 seconds. The data client subscribes its quotes again. The execution client reads the
  balance, the positions and the working orders again, and tells Nautilus what it missed of every
  order it holds open: the acceptance, fills, an amendment, the cancel or expiry, from cTrader's
  order history.
- **Startup** is Nautilus's own reconciliation from the client's reports: open orders, open
  positions and the deals of the lookback (24 hours when none is given).
- The transport is JSON over WebSocket (port 5036) on asyncio, so no protobuf.

# Dependencies

- Python 3.12 to 3.14
- [nautilus_trader](https://github.com/nautechsystems/nautilus_trader) 2.0.0rc9, from Nautech's
  package index (`https://packages.nautechsystems.io/simple`)
- [websockets](https://github.com/python-websockets/websockets) 17 or newer
