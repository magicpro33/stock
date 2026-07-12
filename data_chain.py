"""
data_chain.py — field-level data completion for the Analyze flow.

Priority per field (freshest wins):
  price/quote:      Alpaca snapshot -> yfinance -> nightly dump
  OHLCV history:    yfinance -> nightly dump _hist, then topped up to the
                    latest session with Alpaca daily bars
  fundamentals/info: yfinance -> nightly dump (comprehensive field map)
  computed metrics: live computation -> nightly dump pre-computed values

Alpaca credentials are read from Streamlit secrets or environment under any
of: ALPACA_API_KEY / ALPACA_SECRET_KEY, ALPACA_KEY_ID / ALPACA_SECRET,
APCA_API_KEY_ID / APCA_API_SECRET_KEY. If absent, the Alpaca layer is
skipped silently and the chain still guarantees dump-level completeness.
"""
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

ALPACA_DATA = "https://data.alpaca.markets/v2/stocks"
_TIMEOUT = 6

REQUIRED_INFO = [
    "currentPrice", "marketCap", "sector", "trailingPE",
    "revenueGrowth", "earningsGrowth", "fiftyDayAverage",
    "shortPercentOfFloat", "shortRatio",
]

# nightly dump field -> yfinance-style info key (+ optional transform)
_DUMP_INFO_MAP = {
    "Sector":               ("sector", None),
    "Price":                ("currentPrice", None),
    "Price ":               ("regularMarketPrice", None),   # alias handled below
    "MarketCap":            ("marketCap", None),
    "P/E":                  ("trailingPE", None),
    "RevenueGrowth":        ("revenueGrowth", None),
    "EarningsGrowth":       ("earningsGrowth", None),
    "MA50":                 ("fiftyDayAverage", None),
    "DividendYieldPct":     ("dividendYield", lambda v: v / 100.0),
    "DividendRate":         ("dividendRate", None),
    "DividendPayoutRatio":  ("payoutRatio", lambda v: v / 100.0),
    "ExDividendDate":       ("exDividendDate", None),
    "ShortPctFloat":        ("shortPercentOfFloat", None),
    "DaysToCover":          ("shortRatio", None),
}

_RAW_KEYS = [
    "OE_Yield", "ROIC", "ROIC_Trend", "RevenueGrowth", "EarningsGrowth",
    "Piotroski", "OBV", "MFI", "PCV", "RSI", "MACD", "GoldenCross",
    "MFISweetSpot", "NoBearDiv", "MA50Proximity", "ShortPctFloatRaw",
    "DaysToCover", "ShortChange", "ShortSqueeze",
]


# ── credentials ──────────────────────────────────────────────────────
_KEY_PAIRS = [
    ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
    ("ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"),
    ("ALPACA_KEY_ID", "ALPACA_SECRET"),
    ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"),
]


def alpaca_keys_detail():
    """(key_id, secret, matched_name) — checks Streamlit secrets, then env."""
    getters = []
    try:
        import streamlit as st
        getters.append(("secrets", lambda k: st.secrets.get(k, "")))
    except Exception:
        pass
    getters.append(("env", lambda k: os.environ.get(k, "")))
    for kid_name, sec_name in _KEY_PAIRS:
        for where, g in getters:
            try:
                kid, sec = g(kid_name), g(sec_name)
            except Exception:
                continue
            if kid and sec:
                return kid, sec, f"{kid_name} ({where})"
    return None, None, None


def alpaca_keys():
    kid, sec, _ = alpaca_keys_detail()
    return kid, sec


def _mask(key: str) -> str:
    return f"{key[:4]}…{key[-4:]}" if key and len(key) > 8 else "set"


def _alpaca_get(path, params=None):
    """Returns (json_or_None, status_string)."""
    kid, sec, name = alpaca_keys_detail()
    if not kid:
        return None, "no key configured"
    ident = f"key {_mask(kid)} via {name}"
    try:
        r = requests.get(f"{ALPACA_DATA}/{path}", params=params or {},
                         headers={"APCA-API-KEY-ID": kid,
                                  "APCA-API-SECRET-KEY": sec},
                         timeout=_TIMEOUT)
        if r.status_code == 200:
            return r.json(), f"OK — {ident}"
        return None, f"HTTP {r.status_code} — {ident}"
    except Exception as e:
        return None, f"request failed ({type(e).__name__}) — {ident}"


# ── Alpaca sources ───────────────────────────────────────────────────
def alpaca_snapshot(ticker: str) -> dict:
    """Latest trade + daily/prev-daily bars in one call. {} if unavailable."""
    data, status = _alpaca_get(f"{ticker}/snapshot", {"feed": "iex"})
    if not data:
        return {"_status": status}
    out = {}
    lt = data.get("latestTrade") or {}
    db = data.get("dailyBar") or {}
    pv = data.get("prevDailyBar") or {}
    price = lt.get("p") or db.get("c")
    if price:
        out["currentPrice"] = float(price)
        out["regularMarketPrice"] = float(price)
    if pv.get("c"):
        out["previousClose"] = float(pv["c"])
        if price:
            out["regularMarketChangePercent"] = (float(price) / float(pv["c"]) - 1) * 100
    if db.get("v"):
        out["volume"] = float(db["v"])
    if lt.get("t"):
        out["_alpaca_asof"] = str(lt["t"])[:19].replace("T", " ")
    out["_status"] = status
    return out


def alpaca_daily_bars(ticker: str, start_iso: str) -> pd.DataFrame:
    """Daily OHLCV bars from start_iso (inclusive). Empty df if unavailable."""
    data, _status = _alpaca_get(f"{ticker}/bars",
                       {"timeframe": "1Day", "start": start_iso,
                        "adjustment": "split", "feed": "iex", "limit": 400})
    bars = (data or {}).get("bars") or []
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "Date": pd.Timestamp(b["t"][:10]),
        "Open": b["o"], "High": b["h"], "Low": b["l"],
        "Close": b["c"], "Volume": b["v"],
    } for b in bars]).set_index("Date")
    return df[["Open", "High", "Low", "Close", "Volume"]].astype(float)


# ── nightly dump sources ─────────────────────────────────────────────
_DUMP_PATH = os.path.join(os.path.dirname(__file__), "data", "stock_data.json.gz")


def _cache(fn):
    try:
        import streamlit as st
        return st.cache_data(ttl=3600, show_spinner=False)(fn)
    except Exception:
        _memo = {}
        def wrap(*a):
            if a not in _memo:
                _memo[a] = fn(*a)
            return _memo[a]
        return wrap


@_cache
def _load_dump_panel():
    """Compact float32 OHLCV panel from the nightly dump (~27 MB resident).
    Needed because the app's nightly loader strips _hist to save memory."""
    import gzip, json
    with gzip.open(_DUMP_PATH, "rt") as f:
        data = json.load(f)
    rows = [r for r in data if r.get("_hist", {}).get("dates")]
    all_dates = sorted({d for r in rows for d in r["_hist"]["dates"]})
    dix = {d: i for i, d in enumerate(all_dates)}
    T, N = len(all_dates), len(rows)
    panel = {f: np.full((T, N), np.nan, dtype=np.float32)
             for f in ("open", "high", "low", "close", "volume")}
    tickers = {}
    for j, r in enumerate(rows):
        h = r["_hist"]
        ix = [dix[d] for d in h["dates"]]
        for f in panel:
            panel[f][ix, j] = h[f]
        tickers[r["Ticker"]] = j
    del data, rows
    return panel, tickers, pd.to_datetime(all_dates)


def dump_hist_for(ticker: str) -> pd.DataFrame:
    """Full OHLCV history for one ticker straight from the dump file."""
    try:
        panel, tickers, idx = _load_dump_panel()
    except Exception:
        return pd.DataFrame()
    j = tickers.get(ticker)
    if j is None:
        return pd.DataFrame()
    df = pd.DataFrame({
        "Open": panel["open"][:, j], "High": panel["high"][:, j],
        "Low": panel["low"][:, j], "Close": panel["close"][:, j],
        "Volume": panel["volume"][:, j],
    }, index=idx).dropna(subset=["Close"])
    return df.astype(float)


def dump_hist_df(row: dict) -> pd.DataFrame:
    h = (row or {}).get("_hist") or {}
    if not h.get("dates"):
        return pd.DataFrame()
    df = pd.DataFrame({
        "Open": h["open"], "High": h["high"], "Low": h["low"],
        "Close": h["close"], "Volume": h["volume"],
    }, index=pd.to_datetime(h["dates"]))
    return df.astype(float)


def dump_info_patch(row: dict) -> dict:
    out = {}
    for dump_key, (info_key, fn) in _DUMP_INFO_MAP.items():
        v = (row or {}).get(dump_key.strip())
        if v is None or (isinstance(v, float) and not np.isfinite(v)):
            continue
        out[info_key] = fn(v) if fn else v
    if "currentPrice" in out:
        out.setdefault("regularMarketPrice", out["currentPrice"])
    return out


def patch_raw_from_dump(raw: dict, row: dict) -> dict:
    """Fill None/NaN computed metrics from the nightly pre-computed values."""
    raw = dict(raw or {})
    for k in _RAW_KEYS:
        v = raw.get(k)
        missing = v is None or (isinstance(v, float) and not np.isfinite(v))
        if missing and row is not None and row.get(k) is not None:
            raw[k] = row[k]
    return raw


# ── orchestrator ─────────────────────────────────────────────────────
def _hist_is_stale(hist: pd.DataFrame) -> bool:
    if hist is None or hist.empty:
        return True
    last = pd.Timestamp(hist.index[-1]).tz_localize(None).normalize()
    today = pd.Timestamp(datetime.utcnow().date())
    bdays = pd.bdate_range(last + timedelta(days=1), today)
    return len(bdays) > 1          # more than the (possibly still-open) session


def complete_info_hist(ticker: str, info: dict, hist: pd.DataFrame, row: dict):
    """Field-level completion. Returns (info, hist, sources, missing)."""
    info = dict(info or {})
    sources = {"price": "yfinance" if info.get("currentPrice") else None,
               "history": "yfinance" if hist is not None and not hist.empty else None,
               "filled_from_dump": [], "history_topup": None}

    # 1. history: dump fallback (row _hist first, then the dump file itself,
    #    since the app's nightly loader strips _hist from cached rows)
    if hist is None or hist.empty or len(hist) < 30:
        dh = dump_hist_df(row)
        if dh.empty:
            dh = dump_hist_for(ticker)
        if not dh.empty:
            hist = dh
            sources["history"] = "nightly dump"

    # 2. history: Alpaca top-up to the latest session
    if hist is not None and not hist.empty and _hist_is_stale(hist):
        last = pd.Timestamp(hist.index[-1]).tz_localize(None)
        bars = alpaca_daily_bars(ticker, (last + timedelta(days=1)).strftime("%Y-%m-%d"))
        if not bars.empty:
            tz = getattr(hist.index, "tz", None)
            if tz is not None:
                bars.index = bars.index.tz_localize(tz)
            hist = pd.concat([hist, bars])
            hist = hist[~hist.index.duplicated(keep="last")]
            sources["history_topup"] = f"Alpaca (+{len(bars)} bars)"

    # 3. quote: Alpaca snapshot overrides — freshest price wins
    snap = alpaca_snapshot(ticker)
    sources["alpaca"] = snap.get("_status", "no key configured")
    if snap.get("currentPrice"):
        info.update({k: v for k, v in snap.items() if not k.startswith("_")})
        sources["price"] = f"Alpaca ({snap.get('_alpaca_asof', 'live')})"
    elif not info.get("currentPrice") and hist is not None and not hist.empty:
        info["currentPrice"] = float(hist["Close"].iloc[-1])
        info.setdefault("regularMarketPrice", info["currentPrice"])
        sources["price"] = sources.get("history") or "history"

    # 4. info gaps: comprehensive dump fill
    patch = dump_info_patch(row)
    for k, v in patch.items():
        cur = info.get(k)
        empty = cur is None or cur == "" or (isinstance(cur, float) and not np.isfinite(cur))
        if empty:
            info[k] = v
            sources["filled_from_dump"].append(k)

    missing = [k for k in REQUIRED_INFO
               if info.get(k) is None
               or (isinstance(info.get(k), float) and not np.isfinite(info[k]))]
    return info, hist, sources, missing


def sources_caption(sources: dict, missing: list) -> str:
    bits = []
    if sources.get("price"):
        bits.append(f"price: {sources['price']}")
    if sources.get("history"):
        h = sources["history"]
        if sources.get("history_topup"):
            h += f" + {sources['history_topup']}"
        bits.append(f"history: {h}")
    alp = sources.get("alpaca", "")
    if alp:
        bits.append("Alpaca: " + ("✅ " + alp if alp.startswith("OK") else "⚠️ " + alp))
    if sources.get("filled_from_dump"):
        bits.append(f"filled from nightly: {len(sources['filled_from_dump'])} fields")
    bits.append("missing: " + (", ".join(missing) if missing else "none"))
    return "🔗 Data sources — " + " · ".join(bits)


def status_report(sources: dict, missing: list) -> str:
    """Markdown detail block for the data-source status expander."""
    kid, _sec, name = alpaca_keys_detail()
    lines = ["**Alpaca API**"]
    if kid:
        lines.append(f"- Key detected: `{_mask(kid)}` from `{name}`")
        lines.append(f"- Last call: {sources.get('alpaca', 'not attempted')}")
    else:
        lines.append("- No key found. Add `ALPACA_API_KEY` + `ALPACA_SECRET_KEY` "
                     "(or `APCA_API_KEY_ID` + `APCA_API_SECRET_KEY`) to Streamlit secrets.")
    try:
        import streamlit as st
        av = bool(st.secrets.get("AV_API_KEY", "")) or bool(os.environ.get("AV_API_KEY", ""))
    except Exception:
        av = bool(os.environ.get("AV_API_KEY", ""))
    lines.append("")
    lines.append(f"**Alpha Vantage**: key {'detected' if av else 'not found'}")
    lines.append("")
    lines.append(f"**Price source**: {sources.get('price', '—')}")
    h = sources.get("history", "—")
    if sources.get("history_topup"):
        h += f" + {sources['history_topup']}"
    lines.append(f"**History source**: {h}")
    filled = sources.get("filled_from_dump") or []
    lines.append(f"**Filled from nightly dump** ({len(filled)}): "
                 + (", ".join(f"`{f}`" for f in filled) if filled else "none"))
    lines.append(f"**Still missing**: "
                 + (", ".join(f"`{m}`" for m in missing) if missing else "none ✅"))
    return "\n".join(lines)
