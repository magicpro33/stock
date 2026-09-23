# ─────────────────────────────────────────────────────────────────────────────
# nightly_scan.py
#
# Runs every night via GitHub Actions (see .github/workflows/nightly_scan.yml).
# Downloads all stock data for NYSE, NASDAQ, and S&P 500, computes every
# metric, and saves the results as a compressed JSON file that the Streamlit
# app loads instantly instead of running a live scan.
#
# Usage (manual):
#   pip install yfinance pandas numpy requests
#   python nightly_scan.py
#
# Output:
#   data/stock_data.json.gz   — all results, one dict per ticker (the app reads this)
#   data/scan_meta.json       — timestamp, counts, status per exchange
#   data/fundamentals.parquet — flat one-row-per-ticker table, no history payload
#   data/fundamentals.csv     — same, for anything that cannot read parquet
#   data/prices.parquet       — tidy long OHLCV (ticker, date, o/h/l/c/v)
#
# Each record now also carries "_analyzer": the yfinance profile fields the
# Money Weather Stock Lookup tab renders (name, industry, beta, margins, ROE,
# analyst targets, 52w range, quarterly EPS history). Those come from `info`
# and the statements this scan ALREADY downloads, so caching them adds no API
# calls and lets the app render a full analysis without a live yfinance hit.
#
# Incremental reuse: last night's data/stock_data.json.gz is the base. Names
# that already have a good profile, financials, and history only spend
# bandwidth on bars they are missing (usually just the latest session).
# Yahoo rate-limits were leaving the dump ~50-80% complete because every
# name re-downloaded a full year + info + 3 statements. Skip what we have.
# ─────────────────────────────────────────────────────────────────────────────

import os
os.environ.setdefault("YF_DISABLE_CURL_CFFI", "1")
import sys
import copy
import gzip
import json
import time
import random
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf
try:
    from alpha_vantage_fallback import (
        av_fill_info, av_fill_history, av_fill_financials,
        av_needs_fallback, av_needs_history_fallback, av_needs_financials_fallback,
    )
    _AV_AVAILABLE = True
except ImportError:
    _AV_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR      = os.path.join(os.path.dirname(__file__), "data")
DATA_FILE       = os.path.join(OUTPUT_DIR, "stock_data.json.gz")
META_FILE       = os.path.join(OUTPUT_DIR, "scan_meta.json")

# Worker count — KEEP THIS LOW for nightly jobs.
# GitHub Actions IPs are heavily rate-limited by Yahoo Finance.
# 3 workers = ~3 requests/sec. Too fast = empty responses for everything.
WORKERS         = 3
# Pause between batches (seconds) — lets Yahoo's rate limiter reset
BATCH_PAUSE     = 8
# Batch size — small batches + pauses prevents sustained hammering
BATCH_SIZE      = 30
MFI_PERIOD      = 14
RANGE_DAYS      = 30

ETF_KEYWORDS = [
    "etf", "ishares", "invesco", "vanguard", "spdr", "proshares",
    "direxion", "wisdomtree", "vaneck", "schwab select",
    "fidelity select", "global x", "ark ", "pimco",
    "debenture", "warrant",
]


# ── Session / history helpers ─────────────────────────────────────────────────

def _last_completed_session(now=None) -> str:
    """Most recent US equity session that should already have a close.

    Before ~5pm ET, today's bar may not exist yet. Weekends roll to Friday.
    Holidays are not modelled — a later coverage check reports how many
    names actually printed the planned session.
    """
    try:
        now = now or pd.Timestamp.now(tz="America/New_York")
        if getattr(now, "tzinfo", None) is not None:
            now = now.tz_convert("America/New_York")
    except Exception:
        now = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=4)
    hour = int(now.hour)
    d = pd.Timestamp(year=int(now.year), month=int(now.month), day=int(now.day))
    if hour < 17:
        d -= pd.Timedelta(days=1)
    while int(d.dayofweek) >= 5:
        d -= pd.Timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def _normalize_hist(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten columns, drop the volume-only last stub, tz-naive dates."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        chosen = None
        for lvl in range(out.columns.nlevels):
            vals = {str(v).lower().replace(" ", "") for v in out.columns.get_level_values(lvl)}
            if "close" in vals or "adjclose" in vals:
                out.columns = out.columns.get_level_values(lvl)
                chosen = lvl
                break
        if chosen is None:
            out.columns = out.columns.get_level_values(-1)
    colmap = {}
    for c in out.columns:
        cl = str(c).lower().replace(" ", "")
        if cl in ("open", "high", "low", "close", "volume", "dividends"):
            colmap[c] = "Dividends" if cl == "dividends" else cl.title()
    if colmap:
        out = out.rename(columns=colmap)
        out = out.loc[:, ~out.columns.duplicated(keep="first")]
    if "Close" not in out.columns:
        for c in list(out.columns):
            if str(c).lower().replace(" ", "") == "adjclose":
                out = out.rename(columns={c: "Close"})
                break
    if "Close" not in out.columns:
        return pd.DataFrame()
    out.index = pd.to_datetime(out.index)
    try:
        if getattr(out.index, "tz", None) is not None:
            out.index = out.index.tz_convert("America/New_York").tz_localize(None)
    except Exception:
        try:
            out.index = out.index.tz_localize(None)
        except Exception:
            pass
    out.index = pd.DatetimeIndex(out.index).normalize()
    out = out[~out.index.duplicated(keep="last")].sort_index()
    close = pd.to_numeric(out["Close"], errors="coerce")
    # yfinance often appends a post-close stub: Volume filled, OHLC NaN.
    # Keeping it makes the dump's last date an empty session.
    return out.loc[close.notna()]


def _hist_last(hist) -> str:
    """Last YYYY-MM-DD in a history frame or `_hist` cache dict."""
    if isinstance(hist, pd.DataFrame):
        if hist.empty:
            return ""
        return pd.Timestamp(hist.index[-1]).strftime("%Y-%m-%d")
    dates = (hist or {}).get("dates") or []
    return str(dates[-1]) if dates else ""


def _cache_from_df(hist: pd.DataFrame) -> dict:
    h = _normalize_hist(hist)
    if h.empty:
        return {}
    def _col(name):
        if name not in h.columns:
            return [None] * len(h)
        s = pd.to_numeric(h[name], errors="coerce")
        if name == "Volume":
            return s.fillna(0).astype("int64").tolist()
        return s.round(4).tolist()
    return {
        "dates":  h.index.strftime("%Y-%m-%d").tolist(),
        "open":   _col("Open"),
        "high":   _col("High"),
        "low":    _col("Low"),
        "close":  _col("Close"),
        "volume": _col("Volume"),
    }


def _df_from_cache(cache: dict) -> pd.DataFrame:
    d = (cache or {}).get("dates") or []
    if not d:
        return pd.DataFrame()
    n = len(d)
    def _pad(key):
        vals = list((cache or {}).get(key) or [])
        if len(vals) < n:
            vals = vals + [None] * (n - len(vals))
        return vals[:n]
    df = pd.DataFrame({
        "Open": _pad("open"), "High": _pad("high"), "Low": _pad("low"),
        "Close": _pad("close"), "Volume": _pad("volume"),
    }, index=pd.to_datetime(d))
    return _normalize_hist(df)


def _merge_hist(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    frames = [_normalize_hist(x) for x in (a, b)]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return _normalize_hist(out)


def _apply_hist_to_row(row: dict, hist: pd.DataFrame) -> None:
    """Write spliced OHLCV back onto a scan row and refresh last-bar fields."""
    h = _normalize_hist(hist)
    if h.empty:
        return
    row["_hist"] = _cache_from_df(h)
    try:
        px = float(h["Close"].iloc[-1])
        if np.isfinite(px) and px > 0:
            row["Price"] = px
            an = row.get("_analyzer")
            if isinstance(an, dict):
                an["currentPrice"] = round(px, 4)
    except Exception:
        pass
    try:
        vs = get_volume_signals(h, MFI_PERIOD)
        ts = calculate_technical_signals(h)
        rd = calculate_price_range(h, RANGE_DAYS)
        row["MA50"] = (round(h["Close"].rolling(50).mean().iloc[-1], 2)
                       if len(h) >= 50 else None)
        row["OBV"] = vs["OBV"]
        row["MFI"] = vs["MFI"]
        row["PCV"] = vs["PCV"]
        row["RSI"] = ts["RSI"]
        row["MACD"] = ts["MACD"]
        row["GoldenCross"] = ts["GoldenCross"]
        row["MFISweetSpot"] = ts["MFISweetSpot"]
        row["NoBearDiv"] = ts["NoBearDiv"]
        row["MA50Proximity"] = ts["MA50Proximity"]
        row["RangeHigh"] = rd["RangeHigh"]
        row["RangeLow"] = rd["RangeLow"]
        row["RangePct"] = rd["RangePct"]
        row["RangePos"] = rd["RangePos"]
        row["CleanSetupScore"] = calculate_clean_setup(h)
    except Exception:
        pass


def _download_batch(tickers: list, start: str, end: str) -> dict:
    """One yf.download for the batch. `end` is exclusive (yfinance convention)."""
    out = {}
    if not tickers:
        return out
    kwargs = dict(
        tickers=" ".join(tickers), start=start, end=end, interval="1d",
        actions=True, group_by="ticker", threads=False, progress=False,
        auto_adjust=True,
    )
    dl = pd.DataFrame()
    try:
        dl = yf.download(**kwargs)
    except Exception as e:
        log.warning(f"  Batch download failed ({type(e).__name__}: {e})")
        return out
    if dl is None or getattr(dl, "empty", True):
        return out
    if isinstance(dl.columns, pd.MultiIndex) and dl.columns.nlevels >= 2:
        lv0 = {str(v) for v in dl.columns.get_level_values(0)}
        lv1 = {str(v) for v in dl.columns.get_level_values(1)}
        ticker_level = 0 if any(t in lv0 for t in tickers) else (
            1 if any(t in lv1 for t in tickers) else 0)
        for t in tickers:
            try:
                frame = dl[t] if ticker_level == 0 else dl.xs(t, axis=1, level=1)
            except Exception:
                continue
            h = _normalize_hist(frame)
            if not h.empty:
                out[t] = h
    else:
        h = _normalize_hist(dl)
        if not h.empty:
            out[tickers[0] if len(tickers) == 1 else tickers[0]] = h
    return out


def _load_prev_dump() -> dict:
    """Ticker → last night's record, so a failed fetch does not delete a name."""
    if not os.path.isfile(DATA_FILE):
        return {}
    try:
        with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
            rows = json.load(f)
        return {str(r.get("Ticker") or "").strip().upper(): r
                for r in rows if r.get("Ticker")}
    except Exception as e:
        log.warning(f"  Previous dump unreadable: {e}")
        return {}


FIN_RETRY_MAX = 3
_FIN_EVIDENCE = ("ROIC", "Piotroski", "GrossMargin", "OwnerEarnings", "ROIC_Trend")


def _has_num(v) -> bool:
    if v is None or v == "" or v == "None":
        return False
    try:
        return bool(np.isfinite(float(v)))
    except (TypeError, ValueError):
        return False


def _row_copy(row: dict) -> dict:
    """Copy a dump row without aliasing nested hist/analyzer."""
    return copy.deepcopy(row)


def _hist_ok(row, min_bars: int = 120) -> bool:
    dates = ((row or {}).get("_hist") or {}).get("dates") or []
    return len(dates) >= int(min_bars)


def _info_ok(row) -> bool:
    """Previous row already has a real Yahoo profile (not a rate-limit stub)."""
    if not row:
        return False
    if row.get("_info_ok"):
        return True
    an = row.get("_analyzer") if isinstance(row.get("_analyzer"), dict) else {}
    sector = str(row.get("Sector") or an.get("sector") or "").strip()
    if not sector or sector.lower() in ("unknown", "none", "nan", ""):
        return False
    if not (an.get("shortName") or an.get("longName") or an.get("longBusinessSummary")):
        return False
    if not (_has_num(row.get("MarketCap")) or _has_num(an.get("marketCap"))):
        return False
    return True


def _fin_ok(row) -> bool:
    """Statements were pulled (or we already tried enough times)."""
    if not row:
        return False
    if row.get("_fin_ok"):
        return True
    try:
        if int(row.get("_fin_tries") or 0) >= FIN_RETRY_MAX:
            return True
    except (TypeError, ValueError):
        pass
    return any(_has_num(row.get(k)) for k in _FIN_EVIDENCE)


def _info_from_row(row: dict) -> dict:
    """Rebuild a yfinance-like info dict from a cached dump row."""
    an = row.get("_analyzer") if isinstance(row.get("_analyzer"), dict) else {}
    return {
        "quoteType": "EQUITY",
        "symbol": row.get("Ticker"),
        "shortName": an.get("shortName"),
        "longName": an.get("longName"),
        "longBusinessSummary": an.get("longBusinessSummary"),
        "sector": row.get("Sector") or an.get("sector"),
        "industry": an.get("industry"),
        "currentPrice": row.get("Price") or an.get("currentPrice"),
        "regularMarketPrice": row.get("Price") or an.get("currentPrice"),
        "marketCap": row.get("MarketCap") or an.get("marketCap"),
        "trailingPE": row.get("P/E") or an.get("trailingPE"),
        "revenueGrowth": row.get("RevenueGrowth") or an.get("revenueGrowth"),
        "earningsGrowth": row.get("EarningsGrowth") or an.get("earningsGrowth"),
        "shortPercentOfFloat": row.get("ShortPctFloat") or an.get("shortPercentOfFloat"),
        "shortRatio": row.get("DaysToCover") or an.get("shortRatio"),
        "dividendRate": row.get("DividendRate") or an.get("dividendRate"),
        "dividendYield": an.get("dividendYield"),
        "payoutRatio": row.get("DividendPayoutRatio") or an.get("payoutRatio"),
        "exDividendDate": row.get("ExDividendDate"),
        "trailingAnnualDividendRate": an.get("dividendRate") or row.get("DividendRate"),
        "trailingAnnualDividendYield": an.get("dividendYield"),
    }


def _stamp_info(row: dict, ok: bool) -> None:
    if ok:
        row["_info_ok"] = True


def _stamp_fin(row: dict, got_statements: bool, prev=None) -> None:
    tries = 0
    try:
        tries = int((prev or row).get("_fin_tries") or 0)
    except (TypeError, ValueError):
        tries = 0
    if got_statements:
        row["_fin_ok"] = True
        row["_fin_tries"] = 0
    else:
        tries += 1
        row["_fin_tries"] = tries
        if tries >= FIN_RETRY_MAX:
            row["_fin_ok"] = True


def _field_completeness(rows, planned: str) -> float:
    """Share of expected dump slots that are actually filled."""
    keys = (
        "MarketCap", "P/E", "RevenueGrowth", "EarningsGrowth", "Piotroski",
        "GoldenCross", "ROIC", "DividendYieldPct", "DividendRate",
        "ShortPctFloat", "DaysToCover", "ShortSqueeze", "CleanSetupScore",
        "MFI", "OE_Yield", "PCV", "ROIC_Trend",
    )
    if not rows:
        return 0.0
    ok = 0.0
    slots = 0.0
    for r in rows:
        slots += 2 + len(keys)
        if (_hist_last(r) or "") >= planned:
            ok += 1
        closes = ((r.get("_hist") or {}).get("close") or [])
        if closes and closes[-1] is not None:
            try:
                if np.isfinite(float(closes[-1])):
                    ok += 1
            except (TypeError, ValueError):
                pass
        for k in keys:
            if _has_num(r.get(k)):
                ok += 1
    return round(ok / slots, 4) if slots else 0.0


# ── Ticker loaders ────────────────────────────────────────────────────────────

def _fetch_exchange_tickers(exchange: str) -> list:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; stockscreener-nightly/1.0)",
        "Accept":     "text/plain,application/json,*/*",
    }

    # Source 1 — NASDAQ Trader symbol directory
    try:
        if exchange == "nasdaq":
            url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        else:
            url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

        resp  = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        lines = [l for l in resp.text.strip().splitlines() if l.strip()]

        if len(lines) > 2:
            header = [h.strip() for h in lines[0].split("|")]
            sym_i  = header.index("Symbol")     if "Symbol"     in header else 0
            etf_i  = header.index("ETF")        if "ETF"        in header else None
            test_i = header.index("Test Issue") if "Test Issue" in header else None
            exch_i = header.index("Exchange")   if "Exchange"   in header else None

            tickers = []
            for line in lines[1:]:
                if line.startswith("File Creation Time"):
                    continue
                parts = line.split("|")
                if len(parts) <= sym_i:
                    continue
                sym = parts[sym_i].strip()
                if not sym or sym == "Symbol":
                    continue
                if etf_i  is not None and len(parts) > etf_i  and parts[etf_i].strip()  == "Y":
                    continue
                if test_i is not None and len(parts) > test_i and parts[test_i].strip() == "Y":
                    continue
                if exch_i is not None and exchange == "nyse":
                    exch_val = parts[exch_i].strip() if len(parts) > exch_i else ""
                    if exch_val not in ("N", "A", "P", "Z", "V"):
                        continue
                tickers.append(sym.replace(".", "-"))

            if len(tickers) >= 500:
                log.info(f"  {exchange.upper()}: {len(tickers)} tickers from NASDAQ Trader")
                return tickers
    except Exception as e:
        log.warning(f"  NASDAQ Trader failed for {exchange}: {e}")

    # Source 2 — NASDAQ screener API paginated
    try:
        base     = (f"https://api.nasdaq.com/api/screener/stocks"
                    f"?tableonly=true&limit=1000&exchange={exchange}&offset=")
        all_rows = []
        resp     = requests.get(base + "0", headers=headers, timeout=30)
        resp.raise_for_status()
        table    = resp.json().get("data", {}).get("table", {}) or {}
        raw_tot  = str(table.get("totalrecords") or "0")
        total    = int(raw_tot.replace(",", "").strip() or "0")
        all_rows.extend(table.get("rows") or [])
        for offset in range(1000, total, 1000):
            try:
                pr = requests.get(base + str(offset), headers=headers, timeout=30)
                pr.raise_for_status()
                all_rows.extend(
                    (pr.json().get("data", {}).get("table", {}) or {}).get("rows") or []
                )
            except Exception:
                continue
        tickers = [r["symbol"].strip() for r in all_rows
                   if isinstance(r, dict) and r.get("symbol")]
        if len(tickers) >= 500:
            log.info(f"  {exchange.upper()}: {len(tickers)} tickers from NASDAQ API")
            return tickers
    except Exception as e:
        log.warning(f"  NASDAQ API failed for {exchange}: {e}")

    # Source 3 — SEC EDGAR
    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers_exchange.json",
            headers={"User-Agent": "stockscreener-nightly/1.0 contact@example.com"},
            timeout=30,
        )
        resp.raise_for_status()
        data   = resp.json()
        fields = data.get("fields", [])
        rows   = data.get("data", [])
        exch_i = fields.index("exchange") if "exchange" in fields else 3
        tick_i = fields.index("ticker")   if "ticker"   in fields else 2
        target = "NYSE" if exchange == "nyse" else "Nasdaq"
        tickers = [
            row[tick_i].strip().replace(".", "-")
            for row in rows
            if len(row) > max(exch_i, tick_i)
            and str(row[exch_i]).strip().lower() == target.lower()
            and row[tick_i]
        ]
        log.info(f"  {exchange.upper()}: {len(tickers)} tickers from SEC EDGAR")
        return tickers
    except Exception as e:
        log.warning(f"  SEC EDGAR failed for {exchange}: {e}")
        return []


def _clean_tickers(tickers: list) -> list:
    """
    Remove tickers that yfinance cannot look up:
    - Preferred shares: contain $ (e.g. ABR$E)
    - Test / index symbols: contain ^ or ~
    - NASDAQ 5th-letter series: 5+ character symbols ending W/R/U/WS
      (warrant / right / unit). Do NOT drop 1-4 letter names — LOW
      (Lowe's), CAR, AIR are real equities.
    """
    cleaned = []
    for t in tickers:
        if "$" in t or "^" in t or "~" in t:
            continue
        if t.endswith("WS"):
            continue
        if len(t) >= 5 and t.endswith(("W", "R", "U")):
            continue
        cleaned.append(t)
    return cleaned


def load_all_tickers() -> dict:
    """Returns {exchange_key: [tickers]} for all three exchanges, deduplicated."""
    result = {}

    # S&P 500 — use SEC EDGAR directly (no HTML parser needed, pure JSON)
    # Falls back to Wikipedia with explicit lxml if EDGAR fails.
    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers_exchange.json",
            headers={"User-Agent": "stockscreener-nightly/1.0 contact@example.com"},
            timeout=30,
        )
        resp.raise_for_status()
        data   = resp.json()
        fields = data.get("fields", [])
        rows   = data.get("data", [])
        exch_i = fields.index("exchange") if "exchange" in fields else 3
        tick_i = fields.index("ticker")   if "ticker"   in fields else 2
        # S&P 500 members appear in both NYSE and Nasdaq; use a known S&P list
        # as a cross-reference. For now tag everything as sp500 via Wikipedia.
        raise Exception("Force Wikipedia fallback for S&P 500")
    except Exception:
        pass

    try:
        url  = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
        # Parse HTML table without pandas.read_html to avoid lxml dependency
        import re as _re
        rows_html = _re.findall(r'<tr[^>]*>.*?</tr>', resp.text, _re.DOTALL)
        sp = []
        for row in rows_html:
            cells = _re.findall(r'<td[^>]*>(.*?)</td>', row, _re.DOTALL)
            if cells:
                # First cell is the ticker — strip HTML tags
                raw = _re.sub(r'<[^>]+>', '', cells[0]).strip()
                if raw and (raw.isalpha() or ("-" in raw and raw.replace("-", "").isalpha())):
                    sp.append(raw.replace(".", "-"))
        if len(sp) >= 400:
            result["sp500"] = sp
            log.info(f"  SP500:  {len(sp)} tickers (Wikipedia)")
        else:
            raise Exception(f"Only {len(sp)} tickers parsed")
    except Exception as e:
        log.warning(f"  SP500 load failed: {e} — using empty list")
        result["sp500"] = []

    # NYSE + NASDAQ
    for exch in ("nyse", "nasdaq"):
        raw     = _fetch_exchange_tickers(exch)
        cleaned = _clean_tickers(raw)
        seen, unique = set(), []
        for t in cleaned:
            if t not in seen:
                seen.add(t)
                unique.append(t)
        result[exch] = unique

    return result


# ── Metric compute functions (copied from app.py — keep in sync) ──────────────

def is_etf_or_fund(info: dict) -> bool:
    qt = (info.get("quoteType") or "").lower()
    if qt in ("etf", "mutualfund", "index", "future", "option", "currency", "cryptocurrency"):
        return True
    if qt == "equity":
        return False
    name = (info.get("longName") or info.get("shortName") or "").lower()
    return any(kw in name for kw in ETF_KEYWORDS)


def _get_fin_value(fin, *labels):
    for label in labels:
        if label in fin.index:
            return fin.loc[label]
    return None


def _get_bal_value(bal, *labels):
    for label in labels:
        if label in bal.index:
            return bal.loc[label]
    return None


def get_volume_signals(hist: pd.DataFrame, mfi_period: int) -> dict:
    default = {"OBV": 0.0, "MFI": 0.0, "PCV": 0.0}
    try:
        if hist.empty or len(hist) < 10:
            return default
        close, high, low, vol = hist["Close"], hist["High"], hist["Low"], hist["Volume"]
        mask = vol > 0
        close, high, low, vol = close[mask], high[mask], low[mask], vol[mask]
        if len(close) < 10:
            return default

        # OBV
        direction = np.sign(close.diff().fillna(0))
        obv       = (direction * vol).cumsum()
        obv_win   = min(20, len(obv))
        obv_slope = np.polyfit(range(obv_win), obv.iloc[-obv_win:].values, 1)[0]
        obv_score = 1.0 if obv_slope > 0 else 0.0

        # MFI
        eff  = min(mfi_period, max(5, len(close) // 2))
        tp   = (high + low + close) / 3
        rmf  = tp * vol
        tpd  = tp.diff()
        pos  = rmf.where(tpd > 0, 0).rolling(eff).sum()
        neg  = rmf.where(tpd < 0, 0).rolling(eff).sum()
        ap   = neg == 0
        mfr  = pos / neg.replace(0, np.nan)
        mfi  = 100 - (100 / (1 + mfr))
        mfi  = mfi.where(~(ap & (pos > 0)), 100.0)
        mfi_val = None
        for i in range(1, 6):
            c = mfi.iloc[-i]
            if pd.notnull(c):
                mfi_val = float(c)
                break
        mfi_score = round(mfi_val / 100.0, 4) if mfi_val is not None else 0.0

        # PCV
        pcv_win = min(20, len(close))
        rec     = pd.DataFrame({"Close": close, "Volume": vol}).iloc[-pcv_win:]
        up      = rec["Close"] > rec["Close"].shift(1)
        up_vol  = rec.loc[up, "Volume"].sum()
        tot_vol = rec["Volume"].sum()
        pcv     = max(0.0, (up_vol / tot_vol - 0.5) / 0.5) if tot_vol > 0 else 0.0

        return {"OBV": round(obv_score, 4), "MFI": round(mfi_score, 4), "PCV": round(pcv, 4)}
    except Exception:
        return default


def calculate_technical_signals(hist: pd.DataFrame) -> dict:
    default = {"RSI": 0.0, "MACD": 0.0, "GoldenCross": 0.0,
               "MFISweetSpot": 0.0, "NoBearDiv": 0.5, "MA50Proximity": 0.0}
    try:
        if hist.empty or len(hist) < 26:
            return default
        close = hist["Close"].dropna()
        if len(close) < 26:
            return default

        # RSI
        rsi_score = 0.0
        try:
            delta = close.diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            rs    = gain / loss.replace(0, np.nan)
            rsi_s = 100 - (100 / (1 + rs))
            rv    = rsi_s.dropna()
            if not rv.empty:
                r = rv.iloc[-1]
                if   55 <= r <= 70: rsi_score = 1.0
                elif 50 <= r <  55: rsi_score = 0.6
                elif 70 <  r <= 80: rsi_score = 0.2
        except Exception:
            pass

        # MACD
        macd_score = 0.0
        try:
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            ml    = ema12 - ema26
            sig   = ml.ewm(span=9, adjust=False).mean()
            hm    = (ml - sig).dropna()
            if len(hm) >= 2:
                hn, hp = hm.iloc[-1], hm.iloc[-2]
                if hn > 0 and hn > hp:   macd_score = 1.0
                elif hn > 0:             macd_score = 0.6
                elif hn > hp:            macd_score = 0.2
        except Exception:
            pass

        # Golden Cross
        gc_score = 0.0
        try:
            if len(close) >= 200:
                ma50  = close.rolling(50).mean().iloc[-1]
                ma200 = close.rolling(200).mean().iloc[-1]
                if pd.notnull(ma50) and pd.notnull(ma200) and ma200 > 0:
                    d = (ma50 - ma200) / ma200
                    gc_score = 1.0 if d > 0.02 else (0.5 if d >= -0.02 else 0.0)
        except Exception:
            pass

        # MFI Sweet Spot
        mfi_sweet = 0.0
        try:
            h2, l2 = hist["High"].dropna(), hist["Low"].dropna()
            v2     = hist["Volume"].dropna().where(hist["Volume"] > 0)
            idx    = close.index.intersection(h2.index).intersection(l2.index).intersection(v2.index)
            c2, hh, ll, vv = close[idx], h2[idx], l2[idx], v2[idx]
            tp  = (hh + ll + c2) / 3
            rmf = tp * vv
            tpd = tp.diff()
            pos = rmf.where(tpd > 0, 0).rolling(14).sum()
            neg = rmf.where(tpd < 0, 0).rolling(14).sum()
            mfr = pos / neg.replace(0, np.nan)
            mfi = (100 - (100 / (1 + mfr))).dropna()
            if not mfi.empty:
                mv = mfi.iloc[-1]
                if   55 <= mv <= 75: mfi_sweet = 1.0
                elif 75 <  mv <= 80: mfi_sweet = 0.7
                elif 80 <  mv <= 90: mfi_sweet = 0.3
        except Exception:
            pass

        # No Bearish Divergence
        no_bear = 0.5
        try:
            w = 20
            if len(close) >= w * 2:
                mid     = len(close) - w
                ph      = close.iloc[mid - w:mid].max() < close.iloc[mid:].max()
                h2i, l2i = hist["High"], hist["Low"]
                tp_d    = (h2i + l2i + close) / 3
                rmf_d   = (tp_d * hist["Volume"]).where(hist["Volume"] > 0)
                tpd_d   = tp_d.diff()
                pd_     = rmf_d.where(tpd_d > 0, 0).rolling(14).sum()
                nd_     = rmf_d.where(tpd_d < 0, 0).rolling(14).sum()
                mfi_d   = (100 - (100 / (1 + pd_ / nd_.replace(0, np.nan)))).dropna()
                if len(mfi_d) >= w * 2:
                    mh = mfi_d.iloc[-(w*2):-w].max() < mfi_d.iloc[-w:].max()
                    no_bear = 1.0 if (ph and mh) else (0.0 if (ph and not mh) else 0.5)
        except Exception:
            pass

        # MA50 Proximity
        ma50p = 0.0
        try:
            if len(close) >= 50:
                ma50v = close.rolling(50).mean().iloc[-1]
                pv    = close.iloc[-1]
                if pd.notnull(ma50v) and ma50v > 0:
                    pct = (pv - ma50v) / ma50v
                    if   0.0  <= pct <= 0.05: ma50p = 1.0
                    elif 0.05 <  pct <= 0.10: ma50p = 0.7
                    elif 0.10 <  pct <= 0.20: ma50p = 0.3
        except Exception:
            pass

        return {
            "RSI":           round(rsi_score, 4),
            "MACD":          round(macd_score, 4),
            "GoldenCross":   round(gc_score, 4),
            "MFISweetSpot":  round(mfi_sweet, 4),
            "NoBearDiv":     round(no_bear, 4),
            "MA50Proximity": round(ma50p, 4),
        }
    except Exception:
        return default


def calculate_price_range(hist: pd.DataFrame, range_days: int) -> dict:
    default = {"RangeHigh": None, "RangeLow": None, "RangePct": None, "RangePos": None}
    try:
        if hist.empty or len(hist) < range_days:
            return default
        win = hist["Close"].iloc[-range_days:]
        hi  = round(win.max(), 2)
        lo  = round(win.min(), 2)
        mid = (hi + lo) / 2
        if mid == 0:
            return default
        rp  = round((hi - lo) / mid * 100, 2)
        pos = round((win.iloc[-1] - lo) / (hi - lo), 4) if (hi - lo) > 0 else 0.5
        return {"RangeHigh": hi, "RangeLow": lo, "RangePct": rp, "RangePos": pos}
    except Exception:
        return default


def calculate_piotroski(fin, bal, cf):
    """
    Piotroski-style quality score (6 components), computed component-by-
    component with label fallbacks. Missing rows SKIP that component
    instead of nulling the whole score. Returns None only if we can't
    compute a single component.
    """
    try:
        score, computed = 0, 0

        ni      = _get_fin_value(fin, "Net Income", "NetIncome",
                                 "Net Income Common Stockholders")
        assets  = _get_bal_value(bal, "Total Assets", "TotalAssets")
        ocf     = _get_fin_value(cf,  "Operating Cash Flow", "OperatingCashFlow",
                                 "Total Cash From Operating Activities",
                                 "Cash Flow From Continuing Operating Activities")
        ltd     = _get_bal_value(bal, "Long Term Debt", "LongTermDebt",
                                 "Long Term Debt And Capital Lease Obligation",
                                 "Total Debt")
        ca      = _get_bal_value(bal, "Current Assets", "Total Current Assets",
                                 "CurrentAssets")
        cl      = _get_bal_value(bal, "Current Liabilities",
                                 "Total Current Liabilities", "CurrentLiabilities")

        # 1-2. ROA positive, ROA improving
        if ni is not None and assets is not None and len(ni) >= 1 and len(assets) >= 1:
            roa0 = ni.iloc[0] / assets.iloc[0]
            if pd.notna(roa0):
                computed += 1
                if roa0 > 0: score += 1
                if len(ni) >= 2 and len(assets) >= 2:
                    roa1 = ni.iloc[1] / assets.iloc[1]
                    if pd.notna(roa1):
                        computed += 1
                        if roa0 > roa1: score += 1

        # 3. Operating cash flow positive
        if ocf is not None and len(ocf) >= 1 and pd.notna(ocf.iloc[0]):
            computed += 1
            if ocf.iloc[0] > 0: score += 1

        # 4. Accruals: OCF > Net Income
        if (ocf is not None and ni is not None and len(ocf) >= 1 and len(ni) >= 1
                and pd.notna(ocf.iloc[0]) and pd.notna(ni.iloc[0])):
            computed += 1
            if ocf.iloc[0] > ni.iloc[0]: score += 1

        # 5. Leverage decreasing
        if ltd is not None and len(ltd) >= 2 and pd.notna(ltd.iloc[0]) and pd.notna(ltd.iloc[1]):
            computed += 1
            if ltd.iloc[0] < ltd.iloc[1]: score += 1

        # 6. Current ratio improving
        if (ca is not None and cl is not None and len(ca) >= 2 and len(cl) >= 2
                and cl.iloc[0] and cl.iloc[1]):
            cr0, cr1 = ca.iloc[0] / cl.iloc[0], ca.iloc[1] / cl.iloc[1]
            if pd.notna(cr0) and pd.notna(cr1):
                computed += 1
                if cr0 > cr1: score += 1

        return score if computed >= 3 else None
    except Exception:
        return None


def get_owner_earnings(cf, fin, info):
    """
    Buffett owner earnings = Net Income + D&A - CapEx, with full label
    fallbacks (yfinance uses several D&A row names; 'Depreciation' alone
    is rare). Missing D&A degrades gracefully to NI - CapEx.
    """
    try:
        ni_s  = _get_fin_value(fin, "Net Income", "NetIncome",
                               "Net Income Common Stockholders")
        # D&A lives in the cash-flow statement under many names
        da_s  = _get_fin_value(cf, "Depreciation And Amortization",
                               "Depreciation Amortization Depletion",
                               "Reconciled Depreciation", "Depreciation",
                               "DepreciationAndAmortization")
        cap_s = _get_fin_value(cf, "Capital Expenditure", "Capital Expenditures",
                               "CapitalExpenditure")
        if ni_s is None or cap_s is None or not len(ni_s) or not len(cap_s):
            return None, None
        ni  = ni_s.iloc[0]
        cap = cap_s.iloc[0]
        da  = da_s.iloc[0] if (da_s is not None and len(da_s)) else 0
        if pd.isna(ni) or pd.isna(cap):
            return None, None
        if pd.isna(da):
            da = 0
        oe = ni + da - abs(cap)
        mc = info.get("marketCap")
        return oe, (oe / mc if mc else None)
    except Exception:
        return None, None


def calculate_gross_margin(fin) -> float | None:
    """
    Gross margin = Gross Profit / Total Revenue, with label fallbacks.
    Falls back to (Revenue - Cost of Revenue) / Revenue when Gross Profit
    isn't reported. Returns a 0-1 ratio (0.75 = 75%) or None.
    """
    try:
        rev = _get_fin_value(fin, "Total Revenue", "TotalRevenue", "Operating Revenue")
        if rev is None or not len(rev) or not rev.iloc[0]:
            return None
        gp = _get_fin_value(fin, "Gross Profit", "GrossProfit")
        if gp is not None and len(gp) and pd.notna(gp.iloc[0]):
            m = gp.iloc[0] / rev.iloc[0]
        else:
            cogs = _get_fin_value(fin, "Cost Of Revenue", "CostOfRevenue",
                                  "Reconciled Cost Of Revenue")
            if cogs is None or not len(cogs) or pd.isna(cogs.iloc[0]):
                return None
            m = (rev.iloc[0] - cogs.iloc[0]) / rev.iloc[0]
        return round(float(m), 4) if -1 < m < 1.5 else None
    except Exception:
        return None


def _invested_capital(bal, i=0):
    """Invested capital with a fallback ladder.

    Banks, insurers and other financials publish an UNCLASSIFIED balance sheet:
    there is no "Total Current Liabilities" line at all. The old formula
    required it, so ~77% of Financial Services names returned None for ROIC
    (a quarter of the whole universe). Ladder, best first:
      1. Assets - Current Liabilities - Cash   (classic, non-financials)
      2. Total Debt + Total Equity             (capital actually employed —
                                                works for ANY balance sheet)
      3. Total Assets - Total Liabilities + Total Debt
    """
    def g(*labels):
        s = _get_bal_value(bal, *labels)
        if s is None or i >= len(s):
            return None
        v = s.iloc[i]
        return None if pd.isna(v) else float(v)

    assets = g("Total Assets", "TotalAssets")
    cur_li = g("Total Current Liabilities", "TotalCurrentLiabilities",
               "Current Liabilities", "CurrentLiabilities")
    cash = g("Cash And Cash Equivalents",
             "Cash Cash Equivalents And Short Term Investments",
             "CashAndCashEquivalents", "Cash") or 0.0

    # 1. classic
    if assets is not None and cur_li is not None:
        ic = assets - cur_li - cash
        if ic and not pd.isna(ic):
            return ic

    # 2. debt + equity — the definition that works for financials
    equity = g("Total Equity Gross Minority Interest", "Stockholders Equity",
               "Total Stockholder Equity", "StockholdersEquity",
               "Common Stock Equity")
    debt = g("Total Debt", "TotalDebt")
    if debt is None:
        ld = g("Long Term Debt", "LongTermDebt") or 0.0
        sd = g("Current Debt", "Short Long Term Debt", "CurrentDebt",
               "Short Term Debt") or 0.0
        debt = (ld + sd) if (ld or sd) else None
    if equity is not None:
        ic = equity + (debt or 0.0)
        if ic and not pd.isna(ic) and ic > 0:
            return ic

    # 3. assets - total liabilities + debt
    tot_li = g("Total Liabilities Net Minority Interest", "Total Liabilities",
               "TotalLiabilities")
    if assets is not None and tot_li is not None:
        ic = assets - tot_li + (debt or 0.0)
        if ic and not pd.isna(ic) and ic > 0:
            return ic
    return None


def calculate_roic(fin, bal):
    try:
        ebit_s = _get_fin_value(fin, "EBIT","Ebit","Operating Income","OperatingIncome",
                                "Operating Revenue","EBITDA","Ebitda")
        if ebit_s is None:
            # financials often report Pretax Income but no Operating Income
            ebit_s = _get_fin_value(fin, "Pretax Income", "Income Before Tax",
                                    "PretaxIncome", "Net Income")
        if ebit_s is None or len(ebit_s) == 0: return None
        ic = _invested_capital(bal, 0)
        if ic is None or ic == 0: return None
        ebit = ebit_s.iloc[0]
        if pd.isna(ebit): return None
        roic = float(ebit) * 0.79 / ic
        return None if pd.isna(roic) else roic
    except Exception:
        return None


def calculate_roic_trend(fin, bal):
    """Year-over-year change in ROIC, using the same invested-capital ladder
    so financials get a trend too (previously None for ~77% of them)."""
    try:
        ebit_s = _get_fin_value(fin, "EBIT","Ebit","Operating Income","OperatingIncome",
                                "EBITDA","Ebitda")
        if ebit_s is None:
            ebit_s = _get_fin_value(fin, "Pretax Income", "Income Before Tax",
                                    "PretaxIncome", "Net Income")
        if ebit_s is None or len(ebit_s) < 2: return None
        def roic_at(i):
            ic = _invested_capital(bal, i)
            if ic is None or ic == 0 or i >= len(ebit_s): return None
            v = ebit_s.iloc[i]
            if pd.isna(v): return None
            return float(v) / ic
        r0, r1 = roic_at(0), roic_at(1)
        return (r0 - r1) if (r0 is not None and r1 is not None) else None
    except Exception:
        return None


# ── Short interest ───────────────────────────────────────────────────────────

def _normalize_yield(v):
    """Return a decimal yield (0.0312 = 3.12%) from either format.

    yfinance changed `dividendYield` from a decimal to a percentage in 2025,
    while `trailingAnnualDividendYield` stayed a decimal. Feeding a
    percent-scaled value into `* 100` produced yields like 312% or 15010%,
    which the calendar app then silently discarded via its >50% filter.
    """
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    if v > 1.0:            # value arrived percent-scaled
        v = v / 100.0
    return v if v <= 0.60 else None


def calculate_dividend_score(info: dict, dividends_history=None, price=None) -> dict:
    """Compute composite dividend quality score — same logic as app.py."""
    default = {
        "DividendYieldPct":  None,
        "DividendRate":      None,
        "PayoutRatio":       None,
        "DividendFrequency": "None",
        "DividendScore":     0.0,
        "DividendBasis":     None,
    }
    try:
        # Keep yield and rate on the SAME basis. Mixing a trailing yield with a
        # forward rate makes DividendYieldPct disagree with DividendRate/Price
        # downstream. Prefer the trailing pair; fall back to the forward pair.
        t_yield = _normalize_yield(info.get("trailingAnnualDividendYield"))
        t_rate  = info.get("trailingAnnualDividendRate")
        f_yield = _normalize_yield(info.get("dividendYield"))
        f_rate  = info.get("dividendRate")
        if t_yield is not None or t_rate:
            yield_raw, div_rate, basis = t_yield, t_rate, "trailing"
        else:
            yield_raw, div_rate, basis = f_yield, f_rate, "forward"
        if yield_raw is None and t_yield is None:
            yield_raw = f_yield
        if not div_rate:
            div_rate = t_rate or f_rate
        payout = info.get("payoutRatio")
        if not yield_raw and not div_rate:
            return default
        try:
            div_rate = float(div_rate) if div_rate else None
        except (TypeError, ValueError):
            div_rate = None
        # Ground truth: rate / price. Keeps the stored yield consistent with the
        # stored rate so the app never shows a yield that contradicts Div/Share.
        implied = None
        try:
            if div_rate and price and float(price) > 0:
                implied = div_rate / float(price)
        except (TypeError, ValueError):
            implied = None
        if implied is not None and 0 < implied <= 0.60:
            yield_raw = implied
        yield_pct = round(yield_raw * 100, 2) if yield_raw else None
        y = yield_pct or 0.0
        if y >= 8:    yield_score = 0.60
        elif y >= 6:  yield_score = 0.55
        elif y >= 4:  yield_score = 0.45
        elif y >= 3:  yield_score = 0.35
        elif y >= 2:  yield_score = 0.20
        elif y >= 1:  yield_score = 0.10
        else:         yield_score = 0.0
        if payout is None:
            payout_score = 0.10
        else:
            p = payout * 100
            if p <= 0:      payout_score = 0.0
            elif p <= 40:   payout_score = 0.20
            elif p <= 60:   payout_score = 0.18
            elif p <= 80:   payout_score = 0.12
            elif p <= 100:  payout_score = 0.05
            else:           payout_score = 0.0
        freq_label = "None"
        freq_score = 0.0
        if dividends_history is not None and not dividends_history.empty:
            try:
                _idx = dividends_history.index
                if getattr(_idx, "tz", None) is not None:
                    dividends_history = dividends_history.tz_localize(None) \
                        if not hasattr(_idx, "tz_convert") \
                        else dividends_history.copy()
                    dividends_history.index = _idx.tz_convert(None) \
                        if _idx.tz is not None else _idx
                one_yr_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
                recent = dividends_history[dividends_history.index >= one_yr_ago]
                n = len(recent)
                if n >= 10:
                    freq_label = "Monthly";     freq_score = 0.20
                elif n >= 3:
                    freq_label = "Quarterly";   freq_score = 0.15
                elif n == 2:
                    freq_label = "Semi-Annual"; freq_score = 0.10
                elif n == 1:
                    freq_label = "Annual";      freq_score = 0.05
                else:
                    freq_label = "Irregular";   freq_score = 0.03
            except Exception:
                freq_label = "Unknown"; freq_score = 0.05
        elif info.get("dividendRate") and yield_raw:
            freq_label = "Quarterly (est)"; freq_score = 0.12
        composite = round(min(yield_score + payout_score + freq_score, 1.0), 4)
        return {
            "DividendYieldPct":  yield_pct,
            "DividendRate":      round(div_rate, 4) if div_rate else None,
            "PayoutRatio":       round(payout * 100, 1) if payout is not None else None,
            "DividendFrequency": freq_label,
            "DividendScore":     composite,
            "DividendBasis":     basis,
        }
    except Exception:
        return default


def calculate_clean_setup(hist) -> float:
    """
    0-1 bull-pattern score, porting the empirically tuned CleanSetup.pine:
      pivot length 12 · RSI band 40-60 · pullback <5% · pole 5-20%
      liquidity gate: price >= $5 and 20-day avg volume >= 250k
      trend alignment: close > EMA50 and EMA50 > EMA200 (EMA100 fallback
      when fewer than 200 bars are available)

    Components (weights sum to 1.0):
      trend alignment  0.30   the pine script's requireTrend condition
      bull flag        0.25   pole 5-20% with pullback <5% from 12-bar high
      higher-low seq   0.20   last two pivot-12 lows ascending
      RSI 40-60 band   0.15   consolidating, not overbought/oversold
      volume confirm   0.10   last bar volume > 20-bar average
    Liquidity gate failure returns 0.0 (hard filter, as in the script).
    """
    try:
        if hist is None or len(hist) < 60:
            return 0.0
        close = hist["Close"]
        vol   = hist["Volume"]
        price = float(close.iloc[-1])
        avg_vol = float(vol.tail(20).mean())
        if price < 5 or avg_vol < 250_000:
            return 0.0

        # ── trend alignment ────────────────────────────────────
        ema50 = close.ewm(span=50, adjust=False).mean()
        long_span = 200 if len(close) >= 200 else 100
        ema_long  = close.ewm(span=long_span, adjust=False).mean()
        trend = price > ema50.iloc[-1] > ema_long.iloc[-1]

        # ── RSI(14) band 40-60 ─────────────────────────────────
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, np.nan)
        rsi   = (100 - 100 / (1 + rs)).iloc[-1]
        rsi_band = bool(pd.notna(rsi) and 40 <= rsi <= 60)

        # ── volume confirmation ────────────────────────────────
        vol_conf = bool(vol.iloc[-1] > avg_vol)

        # ── bull flag: pole 5-20%, pullback <5% from 12-bar high ─
        hi12 = float(close.tail(12).max())
        pullback = (hi12 - price) / hi12 if hi12 > 0 else 1.0
        pole_base = float(close.iloc[-32]) if len(close) >= 32 else float(close.iloc[0])
        pole = hi12 / pole_base - 1 if pole_base > 0 else 0.0
        flag = bool(0.05 <= pole <= 0.20 and pullback < 0.05)

        # ── higher-low sequence, pivot length 12 (vectorized) ───
        lows = hist["Low"]
        roll_min = lows.rolling(25, center=True).min()
        piv_mask = (lows == roll_min) & roll_min.notna()
        piv_lows = lows[piv_mask]
        # collapse consecutive equal pivots
        piv_vals = piv_lows[piv_lows.diff().fillna(1) != 0].tolist()
        higher_low = bool(len(piv_vals) >= 2 and piv_vals[-1] > piv_vals[-2])

        score = (0.30 * trend + 0.25 * flag + 0.20 * higher_low
                 + 0.15 * rsi_band + 0.10 * vol_conf)
        return round(float(score), 4)
    except Exception:
        return 0.0


def calculate_short_squeeze(info: dict) -> dict:
    """Compute short interest metrics from yfinance info dict. No extra API calls."""
    default = {
        "ShortPctFloat":    None,
        "DaysToCover":      None,
        "ShortChange":      None,
        "ShortSqueeze":     0.0,
        "ShortPctFloatRaw": None,
    }
    try:
        spf   = info.get("shortPercentOfFloat")
        dtc   = info.get("shortRatio")
        ss    = info.get("sharesShort")
        ss_pm = info.get("sharesShortPriorMonth")

        short_change = None
        if ss and ss_pm and ss_pm > 0:
            short_change = round((ss - ss_pm) / ss_pm, 4)

        squeeze = 0.0
        if spf is not None:
            spf_pct = spf * 100
            if spf_pct >= 20:   squeeze += 0.5
            elif spf_pct >= 10: squeeze += 0.3
            elif spf_pct >= 5:  squeeze += 0.15
        if dtc is not None:
            if dtc >= 10:   squeeze += 0.3
            elif dtc >= 5:  squeeze += 0.2
            elif dtc >= 3:  squeeze += 0.1
        if short_change is not None and short_change < -0.05:
            squeeze += 0.2
        squeeze = min(round(squeeze, 4), 1.0)

        return {
            "ShortPctFloat":    round(spf, 4) if spf is not None else None,
            "DaysToCover":      round(dtc, 1) if dtc is not None else None,
            "ShortChange":      short_change,
            "ShortSqueeze":     squeeze,
            "ShortPctFloatRaw": round(spf * 100, 1) if spf is not None else None,
        }
    except Exception:
        return default


# ── Per-ticker worker ─────────────────────────────────────────────────────────

def process_ticker(args):
    """
    args = (ticker, mfi_period, range_days, prefetched_hist, prev_row)
    prefetched_hist: OHLCV(+Dividends) from the batch yf.download (often just
    the last two weeks when last night's dump already had a long history).
    prev_row: last night's record — profile/financials are reused when present
    so Yahoo bandwidth goes to missing fields and the latest session.
    """
    t, mfi_period, range_days, pre_hist, prev_row = args
    prev_row = prev_row if isinstance(prev_row, dict) else None
    fetch_info = not _info_ok(prev_row)
    fetch_fin = not _fin_ok(prev_row)
    hist = _merge_hist(
        _df_from_cache((prev_row or {}).get("_hist")),
        pre_hist if isinstance(pre_hist, pd.DataFrame) else pd.DataFrame(),
    )

    # Nothing missing from Yahoo — splice new bars locally and stop.
    if prev_row and not fetch_info and not fetch_fin:
        row = _row_copy(prev_row)
        if not hist.empty:
            _apply_hist_to_row(row, hist)
        return row

    # 3 attempts with increasing backoff — nightly job has time to spare.
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(attempt * 5 + random.uniform(0, 3))

            need_stock = fetch_info or fetch_fin or hist.empty or len(hist) < 30
            stock = yf.Ticker(t) if need_stock else None
            _av_key = _get_av_key() if _AV_AVAILABLE else ""
            info = {}
            if fetch_info and stock is not None:
                info = stock.info or {}

            # Valid info has quoteType/symbol; rate-limited stubs have 1-2 null keys
            if fetch_info:
                if not (info.get("quoteType") or info.get("symbol") or len(info) >= 10):
                    if _AV_AVAILABLE and _av_key:
                        try:
                            info = av_fill_info(t, info, _av_key)
                        except Exception:
                            pass
                    if not info or len(info) < 5:
                        if prev_row:
                            info = _info_from_row(prev_row)
                            fetch_info = False
                        elif attempt < 2:
                            time.sleep(10 + random.uniform(0, 5))
                            continue
                        else:
                            return None

                if fetch_info and _AV_AVAILABLE and _av_key and av_needs_fallback(info):
                    try:
                        info = av_fill_info(t, info, _av_key)
                    except Exception:
                        pass

                if fetch_info and is_etf_or_fund(info):
                    return None
            else:
                info = _info_from_row(prev_row or {})

            price = info.get("currentPrice") or info.get("regularMarketPrice")

            # Batch hist (already merged with last night) is enough for most
            # names. Only hit Yahoo again when we still don't have 30 bars.
            if (hist.empty or len(hist) < 30) and stock is not None:
                try:
                    hist = _merge_hist(hist, stock.history(
                        period="1y", actions=True, auto_adjust=True))
                    if hist.empty or len(hist) < 30:
                        hist = _merge_hist(hist, stock.history(
                            period="6mo", actions=True, auto_adjust=True))
                except Exception:
                    hist = hist if isinstance(hist, pd.DataFrame) else pd.DataFrame()
                try:
                    hist = _merge_hist(hist, stock.history(
                        period="10d", actions=True, auto_adjust=True))
                except Exception:
                    pass

            # AV fallback for missing history
            if _AV_AVAILABLE and _av_key and av_needs_history_fallback(hist):
                try:
                    av_hist = av_fill_history(t, _av_key)
                    if av_hist is not None and not av_hist.empty:
                        hist = _merge_hist(hist, av_hist)
                except Exception:
                    pass

            if not price and not hist.empty:
                try:
                    price = float(hist["Close"].iloc[-1])
                except Exception:
                    price = None
            if not price:
                if prev_row and _has_num(prev_row.get("Price")):
                    price = prev_row.get("Price")
                else:
                    return None

            fin = bal = cf = pd.DataFrame()
            got_statements = False
            if fetch_fin and stock is not None:
                try:
                    fin = stock.financials
                except Exception:
                    fin = pd.DataFrame()
                try:
                    bal = stock.balance_sheet
                except Exception:
                    bal = pd.DataFrame()
                try:
                    cf = stock.cashflow
                except Exception:
                    cf = pd.DataFrame()
                if _AV_AVAILABLE and _av_key and av_needs_financials_fallback(fin, bal, cf):
                    try:
                        av_fin, av_bal, av_cf = av_fill_financials(t, _av_key)
                        if fin.empty and not av_fin.empty:   fin = av_fin
                        if bal.empty and not av_bal.empty:   bal = av_bal
                        if cf.empty  and not av_cf.empty:    cf  = av_cf
                    except Exception:
                        pass
                got_statements = not (
                    getattr(fin, "empty", True)
                    and getattr(bal, "empty", True)
                    and getattr(cf, "empty", True)
                )

            # Dividends ride along in the history frame (actions=True) —
            # saves one HTTP request per ticker vs stock.dividends
            try:
                if "Dividends" in hist.columns:
                    div_hist = hist["Dividends"][hist["Dividends"] > 0]
                elif fetch_info and stock is not None:
                    div_hist = stock.dividends
                else:
                    div_hist = pd.Series(dtype=float)
            except Exception:
                div_hist = pd.Series(dtype=float)

            # yfinance often appends a session stub after the close:
            # Volume is filled, Open/High/Low/Close are NaN. If we keep
            # that bar, Money Weather's last date is an empty day, it
            # forward-fills yesterday's close, and every sector prints 0%.
            hist = _normalize_hist(hist)

            vol_signals  = get_volume_signals(hist, mfi_period)
            tech_signals = calculate_technical_signals(hist)
            range_data   = calculate_price_range(hist, range_days)
            if fetch_info:
                short_data = calculate_short_squeeze(info)
                div_data = calculate_dividend_score(
                    info, div_hist if not div_hist.empty else None, price)
            else:
                short_data = {
                    "ShortPctFloat": (prev_row or {}).get("ShortPctFloat"),
                    "ShortPctFloatRaw": (prev_row or {}).get("ShortPctFloatRaw"),
                    "DaysToCover": (prev_row or {}).get("DaysToCover"),
                    "ShortChange": (prev_row or {}).get("ShortChange"),
                    "ShortSqueeze": (prev_row or {}).get("ShortSqueeze"),
                }
                div_data = {
                    "DividendYieldPct": (prev_row or {}).get("DividendYieldPct"),
                    "DividendRate": (prev_row or {}).get("DividendRate"),
                    "PayoutRatio": (prev_row or {}).get("DividendPayoutRatio"),
                    "DividendFrequency": (prev_row or {}).get("DividendFrequency"),
                    "DividendScore": (prev_row or {}).get("DividendScore"),
                    "DividendBasis": (prev_row or {}).get("DividendBasis"),
                }
            clean_setup  = calculate_clean_setup(hist)
            if fetch_fin:
                gross_margin = calculate_gross_margin(fin)
                owner_earnings, oe_yield = get_owner_earnings(cf, fin, info)
                roic = calculate_roic(fin, bal)
                roic_trend = calculate_roic_trend(fin, bal)
                piotroski = calculate_piotroski(fin, bal, cf)
            else:
                gross_margin = (prev_row or {}).get("GrossMargin")
                owner_earnings = (prev_row or {}).get("OwnerEarnings")
                oe_yield = (prev_row or {}).get("OE_Yield")
                roic = (prev_row or {}).get("ROIC")
                roic_trend = (prev_row or {}).get("ROIC_Trend")
                piotroski = (prev_row or {}).get("Piotroski")
            ma50         = (round(hist["Close"].rolling(50).mean().iloc[-1], 2)
                            if len(hist) >= 50 else None)

            hist_cache = _cache_from_df(hist)

            # ── ANALYZER PACK ────────────────────────────────────────
            # Everything the Money Weather "Stock Lookup" tab renders. It used
            # to fetch these live from yfinance on every lookup, which is slow
            # and rate-limited. `info`, `fin`, `bal` and `cf` are ALREADY
            # loaded above for the scoring, so caching them costs no extra API
            # calls — the app can then render a full analysis offline and only
            # hit the live feeds for an up-to-the-minute price.
            def _num(x):
                try:
                    f = float(x)
                    return f if np.isfinite(f) else None
                except (TypeError, ValueError):
                    return None

            if fetch_info:
                analyzer = {
                    "shortName":     info.get("shortName") or info.get("longName"),
                    "longName":      info.get("longName"),
                    "longBusinessSummary": info.get("longBusinessSummary"),
                    "website":       info.get("website"),
                    "fullTimeEmployees": _num(info.get("fullTimeEmployees")),
                    "city":          info.get("city"),
                    "state":         info.get("state"),
                    "country":       info.get("country"),
                    "exchange":      info.get("exchange"),
                    "sector":        info.get("sector"),
                    "industry":      info.get("industry"),
                    "currentPrice":  _num(price),
                    "trailingPE":    _num(info.get("trailingPE")),
                    "marketCap":     _num(info.get("marketCap")),
                    "beta":          _num(info.get("beta")),
                    "forwardPE":     _num(info.get("forwardPE")),
                    "priceToBook":   _num(info.get("priceToBook")),
                    "priceToSales":  _num(info.get("priceToSalesTrailing12Months")),
                    "fiftyTwoWeekHigh": _num(info.get("fiftyTwoWeekHigh")),
                    "fiftyTwoWeekLow":  _num(info.get("fiftyTwoWeekLow")),
                    "profitMargins":    _num(info.get("profitMargins")),
                    "operatingMargins": _num(info.get("operatingMargins")),
                    "grossMargins":     _num(info.get("grossMargins")),
                    "returnOnEquity":   _num(info.get("returnOnEquity")),
                    "returnOnAssets":   _num(info.get("returnOnAssets")),
                    "debtToEquity":     _num(info.get("debtToEquity")),
                    "currentRatio":     _num(info.get("currentRatio")),
                    "quickRatio":       _num(info.get("quickRatio")),
                    "freeCashflow":     _num(info.get("freeCashflow")),
                    "operatingCashflow": _num(info.get("operatingCashflow")),
                    "totalRevenue":     _num(info.get("totalRevenue")),
                    "targetMeanPrice":  _num(info.get("targetMeanPrice")),
                    "targetLowPrice":   _num(info.get("targetLowPrice")),
                    "targetHighPrice":  _num(info.get("targetHighPrice")),
                    "numberOfAnalystOpinions": _num(info.get("numberOfAnalystOpinions")),
                    "recommendationKey": info.get("recommendationKey"),
                    "sharesOutstanding": _num(info.get("sharesOutstanding")),
                    "floatShares":       _num(info.get("floatShares")),
                    "epsTrailingTwelveMonths": _num(info.get("trailingEps")),
                    "epsForward":        _num(info.get("forwardEps")),
                    "shortPercentOfFloat": _num(info.get("shortPercentOfFloat")),
                    "shortRatio":        _num(info.get("shortRatio")),
                    "revenueGrowth":     _num(info.get("revenueGrowth")),
                    "earningsGrowth":    _num(info.get("earningsGrowth")),
                    "dividendRate":      _num(info.get("dividendRate")),
                    "dividendYield":     _num(info.get("dividendYield")),
                    "grossMarginCalc":   gross_margin,
                }
            else:
                analyzer = dict((prev_row or {}).get("_analyzer") or {})
                analyzer["currentPrice"] = _num(price)
                if gross_margin is not None:
                    analyzer["grossMarginCalc"] = gross_margin
            # Official first print when Yahoo has it; else first bar we stored.
            _ft = None
            for _fk in ("firstTradeDateEpochUtc", "firstTradeDateMilliseconds"):
                _fv = info.get(_fk)
                if _fv:
                    try:
                        _sec = float(_fv)
                        if _sec > 1e12:
                            _sec /= 1000.0
                        if _sec > 0:
                            _ft = datetime.fromtimestamp(_sec, tz=timezone.utc).strftime("%Y-%m-%d")
                            break
                    except (TypeError, ValueError, OSError, OverflowError):
                        pass
            if not _ft and hist_cache.get("dates"):
                _ft = hist_cache["dates"][0]
            if not _ft and prev_row:
                _ft = prev_row.get("FirstTradeDate") or (analyzer.get("firstTradeDate"))
            analyzer["firstTradeDate"] = _ft
            # quarterly EPS history — only when we already paid for a live profile
            if fetch_info and stock is not None:
                try:
                    eh = getattr(stock, "earnings_history", None)
                    if eh is not None and hasattr(eh, "empty") and not eh.empty:
                        cmap = {c.lower(): c for c in eh.columns}
                        ac, ec = cmap.get("epsactual"), cmap.get("epsestimate")
                        sc = cmap.get("surprisepercent")
                        rows_eps = []
                        for idx_, row_ in eh.tail(8).iterrows():
                            try:
                                q = pd.to_datetime(idx_, errors="coerce")
                                ql = q.strftime("%b %Y") if pd.notna(q) else str(idx_)
                            except Exception:
                                ql = str(idx_)
                            rows_eps.append({
                                "quarter":  ql,
                                "actual":   _num(row_.get(ac)) if ac else None,
                                "estimate": _num(row_.get(ec)) if ec else None,
                                "surprise": _num(row_.get(sc)) if sc else None,
                            })
                        analyzer["eps_history"] = rows_eps
                except Exception:
                    pass

            out = {
                "Ticker":         t,
                "Sector":         (info.get("sector")
                                   or (prev_row or {}).get("Sector")
                                   or "Unknown"),
                "_analyzer":      analyzer,
                "Price":          price,
                "MarketCap":      info.get("marketCap") if fetch_info else (prev_row or {}).get("MarketCap"),
                "P/E":            info.get("trailingPE") if fetch_info else (prev_row or {}).get("P/E"),
                "OwnerEarnings":  owner_earnings,
                "OE_Yield":       oe_yield,
                "ROIC":           roic,
                "ROIC_Trend":     roic_trend,
                "RevenueGrowth":  info.get("revenueGrowth") if fetch_info else (prev_row or {}).get("RevenueGrowth"),
                "EarningsGrowth": info.get("earningsGrowth") if fetch_info else (prev_row or {}).get("EarningsGrowth"),
                "Piotroski":      piotroski,
                "MA50":           ma50,
                "OBV":            vol_signals["OBV"],
                "MFI":            vol_signals["MFI"],
                "PCV":            vol_signals["PCV"],
                "RSI":            tech_signals["RSI"],
                "MACD":           tech_signals["MACD"],
                "GoldenCross":    tech_signals["GoldenCross"],
                "MFISweetSpot":   tech_signals["MFISweetSpot"],
                "NoBearDiv":      tech_signals["NoBearDiv"],
                "MA50Proximity":  tech_signals["MA50Proximity"],
                "RangeHigh":      range_data["RangeHigh"],
                "RangeLow":       range_data["RangeLow"],
                "RangePct":       range_data["RangePct"],
                "RangePos":       range_data["RangePos"],
                "ShortPctFloat":  short_data["ShortPctFloat"],
                "ShortPctFloatRaw": short_data["ShortPctFloatRaw"],
                "DaysToCover":    short_data["DaysToCover"],
                "ShortChange":    short_data["ShortChange"],
                "ShortSqueeze":   short_data["ShortSqueeze"],
                "DividendYieldPct":    div_data["DividendYieldPct"],
                "DividendRate":        div_data["DividendRate"],
                "DividendPayoutRatio": div_data["PayoutRatio"],
                "DividendFrequency":   div_data["DividendFrequency"],
                "DividendScore":       div_data["DividendScore"],
                "DividendBasis":        div_data.get("DividendBasis"),
                "CleanSetupScore":     clean_setup,
                "GrossMargin":         gross_margin,
                "ExDividendDate":      info.get("exDividendDate") if fetch_info else (prev_row or {}).get("ExDividendDate"),
                "FirstTradeDate":      _ft,
                "_hist":          hist_cache,
                "_exchange":      "",
            }
            if prev_row:
                for _k, _v in prev_row.items():
                    if _k not in out and _k not in ("_hist", "_analyzer"):
                        out[_k] = _v
            if fetch_info:
                _stamp_info(out, True)
            elif prev_row:
                if prev_row.get("_info_ok"):
                    out["_info_ok"] = True
            if fetch_fin:
                _stamp_fin(out, got_statements, prev_row)
            elif prev_row:
                if prev_row.get("_fin_ok"):
                    out["_fin_ok"] = True
                if prev_row.get("_fin_tries") is not None:
                    out["_fin_tries"] = prev_row.get("_fin_tries")
            return out

        except Exception as e:
            if attempt == 2:
                log.debug(f"    {t}: {type(e).__name__}: {e}")
                return None
            continue

    return None  # all attempts exhausted

# ── Scan one exchange ─────────────────────────────────────────────────────────

# (scan_exchange removed — main() owns the batch loop)

# ── Main ──────────────────────────────────────────────────────────────────────

def _get_av_key() -> str:
    """Read AV_API_KEY from environment (nightly scan runs as GitHub Action)."""
    return os.environ.get("AV_API_KEY", "")


def main():
    start_utc = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info(f"Nightly scan started  {start_utc.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── 1. Load tickers for all exchanges ────────────────────────────
    log.info("Loading ticker lists...")
    all_tickers = load_all_tickers()

    # Deduplicate across exchanges (a ticker in S&P 500 is also in NYSE)
    # We process each ticker once and tag it with all exchanges it belongs to
    ticker_to_exchanges = {}
    for exch, tickers in all_tickers.items():
        for t in tickers:
            ticker_to_exchanges.setdefault(t, set()).add(exch)

    unique_tickers = list(ticker_to_exchanges.keys())
    log.info(f"Unique tickers across all exchanges: {len(unique_tickers)}")
    for exch, tl in all_tickers.items():
        log.info(f"  {exch.upper()}: {len(tl)}")

    planned = _last_completed_session()
    hist_end = (pd.Timestamp(planned) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    hist_start = (pd.Timestamp(planned) - pd.Timedelta(days=400)).strftime("%Y-%m-%d")
    log.info(f"Target session (last completed US close): {planned}  "
             f"history window {hist_start} → {hist_end} (end exclusive)")
    prev_dump = _load_prev_dump()
    if prev_dump:
        log.info(f"  Previous dump on disk: {len(prev_dump)} tickers "
                 f"(reuse what is already complete; fetch only holes)")

    # ── 2. Classify: skip Yahoo for fields last night already filled ──
    # A name that already has profile + financials + the target session
    # costs zero bandwidth. A name with a long history only downloads
    # the last ~14 days. Profile/statements are fetched only when missing.
    classified = {}
    incremental_tickers = []
    full_hist_tickers = []
    fetch_tickers = []
    n_reuse = n_hist_only = n_fetch = n_skip_hist = 0
    n_need_info = n_need_fin = 0
    for t in unique_tickers:
        prev = prev_dump.get(t)
        info_ok = _info_ok(prev)
        fin_ok = _fin_ok(prev)
        hist_ok = _hist_ok(prev)
        on_tgt = bool(hist_ok and (_hist_last(prev) or "") >= planned)
        if not info_ok:
            n_need_info += 1
        if not fin_ok:
            n_need_fin += 1
        if info_ok and fin_ok and on_tgt:
            classified[t] = "reuse"
            n_reuse += 1
            n_skip_hist += 1
        elif info_ok and fin_ok:
            classified[t] = "hist"
            n_hist_only += 1
            incremental_tickers.append(t)
        else:
            classified[t] = "fetch"
            n_fetch += 1
            fetch_tickers.append(t)
            if hist_ok:
                incremental_tickers.append(t)
            else:
                full_hist_tickers.append(t)

    log.info(f"  Reuse as-is (through {planned}, profile+financials ok): {n_reuse}")
    log.info(f"  History splice only (profile+financials ok): {n_hist_only}")
    log.info(f"  Need Yahoo profile and/or financials: {n_fetch} "
             f"(profile holes {n_need_info}, statement holes {n_need_fin})")
    log.info(f"  Hist downloads: {len(incremental_tickers)} recent / "
             f"{len(full_hist_tickers)} full-year")

    recent_start = (pd.Timestamp(planned) - pd.Timedelta(days=14)).strftime("%Y-%m-%d")
    batch_hist = {}

    def _hist_batches(names, start, end, label):
        got = 0
        for i in range(0, len(names), BATCH_SIZE):
            chunk = names[i:i + BATCH_SIZE]
            part = _download_batch(chunk, start, end)
            batch_hist.update(part)
            got += len(part)
            if i + BATCH_SIZE < len(names):
                time.sleep(BATCH_PAUSE)
            if i == 0 or (i // BATCH_SIZE + 1) % 10 == 0:
                log.info(f"  {label} hist {i + len(chunk)}/{len(names)} "
                         f"({len(part)}/{len(chunk)} frames)")
        return got

    n_inc = _hist_batches(incremental_tickers, recent_start, hist_end, "recent")
    n_full = _hist_batches(full_hist_tickers, hist_start, hist_end, "full")
    log.info(f"  History frames: {n_inc} incremental + {n_full} full")

    all_results = []
    done_count = 0
    total = len(unique_tickers)

    for t in unique_tickers:
        kind = classified.get(t)
        prev = prev_dump.get(t)
        if kind in ("reuse", "hist"):
            row = _row_copy(prev)
            hdf = batch_hist.get(t)
            if hdf is not None and not getattr(hdf, "empty", True):
                merged = _merge_hist(_df_from_cache(row.get("_hist")), hdf)
                if not merged.empty:
                    _apply_hist_to_row(row, merged)
            row["_exchanges"] = list(ticker_to_exchanges.get(t, set()))
            all_results.append(row)
            done_count += 1

    log.info(f"Starting Yahoo backfill: {len(fetch_tickers)} tickers · "
             f"{WORKERS} workers · batches of {BATCH_SIZE}")
    remaining = list(fetch_tickers)
    batch_num = 0
    fetch_done = 0
    while remaining:
        batch = remaining[:BATCH_SIZE]
        remaining = remaining[BATCH_SIZE:]
        batch_num += 1
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(
                    process_ticker,
                    (t, MFI_PERIOD, RANGE_DAYS, batch_hist.get(t), prev_dump.get(t))
                ): t
                for t in batch
            }
            for future in as_completed(futures):
                t = futures[future]
                try:
                    result = future.result(timeout=120)
                except Exception:
                    result = None
                if result:
                    result["_exchanges"] = list(ticker_to_exchanges.get(t, set()))
                    all_results.append(result)
                else:
                    prev = prev_dump.get(t)
                    if prev:
                        row = _row_copy(prev)
                        hdf = batch_hist.get(t)
                        if hdf is not None and not getattr(hdf, "empty", True):
                            merged = _merge_hist(_df_from_cache(row.get("_hist")), hdf)
                            if not merged.empty:
                                _apply_hist_to_row(row, merged)
                        row["_exchanges"] = list(ticker_to_exchanges.get(t, set()))
                        all_results.append(row)
                fetch_done += 1
                done_count += 1
        if batch_num % 10 == 0 or not remaining:
            pct = int(done_count / total * 100) if total else 100
            log.info(f"  Backfill {batch_num} · {fetch_done}/{len(fetch_tickers)} "
                     f"Yahoo · {done_count}/{total} ({pct}%)")
        if remaining:
            time.sleep(BATCH_PAUSE)

    # ── 2b. Splice the last session onto names Yahoo omitted ─────────
    # Batch period="1y" / incomplete last bars left ~85% of the universe
    # a day behind. A short start/end refetch is cheap and is what the
    # Money Weather date picker actually reads.
    def _row_last(r):
        return _hist_last(r.get("_hist"))

    # Re-runs (and a previous dump that already had tonight's session)
    # should not lose bars the batch download skipped.
    if prev_dump:
        n_merged = 0
        for r in all_results:
            t = str(r.get("Ticker") or "").strip().upper()
            prev = prev_dump.get(t)
            if not prev:
                continue
            merged = _merge_hist(
                _df_from_cache(prev.get("_hist")),
                _df_from_cache(r.get("_hist")),
            )
            if merged.empty or _hist_last(merged) == _row_last(r):
                continue
            _apply_hist_to_row(r, merged)
            n_merged += 1
        if n_merged:
            log.info(f"  Merged previous-dump history into {n_merged} tickers")

    stragglers = [r for r in all_results if (_row_last(r) or "") < planned]
    if stragglers:
        log.info(f"Refetching last bars for {len(stragglers)} tickers "
                 f"missing {planned}...")
        splice_start = (pd.Timestamp(planned) - pd.Timedelta(days=14)).strftime("%Y-%m-%d")
        by_t = {r["Ticker"]: r for r in all_results}
        names = [r["Ticker"] for r in stragglers]
        spliced = 0
        for i in range(0, len(names), BATCH_SIZE):
            chunk = names[i:i + BATCH_SIZE]
            fresh = _download_batch(chunk, splice_start, hist_end)
            for t, hdf in fresh.items():
                row = by_t.get(t)
                if not row:
                    continue
                merged = _merge_hist(_df_from_cache(row.get("_hist")), hdf)
                if _hist_last(merged) >= planned:
                    _apply_hist_to_row(row, merged)
                    spliced += 1
            if i + BATCH_SIZE < len(names):
                time.sleep(max(3, BATCH_PAUSE // 2))
        log.info(f"  Spliced {planned} onto {spliced}/{len(stragglers)} stragglers")

    still = [r for r in all_results if (_row_last(r) or "") < planned]
    if still:
        cap = min(250, len(still))
        log.info(f"  Per-ticker 10d history for {cap} remaining stragglers...")
        got = 0
        for r in still[:cap]:
            try:
                h = _normalize_hist(
                    yf.Ticker(r["Ticker"]).history(period="10d", actions=True,
                                                   auto_adjust=True))
                merged = _merge_hist(_df_from_cache(r.get("_hist")), h)
                if _hist_last(merged) >= planned:
                    _apply_hist_to_row(r, merged)
                    got += 1
            except Exception:
                pass
            time.sleep(0.12)
        log.info(f"  Per-ticker recovered {got}/{cap}")

    # Carry forward names that failed tonight so a rate-limit blip does
    # not delete them from the published dump.
    have = {str(r.get("Ticker") or "").strip().upper() for r in all_results}
    carried = 0
    for t, row in prev_dump.items():
        if t and t not in have:
            all_results.append(row)
            have.add(t)
            carried += 1
    if carried:
        log.info(f"  Carried forward {carried} tickers from the previous dump")

    if not all_results and prev_dump:
        log.error("Tonight's scan produced 0 rows — keeping previous dump")
        all_results = list(prev_dump.values())
        carried = len(all_results)

    last_counts = Counter(_row_last(r) or "none" for r in all_results)
    n_target = int(last_counts.get(planned, 0))
    coverage = (n_target / len(all_results)) if all_results else 0.0
    completeness = _field_completeness(all_results, planned)
    log.info(f"  Last-date coverage of {planned}: {n_target}/{len(all_results)} "
             f"({coverage:.0%})")
    log.info(f"  Field completeness: {completeness:.0%}")
    log.info(f"  Last-date histogram: {dict(last_counts.most_common(6))}")
    if coverage < 0.70:
        log.warning(f"  LOW COVERAGE of {planned} — Yahoo likely lagged. "
                    "Stormwatch will still prefer this date when enough names printed.")

    # ── 3. Save results ───────────────────────────────────────────────
    log.info(f"Saving {len(all_results)} results...")

    date_tag = start_utc.strftime("%Y-%m-%d")

    # ── Primary files (always overwritten — what the app reads) ──────
    # Compressed JSON — readable by the Streamlit app with json + gzip
    with gzip.open(DATA_FILE, "wt", encoding="utf-8") as f:
        json.dump(all_results, f, default=str)

    log.info(f"  Saved: {DATA_FILE}  ({os.path.getsize(DATA_FILE) / 1024:.0f} KB compressed)")

    # ── Reusable exports for OTHER projects ──────────────────────────
    # stock_data.json.gz is one big nested blob: every consumer has to gunzip
    # it, parse ~5,700 records and strip out the embedded price history just to
    # read a P/E. These two files are the flat, boring version — load them with
    # one pandas call, no JSON walking, no history payload.
    #
    #   fundamentals.parquet / .csv  one row per ticker, every scalar field,
    #                                price history and the analyzer pack removed
    #   prices.parquet               tidy long OHLCV (ticker, date, o/h/l/c/v)
    #
    # Anything downstream — the data-centre infrastructure work, notebooks, a
    # different app — can read these directly and never touch the raw dump.
    try:
        flat = []
        for r in all_results:
            row = {k: v for k, v in r.items()
                   if k not in ("_hist", "_analyzer") and not isinstance(v, (dict, list))}
            a = r.get("_analyzer") or {}
            for k, v in a.items():
                if k != "eps_history" and not isinstance(v, (dict, list)):
                    row[f"an_{k}"] = v
            flat.append(row)
        fdf = pd.DataFrame(flat)
        fund_pq = os.path.join(OUTPUT_DIR, "fundamentals.parquet")
        fund_csv = os.path.join(OUTPUT_DIR, "fundamentals.csv")
        try:
            fdf.to_parquet(fund_pq, index=False)
            log.info(f"  Saved: {fund_pq}  ({os.path.getsize(fund_pq)/1024:.0f} KB, "
                     f"{len(fdf)} rows x {fdf.shape[1]} cols)")
        except Exception as _pe:
            log.warning(f"  parquet export skipped ({_pe}) — CSV still written")
        fdf.to_csv(fund_csv, index=False)
        log.info(f"  Saved: {fund_csv}  ({os.path.getsize(fund_csv)/1024:.0f} KB)")
    except Exception as e:
        log.warning(f"  fundamentals export failed: {e}")

    try:
        px_rows = []
        for r in all_results:
            h = r.get("_hist") or {}
            d = h.get("dates") or []
            if not d:
                continue
            t = r.get("Ticker")
            for i, dt_ in enumerate(d):
                px_rows.append((t, dt_, h["open"][i], h["high"][i],
                                h["low"][i], h["close"][i], h["volume"][i]))
        if px_rows:
            pdf = pd.DataFrame(px_rows, columns=["ticker", "date", "open", "high",
                                                 "low", "close", "volume"])
            px_pq = os.path.join(OUTPUT_DIR, "prices.parquet")
            try:
                pdf.to_parquet(px_pq, index=False)
                log.info(f"  Saved: {px_pq}  ({os.path.getsize(px_pq)/1024/1024:.1f} MB, "
                         f"{len(pdf):,} bars)")
            except Exception as _pe:
                log.warning(f"  prices parquet skipped ({_pe})")
    except Exception as e:
        log.warning(f"  prices export failed: {e}")

    # ── Dated archive copy (for rollback if a bad scan runs) ─────────
    archive_data = os.path.join(OUTPUT_DIR, f"stock_data_{date_tag}.json.gz")
    archive_meta = os.path.join(OUTPUT_DIR, f"scan_meta_{date_tag}.json")
    import shutil
    shutil.copy2(DATA_FILE, archive_data)
    log.info(f"  Archive: {archive_data}")

    # ── Purge archive files older than 3 days ────────────────────────
    import glob
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=3)
    purged = 0
    for pattern in ("stock_data_*.json.gz", "scan_meta_*.json"):
        for fpath in glob.glob(os.path.join(OUTPUT_DIR, pattern)):
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                if mtime < cutoff:
                    os.remove(fpath)
                    log.info(f"  Purged old archive: {os.path.basename(fpath)}")
                    purged += 1
            except Exception as _e:
                log.warning(f"  Could not purge {fpath}: {_e}")
    if purged == 0:
        log.info("  No old archives to purge.")

    # Metadata file — app reads this to show "last updated" banner
    end_utc   = datetime.now(timezone.utc)
    elapsed   = round((end_utc - start_utc).total_seconds() / 60, 1)
    meta = {
        "scanned_at_utc":  end_utc.strftime("%Y-%m-%d %H:%M UTC"),
        "scanned_at_display": end_utc.strftime("%Y-%m-%d %I:%M %p UTC"),
        "elapsed_minutes": elapsed,
        "total_tickers":   len(unique_tickers),
        "valid_results":   len(all_results),
        "target_session":  planned,
        "target_coverage": round(coverage, 4),
        "target_printed":  n_target,
        "carried_forward": carried,
        "reused_as_is": n_reuse,
        "hist_only": n_hist_only,
        "yahoo_backfill": n_fetch,
        "hist_incremental": len(incremental_tickers),
        "hist_full": len(full_hist_tickers),
        "profile_holes": n_need_info,
        "statement_holes": n_need_fin,
        "field_completeness": completeness,
        "last_date_counts": dict(last_counts.most_common(8)),
        "exchanges": {
            exch: len(tl) for exch, tl in all_tickers.items()
        },
    }
    with open(META_FILE, "w") as f:
        json.dump(meta, f, indent=2)

    log.info(f"  Saved: {META_FILE}")
    shutil.copy2(META_FILE, archive_meta)
    log.info(f"  Archive: {archive_meta}")
    log.info("=" * 60)
    log.info(f"Done.  {len(all_results)} stocks · {elapsed} minutes")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
