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
#   data/stock_data.json.gz   — all results, one dict per ticker
#   data/scan_meta.json       — timestamp, counts, status per exchange
# ─────────────────────────────────────────────────────────────────────────────

import os
import sys
import gzip
import json
import time
import random
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

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
    - Preferred shares: contain $ (e.g. ABR$E → not supported by yfinance)
    - Warrants: end in W or WS
    - Rights: end in R  
    - Units: end in U
    - Test symbols: contain ^ or ~
    These are all legitimate securities but useless for equity screening.
    """
    cleaned = []
    for t in tickers:
        if "$" in t:          continue   # preferred share classes
        if "^" in t:          continue   # index symbols
        if "~" in t:          continue   # test symbols
        if t.endswith("W"):   continue   # warrants
        if t.endswith("WS"):  continue   # warrants (series)
        if t.endswith("R"):   continue   # rights (most — some valid tickers end in R)
        if t.endswith("U"):   continue   # units
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
                if raw and raw.isalpha() or ("-" in raw and raw.replace("-","").isalpha()):
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
    try:
        score = 0
        roa   = fin.loc["Net Income"] / bal.loc["Total Assets"]
        if roa.iloc[0] > 0:                                       score += 1
        if cf.loc["Operating Cash Flow"].iloc[0] > 0:             score += 1
        if roa.iloc[0] > roa.iloc[1]:                             score += 1
        if cf.loc["Operating Cash Flow"].iloc[0] > fin.loc["Net Income"].iloc[0]: score += 1
        if bal.loc["Long Term Debt"].iloc[0] < bal.loc["Long Term Debt"].iloc[1]: score += 1
        if (bal.loc["Current Assets"].iloc[0] / bal.loc["Current Liabilities"].iloc[0]) > \
           (bal.loc["Current Assets"].iloc[1] / bal.loc["Current Liabilities"].iloc[1]): score += 1
        return score
    except Exception:
        return None


def get_owner_earnings(cf, fin, info):
    try:
        oe = (fin.loc["Net Income"].iloc[0]
              + cf.loc["Depreciation"].iloc[0]
              - abs(cf.loc["Capital Expenditure"].iloc[0]))
        mc = info.get("marketCap")
        return oe, (oe / mc if mc else None)
    except Exception:
        return None, None


def calculate_roic(fin, bal):
    try:
        ebit_s = _get_fin_value(fin, "EBIT","Ebit","Operating Income","OperatingIncome","EBITDA","Ebitda")
        if ebit_s is None: return None
        assets = _get_bal_value(bal, "Total Assets","TotalAssets")
        liab   = _get_bal_value(bal, "Total Current Liabilities","TotalCurrentLiabilities","Current Liabilities","CurrentLiabilities")
        if assets is None or liab is None: return None
        cash_s = _get_bal_value(bal, "Cash And Cash Equivalents","Cash Cash Equivalents And Short Term Investments","CashAndCashEquivalents","Cash")
        cash   = cash_s.iloc[0] if cash_s is not None else 0
        ic     = assets.iloc[0] - liab.iloc[0] - cash
        if pd.isna(ic) or ic == 0: return None
        roic   = ebit_s.iloc[0] * 0.79 / ic
        return None if pd.isna(roic) else roic
    except Exception:
        return None


def calculate_roic_trend(fin, bal):
    try:
        ebit_s = _get_fin_value(fin, "EBIT","Ebit","Operating Income","OperatingIncome","EBITDA","Ebitda")
        if ebit_s is None or len(ebit_s) < 2: return None
        assets = _get_bal_value(bal, "Total Assets","TotalAssets")
        liab   = _get_bal_value(bal, "Total Current Liabilities","TotalCurrentLiabilities","Current Liabilities","CurrentLiabilities")
        if assets is None or liab is None or len(assets) < 2 or len(liab) < 2: return None
        def roic_at(i):
            ic = assets.iloc[i] - liab.iloc[i]
            if pd.isna(ic) or ic == 0: return None
            v  = ebit_s.iloc[i] / ic
            return None if pd.isna(v) else v
        r0, r1 = roic_at(0), roic_at(1)
        return (r0 - r1) if (r0 is not None and r1 is not None) else None
    except Exception:
        return None


# ── Short interest ───────────────────────────────────────────────────────────

def calculate_dividend_score(info: dict, dividends_history=None) -> dict:
    """Compute composite dividend quality score — same logic as app.py."""
    default = {
        "DividendYieldPct":  None,
        "DividendRate":      None,
        "PayoutRatio":       None,
        "DividendFrequency": "None",
        "DividendScore":     0.0,
    }
    try:
        yield_raw = (info.get("trailingAnnualDividendYield") or info.get("dividendYield"))
        div_rate  = (info.get("trailingAnnualDividendRate") or info.get("dividendRate"))
        payout    = info.get("payoutRatio")
        if not yield_raw and not div_rate:
            return default
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
                one_yr_ago = pd.Timestamp.now(tz="UTC") - pd.DateOffset(years=1)
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
        }
    except Exception:
        return default


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
    t, mfi_period, range_days = args
    # 3 attempts with increasing backoff — nightly job has time to spare.
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(attempt * 5 + random.uniform(0, 3))

            stock = yf.Ticker(t)
            info  = stock.info or {}

            if not info or len(info) < 5:
                # Rate-limited: wait and retry
                if attempt < 2:
                    time.sleep(10 + random.uniform(0, 5))
                    continue
                return None

            if is_etf_or_fund(info):
                return None

            price = info.get("currentPrice") or info.get("regularMarketPrice")
            if not price:
                return None

            try:
                hist = stock.history(period="1y")
                if hist.empty or len(hist) < 30:
                    hist = stock.history(period="6mo")
            except Exception:
                hist = pd.DataFrame()

            try:
                fin = stock.financials
            except Exception:
                fin = pd.DataFrame()
            try:
                div_hist = stock.dividends
            except Exception:
                div_hist = pd.Series(dtype=float)
            try:
                bal = stock.balance_sheet
            except Exception:
                bal = pd.DataFrame()
            try:
                cf = stock.cashflow
            except Exception:
                cf = pd.DataFrame()

            vol_signals  = get_volume_signals(hist, mfi_period)
            tech_signals = calculate_technical_signals(hist)
            range_data   = calculate_price_range(hist, range_days)
            short_data   = calculate_short_squeeze(info)
            div_data     = calculate_dividend_score(info, div_hist if not div_hist.empty else None)
            ma50         = (round(hist["Close"].rolling(50).mean().iloc[-1], 2)
                            if len(hist) >= 50 else None)
            owner_earnings, oe_yield = get_owner_earnings(cf, fin, info)

            hist_cache = {}
            if not hist.empty:
                hist_cache = {
                    "dates":  hist.index.strftime("%Y-%m-%d").tolist(),
                    "open":   hist["Open"].tolist(),
                    "high":   hist["High"].tolist(),
                    "low":    hist["Low"].tolist(),
                    "close":  hist["Close"].tolist(),
                    "volume": hist["Volume"].tolist(),
                }

            return {
                "Ticker":         t,
                "Sector":         info.get("sector", "Unknown"),
                "Price":          price,
                "MarketCap":      info.get("marketCap"),
                "P/E":            info.get("trailingPE"),
                "OwnerEarnings":  owner_earnings,
                "OE_Yield":       oe_yield,
                "ROIC":           calculate_roic(fin, bal),
                "ROIC_Trend":     calculate_roic_trend(fin, bal),
                "RevenueGrowth":  info.get("revenueGrowth"),
                "EarningsGrowth": info.get("earningsGrowth"),
                "Piotroski":      calculate_piotroski(fin, bal, cf),
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
                "_hist":          hist_cache,
                "_exchange":      "",
            }

        except Exception:
            if attempt == 2:
                return None
            continue

    return None  # all attempts exhausted

# ── Scan one exchange ─────────────────────────────────────────────────────────

def scan_exchange(exchange_key: str, tickers: list) -> list:
    results   = []
    total     = len(tickers)
    done      = 0
    last_log  = 0

    log.info(f"  Scanning {total} tickers ({exchange_key.upper()}) with {WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(process_ticker, (t, MFI_PERIOD, RANGE_DAYS)): t
            for t in tickers
        }
        for future in as_completed(futures):
            try:
                result = future.result(timeout=20)
            except Exception:
                result = None
            if result:
                result["_exchange"] = exchange_key
                results.append(result)
            done += 1
            pct = int(done / total * 100)
            if pct >= last_log + 10 or done == total:
                last_log = pct
                log.info(f"    {done}/{total} ({pct}%)  ·  {len(results)} valid")

    # Polite pause between exchanges to let yfinance rate limits reset
    time.sleep(30)
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

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

    # ── 2. Scan all tickers in small batches with pauses ────────────
    # Small batches + pauses prevent sustained rate limiting from Yahoo.
    # At BATCH_SIZE=30, BATCH_PAUSE=8s, WORKERS=3:
    #   ~7000 tickers / 30 per batch = ~233 batches
    #   233 batches × (30 tickers / 3 workers × ~2s avg + 8s pause) ≈ 190 min
    # Well within the 210-minute timeout.
    all_results  = []
    done_count   = 0
    remaining    = list(unique_tickers)
    total        = len(unique_tickers)
    batch_num    = 0
    log.info(f"Starting download: {total} tickers · {WORKERS} workers · "
             f"batches of {BATCH_SIZE} · {BATCH_PAUSE}s pause between batches")

    while remaining:
        batch      = remaining[:BATCH_SIZE]
        remaining  = remaining[BATCH_SIZE:]
        batch_num += 1

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(process_ticker, (t, MFI_PERIOD, RANGE_DAYS)): t
                for t in batch
            }
            for future in as_completed(futures):
                t = futures[future]
                try:
                    result = future.result(timeout=45)
                except Exception:
                    result = None
                if result:
                    result["_exchanges"] = list(ticker_to_exchanges.get(t, set()))
                    all_results.append(result)
                done_count += 1

        # Log progress every 10 batches
        if batch_num % 10 == 0 or not remaining:
            pct = int(done_count / total * 100)
            pass_rate = len(all_results) / done_count if done_count else 0
            log.info(f"  Batch {batch_num} · {done_count}/{total} ({pct}%) · "
                     f"{len(all_results)} valid · pass rate {pass_rate:.0%}")

        # Pause between batches — lets Yahoo's rate limiter breathe
        if remaining:
            time.sleep(BATCH_PAUSE)

    # ── 3. Save results ───────────────────────────────────────────────
    log.info(f"Saving {len(all_results)} results...")

    date_tag = end_utc.strftime("%Y-%m-%d") if 'end_utc' in dir() else start_utc.strftime("%Y-%m-%d")

    # ── Primary files (always overwritten — what the app reads) ──────
    # Compressed JSON — readable by the Streamlit app with json + gzip
    with gzip.open(DATA_FILE, "wt", encoding="utf-8") as f:
        json.dump(all_results, f, default=str)

    log.info(f"  Saved: {DATA_FILE}  ({os.path.getsize(DATA_FILE) / 1024:.0f} KB compressed)")

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
        "scanned_at_est":  end_utc.strftime("%Y-%m-%d %I:%M %p UTC"),   # app converts to local
        "elapsed_minutes": elapsed,
        "total_tickers":   len(unique_tickers),
        "valid_results":   len(all_results),
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
