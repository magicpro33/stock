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
    args = (ticker, mfi_period, range_days, prefetched_hist)
    prefetched_hist: OHLCV(+Dividends) DataFrame from the batch yf.download,
    or None — in which case we fall back to a per-ticker history fetch.
    """
    t, mfi_period, range_days, pre_hist = args
    # 3 attempts with increasing backoff — nightly job has time to spare.
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(attempt * 5 + random.uniform(0, 3))

            stock   = yf.Ticker(t)
            _av_key = _get_av_key() if _AV_AVAILABLE else ""
            info    = stock.info or {}

            # Valid info has quoteType/symbol; rate-limited stubs have 1-2 null keys
            if not (info.get("quoteType") or info.get("symbol") or len(info) >= 10):
                # Rate-limited — try Alpha Vantage before giving up
                if _AV_AVAILABLE and _av_key:
                    try:
                        info = av_fill_info(t, info, _av_key)
                    except Exception:
                        pass
                if not info or len(info) < 5:
                    if attempt < 2:
                        time.sleep(10 + random.uniform(0, 5))
                        continue
                    return None

            # AV patch for unknown sector even when info is otherwise healthy
            if _AV_AVAILABLE and _av_key and av_needs_fallback(info):
                try:
                    info = av_fill_info(t, info, _av_key)
                except Exception:
                    pass

            if is_etf_or_fund(info):
                return None

            price = info.get("currentPrice") or info.get("regularMarketPrice")
            if not price:
                return None

            # Use batch-downloaded history when available (1 request per 30
            # tickers instead of 1 per ticker); fall back to per-ticker fetch.
            hist = pre_hist if pre_hist is not None else pd.DataFrame()
            if hist.empty or len(hist) < 30:
                try:
                    hist = stock.history(period="1y", actions=True)
                    if hist.empty or len(hist) < 30:
                        hist = stock.history(period="6mo", actions=True)
                except Exception:
                    hist = pd.DataFrame()

            # AV fallback for missing history
            if _AV_AVAILABLE and _av_key and av_needs_history_fallback(hist):
                try:
                    av_hist = av_fill_history(t, _av_key)
                    if not av_hist.empty:
                        hist = av_hist
                except Exception:
                    pass

            try:
                fin = stock.financials
            except Exception:
                fin = pd.DataFrame()
            # Dividends ride along in the history frame (actions=True) —
            # saves one HTTP request per ticker vs stock.dividends
            try:
                if "Dividends" in hist.columns:
                    div_hist = hist["Dividends"][hist["Dividends"] > 0]
                else:
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

            # AV fallback for missing financials
            if _AV_AVAILABLE and _av_key and av_needs_financials_fallback(fin, bal, cf):
                try:
                    av_fin, av_bal, av_cf = av_fill_financials(t, _av_key)
                    if fin.empty and not av_fin.empty:   fin = av_fin
                    if bal.empty and not av_bal.empty:   bal = av_bal
                    if cf.empty  and not av_cf.empty:    cf  = av_cf
                except Exception:
                    pass

            vol_signals  = get_volume_signals(hist, mfi_period)
            tech_signals = calculate_technical_signals(hist)
            range_data   = calculate_price_range(hist, range_days)
            short_data   = calculate_short_squeeze(info)
            div_data     = calculate_dividend_score(info, div_hist if not div_hist.empty else None)
            clean_setup  = calculate_clean_setup(hist)
            gross_margin = calculate_gross_margin(fin)
            ma50         = (round(hist["Close"].rolling(50).mean().iloc[-1], 2)
                            if len(hist) >= 50 else None)
            owner_earnings, oe_yield = get_owner_earnings(cf, fin, info)

            hist_cache = {}
            if not hist.empty:
                hist_cache = {
                    "dates":  hist.index.strftime("%Y-%m-%d").tolist(),
                    "open":   hist["Open"].round(4).tolist(),
                    "high":   hist["High"].round(4).tolist(),
                    "low":    hist["Low"].round(4).tolist(),
                    "close":  hist["Close"].round(4).tolist(),
                    "volume": hist["Volume"].fillna(0).astype("int64").tolist(),
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
                "CleanSetupScore":     clean_setup,
                "GrossMargin":         gross_margin,
                "ExDividendDate":      info.get("exDividendDate"),
                "_hist":          hist_cache,
                "_exchange":      "",
            }

        except Exception as e:
            # Network/fetch errors are worth retrying; anything that got past
            # the fetches and blew up in computation will fail identically on
            # retry — bail immediately instead of re-downloading 3×.
            if attempt == 2 or "hist" in dir():
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

    # ── 2. Scan all tickers in small batches with pauses ────────────
    # History is batch-downloaded (1 request per 30 tickers, dividends
    # included via actions=True). Per-ticker requests drop from ~6 to 4
    # (info + 3 financial statements) — ~12,000 fewer requests per night.
    # At BATCH_SIZE=30, BATCH_PAUSE=8s, WORKERS=3 → ≈150 min, well inside
    # the 210-minute timeout, with materially lower rate-limit exposure.
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

        # ── Batch history download: 1 HTTP request for the whole batch ──
        # (vs 1 request per ticker). Dividends included via actions=True.
        batch_hist = {}
        try:
            dl = yf.download(
                tickers=" ".join(batch), period="1y", actions=True,
                group_by="ticker", threads=False, progress=False,
                auto_adjust=True,
            )
            if not dl.empty:
                if isinstance(dl.columns, pd.MultiIndex):
                    for t in batch:
                        if t in dl.columns.get_level_values(0):
                            h = dl[t].dropna(how="all")
                            if not h.empty:
                                batch_hist[t] = h
                else:
                    # single-ticker batch returns flat columns
                    batch_hist[batch[0]] = dl.dropna(how="all")
        except Exception as e:
            log.warning(f"  Batch download failed ({type(e).__name__}) — "
                        f"falling back to per-ticker history for this batch")

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(process_ticker,
                                (t, MFI_PERIOD, RANGE_DAYS, batch_hist.get(t))): t
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

    date_tag = start_utc.strftime("%Y-%m-%d")

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
        "scanned_at_display": end_utc.strftime("%Y-%m-%d %I:%M %p UTC"),
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
