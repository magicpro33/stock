"""
alpha_vantage_fallback.py
─────────────────────────
Alpha Vantage fallback data source for the hybrid stock screener.

Called ONLY when yfinance fails or returns sparse/unknown data — so
the free-tier call budget (25/day) is preserved for stocks that need it.

Usage
-----
    from alpha_vantage_fallback import av_fill_info, av_fill_history, av_fill_financials

    # Patch a sparse yfinance info dict
    info = av_fill_info(ticker, info, api_key)

    # Replace missing price history
    hist = av_fill_history(ticker, api_key)     # returns pd.DataFrame or None

    # Replace missing financial statements
    fin, bal, cf = av_fill_financials(ticker, api_key)

API key setup
-------------
Set in Streamlit Cloud → Settings → Secrets:
    AV_API_KEY = "your_key_here"

Or as an environment variable:
    export AV_API_KEY=your_key_here

Free tier limits
----------------
  - 25 requests/day
  - 5 requests/minute
  - OVERVIEW, TIME_SERIES_DAILY, INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW

Get a free key at: https://www.alphavantage.co/support/#api-key
"""

import os
import time
import requests
import pandas as pd

# ── Rate limit state (module-level, shared across all calls in a process) ────
import threading
_AV_LOCK           = threading.Lock()
_AV_LAST_CALL_TIME = 0.0
_AV_MIN_INTERVAL   = 12.5   # seconds between calls (free tier: 5/min = 12s apart)
                             # set to 3.5 for premium (100/min)
_AV_CALLS_MADE     = 0
_AV_MAX_CALLS      = int(os.environ.get("AV_MAX_CALLS_PER_RUN", "20"))
                             # per-run budget: free tier is 25/day — leave headroom

AV_BASE = "https://www.alphavantage.co/query"


def _get_api_key() -> str:
    """
    Resolve the Alpha Vantage API key.
    Checks Streamlit secrets first, then environment variable.
    Returns empty string if not configured — callers check for this.
    """
    # Try Streamlit secrets (only available when running inside Streamlit)
    try:
        import streamlit as st
        key = st.secrets.get("AV_API_KEY", "")
        if key:
            return key
    except Exception:
        pass
    # Fall back to environment variable
    return os.environ.get("AV_API_KEY", "")


def _av_get(params: dict, api_key: str) -> dict:
    """
    Make one rate-limited GET request to Alpha Vantage.
    Returns parsed JSON dict or empty dict on any error.
    Enforces minimum interval between calls to stay within rate limits.
    """
    global _AV_LAST_CALL_TIME, _AV_CALLS_MADE

    # Per-run budget: once exhausted, fail fast instead of blocking workers
    with _AV_LOCK:
        if _AV_CALLS_MADE >= _AV_MAX_CALLS:
            return {}
        # Serialize the rate wait inside the lock so threads can't race the clock
        elapsed = time.time() - _AV_LAST_CALL_TIME
        if elapsed < _AV_MIN_INTERVAL:
            time.sleep(_AV_MIN_INTERVAL - elapsed)
        _AV_LAST_CALL_TIME = time.time()
        _AV_CALLS_MADE += 1

    params["apikey"] = api_key
    try:
        resp = requests.get(AV_BASE, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            # AV returns {"Note": "..."} when rate-limited
            if "Note" in data or "Information" in data:
                return {}
            return data
    except Exception:
        pass
    return {}


def _parse_av_float(val) -> float | None:
    """Safe conversion of AV string values to float. Returns None on failure."""
    try:
        v = float(str(val).replace(",", "").replace("%", "").strip())
        return None if (v != v) else v   # NaN check
    except Exception:
        return None


# ── Sector name normalisation ─────────────────────────────────────────────────
# AV uses slightly different sector names than yfinance — map to yfinance canonical
_AV_SECTOR_MAP = {
    "technology":                    "Technology",
    "health care":                   "Healthcare",
    "healthcare":                    "Healthcare",
    "financials":                    "Financial Services",
    "financial services":            "Financial Services",
    "consumer discretionary":        "Consumer Cyclical",
    "consumer cyclical":             "Consumer Cyclical",
    "consumer staples":              "Consumer Defensive",
    "consumer defensive":            "Consumer Defensive",
    "industrials":                   "Industrials",
    "communication services":        "Communication Services",
    "utilities":                     "Utilities",
    "real estate":                   "Real Estate",
    "materials":                     "Basic Materials",
    "basic materials":               "Basic Materials",
    "energy":                        "Energy",
    "life sciences":                 "Healthcare",
    "electronic technology":         "Technology",
    "retail trade":                  "Consumer Cyclical",
    "finance":                       "Financial Services",
    "transportation":                "Industrials",
    "process industries":            "Basic Materials",
    "producer manufacturing":        "Industrials",
    "commercial services":           "Industrials",
    "health technology":             "Healthcare",
    "health services":               "Healthcare",
    "distribution services":         "Industrials",
    "consumer services":             "Consumer Cyclical",
    "non-energy minerals":           "Basic Materials",
    "energy minerals":               "Energy",
}

def _normalise_av_sector(raw: str) -> str:
    """Map AV sector name to yfinance canonical name."""
    if not raw or raw.strip().lower() in ("none", "n/a", "-", ""):
        return "Unknown"
    return _AV_SECTOR_MAP.get(raw.strip().lower(), raw.strip())


# ── Public API ────────────────────────────────────────────────────────────────

def av_fill_info(ticker: str, existing_info: dict, api_key: str = "") -> dict:
    """
    Fill gaps in a yfinance info dict using Alpha Vantage COMPANY_OVERVIEW.

    Only patches fields that are None/missing in existing_info — never
    overwrites valid yfinance data with AV data.

    Parameters
    ----------
    ticker        : stock ticker symbol
    existing_info : the yfinance info dict (may be sparse or empty)
    api_key       : AV API key (fetched from secrets if empty)

    Returns
    -------
    Patched info dict.  Returns existing_info unchanged if AV call fails.
    """
    key = api_key or _get_api_key()
    if not key:
        return existing_info

    data = _av_get({"function": "OVERVIEW", "symbol": ticker}, key)
    if not data or data.get("Symbol", "").upper() != ticker.upper():
        return existing_info

    info = dict(existing_info)   # don't mutate original

    # ── Core identity fields ──────────────────────────────────────
    if not info.get("longName"):
        info["longName"] = data.get("Name") or data.get("longName")
    if not info.get("sector") or info.get("sector") == "Unknown":
        raw_sector = data.get("Sector", "")
        info["sector"] = _normalise_av_sector(raw_sector)

    # ── Price / valuation ─────────────────────────────────────────
    if not info.get("currentPrice"):
        price = _parse_av_float(data.get("50DayMovingAverage"))  # rough fallback
        if price:
            info["currentPrice"] = price
    if not info.get("marketCap"):
        info["marketCap"] = _parse_av_float(data.get("MarketCapitalization"))
    if not info.get("trailingPE"):
        info["trailingPE"] = _parse_av_float(data.get("TrailingPE"))

    # ── Growth / fundamentals ─────────────────────────────────────
    if not info.get("revenueGrowth"):
        # AV gives quarterly revenue growth YoY
        qrg = _parse_av_float(data.get("QuarterlyRevenueGrowthYOY"))
        info["revenueGrowth"] = qrg
    if not info.get("earningsGrowth"):
        qeg = _parse_av_float(data.get("QuarterlyEarningsGrowthYOY"))
        info["earningsGrowth"] = qeg

    # ── Dividend ──────────────────────────────────────────────────
    if not info.get("dividendYield"):
        dy = _parse_av_float(data.get("DividendYield"))
        if dy:
            info["dividendYield"] = dy
    if not info.get("dividendRate"):
        dr = _parse_av_float(data.get("DividendPerShare"))
        if dr:
            info["dividendRate"] = dr
    if not info.get("payoutRatio"):
        pr = _parse_av_float(data.get("PayoutRatio"))
        if pr is not None:
            info["payoutRatio"] = pr

    # ── Short interest ────────────────────────────────────────────
    if not info.get("shortPercentOfFloat"):
        spf = _parse_av_float(data.get("ShortPercentFloat"))
        if spf:
            info["shortPercentOfFloat"] = spf / 100 if spf > 1 else spf
    if not info.get("shortRatio"):
        sr = _parse_av_float(data.get("ShortRatio"))
        if sr:
            info["shortRatio"] = sr

    # ── 52-week range (useful for range position) ─────────────────
    if not info.get("fiftyTwoWeekHigh"):
        info["fiftyTwoWeekHigh"] = _parse_av_float(data.get("52WeekHigh"))
    if not info.get("fiftyTwoWeekLow"):
        info["fiftyTwoWeekLow"] = _parse_av_float(data.get("52WeekLow"))

    # Tag the source so logs can show which fields came from AV
    info["_av_patched"] = True
    return info


def av_fill_history(ticker: str, api_key: str = "") -> pd.DataFrame:
    """
    Fetch 1 year of daily OHLCV history from Alpha Vantage.
    Returns a DataFrame with columns [Open, High, Low, Close, Volume]
    indexed by date — same shape as yfinance stock.history().
    Returns empty DataFrame on failure.
    """
    key = api_key or _get_api_key()
    if not key:
        return pd.DataFrame()

    data = _av_get({
        "function":    "TIME_SERIES_DAILY",
        "symbol":      ticker,
        "outputsize":  "compact",   # last 100 trading days
    }, key)

    ts = data.get("Time Series (Daily)", {})
    if not ts:
        return pd.DataFrame()

    try:
        rows = []
        for date_str, vals in sorted(ts.items()):
            rows.append({
                "Date":   pd.Timestamp(date_str),
                "Open":   float(vals.get("1. open",  0)),
                "High":   float(vals.get("2. high",  0)),
                "Low":    float(vals.get("3. low",   0)),
                "Close":  float(vals.get("4. close", 0)),
                "Volume": float(vals.get("5. volume",0)),
            })
        df = pd.DataFrame(rows).set_index("Date")
        df.index = pd.DatetimeIndex(df.index)
        return df
    except Exception:
        return pd.DataFrame()


def av_fill_financials(ticker: str, api_key: str = "") -> tuple:
    """
    Fetch income statement, balance sheet, and cash flow from Alpha Vantage.
    Returns (fin, bal, cf) as pandas DataFrames — same shape as yfinance.
    Returns (empty, empty, empty) on failure.
    Uses 3 API calls.
    """
    key = api_key or _get_api_key()
    if not key:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    empty = pd.DataFrame()

    def _parse_annual(data: dict, report_key: str) -> pd.DataFrame:
        """
        Parse AV annual reports into a DataFrame matching yfinance shape:
        rows = line items, columns = fiscal year dates.
        """
        reports = data.get(report_key, [])
        if not reports:
            return empty
        try:
            # Take up to 4 most recent annual reports
            cols = {}
            for rep in reports[:4]:
                date = rep.get("fiscalDateEnding", "")
                col = {}
                for k, v in rep.items():
                    if k == "fiscalDateEnding":
                        continue
                    val = _parse_av_float(v)
                    if val is not None:
                        col[k] = val
                cols[date] = col
            df = pd.DataFrame(cols)
            df.index.name = None
            return df
        except Exception:
            return empty

    # ── Income statement ──────────────────────────────────────────
    fin_data = _av_get({"function": "INCOME_STATEMENT", "symbol": ticker}, key)
    fin = _parse_annual(fin_data, "annualReports")

    # Map AV field names to yfinance equivalents so compute functions work
    _fin_rename = {
        "totalRevenue":           "Total Revenue",
        "netIncome":              "Net Income",
        "ebit":                   "EBIT",
        "operatingIncome":        "Operating Income",
        "depreciationAndAmortization": "Reconciled Depreciation",
    }
    if not fin.empty:
        fin = fin.rename(index={k: v for k, v in _fin_rename.items() if k in fin.index})

    # ── Balance sheet ─────────────────────────────────────────────
    bal_data = _av_get({"function": "BALANCE_SHEET", "symbol": ticker}, key)
    bal = _parse_annual(bal_data, "annualReports")

    _bal_rename = {
        "totalAssets":            "Total Assets",
        "totalLiabilities":       "Total Liabilities Net Minority Interest",
        "totalShareholderEquity": "Stockholders Equity",
        "shortLongTermDebtTotal": "Total Debt",
        "cashAndCashEquivalentsAtCarryingValue": "Cash And Cash Equivalents",
    }
    if not bal.empty:
        bal = bal.rename(index={k: v for k, v in _bal_rename.items() if k in bal.index})

    # ── Cash flow ─────────────────────────────────────────────────
    cf_data = _av_get({"function": "CASH_FLOW", "symbol": ticker}, key)
    cf = _parse_annual(cf_data, "annualReports")

    _cf_rename = {
        "operatingCashflow":          "Operating Cash Flow",
        "capitalExpenditures":        "Capital Expenditure",
        "netIncome":                  "Net Income",
        "depreciationDepletionAndAmortization": "Depreciation And Amortization",
    }
    if not cf.empty:
        cf = cf.rename(index={k: v for k, v in _cf_rename.items() if k in cf.index})

    return fin, bal, cf


def av_needs_fallback(info: dict) -> bool:
    """
    Returns True if yfinance info is sparse enough to warrant an AV fallback.
    Criteria:
      - Less than 10 keys in info dict (rate-limited stub)
      - Missing sector or sector is 'Unknown'
      - Missing price
    """
    if not info or len(info) < 10:
        return True
    if not info.get("sector") or info.get("sector") in ("Unknown", ""):
        return True
    if not info.get("currentPrice") and not info.get("regularMarketPrice"):
        return True
    return False


def av_needs_history_fallback(hist: pd.DataFrame) -> bool:
    """Returns True if price history is missing or too short to be useful."""
    return hist is None or hist.empty or len(hist) < 20


def av_needs_financials_fallback(fin, bal, cf) -> bool:
    """Returns True if all three financial statements are empty."""
    def _empty(df):
        return df is None or (hasattr(df, "empty") and df.empty)
    return _empty(fin) and _empty(bal) and _empty(cf)
