# -----------------------------------
# REQUIREMENTS
# pip install streamlit yfinance pandas numpy openpyxl tqdm requests
#
# RUN:
#   streamlit run app.py
# -----------------------------------

import io
import sys
import time
import requests
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from openpyxl.utils import get_column_letter
import yfinance as yf

# ───────────────────────────────────────────────────────────────
# PAGE CONFIG
# ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Screener",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Hybrid Stock Screener")
st.caption("Screens S&P 500, NYSE, or NASDAQ for individual companies — ETFs, funds, and index products excluded.")

tab_screener, tab_analyze = st.tabs(["📊 Screener", "🔍 Analyze a Stock"])

# ───────────────────────────────────────────────────────────────
# METRIC CONFIG  — name, default weight, full description
# ───────────────────────────────────────────────────────────────
METRICS = {
    "OE_Yield": {
        "label":   "OE Yield",
        "weight":  3,
        "desc":    "Owner Earnings Yield = (Net Income + Depreciation − CapEx) ÷ Market Cap. "
                   "Warren Buffett's preferred measure of true cash profitability relative to price. "
                   "Higher is better — above 5% is considered good value.",
    },
    "ROIC": {
        "label":   "ROIC",
        "weight":  2,
        "desc":    "Return on Invested Capital = NOPAT ÷ Invested Capital. "
                   "Measures how efficiently a company generates profit from the capital deployed. "
                   "Above 15% suggests a durable competitive advantage (moat).",
    },
    "ROIC_Trend": {
        "label":   "ROIC Trend",
        "weight":  2,
        "desc":    "Year-over-year change in ROIC. A rising ROIC means the company's moat is "
                   "widening — it's earning more from each dollar of capital over time. "
                   "Positive = improving; negative = deteriorating.",
    },
    "RevenueGrowth": {
        "label":   "Revenue Growth",
        "weight":  1,
        "desc":    "Year-over-year revenue growth rate. Measures top-line expansion. "
                   "Above 5% annually is considered healthy for a mature company; "
                   "high-growth companies often show 15–30%+.",
    },
    "EarningsGrowth": {
        "label":   "Earnings Growth",
        "weight":  1,
        "desc":    "Year-over-year earnings (EPS) growth rate. Confirms that revenue growth "
                   "is translating into real profit. Ideally grows faster than revenue, "
                   "indicating improving operating leverage.",
    },
    "Piotroski": {
        "label":   "Piotroski Score",
        "weight":  1,
        "desc":    "9-point accounting health score (F-Score) developed by Stanford professor "
                   "Joseph Piotroski. Tests profitability, leverage, and operating efficiency. "
                   "7–9 = financially strong; 0–2 = weak or potentially distressed.",
    },
    "OBV": {
        "label":   "OBV (On-Balance Volume)",
        "weight":  1,
        "desc":    "On-Balance Volume tracks cumulative buying vs selling pressure by adding "
                   "volume on up-days and subtracting on down-days. A rising OBV slope over "
                   "the last 20 days means institutions are quietly accumulating shares. "
                   "Score: 1.0 if slope is positive, 0.0 if negative.",
    },
    "MFI": {
        "label":   "MFI (Money Flow Index)",
        "weight":  1,
        "desc":    "Money Flow Index is a volume-weighted RSI (0–100). It measures whether "
                   "money is flowing into or out of a stock. Above 50 = net buying pressure; "
                   "above 60 = strong buying. Score scales from 0 (MFI=50) to 1.0 (MFI=100).",
    },
    "PCV": {
        "label":   "PCV (Price-Confirmed Volume)",
        "weight":  1,
        "desc":    "Price-Confirmed Volume measures what fraction of recent volume occurred "
                   "on days the stock closed higher than the prior day. If >55% of volume "
                   "happens on up-days, buyers are in control. Score scales from 0 (50/50) "
                   "to 1.0 (all up-day volume).",
    },
}

ALL_SECTORS = [
    "All Sectors",
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
]

EXCHANGES = {
    "S&P 500":  "sp500",
    "NYSE":     "nyse",
    "NASDAQ":   "nasdaq",
}

# Keywords used to detect and exclude non-company securities (ETFs, funds, trusts, etc.)
# These are matched against the security's longName / shortName from yfinance.
ETF_KEYWORDS = [
    "etf", "fund", "index", "trust", "ishares", "invesco", "vanguard",
    "spdr", "proshares", "direxion", "wisdomtree", "vaneck", "schwab",
    "fidelity select", "global x", "ark ", "pimco", "blackrock",
    "portfolio", "income", "bond", "treasury", "commodity", "reit index",
    "preferred", "notes", "debenture", "warrant", "certificate",
]

# ───────────────────────────────────────────────────────────────
# CACHE HELPERS — must be defined before sidebar renders
# ───────────────────────────────────────────────────────────────
import pickle as _pickle

def _cache_size_str() -> str:
    total = 0
    for key in ("screener_cache", "analyze_fin_cache"):
        obj = st.session_state.get(key)
        if obj is None:
            continue
        try:
            total += len(_pickle.dumps(obj, protocol=2))
        except Exception:
            total += sys.getsizeof(obj)
    if total == 0:        return "0 KB"
    if total < 1024:      return f"{total} B"
    if total < 1024**2:   return f"{total/1024:.1f} KB"
    return f"{total/1024**2:.2f} MB"

def _clear_all_cache():
    for key in ("screener_cache", "analyze_fin_cache"):
        st.session_state.pop(key, None)

# ───────────────────────────────────────────────────────────────
# SESSION STATE DEFAULTS — set once on first load
# ───────────────────────────────────────────────────────────────
_defaults = {
    "slider_max_price":  250,
    "slider_min_score":  2.0,
    "tog_ma50":          True,
    "tog_range":         False,
    "slider_range_days": 30,
    "slider_range_pct":  10.0,
    "slider_mfi_period": 14,
}
# Metric toggle/weight defaults
for _k, _cfg in METRICS.items():
    _defaults[f"tog_{_k}"] = True
    _defaults[f"wt_{_k}"]  = float(_cfg["weight"])

for _key, _val in _defaults.items():
    if _key not in st.session_state:
        st.session_state[_key] = _val

# ───────────────────────────────────────────────────────────────
# SIDEBAR — FILTERS  (Screener tab only)
# ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Screener Filters")

    # ── Magic Stock preset button ────────────────────────────────
    if st.button("✨ Magic Stock", use_container_width=True,
                 help="Auto-sets: Price ≤$200, Min Score=0, Below 50MA on, "
                      "Range filter on (10% / 30 days), MFI only (weight 5), MFI period 20 days"):
        # Write directly to each widget's session_state key
        st.session_state["slider_max_price"]   = 200
        st.session_state["slider_min_score"]   = 0.0
        st.session_state["tog_ma50"]           = True
        st.session_state["tog_range"]          = True
        st.session_state["slider_range_days"]  = 30
        st.session_state["slider_range_pct"]   = 10.0
        st.session_state["slider_mfi_period"]  = 20
        # Metric toggles — all off except MFI
        for _k in ["OE_Yield","ROIC","ROIC_Trend","RevenueGrowth","EarningsGrowth","Piotroski","OBV","PCV"]:
            st.session_state[f"tog_{_k}"] = False
            st.session_state[f"wt_{_k}"]  = 1.0
        st.session_state["tog_MFI"] = True
        st.session_state["wt_MFI"]  = 5.0
        st.rerun()

    st.divider()

    exchange = st.selectbox(
        "Exchange / Universe",
        options=list(EXCHANGES.keys()),
        index=0,
        help="S&P 500 = ~500 stocks (fast). NYSE/NASDAQ = 2,000–3,500 stocks (slow, 30–60+ min)."
    )

    sector = st.selectbox(
        "Market Sector",
        options=ALL_SECTORS,
        index=0,
        help="Filter to a specific GICS sector. Applied after scan using yfinance data."
    )

    max_price = st.slider(
        "Max Share Price ($)",
        min_value=10, max_value=1000, step=10,
        help="Exclude stocks above this price.",
        key="slider_max_price",
    )

    min_score = st.slider(
        "Min Hybrid Score",
        min_value=0.0, max_value=20.0, step=0.5,
        help="Only show stocks with a score above this threshold.",
        key="slider_min_score",
    )

    top_n = st.slider(
        "Max Results",
        min_value=5, max_value=100, value=30, step=5,
        help="Maximum number of stocks to display."
    )

    use_ma50_filter = st.toggle(
        "Only stocks BELOW 50-day MA",
        help="When on, only shows stocks in a pullback (price < MA50).",
        key="tog_ma50",
    )

    st.divider()
    st.header("📦 Price Range Filter")
    st.caption("Find stocks locked in a tight trading range — coiling before a breakout.")

    use_range_filter = st.toggle(
        "Enable Price Range Filter",
        help="When on, only shows stocks trading within a tight range over the selected period.",
        key="tog_range",
    )

    range_days = st.slider(
        "Range Lookback (days)",
        min_value=5, max_value=180, step=5,
        help="Number of trading days to measure the high/low range over.",
        key="slider_range_days",
    )

    range_max_pct = st.slider(
        "Max Range Width (%)",
        min_value=1.0, max_value=30.0, step=0.5,
        help="Maximum allowed spread between high and low as % of price. "
             "Lower = tighter range. 5–10% finds stocks in consolidation.",
        disabled=not use_range_filter,
        key="slider_range_pct",
    )

    st.divider()
    st.header("📊 Metrics")
    st.caption("Toggle on/off and adjust weight in the final score.")

    metric_enabled = {}
    metric_weight  = {}

    # ── Fundamental metrics ──────────────────────────────────────
    st.markdown("**Fundamental**")
    for key in ["OE_Yield", "ROIC", "ROIC_Trend", "RevenueGrowth", "EarningsGrowth", "Piotroski"]:
        cfg = METRICS[key]
        col_a, col_b = st.columns([1, 2])
        with col_a:
            metric_enabled[key] = st.toggle(cfg["label"], key=f"tog_{key}")
        with col_b:
            metric_weight[key] = st.slider(
                "Weight", min_value=0.0, max_value=5.0, step=0.5,
                key=f"wt_{key}",
                disabled=not metric_enabled[key],
                label_visibility="collapsed",
            )
        st.caption(cfg["desc"])

    # ── Volume / buying pressure metrics ────────────────────────
    st.markdown("**Volume & Buying Pressure**")
    for key in ["OBV", "MFI", "PCV"]:
        cfg = METRICS[key]
        col_a, col_b = st.columns([1, 2])
        with col_a:
            metric_enabled[key] = st.toggle(cfg["label"], key=f"tog_{key}")
        with col_b:
            metric_weight[key] = st.slider(
                "Weight", min_value=0.0, max_value=5.0, step=0.5,
                key=f"wt_{key}",
                disabled=not metric_enabled[key],
                label_visibility="collapsed",
            )
        st.caption(cfg["desc"])

    st.divider()
    st.header("🔧 Performance")
    max_workers = st.slider(
        "Parallel Workers",
        min_value=1, max_value=20, value=5, step=1,
        help="Lower values reduce rate-limiting errors on cloud. Recommended: 3–5 on Streamlit Cloud, 10+ on local PC."
    )

    mfi_period = st.slider(
        "MFI Period (days)",
        min_value=7, max_value=30, step=1,
        help="Money Flow Index lookback window.",
        key="slider_mfi_period",
    )

    st.divider()
    run_btn = st.button("🚀 Run Screener", use_container_width=True, type="primary")

    # ── Cache status panel ────────────────────────────────────────
    st.divider()
    _sc  = st.session_state.get("screener_cache", {})
    _fca = st.session_state.get("analyze_fin_cache", {})
    _sz  = _cache_size_str()

    if _sc.get("results") or _fca:
        _exch_name    = next((k for k, v in EXCHANGES.items() if v == _sc.get("exchange_key")), "?")
        _scan_tickers = len(_sc.get("results", []))
        _fin_tickers  = len(_fca)

        st.markdown("**💾 Cache Status**")
        ci1, ci2 = st.columns(2)
        ci1.metric("Screener Tickers", _scan_tickers if _scan_tickers else "—")
        ci2.metric("Analyzed Stocks",  _fin_tickers  if _fin_tickers  else "—")
        ci3, ci4 = st.columns(2)
        ci3.metric("Cache Size",   _sz)
        ci4.metric("Exchange",     _exch_name if _scan_tickers else "—")

        if _sc.get("scanned_at"):
            st.caption(f"🕐 Last scan: {_sc['scanned_at']}  ·  "
                       f"Cache clears automatically when you close the browser tab.")

        if st.button("🗑️ Clear All Cache", use_container_width=True, key="clear_all_cache_btn"):
            _clear_all_cache()
            st.success("All cache cleared.")
            st.rerun()
    else:
        st.markdown("**💾 Cache Status**")
        st.caption("No cache yet — run a scan or analyze a stock to populate it.  "
                   "Cache is stored in your browser session and clears automatically when the tab is closed.")

# ───────────────────────────────────────────────────────────────

def get_volume_signals(stock, mfi_period):
    """
    Returns a dict with three separate volume/buying-pressure scores, each 0.0–1.0:

    OBV  — On-Balance Volume trend over last 20 days (1.0 = rising, 0.0 = falling)
    MFI  — Money Flow Index scaled above 50 neutral (0.0–1.0)
    PCV  — Price-Confirmed Volume fraction on up-days, scaled above 50% baseline (0.0–1.0)
    """
    default = {"OBV": 0.0, "MFI": 0.0, "PCV": 0.0}
    try:
        hist = stock.history(period="6mo")
        if hist.empty or len(hist) < mfi_period + 5:
            return default
        close, high, low, vol = hist["Close"], hist["High"], hist["Low"], hist["Volume"]

        # ── OBV ──────────────────────────────────────────────────
        direction = np.sign(close.diff().fillna(0))
        obv       = (direction * vol).cumsum()
        obv_slope = np.polyfit(range(20), obv.iloc[-20:].values, 1)[0]
        obv_score = 1.0 if obv_slope > 0 else 0.0

        # ── MFI ──────────────────────────────────────────────────
        typical_price = (high + low + close) / 3
        raw_mf  = typical_price * vol
        tp_diff = typical_price.diff()
        pos_mf  = raw_mf.where(tp_diff > 0, 0).rolling(mfi_period).sum()
        neg_mf  = raw_mf.where(tp_diff < 0, 0).rolling(mfi_period).sum()
        mfr     = pos_mf / neg_mf.replace(0, np.nan)
        mfi_val = (100 - (100 / (1 + mfr))).iloc[-1]
        mfi_score = max(0.0, (mfi_val - 50) / 50) if pd.notnull(mfi_val) else 0.0

        # ── PCV ──────────────────────────────────────────────────
        recent    = hist.iloc[-20:].copy()
        recent["up_day"] = recent["Close"] > recent["Close"].shift(1)
        up_vol    = recent.loc[recent["up_day"], "Volume"].sum()
        total_vol = recent["Volume"].sum()
        pcv_ratio = up_vol / total_vol if total_vol > 0 else 0.5
        pcv_score = max(0.0, (pcv_ratio - 0.5) / 0.5)

        return {
            "OBV": round(obv_score, 4),
            "MFI": round(mfi_score, 4),
            "PCV": round(pcv_score, 4),
        }
    except:
        return default


def calculate_price_range(stock, range_days: int) -> dict:
    """
    Calculates the price range over the last `range_days` trading days.

    Returns:
        RangeHigh  — highest closing price in the period
        RangeLow   — lowest closing price in the period
        RangePct   — range width as % of midpoint price  (High-Low) / Mid × 100
                     Lower = tighter range = stock is consolidating
        RangePos   — where today's price sits in the range  0.0 = at low, 1.0 = at high
                     0.5 = midpoint; useful for spotting stocks near support (low end)
    """
    default = {"RangeHigh": None, "RangeLow": None, "RangePct": None, "RangePos": None}
    try:
        hist = stock.history(period="1y")
        if hist.empty or len(hist) < range_days:
            return default
        window   = hist["Close"].iloc[-range_days:]
        high     = round(window.max(), 2)
        low      = round(window.min(), 2)
        mid      = (high + low) / 2
        if mid == 0:
            return default
        rng_pct  = round((high - low) / mid * 100, 2)
        current  = window.iloc[-1]
        rng_pos  = round((current - low) / (high - low), 4) if (high - low) > 0 else 0.5
        return {
            "RangeHigh": high,
            "RangeLow":  low,
            "RangePct":  rng_pct,
            "RangePos":  rng_pos,
        }
    except:
        return default


def calculate_piotroski(stock):
    try:
        fin, bal, cf = stock.financials, stock.balance_sheet, stock.cashflow
        score = 0
        roa = fin.loc["Net Income"] / bal.loc["Total Assets"]
        if roa.iloc[0] > 0: score += 1
        if cf.loc["Operating Cash Flow"].iloc[0] > 0: score += 1
        if roa.iloc[0] > roa.iloc[1]: score += 1
        if cf.loc["Operating Cash Flow"].iloc[0] > fin.loc["Net Income"].iloc[0]: score += 1
        if bal.loc["Long Term Debt"].iloc[0] < bal.loc["Long Term Debt"].iloc[1]: score += 1
        if (bal.loc["Current Assets"].iloc[0] / bal.loc["Current Liabilities"].iloc[0]) > \
           (bal.loc["Current Assets"].iloc[1] / bal.loc["Current Liabilities"].iloc[1]): score += 1
        return score
    except:
        return None


def get_owner_earnings(stock, info):
    try:
        cf, fin = stock.cashflow, stock.financials
        oe = fin.loc["Net Income"].iloc[0] + cf.loc["Depreciation"].iloc[0] - abs(cf.loc["Capital Expenditure"].iloc[0])
        mc = info.get("marketCap")
        return oe, (oe / mc if mc else None)
    except:
        return None, None


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

def calculate_roic(stock):
    try:
        fin, bal = stock.financials, stock.balance_sheet
        ebit_s = _get_fin_value(fin, "EBIT", "Ebit", "Operating Income", "OperatingIncome", "EBITDA", "Ebitda")
        if ebit_s is None: return None
        ebit = ebit_s.iloc[0]
        assets_s = _get_bal_value(bal, "Total Assets", "TotalAssets")
        if assets_s is None: return None
        liab_s = _get_bal_value(bal, "Total Current Liabilities", "TotalCurrentLiabilities", "Current Liabilities", "CurrentLiabilities")
        if liab_s is None: return None
        cash_s = _get_bal_value(bal, "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments", "CashAndCashEquivalents", "Cash")
        cash = cash_s.iloc[0] if cash_s is not None else 0
        ic = assets_s.iloc[0] - liab_s.iloc[0] - cash
        if pd.isna(ic) or ic == 0: return None
        roic = ebit * 0.79 / ic
        return None if pd.isna(roic) else roic
    except:
        return None


def calculate_roic_trend(stock):
    try:
        fin, bal = stock.financials, stock.balance_sheet
        ebit_s = _get_fin_value(fin, "EBIT", "Ebit", "Operating Income", "OperatingIncome", "EBITDA", "Ebitda")
        if ebit_s is None or len(ebit_s) < 2: return None
        assets_s = _get_bal_value(bal, "Total Assets", "TotalAssets")
        liab_s   = _get_bal_value(bal, "Total Current Liabilities", "TotalCurrentLiabilities", "Current Liabilities", "CurrentLiabilities")
        if assets_s is None or liab_s is None: return None
        if len(assets_s) < 2 or len(liab_s) < 2: return None
        def roic_at(i):
            ic = assets_s.iloc[i] - liab_s.iloc[i]
            if pd.isna(ic) or ic == 0: return None
            v = ebit_s.iloc[i] / ic
            return None if pd.isna(v) else v
        r0, r1 = roic_at(0), roic_at(1)
        return (r0 - r1) if (r0 is not None and r1 is not None) else None
    except:
        return None


def process_ticker(args):
    t, mfi_period, range_days = args
    # Retry up to 3 times with backoff — yfinance can return empty data
    # on the first attempt when running in cloud environments
    for attempt in range(3):
        try:
            time.sleep(attempt * 1.5)   # 0s, 1.5s, 3s between retries
            stock = yf.Ticker(t)
            info  = stock.info

            # yfinance returns an empty/minimal dict for invalid tickers
            if not info or len(info) < 5:
                return None

            # Exclude ETFs, index funds, trusts, and other non-company securities
            if is_etf_or_fund(info):
                return None

            price = info.get("currentPrice") or info.get("regularMarketPrice")
            if price is None:
                return None   # skip tickers with no price data

            owner_earnings, oe_yield = get_owner_earnings(stock, info)
            vol_signals  = get_volume_signals(stock, mfi_period)
            range_data   = calculate_price_range(stock, range_days)
            try:
                hist = stock.history(period="3mo")
                ma50 = round(hist["Close"].rolling(50).mean().iloc[-1], 2) if len(hist) >= 50 else None
            except:
                ma50 = None
            return {
                "Ticker":         t,
                "Sector":         info.get("sector", "Unknown"),
                "Price":          price,
                "MA50":           ma50,
                "RangeHigh":      range_data["RangeHigh"],
                "RangeLow":       range_data["RangeLow"],
                "RangePct":       range_data["RangePct"],
                "RangePos":       range_data["RangePos"],
                "MarketCap":      info.get("marketCap"),
                "P/E":            info.get("trailingPE"),
                "OwnerEarnings":  owner_earnings,
                "OE_Yield":       oe_yield,
                "ROIC":           calculate_roic(stock),
                "ROIC_Trend":     calculate_roic_trend(stock),
                "RevenueGrowth":  info.get("revenueGrowth"),
                "EarningsGrowth": info.get("earningsGrowth"),
                "Piotroski":      calculate_piotroski(stock),
                "OBV":            vol_signals["OBV"],
                "MFI":            vol_signals["MFI"],
                "PCV":            vol_signals["PCV"],
            }
        except Exception:
            if attempt == 2:
                return None   # all 3 attempts failed
            continue
    return None

# ───────────────────────────────────────────────────────────────
# TICKER LOADERS
# ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)   # cache for 24 hours so repeated runs are instant
def load_tickers(exchange_key: str) -> list:
    """Return a list of tickers for the chosen exchange."""
    if exchange_key == "sp500":
        url  = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        df = pd.read_html(resp.text)[0]
        return df["Symbol"].str.replace(".", "-", regex=False).tolist()

    elif exchange_key == "nyse":
        # NASDAQ's free FTP-style screener API works for NYSE too
        url  = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=5000&exchange=nyse"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("table", {}).get("rows", [])
        return [r["symbol"].strip() for r in data if r.get("symbol")]

    elif exchange_key == "nasdaq":
        url  = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=5000&exchange=nasdaq"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("table", {}).get("rows", [])
        return [r["symbol"].strip() for r in data if r.get("symbol")]

    return []


def is_etf_or_fund(info: dict) -> bool:
    """
    Return True if the security is an ETF, index fund, trust, or other
    non-single-company product that should be excluded from the screener.
    Checks yfinance quoteType first (most reliable), then falls back to
    name keyword matching.
    """
    quote_type = (info.get("quoteType") or "").lower()
    if quote_type in ("etf", "mutualfund", "index", "future", "option", "currency", "cryptocurrency"):
        return True

    # Fallback — check name for known fund/ETF keywords
    name = (info.get("longName") or info.get("shortName") or "").lower()
    return any(kw in name for kw in ETF_KEYWORDS)

def build_excel(screened: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        screened.to_excel(writer, index=False, sheet_name="Stock_Picks")
        sheet = writer.book["Stock_Picks"]
        col_map = {cell.value: get_column_letter(cell.column) for cell in sheet[1]}
        for col_name in ["RevenueGrowth", "EarningsGrowth"]:
            if col_name in col_map:
                for cell in sheet[col_map[col_name]][1:]:
                    if cell.value is not None:
                        cell.number_format = "0.00%"
        if "OwnerEarnings" in col_map:
            for cell in sheet[col_map["OwnerEarnings"]][1:]:
                if cell.value is not None:
                    cell.number_format = "$#,##0"
        for idx, col in enumerate(screened.columns, 1):
            col_letter = get_column_letter(idx)
            max_len = max([len(str(c.value)) for c in sheet[col_letter] if c.value] + [len(col)])
            sheet.column_dimensions[col_letter].width = max_len + 2
    return output.getvalue()

# ───────────────────────────────────────────────────────────────
# SCORE COLOR MAP
# ───────────────────────────────────────────────────────────────

def color_score(val):
    try:
        v = float(val)
        if v >= 8:   return "background-color: #1a7a1a; color: white"
        elif v >= 5: return "background-color: #4caf50; color: white"
        elif v >= 3: return "background-color: #ff9800; color: white"
        else:        return "background-color: #f44336; color: white"
    except:
        return ""

# ───────────────────────────────────────────────────────────────
# MAIN RUN LOGIC
# ───────────────────────────────────────────────────────────────

with tab_screener:
 if run_btn:
    active_metrics = [k for k, v in metric_enabled.items() if v]

    exchange_key  = EXCHANGES[exchange]
    sector_label  = sector if sector != "All Sectors" else "All Sectors"

    # ── Check if we already have cached scan data for this exchange ──
    cache         = st.session_state.get("screener_cache", {})
    cache_valid   = (
        cache.get("exchange_key") == exchange_key and
        cache.get("mfi_period")   == mfi_period   and
        cache.get("range_days")   == range_days    and
        bool(cache.get("results"))
    )

    if cache_valid:
        results = cache["results"]
        st.success(
            f"⚡ Using cached data from last scan "
            f"({len(results)} tickers · {cache.get('scanned_at','')}) — "
            f"filters re-applied instantly. Est. time: **~0 sec**. Click **Clear Cache & Rescan** to fetch fresh data."
        )
        if st.button("🔄 Clear Cache & Rescan", key="clear_cache_btn"):
            st.session_state.pop("screener_cache", None)
            st.rerun()
    else:
        # Load tickers for selected exchange (cached for 24hrs)
        with st.spinner(f"Loading {exchange} tickers..."):
            try:
                tickers = load_tickers(exchange_key)
            except Exception as e:
                st.error(f"Failed to load tickers: {e}")
                st.stop()

        if not tickers:
            st.error(f"No tickers returned for {exchange}. Try again later.")
            st.stop()

        st.info(
            f"Scanning **{len(tickers)}** tickers on **{exchange}** · "
            f"Sector: **{sector_label}** · ETFs/funds excluded · Est. time: ~5 min"
        )

        # Progress bar + threaded scan
        progress_bar = st.progress(0, text="Starting scan...")
        results = []
        total = len(tickers)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_ticker, (t, mfi_period, range_days)): t for t in tickers}
            done = 0
            for future in futures:
                result = future.result()
                if result:
                    results.append(result)
                done += 1
                pct = done / total
                progress_bar.progress(pct, text=f"Scanning... {done}/{total} ({int(pct*100)}%)")

        progress_bar.empty()

        if not results:
            st.error(
                "No results returned. This is usually caused by yfinance rate limiting on Streamlit Cloud. "
                "Try these fixes:\n\n"
                "1. **Reduce Parallel Workers** to 3 in the sidebar and run again\n"
                "2. **Wait 60 seconds** and try again — rate limits reset quickly\n"
                "3. If it keeps failing, try scanning a **single sector** instead of all sectors to reduce the number of requests"
            )
            st.stop()

        # ── Store results in session_state cache ──────────────────
        st.session_state["screener_cache"] = {
            "exchange_key": exchange_key,
            "mfi_period":   mfi_period,
            "range_days":   range_days,
            "results":      results,
            "scanned_at":   datetime.now().strftime("%H:%M:%S"),
        }

    # Build DataFrame
    df = pd.DataFrame(results)
    df.replace(["N/A", "None", "-", ""], pd.NA, inplace=True)

    numeric_cols = ["Price", "MA50", "RangeHigh", "RangeLow", "RangePct", "RangePos",
                    "MarketCap", "P/E", "OwnerEarnings", "OE_Yield",
                    "ROIC", "ROIC_Trend", "RevenueGrowth", "EarningsGrowth",
                    "Piotroski", "OBV", "MFI", "PCV"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ROIC_Trend"] = df["ROIC_Trend"].fillna(np.nan)
    for vol_col in ["OBV", "MFI", "PCV"]:
        df[vol_col] = df[vol_col].fillna(0)

    # ── Sector filter — applied here using yfinance sector (authoritative) ──
    if sector != "All Sectors":
        df = df[df["Sector"].str.strip().str.lower() == sector.strip().lower()]
        if df.empty:
            st.error(f"No results found for sector: **{sector}**. The sector name may differ from yfinance labels.")
            st.stop()

    df["P/E"]           = df["P/E"].round(2)
    df["OE_Yield"]      = df["OE_Yield"].round(2)
    df["ROIC"]          = df["ROIC"].round(2)
    df["ROIC_Trend"]    = df["ROIC_Trend"].round(2)
    df["OBV"]           = df["OBV"].round(2)
    df["MFI"]           = df["MFI"].round(2)
    df["PCV"]           = df["PCV"].round(2)
    df["MA50"]          = pd.to_numeric(df["MA50"], errors="coerce").round(2)
    df["RangeHigh"]     = pd.to_numeric(df["RangeHigh"], errors="coerce").round(2)
    df["RangeLow"]      = pd.to_numeric(df["RangeLow"],  errors="coerce").round(2)
    df["RangePct"]      = pd.to_numeric(df["RangePct"],  errors="coerce").round(2)
    df["RangePos"]      = pd.to_numeric(df["RangePos"],  errors="coerce").round(4)

    # Dynamic score — use sidebar weight sliders, skip disabled metrics
    score = pd.Series(0.0, index=df.index)
    for key in METRICS:
        if metric_enabled.get(key, False):
            w = metric_weight.get(key, METRICS[key]["weight"])
            score += df[key].fillna(0) * w
    df["Score"] = score.round(2)

    # Apply filters
    under_price = df["Price"].isna() | (df["Price"] <= max_price)
    # Only apply score threshold when at least one metric is active —
    # if all metrics are off the score is 0 for everything, so filtering by score
    # would return nothing. Instead sort by RangePct (tightest range first).
    if active_metrics:
        above_score = df["Score"] >= min_score
    else:
        above_score = pd.Series(True, index=df.index)
    if use_ma50_filter:
        below_ma50 = df["Price"].isna() | df["MA50"].isna() | (df["Price"] <= df["MA50"])
    else:
        below_ma50 = pd.Series(True, index=df.index)

    # Price range filter — keep only stocks whose range width is within max_range_pct
    if use_range_filter:
        in_range = df["RangePct"].isna() | (df["RangePct"] <= range_max_pct)
    else:
        in_range = pd.Series(True, index=df.index)

    # Sort by Score when metrics are active, otherwise by RangePct (tightest range first)
    sort_col = "Score" if active_metrics else "RangePct"
    sort_asc  = not active_metrics   # ascending for RangePct (tighter = better), descending for Score

    screened = (
        df[under_price & below_ma50 & above_score & in_range]
        .sort_values(sort_col, ascending=sort_asc, na_position="last")
        .head(top_n)
        .reset_index(drop=True)
    )

    # ── Active metrics summary banner ────────────────────────────
    if active_metrics:
        active_labels = [METRICS[k]["label"] for k in active_metrics]
        st.success(f"**Active metrics:** {' · '.join(active_labels)}")
    else:
        st.info("**No metrics active** — results sorted by tightest price range. Score column will show 0.")

    # ── Results summary cards ────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tickers Scanned", len(df))
    col2.metric("Passed Filters",  len(screened))
    col3.metric("Avg Score",       f"{screened['Score'].mean():.2f}" if not screened.empty else "—")
    col4.metric("Top Score",       f"{screened['Score'].max():.2f}"  if not screened.empty else "—")

    st.divider()

    if screened.empty:
        st.warning("No stocks passed the current filters. Try loosening the Score threshold, disabling the MA50 filter, or selecting a different sector.")
    else:
        # Format display columns
        display = screened.copy()
        display["MarketCap"]      = display["MarketCap"].apply(lambda x: f"${x:,.0f}" if pd.notnull(x) else "")
        display["OwnerEarnings"]  = display["OwnerEarnings"].apply(lambda x: f"${x:,.0f}" if pd.notnull(x) else "")
        display["RevenueGrowth"]  = display["RevenueGrowth"].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else "")
        display["EarningsGrowth"] = display["EarningsGrowth"].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else "")
        display["RangePct"]       = display["RangePct"].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "")
        display["RangePos"]       = display["RangePos"].apply(
            lambda x: f"{'▁▂▃▄▅▆▇█'[min(int(x*8),7)] if pd.notnull(x) else '—'} {x:.0%}" if pd.notnull(x) else "—"
        )

        # Round all remaining numeric columns to 2 decimal places
        # Exclude: dollar columns, percentage columns, and range cols already formatted as strings
        skip_cols = {"Ticker", "Sector", "MarketCap", "OwnerEarnings",
                     "RevenueGrowth", "EarningsGrowth", "RangePct", "RangePos",
                     "OE_Yield", "ROIC", "ROIC_Trend"}
        for col in display.columns:
            if col not in skip_cols:
                if pd.api.types.is_numeric_dtype(display[col]):
                    display[col] = display[col].apply(lambda x: round(x, 2) if pd.notnull(x) else x)

        # Hide columns for metrics that are toggled off
        all_metric_keys = list(METRICS.keys())
        hidden_cols = [k for k in all_metric_keys if not metric_enabled.get(k, True) and k in display.columns]
        display = display.drop(columns=hidden_cols, errors="ignore")

        styled = display.style.applymap(color_score, subset=["Score"])

        st.subheader(f"Top {len(screened)} Stocks — {sector_label}")
        st.dataframe(styled, use_container_width=True, height=600)

        # Download button
        st.divider()
        excel_bytes = build_excel(screened)
        st.download_button(
            label="⬇️ Download Excel",
            data=excel_bytes,
            file_name=f"stock_screener_{sector.replace(' ', '_')}_{datetime.today().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

 else:
    st.info("👈 Configure your filters in the sidebar, then click **Run Screener** to start.")
    st.markdown("""
    ### How to use
    1. **Pick an exchange** — S&P 500 (~500 stocks, fast), NYSE or NASDAQ (2,000–3,500 stocks, 30–70 min)
    2. **Pick a sector** — scan one GICS sector or all sectors
    3. **Toggle metrics on/off** — disabled metrics are excluded from scoring AND hidden from results
    4. **Set your filters** — price cap, min score, MA50 toggle
    5. Hit **🚀 Run Screener**

    > ⚠️ **ETFs, index funds, trusts, and other non-company securities are automatically excluded** from all results.

    ### Metric weights (when enabled)
    | Metric | Default Weight | Ideal |
    |---|---|---|
    | OE Yield | ×3 | > 5% |
    | ROIC | ×2 | > 15% |
    | ROIC Trend | ×2 | Positive |
    | Revenue Growth | ×1 | > 5% |
    | Earnings Growth | ×1 | > 5% |
    | Piotroski Score | ×1 | 7–9 |
    | OBV (On-Balance Volume) | ×1 | 1.0 = rising accumulation |
    | MFI (Money Flow Index) | ×1 | > 0.5 = buying pressure |
    | PCV (Price-Confirmed Volume) | ×1 | > 0.5 = up-day volume dominant |

    ### Active filters
    - **Exchange** — defines the universe of tickers to scan
    - **Sector** — narrows results to one GICS sector (applied post-scan)
    - **Price cap** — stocks above your max price are excluded
    - **MA50 toggle** — when on, only stocks trading *below* their 50-day MA are shown
    - **Min score** — removes low-conviction picks from the table
    """)


# ───────────────────────────────────────────────────────────────
# ANALYZE TAB — single ticker deep dive
# ───────────────────────────────────────────────────────────────
with tab_analyze:
    st.subheader("🔍 Single Stock Analyzer")
    st.caption("Enter any ticker and select which metrics to run. Works on any stock — S&P 500, NYSE, NASDAQ, or international.")

    a_col1, a_col2 = st.columns([1, 2])

    with a_col1:
        ticker_input = st.text_input(
            "Stock Ticker",
            placeholder="e.g. AAPL, MSFT, TSLA",
            key="analyze_ticker_input",
            help="Enter the ticker symbol exactly as it appears on the exchange."
        ).strip().upper()

        st.markdown("**Select Metrics to Analyze**")
        analyze_metrics = {}
        for key, cfg in METRICS.items():
            analyze_metrics[key] = st.checkbox(
                cfg["label"], value=True,
                key=f"analyze_{key}", help=cfg["desc"],
            )

        analyze_btn = st.button("🔬 Analyze", type="primary", use_container_width=True)

    # ── On Analyze click: fetch only what is missing from cache ──
    if analyze_btn and ticker_input:
        _screener_cache  = st.session_state.get("screener_cache", {})
        _cached_results  = _screener_cache.get("results", [])
        _cached_row      = next(
            (r for r in _cached_results if r.get("Ticker") == ticker_input), None
        )

        # Ensure fin cache dict exists and has an entry for this ticker
        if "analyze_fin_cache" not in st.session_state:
            st.session_state["analyze_fin_cache"] = {}
        _fin_store = st.session_state["analyze_fin_cache"]
        if ticker_input not in _fin_store:
            _fin_store[ticker_input] = {}          # initialise empty — avoids KeyError
        _fc = _fin_store[ticker_input]             # shorthand reference

        # ── Validator: is a cached value actually usable? ──────────
        def _ok(key):
            v = _fc.get(key)
            if v is None:                                return False
            if isinstance(v, pd.DataFrame) and v.empty:  return False
            if isinstance(v, dict) and not v:             return False  # only reject empty dicts
            return True

        need_info = not _ok("info")
        need_hist = not _ok("hist")
        need_fin  = not _ok("fin")
        need_bal  = not _ok("bal")
        need_cf   = not _ok("cf")
        need_raw  = not _ok("raw")

        anything_missing = any([need_info, need_hist, need_fin, need_bal, need_cf, need_raw])

        if anything_missing:
            missing_labels = [k for k, n in [
                ("info", need_info), ("price history", need_hist),
                ("income stmt", need_fin), ("balance sheet", need_bal),
                ("cash flow", need_cf), ("metrics", need_raw),
            ] if n]
            with st.spinner(f"Fetching for **{ticker_input}**: {', '.join(missing_labels)}..."):
                # Create ticker object once — used for all fetches below
                _stock = yf.Ticker(ticker_input)

                # ── info ─────────────────────────────────────────
                if need_info:
                    try:
                        _info = _stock.info or {}
                        # Store whatever we got — even a partial dict is useful
                        if _info:
                            _fc["info"] = _info
                    except Exception:
                        _info = _fc.get("info", {})
                else:
                    _info = _fc.get("info", {})

                # Patch info with screener row data for any gaps
                if _cached_row:
                    _patch = {
                        "sector":         _cached_row.get("Sector"),
                        "currentPrice":   _cached_row.get("Price"),
                        "marketCap":      _cached_row.get("MarketCap"),
                        "trailingPE":     _cached_row.get("P/E"),
                        "revenueGrowth":  _cached_row.get("RevenueGrowth"),
                        "earningsGrowth": _cached_row.get("EarningsGrowth"),
                    }
                    for k, v in _patch.items():
                        if v is not None and not _info.get(k):
                            _info[k] = v

                # Only abort if we have absolutely nothing — no info AND no screener row
                if not _info and not _cached_row:
                    st.error(f"Could not find data for **{ticker_input}**. "
                             f"Check the ticker symbol and try again.")
                    st.stop()

                # ── price history ─────────────────────────────────
                if need_hist:
                    try:
                        _hist = _stock.history(period="1y")
                        if not _hist.empty:
                            _fc["hist"] = _hist
                    except Exception:
                        _hist = _fc.get("hist", pd.DataFrame())
                else:
                    _hist = _fc["hist"]

                # ── financial statements ──────────────────────────
                if need_fin:
                    try:
                        _fin = _stock.financials
                        _fc["fin"] = _fin
                    except Exception:
                        _fin = _fc.get("fin")
                else:
                    _fin = _fc.get("fin")

                if need_bal:
                    try:
                        _bal = _stock.balance_sheet
                        _fc["bal"] = _bal
                    except Exception:
                        _bal = _fc.get("bal")
                else:
                    _bal = _fc.get("bal")

                if need_cf:
                    try:
                        _cf = _stock.cashflow
                        _fc["cf"] = _cf
                    except Exception:
                        _cf = _fc.get("cf")
                else:
                    _cf = _fc.get("cf")

                # ── metrics (raw scores) ──────────────────────────
                if need_raw:
                    if _cached_row:
                        # Pull directly from screener data — no recalculation needed
                        _raw = {
                            "OE_Yield":       _cached_row.get("OE_Yield"),
                            "ROIC":           _cached_row.get("ROIC"),
                            "ROIC_Trend":     _cached_row.get("ROIC_Trend"),
                            "RevenueGrowth":  _cached_row.get("RevenueGrowth"),
                            "EarningsGrowth": _cached_row.get("EarningsGrowth"),
                            "Piotroski":      _cached_row.get("Piotroski"),
                            "OBV":            _cached_row.get("OBV"),
                            "MFI":            _cached_row.get("MFI"),
                            "PCV":            _cached_row.get("PCV"),
                        }
                    else:
                        try:
                            _oe, _oey = get_owner_earnings(_stock, _info)
                            _vols     = get_volume_signals(_stock, mfi_period)
                            _raw = {
                                "OE_Yield":       _oey,
                                "ROIC":           calculate_roic(_stock),
                                "ROIC_Trend":     calculate_roic_trend(_stock),
                                "RevenueGrowth":  _info.get("revenueGrowth"),
                                "EarningsGrowth": _info.get("earningsGrowth"),
                                "Piotroski":      calculate_piotroski(_stock),
                                "OBV":            _vols["OBV"],
                                "MFI":            _vols["MFI"],
                                "PCV":            _vols["PCV"],
                            }
                        except Exception:
                            _raw = _fc.get("raw", {})
                    _fc["raw"] = _raw
                else:
                    _raw = _fc["raw"]

        else:
            # ── Fully cached — zero API calls ────────────────────
            _info = _fc["info"]
            _hist = _fc["hist"]
            _fin  = _fc.get("fin")
            _bal  = _fc.get("bal")
            _cf   = _fc.get("cf")
            _raw  = _fc["raw"]

        # Persist everything back to store
        _fin_store[ticker_input].update({
            "info": _info, "hist": _hist,
            "fin":  _fin,  "bal":  _bal,
            "cf":   _cf,   "raw":  _raw,
        })

        st.session_state["analyze_data"] = {
            "ticker":      ticker_input,
            "info":        _info,
            "hist":        _hist,
            "fin":         _fin,
            "bal":         _bal,
            "cf":          _cf,
            "raw":         _raw,
            "metrics_sel": dict(analyze_metrics),
            "from_cache":  bool(_cached_row),
        }

    elif analyze_btn and not ticker_input:
        st.warning("Please enter a ticker symbol.")

    # ── Render from session_state — persists across all reruns ──
    with a_col2:
        if "analyze_data" not in st.session_state:
            st.markdown("""
            **How to use:**
            1. Type a ticker symbol on the left (e.g. `NVDA`)
            2. Check the metrics you want to see
            3. Click **Analyze**

            Results show the raw value for each metric alongside a
            color-coded signal: 🟢 Good · 🟡 Neutral · 🔴 Weak

            You can analyze **any publicly traded stock** — not just S&P 500.
            Changing the chart time frame will **not** reset the analysis.
            """)
        else:
            try:
                _d       = st.session_state["analyze_data"]
                info     = _d["info"]
                hist_1y  = _d["hist"]
                fin_stmt = _d["fin"]
                bal_stmt = _d["bal"]
                cf_stmt  = _d["cf"]
                raw      = _d["raw"]
                sticker  = _d["ticker"]
                selected = [k for k, v in _d["metrics_sel"].items() if v]

                name    = info.get("longName") or info.get("shortName") or sticker
                price   = info.get("currentPrice") or info.get("regularMarketPrice")
                mktcap  = info.get("marketCap")

                st.markdown(f"## {name} &nbsp; `{sticker}`")
                # Show data source
                if _d.get("from_cache"):
                    st.caption("⚡ Metrics from screener cache · Financials cached locally — no API calls")
                h1, h2, h3, h4 = st.columns(4)
                h1.metric("Price",    f"${price:,.2f}" if price else "N/A")
                h2.metric("Sector",   info.get("sector", "N/A"))
                h3.metric("Industry", info.get("industry", "N/A"))
                h4.metric("Mkt Cap",  f"${mktcap/1e9:.1f}B" if mktcap else "N/A")
                st.divider()

                if not selected:
                    st.warning("No metrics were selected when Analyze was run.")
                    st.stop()

                THRESHOLDS = {
                    "OE_Yield":       (0.05, 0.02, True),
                    "ROIC":           (0.15, 0.08, True),
                    "ROIC_Trend":     (0.02, 0.0,  True),
                    "RevenueGrowth":  (0.10, 0.05, True),
                    "EarningsGrowth": (0.10, 0.05, True),
                    "Piotroski":      (7,    4,    True),
                    "OBV":            (0.8,  0.4,  True),
                    "MFI":            (0.5,  0.2,  True),
                    "PCV":            (0.5,  0.2,  True),
                }

                def _sig(key, val):
                    if val is None or (isinstance(val, float) and np.isnan(val)): return "⚪"
                    g, n, hi = THRESHOLDS.get(key, (None, None, True))
                    if g is None: return "⚪"
                    if hi:  return "🟢" if val >= g else ("🟡" if val >= n else "🔴")
                    else:   return "🟢" if val <= g else ("🟡" if val <= n else "🔴")

                def _fv(key, val):
                    if val is None or (isinstance(val, float) and np.isnan(val)): return "N/A"
                    if key in ("OE_Yield","RevenueGrowth","EarningsGrowth","ROIC","ROIC_Trend"): return f"{val:.2%}"
                    if key in ("OBV","MFI","PCV"): return f"{val:.4f}"
                    if key == "Piotroski": return f"{int(val)} / 9"
                    return str(round(val, 4))

                def _fb(val):
                    if val is None or (isinstance(val, float) and np.isnan(val)): return "N/A"
                    try:
                        v = float(val)
                        if abs(v)>=1e12: return f"${v/1e12:.2f}T"
                        if abs(v)>=1e9:  return f"${v/1e9:.2f}B"
                        if abs(v)>=1e6:  return f"${v/1e6:.2f}M"
                        if abs(v)>=1e3:  return f"${v/1e3:.2f}K"
                        return f"${v:,.2f}"
                    except: return "N/A"

                def _fp(val):
                    try:    return f"{float(val):.2%}" if val is not None else "N/A"
                    except: return "N/A"

                def _fn(val, d=2):
                    try:    return f"{float(val):,.{d}f}" if val is not None else "N/A"
                    except: return "N/A"

                def irow(label, value, tip=None):
                    # tip renders as a small ℹ tooltip after the label
                    tip_html = (f" <span title='{tip}' style='cursor:help;color:#888;"
                                f"font-size:0.85em;'>ℹ️</span>") if tip else ""
                    st.markdown(
                        f"<div style='display:flex;justify-content:space-between;"
                        f"padding:6px 0;border-bottom:1px solid #2a2a2a;'>"
                        f"<span style='color:#aaa;'>{label}{tip_html}</span>"
                        f"<span style='font-weight:600;'>{value}</span></div>",
                        unsafe_allow_html=True)

                # ── Metric cards ─────────────────────────────────
                st.markdown("### Metric Results")
                cols = st.columns(3)
                for i, key in enumerate(selected):
                    cfg = METRICS[key]; val = raw.get(key)
                    with cols[i % 3]:
                        st.markdown(
                            f"""<div style="border:1px solid #444;border-radius:8px;
                                           padding:14px;margin-bottom:12px;">
                              <div style="font-size:1.1em;font-weight:600;">{_sig(key,val)} {cfg['label']}</div>
                              <div style="font-size:1.8em;font-weight:700;margin:6px 0;">{_fv(key,val)}</div>
                              <div style="font-size:0.78em;color:#aaa;">{cfg['desc'][:120]}...</div>
                            </div>""", unsafe_allow_html=True)

                # ── MA50 ─────────────────────────────────────────
                st.divider()
                if not hist_1y.empty and len(hist_1y) >= 50:
                    ma50_val = round(hist_1y["Close"].rolling(50).mean().iloc[-1], 2)
                    diff_pct = ((price - ma50_val) / ma50_val * 100) if price else 0
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Current Price", f"${price:,.2f}" if price else "N/A",
                              help="The latest traded price of the stock")
                    m2.metric("50-Day MA",     f"${ma50_val:,.2f}",
                              help="50-Day Moving Average — the average closing price over the last 50 trading days. "
                                   "Price below this line = stock is in a short-term downtrend or pullback")
                    m3.metric("vs MA50",       f"{diff_pct:+.1f}%", delta_color="inverse",
                              help="How far the current price is above or below the 50-day MA. "
                                   "Negative = trading below the MA (potential value zone). "
                                   "Positive = trading above the MA (momentum zone)")
                    st.caption("🔴 Above" if price > ma50_val else "🟢 Below" + " its 50-day moving average")

                # ── Price Range ───────────────────────────────────
                st.divider()
                st.markdown("### 📦 Price Range Analysis")
                st.caption(f"Last **{range_days} trading days** — adjust via sidebar")
                if not hist_1y.empty and len(hist_1y) >= range_days:
                    win  = hist_1y["Close"].iloc[-range_days:]
                    rh   = round(win.max(), 2); rl = round(win.min(), 2)
                    mid  = (rh + rl) / 2
                    rp   = round((rh - rl) / mid * 100, 2) if mid > 0 else None
                    rpos = round((win.iloc[-1] - rl) / (rh - rl), 4) if (rh - rl) > 0 else 0.5
                    rc1, rc2, rc3, rc4 = st.columns(4)
                    rc1.metric("Range High", f"${rh:,.2f}",
                               help=f"The highest closing price over the last {range_days} trading days. "
                                    "This acts as the top of the price range — a resistance level. "
                                    "Price approaching this level may face selling pressure.")
                    rc2.metric("Range Low",  f"${rl:,.2f}",
                               help=f"The lowest closing price over the last {range_days} trading days. "
                                    "This acts as the bottom of the price range — a support level. "
                                    "Price near this level may attract buyers.")
                    rc3.metric("Range Width", f"{rp:.1f}%" if rp else "N/A",
                               help="Range Width = (High − Low) ÷ Midpoint × 100. "
                                    "Measures how wide the trading channel is as a % of price. "
                                    "Under 5% = very tight consolidation (coiling). "
                                    "5–10% = normal consolidation. "
                                    "Above 20% = high volatility / wide swings.")
                    rc4.metric("Position",    f"{rpos:.0%}",
                               help="Where the current price sits within the range. "
                                    "0% = at the range low (at support — potential buy zone). "
                                    "50% = exactly at the midpoint. "
                                    "100% = at the range high (at resistance — potential sell zone). "
                                    "Stocks near the low end of a tight range can signal accumulation before a breakout.")
                    bar = "█" * int(rpos*20) + "░" * (20 - int(rpos*20))
                    pos_lbl = "Near Support 🟢" if rpos < 0.25 else ("Near Resistance 🔴" if rpos > 0.75 else "Mid-Range 🟡")
                    st.markdown(
                        f"<div style='font-family:monospace;font-size:1.1em;margin:10px 0;'>"
                        f"${rl:,.2f} |{bar}| ${rh:,.2f}</div>"
                        f"<div style='color:#aaa;font-size:0.9em;'>Current ${price:,.2f} — {pos_lbl}</div>",
                        unsafe_allow_html=True)
                    if rp is not None:
                        if rp <= 5:    st.success(f"🔒 Very Tight Range ({rp:.1f}%) — consolidation/breakout setup")
                        elif rp <= 10: st.info(f"📦 Tight Range ({rp:.1f}%) — consolidating")
                        elif rp <= 20: st.warning(f"📊 Moderate Range ({rp:.1f}%)")
                        else:          st.error(f"📉 Wide Range ({rp:.1f}%) — volatile")
                    cdf = hist_1y["Close"].iloc[-range_days:].to_frame()
                    cdf["High"] = rh; cdf["Low"] = rl
                    st.line_chart(cdf, use_container_width=True)
                else:
                    st.warning("Not enough price history to calculate range.")

                # ── Weighted score ────────────────────────────────
                st.divider()
                ts = sum(
                    float(raw.get(k) or 0) * metric_weight.get(k, METRICS[k]["weight"])
                    for k in selected
                    if raw.get(k) is not None and not (isinstance(raw.get(k), float) and np.isnan(raw.get(k)))
                )
                st.metric("Weighted Score", f"{ts:.2f}")

                # ── Financial tabs ────────────────────────────────
                st.divider()
                st.markdown("## 📋 Full Financial Breakdown")
                ft = st.tabs(["🏢 Overview","💰 Valuation","📈 Income","🏦 Balance Sheet","💵 Cash Flow","📊 Price History"])

                with ft[0]:
                    st.markdown("### 🏢 Company Overview")
                    c1, c2 = st.columns(2)
                    with c1:
                        irow("Full Name",    info.get("longName","N/A"))
                        irow("Exchange",     info.get("exchange","N/A"),   "The stock exchange where shares are listed and traded")
                        irow("Sector",       info.get("sector","N/A"),     "GICS sector classification — broad industry group the company belongs to")
                        irow("Industry",     info.get("industry","N/A"),   "Specific industry within the sector")
                        irow("Country",      info.get("country","N/A"),    "Country where the company is headquartered")
                        irow("Employees",    f"{info.get('fullTimeEmployees'):,}" if info.get("fullTimeEmployees") else "N/A",
                             "Total number of full-time employees")
                        irow("Website",      info.get("website","N/A"))
                    with c2:
                        officers = info.get("companyOfficers",[])
                        irow("CEO",          officers[0].get("name","N/A") if officers else "N/A",
                             "Chief Executive Officer — the top executive responsible for running the company")
                        irow("Fiscal YE",    str(info.get("fiscalYearEnd","N/A")),
                             "Fiscal Year End — the month the company closes its annual accounting period")
                        irow("Audit Risk",   str(info.get("auditRisk","N/A")),
                             "Score 1–10 rating audit-related governance risk. Lower = less risk")
                        irow("Board Risk",   str(info.get("boardRisk","N/A")),
                             "Score 1–10 rating board structure and independence risk. Lower = less risk")
                        irow("Comp Risk",    str(info.get("compensationRisk","N/A")),
                             "Compensation Risk — score 1–10 rating executive pay structure risk. Lower = less risk")
                        irow("SH Rights",    str(info.get("shareHolderRightsRisk","N/A")),
                             "Shareholder Rights Risk — score 1–10. High = management has too much power vs shareholders")
                        irow("Overall Risk", str(info.get("overallRisk","N/A")),
                             "Overall governance risk score 1–10. Combines audit, board, compensation and shareholder rights scores")
                    st.markdown("#### Business Summary")
                    st.markdown(f"<div style='color:#ccc;line-height:1.6'>{info.get('longBusinessSummary','N/A')}</div>", unsafe_allow_html=True)

                with ft[1]:
                    st.markdown("### 💰 Valuation")
                    v1, v2 = st.columns(2)
                    with v1:
                        irow("Market Cap",       _fb(info.get("marketCap")),
                             "Market Capitalization — total market value of all outstanding shares (Share Price × Shares Outstanding)")
                        irow("Enterprise Value", _fb(info.get("enterpriseValue")),
                             "Enterprise Value (EV) — Market Cap + Total Debt − Cash. Represents the true takeover cost of a company")
                        irow("Trailing P/E",     _fn(info.get("trailingPE")),
                             "Trailing Price-to-Earnings — share price divided by actual earnings per share over the last 12 months. Lower = cheaper relative to earnings")
                        irow("Forward P/E",      _fn(info.get("forwardPE")),
                             "Forward Price-to-Earnings — share price divided by analyst-estimated future earnings. Reflects growth expectations")
                        irow("PEG Ratio",        _fn(info.get("pegRatio")),
                             "Price/Earnings-to-Growth — P/E divided by earnings growth rate. Below 1.0 may indicate undervaluation relative to growth")
                        irow("Price/Sales",      _fn(info.get("priceToSalesTrailing12Months")),
                             "Price-to-Sales (P/S) — Market Cap divided by annual revenue. Useful for unprofitable companies. Lower = cheaper")
                        irow("Price/Book",       _fn(info.get("priceToBook")),
                             "Price-to-Book (P/B) — share price divided by book value per share (assets minus liabilities). Below 1.0 = trading below asset value")
                        irow("EV/Revenue",       _fn(info.get("enterpriseToRevenue")),
                             "Enterprise Value divided by annual revenue. Similar to P/S but accounts for debt. Lower = cheaper")
                        irow("EV/EBITDA",        _fn(info.get("enterpriseToEbitda")),
                             "Enterprise Value divided by EBITDA. Popular acquisition valuation multiple. Below 10 is generally considered reasonable")
                    with v2:
                        irow("Trailing EPS",     _fn(info.get("trailingEps")),
                             "Trailing Earnings Per Share — actual net income per share over the last 12 months")
                        irow("Forward EPS",      _fn(info.get("forwardEps")),
                             "Forward Earnings Per Share — analyst-estimated earnings per share for the next 12 months")
                        irow("Book Value/Share", _fn(info.get("bookValue")),
                             "Net assets per share — total assets minus total liabilities, divided by shares outstanding")
                        irow("52-Wk High",       _fn(info.get("fiftyTwoWeekHigh")),
                             "Highest closing price over the last 52 weeks (one year)")
                        irow("52-Wk Low",        _fn(info.get("fiftyTwoWeekLow")),
                             "Lowest closing price over the last 52 weeks (one year)")
                        irow("50-Day MA",        _fn(info.get("fiftyDayAverage")),
                             "50-Day Moving Average — average closing price over the last 50 trading days. Used as a short-term trend indicator")
                        irow("200-Day MA",       _fn(info.get("twoHundredDayAverage")),
                             "200-Day Moving Average — average closing price over the last 200 trading days. Used as a long-term trend indicator")
                        irow("Beta",             _fn(info.get("beta")),
                             "Beta — measures stock volatility vs the S&P 500. Beta > 1 = more volatile than market; Beta < 1 = less volatile")
                        irow("Short % Float",    _fp(info.get("shortPercentOfFloat")),
                             "Short Interest as % of Float — percentage of tradeable shares currently sold short. High % can signal bearish sentiment or potential short squeeze")
                    d1,d2,d3 = st.columns(3)
                    d1.metric("Div Rate",  _fn(info.get("dividendRate")) if info.get("dividendRate") else "None",
                              help="Dividend Rate — annual cash dividend paid per share")
                    d2.metric("Div Yield", _fp(info.get("dividendYield")) if info.get("dividendYield") else "None",
                              help="Dividend Yield — annual dividend as a % of share price. Higher = more income per dollar invested")
                    d3.metric("Payout",    _fp(info.get("payoutRatio")) if info.get("payoutRatio") else "N/A",
                              help="Payout Ratio — percentage of net income paid out as dividends. Above 100% means paying more than it earns")

                with ft[2]:
                    st.markdown("### 📈 Income Statement (Annual)")
                    if fin_stmt is not None and not fin_stmt.empty:
                        fd = fin_stmt.T.copy()
                        fd.index = [str(i)[:10] for i in fd.index]
                        for c in fd.columns: fd[c] = fd[c].apply(lambda x: _fb(x) if pd.notnull(x) else "N/A")
                        st.dataframe(fd, use_container_width=True)
                    else:
                        st.info("Income statement not available.")
                    i1,i2,i3,i4 = st.columns(4)
                    i1.metric("Revenue",    _fb(info.get("totalRevenue")),
                              help="Total Revenue — all money earned from selling products/services before any expenses are deducted")
                    i2.metric("Gross",      _fb(info.get("grossProfits")),
                              help="Gross Profit — Revenue minus Cost of Goods Sold (COGS). Shows profit before operating expenses")
                    i3.metric("EBITDA",     _fb(info.get("ebitda")),
                              help="Earnings Before Interest, Taxes, Depreciation & Amortization — a proxy for operating cash flow and profitability")
                    i4.metric("Net Inc",    _fb(info.get("netIncomeToCommon")),
                              help="Net Income — the bottom line profit after all expenses, interest, and taxes have been deducted")
                    i5,i6,i7,i8 = st.columns(4)
                    i5.metric("Gross Mgn",  _fp(info.get("grossMargins")),
                              help="Gross Margin — Gross Profit ÷ Revenue. Higher % = more money left after production costs")
                    i6.metric("Op Mgn",     _fp(info.get("operatingMargins")),
                              help="Operating Margin — Operating Income ÷ Revenue. Shows profit from core business operations")
                    i7.metric("Net Mgn",    _fp(info.get("profitMargins")),
                              help="Net Profit Margin — Net Income ÷ Revenue. The % of every dollar of revenue that becomes profit")
                    i8.metric("Rev Growth", _fp(info.get("revenueGrowth")),
                              help="Revenue Growth — year-over-year percentage increase in total revenue")

                with ft[3]:
                    st.markdown("### 🏦 Balance Sheet (Annual)")
                    if bal_stmt is not None and not bal_stmt.empty:
                        bd = bal_stmt.T.copy()
                        bd.index = [str(i)[:10] for i in bd.index]
                        for c in bd.columns: bd[c] = bd[c].apply(lambda x: _fb(x) if pd.notnull(x) else "N/A")
                        st.dataframe(bd, use_container_width=True)
                    else:
                        st.info("Balance sheet not available.")
                    b1,b2,b3,b4 = st.columns(4)
                    b1.metric("Cash",      _fb(info.get("totalCash")),
                              help="Total Cash & Short-Term Investments — liquid assets the company can access immediately")
                    b2.metric("Debt",      _fb(info.get("totalDebt")),
                              help="Total Debt — all short-term and long-term borrowings combined")
                    b3.metric("Net Cash",  _fb((info.get("totalCash") or 0)-(info.get("totalDebt") or 0)),
                              help="Net Cash Position — Total Cash minus Total Debt. Positive = more cash than debt (strong balance sheet)")
                    b4.metric("Assets",    _fb(info.get("totalAssets")),
                              help="Total Assets — everything the company owns: cash, property, equipment, intangibles, etc.")
                    b5,b6,b7,b8 = st.columns(4)
                    b5.metric("Cash/Shr",  _fn(info.get("totalCashPerShare")),
                              help="Cash Per Share — total cash divided by shares outstanding. Higher = more cash backing each share")
                    b6.metric("D/E",       _fn(info.get("debtToEquity")),
                              help="Debt-to-Equity Ratio — total debt divided by shareholders equity. Lower = less financial leverage/risk. Above 2.0 can be concerning")
                    b7.metric("Cur Ratio", _fn(info.get("currentRatio")),
                              help="Current Ratio — Current Assets ÷ Current Liabilities. Above 1.0 means the company can cover its short-term debts. Above 2.0 is considered healthy")
                    b8.metric("Qck Ratio", _fn(info.get("quickRatio")),
                              help="Quick Ratio (Acid Test) — like Current Ratio but excludes inventory. Above 1.0 means the company can pay short-term debts without selling inventory")

                with ft[4]:
                    st.markdown("### 💵 Cash Flow (Annual)")
                    if cf_stmt is not None and not cf_stmt.empty:
                        cd = cf_stmt.T.copy()
                        cd.index = [str(i)[:10] for i in cd.index]
                        for c in cd.columns: cd[c] = cd[c].apply(lambda x: _fb(x) if pd.notnull(x) else "N/A")
                        st.dataframe(cd, use_container_width=True)
                    else:
                        st.info("Cash flow not available.")
                    cf1,cf2,cf3,cf4 = st.columns(4)
                    cf1.metric("Op CF",   _fb(info.get("operatingCashflow")),
                               help="Operating Cash Flow — actual cash generated from core business operations. More reliable than net income as a profitability measure")
                    cf2.metric("FCF",     _fb(info.get("freeCashflow")),
                               help="Free Cash Flow — Operating Cash Flow minus Capital Expenditures. Cash left over that can be used for dividends, buybacks, or debt repayment")
                    cf3.metric("CapEx",   _fb(info.get("capitalExpenditures")),
                               help="Capital Expenditures — money spent on buying, maintaining or upgrading physical assets like buildings and equipment")
                    sh = info.get("sharesOutstanding"); fcf = info.get("freeCashflow")
                    cf4.metric("FCF/Shr", _fn(fcf/sh if fcf and sh else None),
                               help="Free Cash Flow Per Share — FCF divided by shares outstanding. How much free cash is generated per share owned")
                    cf5,cf6 = st.columns(2)
                    cf5.metric("ROA", _fp(info.get("returnOnAssets")),
                               help="Return on Assets — Net Income ÷ Total Assets. Measures how efficiently a company uses its assets to generate profit. Above 5% is generally good")
                    cf6.metric("ROE", _fp(info.get("returnOnEquity")),
                               help="Return on Equity — Net Income ÷ Shareholders Equity. Measures how much profit is generated per dollar of shareholder investment. Above 15% is considered strong")

                with ft[5]:
                    st.markdown("### 📊 Interactive Chart")

                    # ── Chart controls row 1 ──────────────────────
                    cc1, cc2, cc3, cc4 = st.columns(4)
                    with cc1:
                        chart_type = st.selectbox(
                            "Chart Type",
                            ["Candlestick", "Heikin Ashi", "Line"],
                            index=0, key="chart_type",
                            help="Heikin Ashi smooths noise by averaging price data — easier to spot trends"
                        )
                    with cc2:
                        interval = st.selectbox(
                            "Candle Interval",
                            ["1m","5m","15m","30m","1h","4h","1d","1wk","1mo"],
                            index=6, key="chart_interval",
                            help=(
                                "1m/5m: last 7 days max  |  "
                                "15m/30m/1h: last 60 days max  |  "
                                "4h: last 60 days max (resampled)  |  "
                                "1d/1wk/1mo: years of data available"
                            )
                        )
                    with cc3:
                        # Restrict time period options based on interval
                        intraday_short = interval in ("1m", "5m")
                        intraday_mid   = interval in ("15m", "30m", "1h", "4h")

                        if intraday_short:
                            period_opts = ["1d", "5d", "7d"]
                            period_def  = 1
                        elif intraday_mid:
                            period_opts = ["1d", "5d", "1mo", "2mo"]
                            period_def  = 2
                        else:
                            period_opts = ["5d","1mo","3mo","6mo","1y","2y","5y","10y","Custom"]
                            period_def  = 4

                        pc = st.selectbox(
                            "Time Period", period_opts,
                            index=period_def, key="price_history_period"
                        )
                    with cc4:
                        chart_theme = st.selectbox(
                            "Theme", ["Dark","Light"], index=0, key="chart_theme"
                        )

                    # ── Custom date range (daily+ only) ───────────
                    if pc == "Custom":
                        dr1, dr2 = st.columns(2)
                        with dr1:
                            custom_start = st.date_input(
                                "Start Date",
                                value=pd.Timestamp.today() - pd.Timedelta(days=365),
                                max_value=pd.Timestamp.today(),
                                key="chart_custom_start",
                            )
                        with dr2:
                            custom_end = st.date_input(
                                "End Date",
                                value=pd.Timestamp.today(),
                                max_value=pd.Timestamp.today(),
                                key="chart_custom_end",
                            )

                    st.markdown("**Overlays**")
                    ov1,ov2,ov3,ov4 = st.columns(4)
                    show_ema20  = ov1.checkbox("EMA 20",  value=True,  key="show_ema20")
                    show_ema50  = ov2.checkbox("EMA 50",  value=True,  key="show_ema50")
                    show_ema200 = ov3.checkbox("EMA 200", value=False, key="show_ema200")
                    show_bb     = ov4.checkbox("Bollinger Bands", value=False, key="show_bb")

                    st.markdown("**Sub-Charts**")
                    sc1,sc2,sc3 = st.columns(3)
                    show_vol  = sc1.checkbox("Volume",  value=True,  key="show_vol")
                    show_rsi  = sc2.checkbox("RSI",     value=True,  key="show_rsi")
                    show_macd = sc3.checkbox("MACD",    value=False, key="show_macd")

                    # ── Fetch history with correct interval ───────
                    # yfinance interval/period compatibility:
                    #   1m        → max period "7d"
                    #   5m/15m/30m/1h → max period "60d"
                    #   4h        → not native; fetch 1h and resample
                    #   1d/1wk/1mo → any period
                    fetch_interval = "1h" if interval == "4h" else interval

                    # Map display period → yfinance period string for fetch
                    period_fetch_map = {
                        "1d":"1d","5d":"5d","7d":"7d","1mo":"1mo","2mo":"2mo",
                        "3mo":"3mo","6mo":"6mo","1y":"2y","2y":"5y",
                        "5y":"10y","10y":"max","Custom":"max",
                    }
                    fetch_period = period_fetch_map.get(pc, "2y")

                    try:
                        _s = yf.Ticker(sticker)
                        if pc == "Custom":
                            hist_full = _s.history(
                                start=str(custom_start),
                                end=str(custom_end),
                                interval=fetch_interval
                            )
                        else:
                            hist_full = _s.history(period=fetch_period, interval=fetch_interval)
                    except Exception as _fe:
                        st.warning(f"Could not fetch {interval} data: {_fe}. Falling back to daily.")
                        hist_full = hist_1y

                    # Resample 1h → 4h if needed
                    if interval == "4h" and not hist_full.empty:
                        hist_full = hist_full.resample("4h").agg({
                            "Open":  "first",
                            "High":  "max",
                            "Low":   "min",
                            "Close": "last",
                            "Volume":"sum",
                        }).dropna()

                    # Slice to display window for daily+ periods
                    period_days_map = {
                        "5d":5,"1mo":21,"3mo":63,"6mo":126,
                        "1y":252,"2y":504,"5y":1260,"10y":2520,
                    }
                    if pc not in ("Custom","1d","5d","7d","1mo","2mo"):
                        dn = period_days_map.get(pc, 252)
                        hd = hist_full.iloc[-dn:].copy() if len(hist_full) >= dn else hist_full.copy()
                    else:
                        hd = hist_full.copy()

                    if not hd.empty:
                        import plotly.graph_objects as go
                        from plotly.subplots import make_subplots

                        bg   = "#0e1117" if chart_theme == "Dark" else "#ffffff"
                        fg   = "#ffffff" if chart_theme == "Dark" else "#000000"
                        grid = "#1f2937" if chart_theme == "Dark" else "#e5e7eb"

                        # ── Compute indicators ────────────────────
                        hd["EMA20"]  = hd["Close"].ewm(span=20,  adjust=False).mean()
                        hd["EMA50"]  = hd["Close"].ewm(span=50,  adjust=False).mean()
                        hd["EMA200"] = hd["Close"].ewm(span=200, adjust=False).mean()

                        # Bollinger Bands (20-day, 2σ)
                        bb_mid        = hd["Close"].rolling(20).mean()
                        bb_std        = hd["Close"].rolling(20).std()
                        hd["BB_mid"]  = bb_mid
                        hd["BB_up"]   = bb_mid + 2 * bb_std
                        hd["BB_low"]  = bb_mid - 2 * bb_std

                        # RSI (14-period)
                        delta  = hd["Close"].diff()
                        gain   = delta.clip(lower=0).rolling(14).mean()
                        loss   = (-delta.clip(upper=0)).rolling(14).mean()
                        rs     = gain / loss.replace(0, np.nan)
                        hd["RSI"] = 100 - (100 / (1 + rs))

                        # MACD (12/26/9)
                        ema12       = hd["Close"].ewm(span=12, adjust=False).mean()
                        ema26       = hd["Close"].ewm(span=26, adjust=False).mean()
                        hd["MACD"]  = ema12 - ema26
                        hd["Signal"]= hd["MACD"].ewm(span=9, adjust=False).mean()
                        hd["Hist"]  = hd["MACD"] - hd["Signal"]

                        # Heikin Ashi OHLC
                        ha = hd.copy()
                        ha["HA_Close"] = (hd["Open"] + hd["High"] + hd["Low"] + hd["Close"]) / 4
                        ha["HA_Open"]  = ((hd["Open"] + hd["Close"]) / 2).shift(1)
                        ha["HA_Open"].iloc[0] = (hd["Open"].iloc[0] + hd["Close"].iloc[0]) / 2
                        ha["HA_High"]  = pd.concat([hd["High"], ha["HA_Open"], ha["HA_Close"]], axis=1).max(axis=1)
                        ha["HA_Low"]   = pd.concat([hd["Low"],  ha["HA_Open"], ha["HA_Close"]], axis=1).min(axis=1)

                        # ── Build subplot layout ──────────────────
                        sub_charts = [s for s, show in [
                            ("Volume", show_vol), ("RSI", show_rsi), ("MACD", show_macd)
                        ] if show]

                        n_rows   = 1 + len(sub_charts)
                        row_h    = [0.55] + [0.45 / max(len(sub_charts), 1)] * len(sub_charts) if sub_charts else [1.0]
                        sub_specs= [[{"secondary_y": False}]] * n_rows

                        fig = make_subplots(
                            rows=n_rows, cols=1,
                            shared_xaxes=True,
                            row_heights=row_h,
                            vertical_spacing=0.03,
                            specs=sub_specs,
                        )

                        # ── Main price chart ──────────────────────
                        if chart_type == "Candlestick":
                            fig.add_trace(go.Candlestick(
                                x=hd.index, open=hd["Open"], high=hd["High"],
                                low=hd["Low"],  close=hd["Close"],
                                name="Price",
                                increasing_line_color="#26a69a",
                                decreasing_line_color="#ef5350",
                                increasing_fillcolor="#26a69a",
                                decreasing_fillcolor="#ef5350",
                            ), row=1, col=1)

                        elif chart_type == "Heikin Ashi":
                            fig.add_trace(go.Candlestick(
                                x=ha.index, open=ha["HA_Open"], high=ha["HA_High"],
                                low=ha["HA_Low"],  close=ha["HA_Close"],
                                name="Heikin Ashi",
                                increasing_line_color="#26a69a",
                                decreasing_line_color="#ef5350",
                                increasing_fillcolor="#26a69a",
                                decreasing_fillcolor="#ef5350",
                            ), row=1, col=1)

                        else:  # Line
                            fig.add_trace(go.Scatter(
                                x=hd.index, y=hd["Close"],
                                name="Close", line=dict(color="#2196f3", width=1.5)
                            ), row=1, col=1)

                        # ── Overlays ──────────────────────────────
                        if show_ema20:
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["EMA20"],
                                name="EMA 20", line=dict(color="#f59e0b", width=1),
                                opacity=0.85), row=1, col=1)
                        if show_ema50:
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["EMA50"],
                                name="EMA 50", line=dict(color="#a78bfa", width=1),
                                opacity=0.85), row=1, col=1)
                        if show_ema200:
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["EMA200"],
                                name="EMA 200", line=dict(color="#f87171", width=1),
                                opacity=0.85), row=1, col=1)
                        if show_bb:
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["BB_up"],
                                name="BB Upper", line=dict(color="#94a3b8", width=1, dash="dot"),
                                opacity=0.6), row=1, col=1)
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["BB_low"],
                                name="BB Lower", line=dict(color="#94a3b8", width=1, dash="dot"),
                                fill="tonexty", fillcolor="rgba(148,163,184,0.08)",
                                opacity=0.6), row=1, col=1)
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["BB_mid"],
                                name="BB Mid", line=dict(color="#94a3b8", width=1, dash="dash"),
                                opacity=0.4), row=1, col=1)

                        # ── Sub-charts ────────────────────────────
                        sub_row = 2
                        if show_vol:
                            colors = ["#26a69a" if hd["Close"].iloc[i] >= hd["Open"].iloc[i]
                                      else "#ef5350" for i in range(len(hd))]
                            fig.add_trace(go.Bar(
                                x=hd.index, y=hd["Volume"],
                                name="Volume", marker_color=colors, opacity=0.7,
                            ), row=sub_row, col=1)
                            fig.update_yaxes(title_text="Volume", row=sub_row, col=1,
                                             title_font=dict(size=10), tickfont=dict(size=9))
                            sub_row += 1

                        if show_rsi:
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["RSI"],
                                name="RSI", line=dict(color="#60a5fa", width=1.5)),
                                row=sub_row, col=1)
                            fig.add_hline(y=70, line_dash="dot", line_color="#ef5350",
                                          line_width=1, row=sub_row, col=1)
                            fig.add_hline(y=30, line_dash="dot", line_color="#26a69a",
                                          line_width=1, row=sub_row, col=1)
                            fig.update_yaxes(title_text="RSI", range=[0,100],
                                             row=sub_row, col=1,
                                             title_font=dict(size=10), tickfont=dict(size=9))
                            sub_row += 1

                        if show_macd:
                            hist_colors = ["#26a69a" if v >= 0 else "#ef5350"
                                           for v in hd["Hist"].fillna(0)]
                            fig.add_trace(go.Bar(x=hd.index, y=hd["Hist"],
                                name="MACD Hist", marker_color=hist_colors, opacity=0.6),
                                row=sub_row, col=1)
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["MACD"],
                                name="MACD", line=dict(color="#60a5fa", width=1.2)),
                                row=sub_row, col=1)
                            fig.add_trace(go.Scatter(x=hd.index, y=hd["Signal"],
                                name="Signal", line=dict(color="#f59e0b", width=1.2)),
                                row=sub_row, col=1)
                            fig.update_yaxes(title_text="MACD", row=sub_row, col=1,
                                             title_font=dict(size=10), tickfont=dict(size=9))

                        # ── Layout ────────────────────────────────
                        fig.update_layout(
                            height=600 + 120 * len(sub_charts),
                            paper_bgcolor=bg, plot_bgcolor=bg,
                            font=dict(color=fg, size=11),
                            legend=dict(
                                orientation="h", yanchor="bottom", y=1.01,
                                xanchor="left", x=0,
                                bgcolor="rgba(0,0,0,0)", font=dict(size=10)
                            ),
                            margin=dict(l=10, r=10, t=30, b=10),
                            xaxis_rangeslider_visible=False,
                            hovermode="x unified",
                        )
                        fig.update_xaxes(
                            gridcolor=grid, showgrid=True,
                            zeroline=False, showspikes=True,
                            spikecolor="#666", spikethickness=1,
                        )
                        fig.update_yaxes(
                            gridcolor=grid, showgrid=True,
                            zeroline=False,
                        )
                        fig.update_yaxes(title_text="Price ($)", row=1, col=1,
                                         title_font=dict(size=10), tickfont=dict(size=9))

                        st.plotly_chart(fig, use_container_width=True)

                        # ── Period stats ──────────────────────────
                        s1, s2, s3, s4 = st.columns(4)
                        pr = (hd["Close"].iloc[-1] - hd["Close"].iloc[0]) / hd["Close"].iloc[0]
                        s1.metric("Period Return", _fp(pr),     delta=f"{pr:+.2%}")
                        s2.metric("Period High",   f"${hd['High'].max():,.2f}")
                        s3.metric("Period Low",    f"${hd['Low'].min():,.2f}")
                        s4.metric("Avg Volume",    _fb(hd["Volume"].mean()).replace("$",""))
                    else:
                        st.info("No price history available.")

            except Exception as e:
                st.error(f"Error rendering analysis: {e}")
