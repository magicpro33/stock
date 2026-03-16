# -----------------------------------
# REQUIREMENTS
# pip install streamlit yfinance pandas numpy openpyxl tqdm requests
#
# RUN:
#   streamlit run app.py
# -----------------------------------

import io
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
# SIDEBAR — FILTERS  (Screener tab only)
# ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Screener Filters")

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
        min_value=10, max_value=1000, value=250, step=10,
        help="Exclude stocks above this price."
    )

    min_score = st.slider(
        "Min Hybrid Score",
        min_value=0.0, max_value=20.0, value=2.0, step=0.5,
        help="Only show stocks with a score above this threshold."
    )

    top_n = st.slider(
        "Max Results",
        min_value=5, max_value=100, value=30, step=5,
        help="Maximum number of stocks to display."
    )

    use_ma50_filter = st.toggle(
        "Only stocks BELOW 50-day MA",
        value=True,
        help="When on, only shows stocks in a pullback (price < MA50)."
    )

    st.divider()
    st.header("📦 Price Range Filter")
    st.caption("Find stocks locked in a tight trading range — coiling before a breakout.")

    use_range_filter = st.toggle(
        "Enable Price Range Filter",
        value=False,
        help="When on, only shows stocks trading within a tight range over the selected period."
    )

    range_days = st.slider(
        "Range Lookback (days)",
        min_value=5, max_value=180, value=30, step=5,
        help="Number of trading days to measure the high/low range over."
    )

    range_max_pct = st.slider(
        "Max Range Width (%)",
        min_value=1.0, max_value=30.0, value=10.0, step=0.5,
        help="Maximum allowed spread between high and low as % of price. "
             "Lower = tighter range. 5–10% finds stocks in consolidation.",
        disabled=not use_range_filter,
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
            metric_enabled[key] = st.toggle(cfg["label"], value=True, key=f"tog_{key}")
        with col_b:
            metric_weight[key] = st.slider(
                f"Weight", min_value=0.0, max_value=5.0,
                value=float(cfg["weight"]), step=0.5,
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
            metric_enabled[key] = st.toggle(cfg["label"], value=True, key=f"tog_{key}")
        with col_b:
            metric_weight[key] = st.slider(
                f"Weight", min_value=0.0, max_value=5.0,
                value=float(cfg["weight"]), step=0.5,
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
        min_value=7, max_value=30, value=14, step=1,
        help="Money Flow Index lookback window."
    )

    st.divider()
    run_btn = st.button("🚀 Run Screener", use_container_width=True, type="primary")

# ───────────────────────────────────────────────────────────────
# SIGNAL FUNCTIONS  (identical logic to stock_screener_2026.py)
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
    # Validate at least one metric is on
    active_metrics = [k for k, v in metric_enabled.items() if v]
    if not active_metrics:
        st.error("Please enable at least one metric before running.")
        st.stop()

    exchange_key  = EXCHANGES[exchange]
    sector_label  = sector if sector != "All Sectors" else "All Sectors"

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

    if exchange == "S&P 500":
        est_time = "~5 min"
    elif exchange == "NYSE":
        est_time = "~10 min"
    else:
        est_time = "~10 min"

    st.info(
        f"Scanning **{len(tickers)}** tickers on **{exchange}** · "
        f"Sector: **{sector_label}** · ETFs/funds excluded · Est. time: {est_time}"
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
    df["EarningsYield"] = df["P/E"].apply(lambda x: round(1/x, 2) if pd.notnull(x) and x > 0 else 0)
    df["ROIC"]          = df["ROIC"].round(2)
    df["ROIC_Trend"]    = df["ROIC_Trend"].round(2)
    df["OBV"]           = df["OBV"].round(4)
    df["MFI"]           = df["MFI"].round(4)
    df["PCV"]           = df["PCV"].round(4)
    df["MA50"]          = pd.to_numeric(df["MA50"], errors="coerce").round(2)
    df["RangeHigh"]     = pd.to_numeric(df["RangeHigh"], errors="coerce").round(2)
    df["RangeLow"]      = pd.to_numeric(df["RangeLow"],  errors="coerce").round(2)
    df["RangePct"]      = pd.to_numeric(df["RangePct"],  errors="coerce").round(2)
    df["RangePos"]      = pd.to_numeric(df["RangePos"],  errors="coerce").round(4)
    df["Rank_EY"]       = df["EarningsYield"].rank(ascending=False, method="min")
    df["Rank_ROIC"]     = df["ROIC"].fillna(0).rank(ascending=False, method="min")
    df["MagicFormula"]  = df["Rank_EY"] + df["Rank_ROIC"]

    # Dynamic score — use sidebar weight sliders, skip disabled metrics
    score = pd.Series(0.0, index=df.index)
    for key in METRICS:
        if metric_enabled.get(key, False):
            w = metric_weight.get(key, METRICS[key]["weight"])
            score += df[key].fillna(0) * w
    df["Score"] = score.round(2)

    # Apply filters
    under_price = df["Price"].isna() | (df["Price"] <= max_price)
    above_score = df["Score"] >= min_score
    if use_ma50_filter:
        below_ma50 = df["Price"].isna() | df["MA50"].isna() | (df["Price"] <= df["MA50"])
    else:
        below_ma50 = pd.Series(True, index=df.index)

    # Price range filter — keep only stocks whose range width is within max_range_pct
    if use_range_filter:
        in_range = df["RangePct"].isna() | (df["RangePct"] <= range_max_pct)
    else:
        in_range = pd.Series(True, index=df.index)

    screened = (
        df[under_price & below_ma50 & above_score & in_range]
        .sort_values("Score", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    # ── Active metrics summary banner ────────────────────────────
    active_labels = [METRICS[k]["label"] for k in active_metrics]
    st.success(f"**Active metrics:** {' · '.join(active_labels)}")

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
        for col in display.columns:
            if col not in ["Ticker", "Sector", "MarketCap", "OwnerEarnings", "RevenueGrowth", "EarningsGrowth"]:
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

    # ── Ticker input + metric checkboxes ────────────────────────
    a_col1, a_col2 = st.columns([1, 2])

    with a_col1:
        ticker_input = st.text_input(
            "Stock Ticker",
            placeholder="e.g. AAPL, MSFT, TSLA",
            help="Enter the ticker symbol exactly as it appears on the exchange."
        ).strip().upper()

        st.markdown("**Select Metrics to Analyze**")
        analyze_metrics = {}
        for key, cfg in METRICS.items():
            analyze_metrics[key] = st.checkbox(
                cfg["label"],
                value=True,
                key=f"analyze_{key}",
                help=cfg["desc"],
            )

        analyze_btn = st.button("🔬 Analyze", type="primary", use_container_width=True)

    with a_col2:
        if not analyze_btn:
            st.markdown("""
            **How to use:**
            1. Type a ticker symbol on the left (e.g. `NVDA`)
            2. Check the metrics you want to see
            3. Click **Analyze**

            Results show the raw value for each metric alongside a
            color-coded signal: 🟢 Good · 🟡 Neutral · 🔴 Weak

            You can analyze **any publicly traded stock** — not just S&P 500.
            """)

        elif not ticker_input:
            st.warning("Please enter a ticker symbol.")

        else:
            with st.spinner(f"Fetching data for **{ticker_input}**..."):
                try:
                    stock = yf.Ticker(ticker_input)

                    # ── Fetch ALL data in one pass to avoid rate limits ──
                    # Each yfinance property is fetched once and stored locally.
                    # All subsequent rendering uses these cached variables only.
                    info       = stock.info or {}
                    if not info or len(info) < 5:
                        st.error(f"Could not find data for **{ticker_input}**. Check the ticker symbol and try again.")
                        st.stop()

                    # Fetch price history once — long enough for all uses
                    # (MA50 needs 50 days, range needs range_days, price history up to 5y)
                    try:
                        hist_1y = stock.history(period="1y")
                    except:
                        hist_1y = pd.DataFrame()

                    # Fetch financial statements once each
                    try:
                        fin_stmt  = stock.financials
                    except:
                        fin_stmt  = None

                    try:
                        bal_stmt  = stock.balance_sheet
                    except:
                        bal_stmt  = None

                    try:
                        cf_stmt   = stock.cashflow
                    except:
                        cf_stmt   = None

                    # Small pause to avoid rate limit on slower connections
                    time.sleep(0.5)

                except Exception as e:
                    st.error(f"Failed to fetch data for **{ticker_input}**: {e}")
                    st.stop()

            # ── All rendering happens OUTSIDE the spinner, using cached data ──
            try:
                    # ── Company header ───────────────────────────
                    name     = info.get("longName") or info.get("shortName") or ticker_input
                    price    = info.get("currentPrice") or info.get("regularMarketPrice")
                    sector_v = info.get("sector", "N/A")
                    industry = info.get("industry", "N/A")
                    mktcap   = info.get("marketCap")

                    st.markdown(f"## {name} &nbsp; `{ticker_input}`")
                    h1, h2, h3, h4 = st.columns(4)
                    h1.metric("Price",    f"${price:,.2f}" if price else "N/A")
                    h2.metric("Sector",   sector_v)
                    h3.metric("Industry", industry)
                    h4.metric("Mkt Cap",  f"${mktcap/1e9:.1f}B" if mktcap else "N/A")
                    st.divider()

                    # ── Run selected metrics ─────────────────────
                    selected = [k for k, v in analyze_metrics.items() if v]
                    if not selected:
                        st.warning("Select at least one metric.")
                        st.stop()

                    # Collect raw values — reuse cached stock object (no new API calls)
                    raw = {}
                    owner_earnings, oe_yield = get_owner_earnings(stock, info)
                    vol_signals = get_volume_signals(stock, mfi_period)

                    raw["OE_Yield"]       = oe_yield
                    raw["ROIC"]           = calculate_roic(stock)
                    raw["ROIC_Trend"]     = calculate_roic_trend(stock)
                    raw["RevenueGrowth"]  = info.get("revenueGrowth")
                    raw["EarningsGrowth"] = info.get("earningsGrowth")
                    raw["Piotroski"]      = calculate_piotroski(stock)
                    raw["OBV"]            = vol_signals["OBV"]
                    raw["MFI"]            = vol_signals["MFI"]
                    raw["PCV"]            = vol_signals["PCV"]

                    # Signal thresholds: (good_threshold, neutral_threshold, higher_is_better)
                    THRESHOLDS = {
                        "OE_Yield":       (0.05,  0.02,  True),
                        "ROIC":           (0.15,  0.08,  True),
                        "ROIC_Trend":     (0.02,  0.0,   True),
                        "RevenueGrowth":  (0.10,  0.05,  True),
                        "EarningsGrowth": (0.10,  0.05,  True),
                        "Piotroski":      (7,     4,     True),
                        "OBV":            (0.8,   0.4,   True),
                        "MFI":            (0.5,   0.2,   True),
                        "PCV":            (0.5,   0.2,   True),
                    }

                    def signal_emoji(key, val):
                        if val is None or (isinstance(val, float) and np.isnan(val)):
                            return "⚪", "N/A"
                        good, neutral, higher = THRESHOLDS.get(key, (None, None, True))
                        if good is None:
                            return "⚪", str(val)
                        if higher:
                            if val >= good:      return "🟢", val
                            elif val >= neutral: return "🟡", val
                            else:                return "🔴", val
                        else:
                            if val <= good:      return "🟢", val
                            elif val <= neutral: return "🟡", val
                            else:                return "🔴", val

                    def fmt_value(key, val):
                        if val is None or (isinstance(val, float) and np.isnan(val)):
                            return "N/A"
                        if key in ("OE_Yield", "RevenueGrowth", "EarningsGrowth", "ROIC", "ROIC_Trend"):
                            return f"{val:.2%}"
                        if key in ("OBV", "MFI", "PCV"):
                            return f"{val:.4f}"
                        if key == "Piotroski":
                            return f"{int(val)} / 9"
                        return str(round(val, 4))

                    # ── Render metric cards ──────────────────────
                    st.markdown("### Metric Results")
                    cols = st.columns(3)
                    for i, key in enumerate(selected):
                        cfg         = METRICS[key]
                        val         = raw.get(key)
                        emoji, _    = signal_emoji(key, val)
                        display_val = fmt_value(key, val)
                        with cols[i % 3]:
                            st.markdown(
                                f"""<div style="border:1px solid #444; border-radius:8px;
                                               padding:14px; margin-bottom:12px;">
                                  <div style="font-size:1.1em; font-weight:600;">{emoji} {cfg['label']}</div>
                                  <div style="font-size:1.8em; font-weight:700; margin:6px 0;">{display_val}</div>
                                  <div style="font-size:0.78em; color:#aaa;">{cfg['desc'][:120]}...</div>
                                </div>""",
                                unsafe_allow_html=True,
                            )

                    # ── MA50 comparison (uses cached hist_1y) ────
                    st.divider()
                    if not hist_1y.empty and len(hist_1y) >= 50:
                        ma50_val = round(hist_1y["Close"].rolling(50).mean().iloc[-1], 2)
                        diff_pct = ((price - ma50_val) / ma50_val) * 100 if price else 0
                        ma_emoji = "🔴 Above" if price > ma50_val else "🟢 Below"
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Current Price", f"${price:,.2f}" if price else "N/A")
                        m2.metric("50-Day MA",     f"${ma50_val:,.2f}")
                        m3.metric("vs MA50",       f"{diff_pct:+.1f}%", delta_color="inverse",
                                  help="Negative = trading below MA50 (potential value zone)")
                        st.caption(f"{ma_emoji} its 50-day moving average")

                    # ── Price Range Analysis (uses cached hist_1y) ──
                    st.divider()
                    st.markdown("### 📦 Price Range Analysis")
                    st.caption(f"Showing price range over the last **{range_days} trading days** (adjust in sidebar)")

                    if not hist_1y.empty and len(hist_1y) >= range_days:
                        window = hist_1y["Close"].iloc[-range_days:]
                        rh     = round(window.max(), 2)
                        rl     = round(window.min(), 2)
                        mid    = (rh + rl) / 2
                        rp     = round((rh - rl) / mid * 100, 2) if mid > 0 else None
                        cur    = window.iloc[-1]
                        rpos   = round((cur - rl) / (rh - rl), 4) if (rh - rl) > 0 else 0.5

                        rc1, rc2, rc3, rc4 = st.columns(4)
                        rc1.metric("Range High",        f"${rh:,.2f}")
                        rc2.metric("Range Low",         f"${rl:,.2f}")
                        rc3.metric("Range Width",       f"{rp:.1f}%" if rp else "N/A",
                                   help="(High − Low) ÷ Midpoint × 100. Lower = tighter consolidation.")
                        rc4.metric("Position in Range", f"{rpos:.0%}",
                                   help="0% = at range low (support), 100% = at range high (resistance).")

                        bar_filled = int(rpos * 20)
                        bar_str    = "█" * bar_filled + "░" * (20 - bar_filled)
                        pos_label  = "Near Support 🟢" if rpos < 0.25 else ("Near Resistance 🔴" if rpos > 0.75 else "Mid-Range 🟡")
                        st.markdown(
                            f"<div style='font-family:monospace;font-size:1.2em;letter-spacing:2px;margin:10px 0;'>"
                            f"Low ${rl:,.2f} &nbsp;|{bar_str}|&nbsp; High ${rh:,.2f}</div>"
                            f"<div style='color:#aaa;font-size:0.9em;'>Current: <b>${price:,.2f}</b> — {pos_label}</div>",
                            unsafe_allow_html=True
                        )
                        if rp is not None:
                            if rp <= 5:    st.success(f"🔒 **Very Tight Range ({rp:.1f}%)** — heavily consolidated. Potential breakout setup.")
                            elif rp <= 10: st.info(f"📦 **Tight Range ({rp:.1f}%)** — consolidating within a defined range.")
                            elif rp <= 20: st.warning(f"📊 **Moderate Range ({rp:.1f}%)** — some volatility but range-bound.")
                            else:          st.error(f"📉 **Wide Range ({rp:.1f}%)** — stock has been volatile over this period.")

                        # Range chart — uses already-fetched hist_1y slice
                        chart_df = hist_1y["Close"].iloc[-range_days:].to_frame()
                        chart_df["Range High"] = rh
                        chart_df["Range Low"]  = rl
                        st.markdown("**Price vs Range Boundaries**")
                        st.line_chart(chart_df, use_container_width=True)
                    else:
                        st.warning("Not enough price history to calculate range for this ticker.")

                    # ── Overall summary score ────────────────────
                    st.divider()
                    total_score  = 0.0
                    max_possible = 0.0
                    for key in selected:
                        val = raw.get(key)
                        w   = metric_weight.get(key, METRICS[key]["weight"])
                        if val is not None and not (isinstance(val, float) and np.isnan(val)):
                            total_score  += float(val) * w
                            max_possible += w
                    st.metric("Weighted Score (selected metrics)", f"{total_score:.2f}",
                              help="Sum of (metric value × weight) for all selected metrics with valid data.")

                    # ════════════════════════════════════════════
                    # FULL FINANCIAL BREAKDOWN — all use cached statements
                    # ════════════════════════════════════════════
                    st.divider()
                    st.markdown("## 📋 Full Financial Breakdown")

                    fin_tabs = st.tabs([
                        "🏢 Company Overview",
                        "💰 Valuation",
                        "📈 Income Statement",
                        "🏦 Balance Sheet",
                        "💵 Cash Flow",
                        "📊 Price History",
                    ])

                    def fmt_big(val):
                        if val is None or (isinstance(val, float) and np.isnan(val)):
                            return "N/A"
                        try:
                            v = float(val)
                            if abs(v) >= 1e12: return f"${v/1e12:.2f}T"
                            if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
                            if abs(v) >= 1e6:  return f"${v/1e6:.2f}M"
                            if abs(v) >= 1e3:  return f"${v/1e3:.2f}K"
                            return f"${v:,.2f}"
                        except:
                            return "N/A"

                    def fmt_pct(val):
                        try:    return f"{float(val):.2%}" if val is not None else "N/A"
                        except: return "N/A"

                    def fmt_num(val, decimals=2):
                        try:    return f"{float(val):,.{decimals}f}" if val is not None else "N/A"
                        except: return "N/A"

                    def info_row(label, value):
                        st.markdown(
                            f"<div style='display:flex;justify-content:space-between;"
                            f"padding:6px 0;border-bottom:1px solid #2a2a2a;'>"
                            f"<span style='color:#aaa;'>{label}</span>"
                            f"<span style='font-weight:600;'>{value}</span></div>",
                            unsafe_allow_html=True,
                        )

                    # ── Tab 1: Company Overview ───────────────────
                    with fin_tabs[0]:
                        st.markdown("### 🏢 Company Overview")
                        ov1, ov2 = st.columns(2)
                        with ov1:
                            info_row("Full Name",         info.get("longName", "N/A"))
                            info_row("Ticker",            ticker_input)
                            info_row("Exchange",          info.get("exchange", "N/A"))
                            info_row("Sector",            info.get("sector", "N/A"))
                            info_row("Industry",          info.get("industry", "N/A"))
                            info_row("Country",           info.get("country", "N/A"))
                            info_row("Employees",         f"{info.get('fullTimeEmployees'):,}" if info.get("fullTimeEmployees") else "N/A")
                            info_row("Website",           info.get("website", "N/A"))
                        with ov2:
                            officers = info.get("companyOfficers", [])
                            ceo_name = officers[0].get("name", "N/A") if officers else "N/A"
                            info_row("CEO",               ceo_name)
                            info_row("Fiscal Year End",   str(info.get("fiscalYearEnd", "N/A")))
                            info_row("Audit Risk",        str(info.get("auditRisk", "N/A")))
                            info_row("Board Risk",        str(info.get("boardRisk", "N/A")))
                            info_row("Compensation Risk", str(info.get("compensationRisk", "N/A")))
                            info_row("Shareholder Rights",str(info.get("shareHolderRightsRisk", "N/A")))
                            info_row("Overall Risk",      str(info.get("overallRisk", "N/A")))
                        st.markdown("#### Business Summary")
                        st.markdown(
                            f"<div style='color:#ccc;line-height:1.6;'>{info.get('longBusinessSummary','No summary available.')}</div>",
                            unsafe_allow_html=True)

                    # ── Tab 2: Valuation ─────────────────────────
                    with fin_tabs[1]:
                        st.markdown("### 💰 Valuation Metrics")
                        v1, v2 = st.columns(2)
                        with v1:
                            info_row("Market Cap",        fmt_big(info.get("marketCap")))
                            info_row("Enterprise Value",  fmt_big(info.get("enterpriseValue")))
                            info_row("Trailing P/E",      fmt_num(info.get("trailingPE")))
                            info_row("Forward P/E",       fmt_num(info.get("forwardPE")))
                            info_row("PEG Ratio",         fmt_num(info.get("pegRatio")))
                            info_row("Price / Sales",     fmt_num(info.get("priceToSalesTrailing12Months")))
                            info_row("Price / Book",      fmt_num(info.get("priceToBook")))
                            info_row("EV / Revenue",      fmt_num(info.get("enterpriseToRevenue")))
                            info_row("EV / EBITDA",       fmt_num(info.get("enterpriseToEbitda")))
                        with v2:
                            info_row("Trailing EPS",      fmt_num(info.get("trailingEps")))
                            info_row("Forward EPS",       fmt_num(info.get("forwardEps")))
                            info_row("Book Value/Share",  fmt_num(info.get("bookValue")))
                            info_row("52-Week High",      fmt_num(info.get("fiftyTwoWeekHigh")))
                            info_row("52-Week Low",       fmt_num(info.get("fiftyTwoWeekLow")))
                            info_row("50-Day MA",         fmt_num(info.get("fiftyDayAverage")))
                            info_row("200-Day MA",        fmt_num(info.get("twoHundredDayAverage")))
                            info_row("Beta",              fmt_num(info.get("beta")))
                            info_row("Short % of Float",  fmt_pct(info.get("shortPercentOfFloat")))
                        st.markdown("#### Dividends")
                        d1, d2, d3 = st.columns(3)
                        d1.metric("Dividend Rate",  fmt_num(info.get("dividendRate")) if info.get("dividendRate") else "None")
                        d2.metric("Dividend Yield", fmt_pct(info.get("dividendYield")) if info.get("dividendYield") else "None")
                        d3.metric("Payout Ratio",   fmt_pct(info.get("payoutRatio")) if info.get("payoutRatio") else "N/A")

                    # ── Tab 3: Income Statement (cached) ─────────
                    with fin_tabs[2]:
                        st.markdown("### 📈 Income Statement (Annual)")
                        if fin_stmt is not None and not fin_stmt.empty:
                            fin_disp = fin_stmt.T.copy()
                            fin_disp.index = [str(i)[:10] for i in fin_disp.index]
                            for c in fin_disp.columns:
                                fin_disp[c] = fin_disp[c].apply(lambda x: fmt_big(x) if pd.notnull(x) else "N/A")
                            st.dataframe(fin_disp, use_container_width=True)
                        else:
                            st.info("Income statement data not available for this ticker.")
                        st.markdown("#### Key Highlights")
                        is1, is2, is3, is4 = st.columns(4)
                        is1.metric("Total Revenue",    fmt_big(info.get("totalRevenue")))
                        is2.metric("Gross Profit",     fmt_big(info.get("grossProfits")))
                        is3.metric("EBITDA",           fmt_big(info.get("ebitda")))
                        is4.metric("Net Income",       fmt_big(info.get("netIncomeToCommon")))
                        is5, is6, is7, is8 = st.columns(4)
                        is5.metric("Gross Margin",     fmt_pct(info.get("grossMargins")))
                        is6.metric("Operating Margin", fmt_pct(info.get("operatingMargins")))
                        is7.metric("Profit Margin",    fmt_pct(info.get("profitMargins")))
                        is8.metric("Revenue Growth",   fmt_pct(info.get("revenueGrowth")))

                    # ── Tab 4: Balance Sheet (cached) ─────────────
                    with fin_tabs[3]:
                        st.markdown("### 🏦 Balance Sheet (Annual)")
                        if bal_stmt is not None and not bal_stmt.empty:
                            bal_disp = bal_stmt.T.copy()
                            bal_disp.index = [str(i)[:10] for i in bal_disp.index]
                            for c in bal_disp.columns:
                                bal_disp[c] = bal_disp[c].apply(lambda x: fmt_big(x) if pd.notnull(x) else "N/A")
                            st.dataframe(bal_disp, use_container_width=True)
                        else:
                            st.info("Balance sheet data not available for this ticker.")
                        st.markdown("#### Key Highlights")
                        bs1, bs2, bs3, bs4 = st.columns(4)
                        bs1.metric("Total Cash",   fmt_big(info.get("totalCash")))
                        bs2.metric("Total Debt",   fmt_big(info.get("totalDebt")))
                        bs3.metric("Net Cash",     fmt_big((info.get("totalCash") or 0) - (info.get("totalDebt") or 0)))
                        bs4.metric("Total Assets", fmt_big(info.get("totalAssets")))
                        bs5, bs6, bs7, bs8 = st.columns(4)
                        bs5.metric("Cash/Share",   fmt_num(info.get("totalCashPerShare")))
                        bs6.metric("Debt/Equity",  fmt_num(info.get("debtToEquity")))
                        bs7.metric("Current Ratio",fmt_num(info.get("currentRatio")))
                        bs8.metric("Quick Ratio",  fmt_num(info.get("quickRatio")))

                    # ── Tab 5: Cash Flow (cached) ─────────────────
                    with fin_tabs[4]:
                        st.markdown("### 💵 Cash Flow Statement (Annual)")
                        if cf_stmt is not None and not cf_stmt.empty:
                            cf_disp = cf_stmt.T.copy()
                            cf_disp.index = [str(i)[:10] for i in cf_disp.index]
                            for c in cf_disp.columns:
                                cf_disp[c] = cf_disp[c].apply(lambda x: fmt_big(x) if pd.notnull(x) else "N/A")
                            st.dataframe(cf_disp, use_container_width=True)
                        else:
                            st.info("Cash flow data not available for this ticker.")
                        st.markdown("#### Key Highlights")
                        cf1, cf2, cf3, cf4 = st.columns(4)
                        cf1.metric("Operating CF",  fmt_big(info.get("operatingCashflow")))
                        cf2.metric("Free CF",       fmt_big(info.get("freeCashflow")))
                        cf3.metric("CapEx",         fmt_big(info.get("capitalExpenditures")))
                        shares = info.get("sharesOutstanding")
                        fcf    = info.get("freeCashflow")
                        cf4.metric("FCF/Share",     fmt_num(fcf / shares if fcf and shares else None))
                        cf5, cf6 = st.columns(2)
                        cf5.metric("Return on Assets", fmt_pct(info.get("returnOnAssets")))
                        cf6.metric("Return on Equity", fmt_pct(info.get("returnOnEquity")))

                    # ── Tab 6: Price History (uses cached hist_1y + selectbox) ──
                    with fin_tabs[5]:
                        st.markdown("### 📊 Price History")
                        period_choice = st.selectbox(
                            "Time Period",
                            ["1mo", "3mo", "6mo", "1y"],
                            index=2,
                            key="price_history_period",
                        )
                        # Slice the already-fetched 1y history instead of a new API call
                        period_days = {"1mo": 21, "3mo": 63, "6mo": 126, "1y": 252}
                        days_needed = period_days.get(period_choice, 126)
                        hist_disp   = hist_1y.iloc[-days_needed:] if len(hist_1y) >= days_needed else hist_1y

                        if not hist_disp.empty:
                            ph1, ph2 = st.columns([3, 1])
                            with ph1:
                                st.markdown("**Closing Price**")
                                st.line_chart(hist_disp["Close"], use_container_width=True)
                            with ph2:
                                first_p    = hist_disp["Close"].iloc[0]
                                last_p     = hist_disp["Close"].iloc[-1]
                                period_ret = (last_p - first_p) / first_p
                                st.metric("Period Return",  fmt_pct(period_ret), delta=f"{period_ret:+.2%}")
                                st.metric("Period High",    f"${hist_disp['High'].max():,.2f}")
                                st.metric("Period Low",     f"${hist_disp['Low'].min():,.2f}")
                                st.metric("Avg Daily Vol",  fmt_big(hist_disp["Volume"].mean()).replace("$", ""))
                            st.markdown("**Volume**")
                            st.bar_chart(hist_disp["Volume"], use_container_width=True)

                            hist_ma = hist_disp["Close"].to_frame()
                            hist_ma["MA20"]  = hist_ma["Close"].rolling(20).mean()
                            hist_ma["MA50"]  = hist_ma["Close"].rolling(50).mean()
                            hist_ma["MA200"] = hist_ma["Close"].rolling(200).mean()
                            ma_chart = hist_ma.dropna()
                            if not ma_chart.empty:
                                st.markdown("**Price with Moving Averages (20 / 50 / 200 day)**")
                                st.line_chart(ma_chart, use_container_width=True)
                        else:
                            st.info("No price history available.")

            except Exception as e:
                    st.error(f"Error rendering analysis for {ticker_input}: {e}")
