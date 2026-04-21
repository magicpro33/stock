"""
dividend_calendar.py
Dividend Capture Calendar for magicpro33/stock

Reads data/stock_data.json.gz written by nightly_scan.py.
If ExDividendDate is not yet in the scan data (old scan),
falls back to fetching ex-dates live from yfinance for just
the dividend-paying stocks so the calendar always works.

Deploy: share.streamlit.io -> New app -> magicpro33/stock -> dividend_calendar.py
"""

import streamlit as st
import pandas as pd
import yfinance as yf
import gzip
import json
import os
import time
import datetime
import calendar as cal_module

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dividend Capture Calendar",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.main-title {
    font-family: 'DM Serif Display', serif;
    font-size: 2.5rem;
    color: #cc0000;
    letter-spacing: -0.02em;
    line-height: 1.1;
    margin-bottom: 0;
}
.main-sub {
    font-size: 0.8rem;
    color: #aaa;
    margin-top: 4px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}
.src-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    border-radius: 6px;
    padding: 6px 14px;
    font-size: 0.78rem;
    font-weight: 500;
    margin: 10px 0 20px;
}
.src-ok   { background: #f0f7f0; border: 1px solid #b8ddb8; color: #1a6b1a; }
.src-warn { background: #fff8e6; border: 1px solid #f0d080; color: #7a5a00; }
.src-err  { background: #fff0f0; border: 1px solid #f0b0b0; color: #8b0000; }
.cal-grid {
    display: grid;
    grid-template-columns: repeat(7, 1fr);
    gap: 4px;
    margin-top: 10px;
}
.cal-hdr {
    text-align: center;
    font-size: 0.65rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #ccc;
    padding: 5px 0;
}
.cal-day {
    background: #f9f9f7;
    border: 1px solid #efefed;
    border-radius: 7px;
    min-height: 88px;
    padding: 8px 7px;
}
.cal-day.today { border: 2px solid #cc0000; }
.cal-day.empty { background: transparent; border: none; }
.cal-num { font-size: 0.7rem; font-weight: 500; color: #ccc; margin-bottom: 5px; }
.cal-day.today .cal-num { color: #cc0000; font-weight: 700; }
.chip {
    display: block;
    border-radius: 3px;
    padding: 2px 5px;
    margin-bottom: 3px;
    font-size: 0.6rem;
    font-weight: 600;
    font-family: 'DM Mono', monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    border-left: 2px solid;
    cursor: default;
}
.t1 { background: rgba(10,61,10,0.12);  color: #1a6b1a; border-color: #2e7d32; }
.t2 { background: rgba(30,90,30,0.10);  color: #2e7d32; border-color: #388e3c; }
.t3 { background: rgba(100,100,10,0.10);color: #827717; border-color: #f9a825; }
.t4 { background: rgba(180,90,0,0.09);  color: #e65100; border-color: #ff9800; }
.t5 { background: rgba(150,40,0,0.08);  color: #bf360c; border-color: #ff5722; }
.stbl { width: 100%; border-collapse: collapse; }
.stbl th {
    font-size: 0.66rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #bbb;
    border-bottom: 1px solid #efefed;
    padding: 8px 10px;
    text-align: left;
}
.stbl td { padding: 10px; border-bottom: 1px solid #f5f5f3; font-size: 0.82rem; }
.stbl tr:last-child td { border-bottom: none; }
.mono { font-family: 'DM Mono', monospace; font-size: 0.78rem; }
.buy-now { background: #cc0000; color: #fff; padding: 2px 8px; border-radius: 4px; font-size: 0.67rem; font-weight: 700; }
.buy-tmr { background: #2e7d32; color: #fff; padding: 2px 8px; border-radius: 4px; font-size: 0.67rem; font-weight: 700; }
</style>
""", unsafe_allow_html=True)


# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "stock_data.json.gz")
META_FILE = os.path.join(BASE_DIR, "data", "scan_meta.json")


# ── Load scan data ────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def load_scan_data():
    """
    Parse stock_data.json.gz into a DataFrame of dividend-paying stocks.
    Returns (df, error_string, has_ex_dates_bool)
    """
    if not os.path.exists(DATA_FILE):
        return None, "data/stock_data.json.gz not found — run nightly_scan.py first", False

    try:
        with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        return None, "Could not read data file: " + str(e), False

    rows = []
    ex_date_count = 0

    for item in raw:
        if not isinstance(item, dict):
            continue

        ticker = (item.get("Ticker") or "").strip()
        if not ticker:
            continue

        # Yield already stored as percentage (e.g. 4.5 = 4.5%)
        yield_raw = item.get("DividendYieldPct")
        if yield_raw is None:
            continue
        try:
            yield_pct = float(yield_raw)
        except (TypeError, ValueError):
            continue

        # Filter: must be positive and sane (cap at 50% — ADR data errors)
        if yield_pct <= 0 or yield_pct > 50:
            continue

        # Div rate
        div_rate = item.get("DividendRate")
        try:
            div_rate = float(div_rate) if div_rate is not None else None
        except (TypeError, ValueError):
            div_rate = None

        # Payout ratio already stored as percentage
        payout = item.get("DividendPayoutRatio")
        try:
            payout = float(payout) if payout is not None else None
            if payout is not None and (payout < 0 or payout > 500):
                payout = None
        except (TypeError, ValueError):
            payout = None

        # ExDividendDate — Unix timestamp stored by fixed nightly_scan.py
        ex_date = None
        ex_ts = item.get("ExDividendDate")
        if ex_ts is not None:
            try:
                ts_val = float(ex_ts)
                if ts_val > 1_000_000_000:
                    ex_date = datetime.datetime.utcfromtimestamp(ts_val).date()
                    ex_date_count += 1
            except (TypeError, ValueError):
                ex_date = None

        rows.append({
            "ticker":    ticker,
            "sector":    item.get("Sector") or "Unknown",
            "price":     item.get("Price"),
            "yield_pct": round(yield_pct, 2),
            "div_rate":  div_rate,
            "payout":    payout,
            "frequency": item.get("DividendFrequency") or "—",
            "div_score": float(item.get("DividendScore") or 0),
            "ex_date":   ex_date,
        })

    if not rows:
        return None, "No valid dividend stocks found in scan data.", False

    df = pd.DataFrame(rows)
    df = df.sort_values("yield_pct", ascending=False).reset_index(drop=True)
    has_ex_dates = ex_date_count > 0
    return df, None, has_ex_dates


@st.cache_data(ttl=1800, show_spinner=False)
def load_meta():
    if not os.path.exists(META_FILE):
        return None
    try:
        with open(META_FILE) as f:
            return json.load(f)
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ex_dates_live(tickers_tuple):
    """
    Fallback: fetch ex-dates from yfinance when scan data doesn't have them yet.
    Only called for the ~filtered dividend stocks, not all 5000+ tickers.
    """
    result = {}
    tickers = list(tickers_tuple)
    prog = st.progress(0, text="Fetching ex-dividend dates from Yahoo Finance...")
    n = len(tickers)
    for i, ticker in enumerate(tickers):
        try:
            info  = yf.Ticker(ticker).info
            ex_ts = info.get("exDividendDate")
            if ex_ts and isinstance(ex_ts, (int, float)) and ex_ts > 1_000_000_000:
                result[ticker] = datetime.datetime.utcfromtimestamp(float(ex_ts)).date()
            else:
                result[ticker] = None
        except Exception:
            result[ticker] = None
        prog.progress((i + 1) / n, text="Fetching ex-dates... " + str(i + 1) + "/" + str(n))
        time.sleep(0.08)
    prog.empty()
    return result


def safe_date(val):
    """Return a datetime.date if val is a real date, else None. Handles pd.NaT safely."""
    if val is None:
        return None
    if isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
        return val
    if isinstance(val, datetime.datetime):
        return val.date()
    # pd.NaT check — do NOT use isinstance(val, datetime.date) for NaT
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return None


# ── Render helpers ────────────────────────────────────────────────────────────
def tier(y):
    if y >= 8:   return "t1"
    if y >= 6:   return "t2"
    if y >= 4:   return "t3"
    if y >= 2.5: return "t4"
    return "t5"


def ycolor(y):
    colors = {
        "t1": "#1a6b1a",
        "t2": "#2e7d32",
        "t3": "#827717",
        "t4": "#e65100",
        "t5": "#bf360c",
    }
    return colors[tier(y)]


def render_calendar(df, year, month):
    today   = datetime.date.today()
    day_map = {}

    for _, row in df.iterrows():
        bd = safe_date(row.get("buy_date"))
        if bd is None:
            continue
        if bd.year == year and bd.month == month:
            day_map.setdefault(bd.day, []).append(row)

    html_parts = ['<div class="cal-grid">']
    for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        html_parts.append('<div class="cal-hdr">' + d + '</div>')

    for week in cal_module.monthcalendar(year, month):
        for day in week:
            if day == 0:
                html_parts.append('<div class="cal-day empty"></div>')
                continue
            is_today = (year == today.year and month == today.month and day == today.day)
            day_cls  = "cal-day today" if is_today else "cal-day"
            html_parts.append('<div class="' + day_cls + '">')
            html_parts.append('<div class="cal-num">' + str(day) + '</div>')
            for row in day_map.get(day, []):
                t    = tier(row["yield_pct"])
                dr   = row.get("div_rate") or 0
                px   = row.get("price") or 0
                ex   = safe_date(row.get("ex_date"))
                freq = str(row.get("frequency") or "—")
                tip  = (
                    "BUY " + row["ticker"] + " before " + str(ex) +
                    " | Yield: " + str(row["yield_pct"]) + "%" +
                    " | $" + "{:.4f}".format(dr) + "/share" +
                    " | Freq: " + freq +
                    " | Price: $" + "{:.2f}".format(px)
                )
                html_parts.append(
                    '<span class="chip ' + t + '" title="' + tip + '">' +
                    row["ticker"] + " " + str(row["yield_pct"]) + "%" +
                    '</span>'
                )
            html_parts.append('</div>')

    html_parts.append('</div>')
    st.markdown("".join(html_parts), unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 💰 Dividend Calendar")
    st.markdown("`magicpro33/stock`")
    st.markdown("---")
    min_yield   = st.slider("Min yield (%)",  0.0, 25.0, 0.0,  0.5)
    max_yield   = st.slider("Max yield (%)",  5.0, 50.0, 25.0, 1.0)
    days_ahead  = st.slider("Days ahead",     30,  180,  90)
    freq_filter = st.selectbox(
        "Frequency",
        ["All", "Monthly", "Quarterly", "Semi-Annual", "Annual"]
    )
    st.markdown("---")
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown(
        "**How it works:**\n"
        "- Loads `data/stock_data.json.gz`\n"
        "  (updated nightly by GitHub Actions)\n"
        "- Ex-dates from scan data if available,\n"
        "  otherwise fetched live from Yahoo\n"
        "- Calendar = day **before** ex-date\n"
        "- Own shares by that day → dividend paid\n"
        "- Chips sorted highest yield first\n"
        "- Hover chip for details"
    )


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">Dividend Capture Calendar</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="main-sub">'
    'buy 24h before ex-date  |  highest yield first  |  magicpro33/stock'
    '</div>',
    unsafe_allow_html=True,
)


# ── Load scan data ────────────────────────────────────────────────────────────
with st.spinner("Loading scan data..."):
    result = load_scan_data()

if result[0] is None:
    st.markdown('<div class="src-badge src-err">&#x2717; ' + result[1] + '</div>', unsafe_allow_html=True)
    st.info(
        "**Setup:** Run `python nightly_scan.py` once locally, "
        "then push data/ to GitHub. GitHub Actions regenerates it nightly."
    )
    st.stop()

df_all, _err, has_ex_dates = result
meta = load_meta()


# ── Sector filter (needs df_all) ──────────────────────────────────────────────
with st.sidebar:
    sector_list   = ["All sectors"] + sorted(df_all["sector"].dropna().unique().tolist())
    sector_filter = st.selectbox("Sector", sector_list)


# ── Apply yield + freq + sector filters ──────────────────────────────────────
df = df_all[
    (df_all["yield_pct"] >= min_yield) &
    (df_all["yield_pct"] <= max_yield)
].copy()

if freq_filter != "All":
    df = df[df["frequency"].str.contains(freq_filter, case=False, na=False)]

if sector_filter != "All sectors":
    df = df[df["sector"] == sector_filter]


# ── Get ex-dates: from scan or live fallback ──────────────────────────────────
today = datetime.date.today()

if has_ex_dates:
    # Fast path: scan already has ExDividendDate
    source_label = "ex-dates from scan data"
    badge_cls    = "src-ok"
else:
    # Fallback: fetch live for just the filtered dividend stocks
    st.info(
        "The scan data does not yet have ex-dividend dates stored. "
        "Fetching live from Yahoo Finance for " + str(len(df)) + " dividend stocks. "
        "Push the updated nightly_scan.py and re-run the scan to make this instant."
    )
    with st.spinner("Fetching live ex-dates from Yahoo Finance..."):
        live_map = fetch_ex_dates_live(tuple(df["ticker"].tolist()))
    df = df.copy()
    df["ex_date"] = df["ticker"].map(live_map)
    source_label = "ex-dates fetched live from Yahoo Finance"
    badge_cls    = "src-warn"

# Compute buy_date safely (day before ex-date)
def compute_buy_date(ex):
    d = safe_date(ex)
    return (d - datetime.timedelta(days=1)) if d is not None else None

df["buy_date"] = df["ex_date"].apply(compute_buy_date)


# ── Badge ─────────────────────────────────────────────────────────────────────
meta_txt = ""
if meta:
    meta_txt = "  |  Last scan: " + str(meta.get("scanned_at_utc", "—"))

ex_found = df["ex_date"].apply(safe_date).notna().sum()
badge_text = (
    str(len(df_all)) + " dividend stocks loaded  |  " +
    str(ex_found) + " with upcoming ex-dates  |  " +
    source_label + meta_txt
)
st.markdown(
    '<div class="src-badge ' + badge_cls + '">&#x2713; ' + badge_text + '</div>',
    unsafe_allow_html=True,
)


# ── Filter to upcoming window ─────────────────────────────────────────────────
cutoff = today + datetime.timedelta(days=days_ahead)

def in_window(buy_date_val):
    d = safe_date(buy_date_val)
    if d is None:
        return False
    return today <= d <= cutoff

df_cal = df[df["buy_date"].apply(in_window)].copy()
df_cal = df_cal.sort_values("yield_pct", ascending=False).reset_index(drop=True)


# ── Metrics ───────────────────────────────────────────────────────────────────
nxt = None
if not df_cal.empty:
    df_sorted = df_cal.sort_values("buy_date")
    nxt = df_sorted.iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Buy signals ahead", len(df_cal))
c2.metric("Avg yield", "{:.1f}%".format(df_cal["yield_pct"].mean()) if not df_cal.empty else "—")
c3.metric(
    "Highest yield",
    "{:.1f}%".format(df_cal["yield_pct"].max()) if not df_cal.empty else "—",
    delta=str(df_cal.iloc[0]["ticker"]) if not df_cal.empty else "",
)
nxt_date_str  = safe_date(nxt["buy_date"]).strftime("%b %d") if nxt is not None else "—"
nxt_ticker    = str(nxt["ticker"]) if nxt is not None else ""
c4.metric("Next buy date", nxt_date_str, delta=nxt_ticker)

st.markdown("---")


# ── Calendar navigation ───────────────────────────────────────────────────────
if "cy" not in st.session_state:
    st.session_state.cy = today.year
if "cm" not in st.session_state:
    st.session_state.cm = today.month

cp, cc, cn = st.columns([1, 5, 1])
with cp:
    if st.button("← Prev"):
        if st.session_state.cm == 1:
            st.session_state.cy -= 1
            st.session_state.cm = 12
        else:
            st.session_state.cm -= 1
with cn:
    if st.button("Next →"):
        if st.session_state.cm == 12:
            st.session_state.cy += 1
            st.session_state.cm = 1
        else:
            st.session_state.cm += 1
with cc:
    month_name = cal_module.month_name[st.session_state.cm]
    st.markdown(
        "<h3 style='text-align:center;font-family:DM Serif Display,serif;margin:0'>" +
        month_name + " " + str(st.session_state.cy) +
        "</h3>",
        unsafe_allow_html=True,
    )

# Yield legend
st.markdown(
    "<div style='display:flex;gap:8px;margin:6px 0 4px;flex-wrap:wrap'>"
    "<span style='background:#1a6b1a;color:#fff;padding:2px 10px;border-radius:3px;"
    "font-size:0.7rem;font-weight:600'>8%+</span>"
    "<span style='background:#2e7d32;color:#fff;padding:2px 10px;border-radius:3px;"
    "font-size:0.7rem;font-weight:600'>6%+</span>"
    "<span style='background:#827717;color:#fff;padding:2px 10px;border-radius:3px;"
    "font-size:0.7rem;font-weight:600'>4%+</span>"
    "<span style='background:#e65100;color:#fff;padding:2px 10px;border-radius:3px;"
    "font-size:0.7rem;font-weight:600'>2.5%+</span>"
    "<span style='background:#bf360c;color:#fff;padding:2px 10px;border-radius:3px;"
    "font-size:0.7rem;font-weight:600'>below 2.5%</span>"
    "</div>",
    unsafe_allow_html=True,
)

render_calendar(df_cal, st.session_state.cy, st.session_state.cm)
st.markdown("<br>", unsafe_allow_html=True)


# ── Upcoming buy signals table ────────────────────────────────────────────────
st.markdown("### Upcoming Buy Signals — Ranked by Yield")

if df_cal.empty:
    st.info(
        "No dividend buy signals found in the next " + str(days_ahead) + " days. "
        "Try widening the date range or adjusting the yield filters."
    )
else:
    df_show = df_cal.sort_values(
        ["buy_date", "yield_pct"], ascending=[True, False]
    ).copy()

    rows_html = []
    for _, row in df_show.iterrows():
        yc      = ycolor(row["yield_pct"])
        bd      = safe_date(row.get("buy_date"))
        ex      = safe_date(row.get("ex_date"))
        da      = (bd - today).days if bd is not None else 0
        dr      = float(row.get("div_rate") or 0)
        px      = float(row.get("price") or 0)
        pr      = row.get("payout")
        freq    = str(row.get("frequency") or "—")
        payout_str = "{:.0f}%".format(pr) if pr is not None else "—"
        bd_str  = bd.strftime("%b %d, %Y") if bd is not None else "—"
        ex_str  = ex.strftime("%b %d, %Y") if ex is not None else "—"

        alert = ""
        if da == 0:
            alert = '<span class="buy-now">BUY TODAY</span>'
        elif da == 1:
            alert = '<span class="buy-tmr">BUY TOMORROW</span>'

        row_html = (
            "<tr>"
            "<td><strong style=\"font-family:'DM Mono',monospace\">" + row["ticker"] + "</strong></td>"
            "<td style='color:#aaa;font-size:0.75rem'>" + str(row["sector"]) + "</td>"
            "<td><span style='background:" + yc + "18;color:" + yc + ";padding:2px 9px;"
            "border-radius:100px;font-family:DM Mono,monospace;"
            "font-size:0.78rem;font-weight:600'>" + str(row["yield_pct"]) + "%</span></td>"
            "<td class='mono'>${:.4f}".format(dr) + "</td>"
            "<td class='mono'>" + payout_str + "</td>"
            "<td style='font-size:0.77rem;color:#888'>" + freq + "</td>"
            "<td class='mono'>${:.2f}".format(px) + "</td>"
            "<td class='mono'>" + bd_str + "</td>"
            "<td class='mono' style='color:#bbb'>" + ex_str + "</td>"
            "<td>" + str(da) + "d " + alert + "</td>"
            "</tr>"
        )
        rows_html.append(row_html)

    table_html = (
        "<table class='stbl'><thead><tr>"
        "<th>Ticker</th><th>Sector</th><th>Yield</th><th>Div/Share</th>"
        "<th>Payout</th><th>Frequency</th><th>Price</th>"
        "<th>Buy Before</th><th>Ex-Date</th><th>Countdown</th>"
        "</tr></thead><tbody>"
        + "".join(rows_html) +
        "</tbody></table>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


# ── Full universe expander ────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
with st.expander(
    "Full dividend universe — " + str(len(df)) + " stocks after filters (" +
    str(len(df_all)) + " total)"
):
    display_df = df[["ticker", "sector", "yield_pct", "div_rate",
                      "payout", "frequency", "price", "ex_date", "buy_date"]].copy()
    display_df = display_df.rename(columns={
        "ticker":    "Ticker",
        "sector":    "Sector",
        "yield_pct": "Yield %",
        "div_rate":  "Div/Share",
        "payout":    "Payout %",
        "frequency": "Frequency",
        "price":     "Price",
        "ex_date":   "Ex-Date",
        "buy_date":  "Buy Before",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(
    "<hr><p style='font-size:0.7rem;color:#ccc;text-align:center'>"
    "github.com/magicpro33/stock  |  data updated nightly via GitHub Actions  |  "
    "Not financial advice  |  Always verify ex-dates before trading"
    "</p>",
    unsafe_allow_html=True,
)
