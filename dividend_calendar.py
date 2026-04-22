"""
dividend_calendar.py  --  Dividend Capture Calendar
magicpro33/stock

Features:
  - Calendar: buy 24h before ex-date, chips sorted by yield
  - Ranked table with frozen header, monthly payout per share column
  - Investment Calculator: enter $ amount -> monthly / annual income
  - Stock Analyzer: enter any ticker -> full metrics breakdown
"""

import streamlit as st
import pandas as pd
import yfinance as yf
import gzip, json, os, time, datetime, calendar as cal_module

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
    font-size: 2.4rem;
    color: #cc0000;
    letter-spacing: -0.02em;
    line-height: 1.1;
    margin-bottom: 0;
}
.main-sub {
    font-size: 0.78rem; color: #aaa; margin-top: 4px;
    letter-spacing: 0.06em; text-transform: uppercase;
}
.src-badge {
    display: inline-flex; align-items: center; gap: 6px;
    border-radius: 6px; padding: 6px 14px;
    font-size: 0.78rem; font-weight: 500; margin: 10px 0 20px;
}
.src-ok   { background:#f0f7f0; border:1px solid #b8ddb8; color:#1a6b1a; }
.src-warn { background:#fff8e6; border:1px solid #f0d080; color:#7a5a00; }
.src-err  { background:#fff0f0; border:1px solid #f0b0b0; color:#8b0000; }

/* Calendar */
.cal-grid { display:grid; grid-template-columns:repeat(7,1fr); gap:4px; margin-top:10px; }
.cal-hdr  { text-align:center; font-size:0.65rem; font-weight:600; letter-spacing:0.12em; text-transform:uppercase; color:#ccc; padding:5px 0; }
.cal-day  { background:#f9f9f7; border:1px solid #efefed; border-radius:7px; min-height:88px; padding:8px 7px; }
.cal-day.today { border:2px solid #cc0000; }
.cal-day.empty { background:transparent; border:none; }
.cal-num  { font-size:0.7rem; font-weight:500; color:#ccc; margin-bottom:5px; }
.cal-day.today .cal-num { color:#cc0000; font-weight:700; }
.chip {
    display:block; border-radius:3px; padding:2px 5px; margin-bottom:3px;
    font-size:0.6rem; font-weight:600; font-family:'DM Mono',monospace;
    white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
    border-left:2px solid; cursor:default;
}
.t1 { background:rgba(10,61,10,.12);   color:#1a6b1a; border-color:#2e7d32; }
.t2 { background:rgba(30,90,30,.10);   color:#2e7d32; border-color:#388e3c; }
.t3 { background:rgba(100,100,10,.10); color:#827717; border-color:#f9a825; }
.t4 { background:rgba(180,90,0,.09);   color:#e65100; border-color:#ff9800; }
.t5 { background:rgba(150,40,0,.08);   color:#bf360c; border-color:#ff5722; }

/* Sticky-header table */
.tbl-wrap {
    max-height: 480px;
    overflow-y: auto;
    border: 1px solid #2a2a2a;
    border-radius: 8px;
    background: #111;
}
.stbl { width: 100%; border-collapse: collapse; }
.stbl thead th {
    position: sticky; top: 0; z-index: 2;
    background: #1a1a1a; color: #999;
    font-size: 0.66rem; letter-spacing: 0.1em; text-transform: uppercase;
    padding: 10px 10px; text-align: left; white-space: nowrap;
    border-bottom: 1px solid #333;
}
.stbl tbody td {
    padding: 10px 10px;
    border-bottom: 1px solid #1e1e1e;
    font-size: 0.82rem;
    color: #d0d0d0;
    background: #111;
    transition: background 0.1s ease, color 0.1s ease;
}
.stbl tbody tr:last-child td { border-bottom: none; }

/* Full-row hover: dark green tint, ALL cells affected */
.stbl tbody tr.tbl-row:hover td { background: #1a2e1a !important; }

/* Per-class cell colors - normal state */
.td-ticker { font-family: 'DM Mono', monospace; font-weight: 700; color: #ffffff; }
.td-sector { font-size: 0.74rem; color: #888; }
.td-num    { font-family: 'DM Mono', monospace; color: #c8c8c8; }
.td-freq   { font-size: 0.76rem; color: #888; }
.td-date   { font-family: 'DM Mono', monospace; color: #777; }
.td-count  { font-family: 'DM Mono', monospace; color: #aaa; white-space: nowrap; }

/* Hover state - all text brightens uniformly */
.stbl tbody tr.tbl-row:hover .td-ticker { color: #ffffff !important; }
.stbl tbody tr.tbl-row:hover .td-sector { color: #a0c8a0 !important; }
.stbl tbody tr.tbl-row:hover .td-num    { color: #e8e8e8 !important; }
.stbl tbody tr.tbl-row:hover .td-freq   { color: #a0c8a0 !important; }
.stbl tbody tr.tbl-row:hover .td-date   { color: #c8c8c8 !important; }
.stbl tbody tr.tbl-row:hover .td-count  { color: #e8e8e8 !important; }

/* Yield badge - CSS custom property for per-row color */
.yield-badge {
    padding: 2px 9px;
    border-radius: 100px;
    font-family: 'DM Mono', monospace;
    font-size: 0.78rem;
    font-weight: 600;
    background: rgba(255,255,255,0.07);
    color: var(--yc);
    border: 1px solid var(--yc);
    opacity: 0.9;
}
.stbl tbody tr.tbl-row:hover .yield-badge { opacity: 1; }
.mono { font-family:'DM Mono',monospace; font-size:0.78rem; }
.buy-now { background:#cc0000; color:#fff; padding:2px 8px; border-radius:4px; font-size:0.67rem; font-weight:700; }
.buy-tmr { background:#2e7d32; color:#fff; padding:2px 8px; border-radius:4px; font-size:0.67rem; font-weight:700; }

/* Calculator / Analyzer cards */
.calc-card {
    background: #f9f9f7;
    border: 1px solid #e8e8e4;
    border-radius: 10px;
    padding: 20px 24px;
    margin-bottom: 20px;
}
.calc-result {
    background: #0a2a0a;
    border-radius: 8px;
    padding: 16px 20px;
    margin-top: 12px;
}
.calc-result-row {
    display: flex; justify-content: space-between; align-items: baseline;
    padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.06);
}
.calc-result-row:last-child { border-bottom: none; }
.calc-label { font-size: 0.78rem; color: #88aa88; }
.calc-value { font-family: 'DM Mono', monospace; font-size: 1rem; font-weight: 600; color: #7fff7f; }
.calc-value.big { font-size: 1.3rem; color: #39ff4a; }

/* Metric bars */
.metric-bar-wrap { margin: 8px 0; }
.metric-bar-label { font-size: 0.72rem; color: #666; margin-bottom: 3px; display:flex; justify-content:space-between; }
.metric-bar-bg { height: 8px; background: #e8e8e4; border-radius: 4px; overflow: hidden; }
.metric-bar-fill { height: 100%; border-radius: 4px; transition: width 0.4s ease; }
.tag-good { background:#e8f5e9; color:#1b5e20; padding:2px 8px; border-radius:100px; font-size:0.68rem; font-weight:600; }
.tag-ok   { background:#fff8e1; color:#e65100; padding:2px 8px; border-radius:100px; font-size:0.68rem; font-weight:600; }
.tag-bad  { background:#ffebee; color:#b71c1c; padding:2px 8px; border-radius:100px; font-size:0.68rem; font-weight:600; }
.section-hdr {
    font-family: 'DM Serif Display', serif;
    font-size: 1.2rem; color: #cc0000;
    margin: 24px 0 12px; padding-bottom: 6px;
    border-bottom: 2px solid #cc0000;
}
</style>
""", unsafe_allow_html=True)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "stock_data.json.gz")
META_FILE = os.path.join(BASE_DIR, "data", "scan_meta.json")


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def load_scan_data():
    if not os.path.exists(DATA_FILE):
        return None, "data/stock_data.json.gz not found. Run nightly_scan.py first.", False

    try:
        with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        return None, "Could not read data file: " + str(e), False

    rows = []
    ex_count = 0
    for item in raw:
        if not isinstance(item, dict):
            continue
        ticker = (item.get("Ticker") or "").strip()
        if not ticker:
            continue
        try:
            yield_pct = float(item.get("DividendYieldPct") or 0)
        except (TypeError, ValueError):
            continue
        if yield_pct <= 0 or yield_pct > 50:
            continue

        try:
            div_rate = float(item.get("DividendRate") or 0) or None
        except (TypeError, ValueError):
            div_rate = None

        try:
            payout = float(item.get("DividendPayoutRatio") or 0) or None
            if payout and (payout < 0 or payout > 500):
                payout = None
        except (TypeError, ValueError):
            payout = None

        # Frequency label (for display)
        freq = item.get("DividendFrequency") or "--"

        # Monthly payout per share — div_rate from yfinance is the ANNUAL rate
        # so monthly is always annual / 12, regardless of payment frequency
        monthly_pay = round(div_rate / 12, 4) if div_rate else None

        # Ex-date
        ex_date = None
        ex_ts = item.get("ExDividendDate")
        if ex_ts:
            try:
                ts = float(ex_ts)
                if ts > 1_000_000_000:
                    ex_date = datetime.datetime.utcfromtimestamp(ts).date()
                    ex_count += 1
            except (TypeError, ValueError):
                pass

        rows.append({
            "ticker":      ticker,
            "sector":      item.get("Sector") or "Unknown",
            "price":       item.get("Price"),
            "yield_pct":   round(yield_pct, 2),
            "div_rate":    div_rate,
            "monthly_pay": monthly_pay,
            "payout":      payout,
            "frequency":   freq,
            "div_score":   float(item.get("DividendScore") or 0),
            "ex_date":     ex_date,
        })

    if not rows:
        return None, "No valid dividend stocks found in scan data.", False

    df = pd.DataFrame(rows)
    df = df.sort_values("yield_pct", ascending=False).reset_index(drop=True)
    return df, None, ex_count > 0


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ex_dates_live(tickers_tuple):
    result = {}
    tickers = list(tickers_tuple)
    prog = st.progress(0, text="Fetching ex-dividend dates from Yahoo Finance...")
    n = len(tickers)
    for i, ticker in enumerate(tickers):
        try:
            info  = yf.Ticker(ticker).info
            ex_ts = info.get("exDividendDate")
            if ex_ts and isinstance(ex_ts, (int, float)) and float(ex_ts) > 1_000_000_000:
                result[ticker] = datetime.datetime.utcfromtimestamp(float(ex_ts)).date()
            else:
                result[ticker] = None
        except Exception:
            result[ticker] = None
        prog.progress((i + 1) / n, text="Fetching ex-dates... " + str(i+1) + "/" + str(n))
        time.sleep(0.08)
    prog.empty()
    return result


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stock_analysis(ticker_sym):
    """Fetch full yfinance data for the stock analyzer tab."""
    try:
        t    = yf.Ticker(ticker_sym.upper().strip())
        info = t.info or {}
        hist = t.history(period="1y")
        divs = t.dividends
        return info, hist, divs, None
    except Exception as e:
        return {}, pd.DataFrame(), pd.Series(dtype=float), str(e)


@st.cache_data(ttl=1800, show_spinner=False)
def load_meta():
    if not os.path.exists(META_FILE):
        return None
    try:
        with open(META_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def safe_date(val):
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        return val.date()
    if isinstance(val, datetime.date):
        return val
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
    return {"t1":"#1a6b1a","t2":"#2e7d32","t3":"#827717","t4":"#e65100","t5":"#bf360c"}[tier(y)]

def render_calendar(df, year, month):
    today   = datetime.date.today()
    day_map = {}
    for _, row in df.iterrows():
        bd = safe_date(row.get("buy_date"))
        if bd and bd.year == year and bd.month == month:
            day_map.setdefault(bd.day, []).append(row)

    parts = ['<div class="cal-grid">']
    for d in ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]:
        parts.append('<div class="cal-hdr">' + d + '</div>')

    for week in cal_module.monthcalendar(year, month):
        for day in week:
            if day == 0:
                parts.append('<div class="cal-day empty"></div>')
                continue
            is_td  = (year == today.year and month == today.month and day == today.day)
            cls    = "cal-day today" if is_td else "cal-day"
            parts.append('<div class="' + cls + '"><div class="cal-num">' + str(day) + '</div>')
            for row in day_map.get(day, []):
                t   = tier(row["yield_pct"])
                dr  = row.get("div_rate") or 0
                px  = row.get("price") or 0
                ex  = safe_date(row.get("ex_date"))
                mp  = row.get("monthly_pay")
                tip = ("BUY " + row["ticker"] + " before " + str(ex) +
                       " | Yield: " + str(row["yield_pct"]) + "%" +
                       " | Monthly: $" + ("{:.4f}".format(mp) if mp else "n/a") +
                       " | Price: $" + "{:.2f}".format(px))
                parts.append(
                    '<span class="chip ' + t + '" title="' + tip + '">' +
                    row["ticker"] + " " + str(row["yield_pct"]) + "%" + '</span>'
                )
            parts.append('</div>')
    parts.append('</div>')
    st.markdown("".join(parts), unsafe_allow_html=True)


def metric_bar(label, value, max_val=1.0, color="#2e7d32", suffix=""):
    pct = min(100, max(0, (value / max_val) * 100)) if max_val else 0
    val_str = ("{:.2f}".format(value) + suffix) if value is not None else "--"
    return (
        '<div class="metric-bar-wrap">'
        '<div class="metric-bar-label"><span>' + label + '</span><span>' + val_str + '</span></div>'
        '<div class="metric-bar-bg"><div class="metric-bar-fill" style="width:' +
        "{:.0f}".format(pct) + '%;background:' + color + '"></div></div></div>'
    )


def tag(val, good_thresh, ok_thresh, fmt="{:.2f}", suffix=""):
    if val is None:
        return "<span style='color:#ccc'>n/a</span>"
    s = fmt.format(val) + suffix
    if val >= good_thresh:
        return '<span class="tag-good">' + s + '</span>'
    elif val >= ok_thresh:
        return '<span class="tag-ok">' + s + '</span>'
    else:
        return '<span class="tag-bad">' + s + '</span>'


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 💰 Dividend Calendar")
    st.markdown("`magicpro33/stock`")
    st.markdown("---")
    min_yield   = st.slider("Min yield (%)",  0.0, 25.0, 0.0,  0.5)
    max_yield   = st.slider("Max yield (%)",  5.0, 50.0, 25.0, 1.0)
    days_ahead  = st.slider("Days ahead",     30,  180,  90)
    freq_filter = st.selectbox("Frequency", ["All","Monthly","Quarterly","Semi-Annual","Annual"])
    max_price   = st.number_input(
        "Max stock price ($)",
        min_value=1,
        max_value=100000,
        value=1000,
        step=1,
        help="Type any dollar amount - only shows stocks at or below this price",
    )
    st.markdown("---")
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown(
        "**How it works:**\n"
        "- Reads `data/stock_data.json.gz` (nightly)\n"
        "- Calendar = day before ex-date\n"
        "- Own shares before that day = dividend paid\n"
        "- Use Calculator tab to model returns\n"
        "- Use Analyzer tab for full stock deep-dive"
    )


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">Dividend Capture Calendar</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="main-sub">buy 24h before ex-date  |  highest yield first  |  magicpro33/stock</div>',
    unsafe_allow_html=True,
)

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading scan data..."):
    scan_result = load_scan_data()
    meta        = load_meta()

if scan_result[0] is None:
    st.markdown('<div class="src-badge src-err">&#x2717; ' + scan_result[1] + '</div>', unsafe_allow_html=True)
    st.info("Run `python nightly_scan.py` and push data/ to GitHub. GitHub Actions regenerates it nightly.")
    st.stop()

df_all, _err, has_ex_dates = scan_result

# Sector filter
with st.sidebar:
    sectors       = ["All sectors"] + sorted(df_all["sector"].dropna().unique().tolist())
    sector_filter = st.selectbox("Sector", sectors)

# Apply filters
df = df_all[
    (df_all["yield_pct"] >= min_yield) &
    (df_all["yield_pct"] <= max_yield) &
    (df_all["price"].fillna(0) <= max_price)
].copy()
if freq_filter != "All":
    df = df[df["frequency"].str.contains(freq_filter, case=False, na=False)]
if sector_filter != "All sectors":
    df = df[df["sector"] == sector_filter]

# Ex-dates: from scan or live fallback
today = datetime.date.today()
if not has_ex_dates:
    st.info("Scan data has no ex-dates yet. Fetching live from Yahoo Finance for " + str(len(df)) + " stocks...")
    with st.spinner("Fetching live ex-dates..."):
        live_map = fetch_ex_dates_live(tuple(df["ticker"].tolist()))
    df = df.copy()
    df["ex_date"] = df["ticker"].map(live_map)
    source_label = "live Yahoo Finance"
    badge_cls    = "src-warn"
else:
    source_label = "scan data"
    badge_cls    = "src-ok"

df["buy_date"] = df["ex_date"].apply(
    lambda d: (safe_date(d) - datetime.timedelta(days=1)) if safe_date(d) else None
)

# Upcoming window
cutoff = today + datetime.timedelta(days=days_ahead)
def in_window(bd):
    d = safe_date(bd)
    return d is not None and today <= d <= cutoff

df_cal = df[df["buy_date"].apply(in_window)].copy().sort_values("yield_pct", ascending=False)

# Badge
meta_txt   = ("  |  Last scan: " + str(meta.get("scanned_at_utc","--"))) if meta else ""
ex_found   = df["ex_date"].apply(safe_date).notna().sum()
st.markdown(
    '<div class="src-badge ' + badge_cls + '">&#x2713; ' +
    str(len(df_all)) + " dividend stocks  |  " +
    str(ex_found) + " with ex-dates (" + source_label + ")" +
    meta_txt + '</div>',
    unsafe_allow_html=True,
)

# ── TABS ──────────────────────────────────────────────────────────────────────
tab_cal, tab_calc, tab_analyze = st.tabs(["📅 Calendar", "💵 Calculator", "🔍 Stock Analyzer"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CALENDAR
# ═══════════════════════════════════════════════════════════════════════════════
with tab_cal:
    # Metrics
    nxt = None
    if not df_cal.empty:
        nxt = df_cal.sort_values("buy_date").iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Buy signals ahead", len(df_cal))
    c2.metric("Avg yield", "{:.1f}%".format(df_cal["yield_pct"].mean()) if not df_cal.empty else "--")
    c3.metric("Highest yield",
              "{:.1f}%".format(df_cal["yield_pct"].max()) if not df_cal.empty else "--",
              delta=str(df_cal.iloc[0]["ticker"]) if not df_cal.empty else "")
    nxt_date = safe_date(nxt["buy_date"]) if nxt is not None else None
    c4.metric("Next buy date",
              nxt_date.strftime("%b %d") if nxt_date else "--",
              delta=str(nxt["ticker"]) if nxt is not None else "")

    st.markdown("---")

    # Calendar nav
    if "cy" not in st.session_state: st.session_state.cy = today.year
    if "cm" not in st.session_state: st.session_state.cm = today.month

    cp, cc, cn = st.columns([1, 5, 1])
    with cp:
        if st.button("← Prev"):
            if st.session_state.cm == 1: st.session_state.cy -= 1; st.session_state.cm = 12
            else: st.session_state.cm -= 1
    with cn:
        if st.button("Next →"):
            if st.session_state.cm == 12: st.session_state.cy += 1; st.session_state.cm = 1
            else: st.session_state.cm += 1
    with cc:
        st.markdown(
            "<h3 style='text-align:center;font-family:DM Serif Display,serif;margin:0'>" +
            cal_module.month_name[st.session_state.cm] + " " + str(st.session_state.cy) +
            "</h3>", unsafe_allow_html=True,
        )

    st.markdown(
        "<div style='display:flex;gap:8px;margin:6px 0 4px;flex-wrap:wrap'>"
        "<span style='background:#1a6b1a;color:#fff;padding:2px 10px;border-radius:3px;font-size:0.7rem;font-weight:600'>8%+</span>"
        "<span style='background:#2e7d32;color:#fff;padding:2px 10px;border-radius:3px;font-size:0.7rem;font-weight:600'>6%+</span>"
        "<span style='background:#827717;color:#fff;padding:2px 10px;border-radius:3px;font-size:0.7rem;font-weight:600'>4%+</span>"
        "<span style='background:#e65100;color:#fff;padding:2px 10px;border-radius:3px;font-size:0.7rem;font-weight:600'>2.5%+</span>"
        "<span style='background:#bf360c;color:#fff;padding:2px 10px;border-radius:3px;font-size:0.7rem;font-weight:600'>below 2.5%</span>"
        "</div>", unsafe_allow_html=True,
    )

    render_calendar(df_cal, st.session_state.cy, st.session_state.cm)
    st.markdown("<br>", unsafe_allow_html=True)

    # ── Ranked table with sticky header ──────────────────────────────────────
    st.markdown('<div class="section-hdr">Upcoming Buy Signals &mdash; Ranked by Yield</div>', unsafe_allow_html=True)

    if df_cal.empty:
        st.info("No buy signals in the next " + str(days_ahead) + " days. Try widening the date range.")
    else:
        df_show = df_cal.sort_values(["buy_date","yield_pct"], ascending=[True,False]).copy()
        df_show["days_away"] = df_show["buy_date"].apply(
            lambda d: (safe_date(d) - today).days if safe_date(d) else 0
        )

        rows_html = []
        for _, row in df_show.iterrows():
            yc   = ycolor(row["yield_pct"])
            bd   = safe_date(row.get("buy_date"))
            ex   = safe_date(row.get("ex_date"))
            da   = int(row["days_away"])
            dr   = float(row.get("div_rate") or 0)
            mp   = row.get("monthly_pay")
            px   = float(row.get("price") or 0)
            pr   = row.get("payout")
            freq = str(row.get("frequency") or "--")
            mp_str = ("$" + "{:.4f}".format(mp)) if mp else "--"
            pr_str = "{:.0f}%".format(pr) if pr else "--"
            bd_str = bd.strftime("%b %d, %Y") if bd else "--"
            ex_str = ex.strftime("%b %d, %Y") if ex else "--"

            alert = ""
            if da == 0:   alert = '<span class="buy-now">BUY TODAY</span>'
            elif da == 1: alert = '<span class="buy-tmr">BUY TOMORROW</span>'

            rows_html.append(
                "<tr class='tbl-row'>"
                "<td class='td-ticker'><strong>" + row["ticker"] + "</strong></td>"
                "<td class='td-sector'>" + str(row["sector"]) + "</td>"
                "<td class='td-yield'><span class='yield-badge " + tier(row["yield_pct"]) + "-badge' "
                "style='--yc:" + yc + "'>" + str(row["yield_pct"]) + "%</span></td>"
                "<td class='mono td-num'>$" + "{:.4f}".format(dr) + "</td>"
                "<td class='mono td-num'>" + mp_str + "</td>"
                "<td class='mono td-num'>" + pr_str + "</td>"
                "<td class='td-freq'>" + freq + "</td>"
                "<td class='mono td-num'>$" + "{:.2f}".format(px) + "</td>"
                "<td class='mono td-date'>" + bd_str + "</td>"
                "<td class='mono td-date'>" + ex_str + "</td>"
                "<td class='td-count'>" + str(da) + "d " + alert + "</td>"
                "</tr>"
            )

        table_html = (
            "<div class='tbl-wrap'>"
            "<table class='stbl'>"
            "<thead><tr>"
            "<th>Ticker</th><th>Sector</th><th>Yield</th><th>Div/Share</th>"
            "<th>Monthly/Share</th><th>Payout</th><th>Frequency</th>"
            "<th>Price</th><th>Buy Before</th><th>Ex-Date</th><th>Countdown</th>"
            "</tr></thead>"
            "<tbody>" + "".join(rows_html) + "</tbody>"
            "</table></div>"
        )
        st.markdown(table_html, unsafe_allow_html=True)

    # Full universe expander
    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("Full dividend universe -- " + str(len(df)) + " stocks (" + str(len(df_all)) + " total)"):
        st.dataframe(
            df[["ticker","sector","yield_pct","div_rate","monthly_pay",
                "payout","frequency","price","ex_date","buy_date"]]
            .rename(columns={
                "ticker":"Ticker","sector":"Sector","yield_pct":"Yield %",
                "div_rate":"Div/Share","monthly_pay":"Monthly/Share",
                "payout":"Payout %","frequency":"Frequency","price":"Price",
                "ex_date":"Ex-Date","buy_date":"Buy Before",
            }),
            use_container_width=True, hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — INVESTMENT CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════
with tab_calc:
    st.markdown('<div class="section-hdr">Dividend Investment Calculator</div>', unsafe_allow_html=True)

    # Pick a stock from the scanned list OR enter any ticker
    calc_mode = st.radio(
        "Stock source",
        ["Pick from dividend list", "Enter any ticker"],
        horizontal=True,
    )

    calc_info = {}
    if calc_mode == "Pick from dividend list":
        if df.empty:
            st.warning("No dividend stocks match current filters.")
            st.stop()
        # Build label list: "ET — 8.10% yield"
        options = [
            row["ticker"] + " -- " + str(row["yield_pct"]) + "% yield  |  " + str(row["frequency"])
            for _, row in df.sort_values("yield_pct", ascending=False).iterrows()
        ]
        selected = st.selectbox("Select stock", options)
        sel_ticker = selected.split(" -- ")[0].strip()
        sel_row    = df[df["ticker"] == sel_ticker].iloc[0]
        calc_info  = {
            "ticker":      sel_ticker,
            "price":       float(sel_row["price"] or 0),
            "yield_pct":   float(sel_row["yield_pct"]),
            "div_rate":    float(sel_row["div_rate"] or 0),
            "monthly_pay": sel_row.get("monthly_pay"),
            "frequency":   sel_row["frequency"],
            "payout":      sel_row.get("payout"),
            "sector":      sel_row["sector"],
        }
    else:
        custom_ticker = st.text_input("Enter ticker symbol", placeholder="e.g. ET, EPD, DOC")
        if custom_ticker:
            with st.spinner("Fetching live data for " + custom_ticker.upper() + "..."):
                live_info, _, _, live_err = fetch_stock_analysis(custom_ticker)
            if live_err or not live_info:
                st.error("Could not fetch data for " + custom_ticker.upper() + ". Check the ticker and try again.")
            else:
                raw_yield = live_info.get("trailingAnnualDividendYield") or live_info.get("dividendYield") or 0
                raw_rate  = live_info.get("trailingAnnualDividendRate") or live_info.get("dividendRate") or 0
                if raw_yield > 0.50: raw_yield = 0
                calc_info = {
                    "ticker":      custom_ticker.upper().strip(),
                    "price":       float(live_info.get("currentPrice") or live_info.get("regularMarketPrice") or 0),
                    "yield_pct":   round(raw_yield * 100, 2),
                    "div_rate":    float(raw_rate),
                    "monthly_pay": round(raw_rate / 3, 4) if raw_rate else None,
                    "frequency":   "Quarterly (est)",
                    "payout":      round((live_info.get("payoutRatio") or 0) * 100, 1) or None,
                    "sector":      live_info.get("sector") or "Unknown",
                }

    if calc_info and calc_info.get("price", 0) > 0:
        st.markdown("---")
        col_inp, col_res = st.columns([1, 1])

        with col_inp:
            st.markdown('<div class="calc-card">', unsafe_allow_html=True)
            st.markdown("**" + calc_info["ticker"] + "** - " + calc_info["sector"])
            st.markdown(
                "Annual yield: **" + str(calc_info["yield_pct"]) + "%**  |  " +
                "Price: **$" + "{:.2f}".format(calc_info["price"]) + "**  |  " +
                "Frequency: **" + calc_info["frequency"] + "**"
            )
            st.markdown("---")
            invest_amt = st.number_input(
                "Investment amount ($)", min_value=1.0, max_value=10_000_000.0,
                value=1000.0, step=100.0, format="%.2f"
            )
            st.markdown('</div>', unsafe_allow_html=True)

        with col_res:
            price      = calc_info["price"]
            yield_pct  = calc_info["yield_pct"]
            div_rate   = calc_info["div_rate"] or 0
            shares     = invest_amt / price if price > 0 else 0
            annual_div = shares * div_rate
            monthly_d  = annual_div / 12
            weekly_d   = annual_div / 52

            # Frequency-based single payment
            freq_lower = calc_info["frequency"].lower()
            if "monthly" in freq_lower:
                single_pay   = annual_div / 12
                single_label = "Per monthly payment"
            elif "semi" in freq_lower:
                single_pay   = annual_div / 2
                single_label = "Per semi-annual payment"
            elif "annual" in freq_lower and "semi" not in freq_lower:
                single_pay   = annual_div
                single_label = "Per annual payment"
            else:
                single_pay   = annual_div / 4
                single_label = "Per quarterly payment"

            st.markdown(
                "<div class='calc-result'>"
                "<div class='calc-result-row'><span class='calc-label'>Shares purchased</span>"
                "<span class='calc-value'>" + "{:.4f}".format(shares) + " shares</span></div>"

                "<div class='calc-result-row'><span class='calc-label'>" + single_label + "</span>"
                "<span class='calc-value'>$" + "{:.2f}".format(single_pay) + "</span></div>"

                "<div class='calc-result-row'><span class='calc-label'>Monthly dividend income</span>"
                "<span class='calc-value'>$" + "{:.2f}".format(monthly_d) + "</span></div>"

                "<div class='calc-result-row'><span class='calc-label'>Annual dividend income</span>"
                "<span class='calc-value big'>$" + "{:.2f}".format(annual_div) + "</span></div>"

                "<div class='calc-result-row'><span class='calc-label'>Yield on investment</span>"
                "<span class='calc-value'>" + str(yield_pct) + "%</span></div>"

                "<div class='calc-result-row'><span class='calc-label'>Weekly dividend income</span>"
                "<span class='calc-value'>$" + "{:.2f}".format(weekly_d) + "</span></div>"
                "</div>",
                unsafe_allow_html=True,
            )

        # Holding period breakdown
        st.markdown("---")
        st.markdown("#### Holding Period Projections")
        periods = [1, 3, 6, 12, 24, 36, 60]
        proj_rows = []
        for m in periods:
            total_div = monthly_d * m
            proj_rows.append({
                "Hold Period":     str(m) + " month" + ("s" if m > 1 else ""),
                "Total Dividends": "$" + "{:.2f}".format(total_div),
                "Return on Invest": "{:.2f}%".format((total_div / invest_amt) * 100),
                "Monthly Income":  "$" + "{:.2f}".format(monthly_d),
            })
        st.dataframe(pd.DataFrame(proj_rows), use_container_width=True, hide_index=True)

        # Monthly dividend accumulation chart
        st.markdown("#### Monthly Income Over 12 Months")
        months    = list(range(1, 13))
        cum_divs  = [monthly_d * m for m in months]
        chart_df  = pd.DataFrame({"Month": months, "Cumulative Dividends ($)": cum_divs})
        st.line_chart(chart_df.set_index("Month"), color="#cc0000")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — STOCK ANALYZER
# ═══════════════════════════════════════════════════════════════════════════════
with tab_analyze:
    st.markdown('<div class="section-hdr">Stock Analyzer</div>', unsafe_allow_html=True)
    st.markdown("Enter any ticker to get a full breakdown - metrics match your screener app exactly.")

    az_col1, az_col2 = st.columns([2, 3])
    with az_col1:
        az_ticker = st.text_input("Ticker symbol", placeholder="e.g. ET, WPM, DOC", key="az_ticker")
        az_button = st.button("Analyze", type="primary")

    if az_button and az_ticker:
        with st.spinner("Fetching data for " + az_ticker.upper() + "..."):
            az_info, az_hist, az_divs, az_err = fetch_stock_analysis(az_ticker)

        if az_err or not az_info:
            st.error("Could not fetch " + az_ticker.upper() + ". Check the ticker symbol.")
        else:
            sym   = az_ticker.upper().strip()
            name  = az_info.get("longName") or az_info.get("shortName") or sym
            sec   = az_info.get("sector") or "Unknown"
            ind   = az_info.get("industry") or "Unknown"
            price = float(az_info.get("currentPrice") or az_info.get("regularMarketPrice") or 0)
            mcap  = az_info.get("marketCap") or 0
            pe    = az_info.get("trailingPE")
            raw_y = az_info.get("trailingAnnualDividendYield") or az_info.get("dividendYield") or 0
            if raw_y > 0.50: raw_y = 0
            div_yield = round(raw_y * 100, 2)
            div_rate  = float(az_info.get("trailingAnnualDividendRate") or az_info.get("dividendRate") or 0)
            payout    = az_info.get("payoutRatio")
            ex_ts     = az_info.get("exDividendDate")
            ex_date   = None
            if ex_ts:
                try:
                    ex_date = datetime.datetime.utcfromtimestamp(float(ex_ts)).date()
                except Exception:
                    pass

            # Frequency
            freq_map2 = {"monthly":1,"quarterly":4,"semi-annual":2,"annual":1}
            pays_yr   = 4
            freq_label2 = "Quarterly"
            if not az_divs.empty:
                one_yr   = pd.Timestamp.now(tz="UTC") - pd.DateOffset(years=1)
                recent_d = az_divs[az_divs.index >= one_yr]
                n = len(recent_d)
                if n >= 10:   pays_yr = 12; freq_label2 = "Monthly"
                elif n >= 3:  pays_yr = 4;  freq_label2 = "Quarterly"
                elif n == 2:  pays_yr = 2;  freq_label2 = "Semi-Annual"
                elif n == 1:  pays_yr = 1;  freq_label2 = "Annual"

            monthly_pay2 = (div_rate / 12) if div_rate else 0

            # ── Header card ───────────────────────────────────────────────────
            h1, h2, h3, h4, h5 = st.columns(5)
            h1.metric("Price",     "$" + "{:.2f}".format(price))
            h2.metric("Div Yield", str(div_yield) + "%")
            h3.metric("Monthly/Share", "$" + "{:.4f}".format(monthly_pay2) if monthly_pay2 else "--")
            h4.metric("Ex-Date",   ex_date.strftime("%b %d, %Y") if ex_date else "--")
            h5.metric("Frequency", freq_label2)

            st.markdown("**" + name + "**  -  " + sec + "  /  " + ind)
            mcap_str = ("$" + "{:.1f}B".format(mcap/1e9) if mcap >= 1e9
                        else "$" + "{:.0f}M".format(mcap/1e6) if mcap >= 1e6 else "n/a")
            st.markdown("Market cap: **" + mcap_str + "**   P/E: **" +
                        ("{:.1f}x".format(pe) if pe else "n/a") + "**   Payout: **" +
                        ("{:.0f}%".format(payout*100) if payout else "n/a") + "**")

            st.markdown("---")
            left_col, right_col = st.columns(2)

            # ── Price chart ───────────────────────────────────────────────────
            with left_col:
                st.markdown("**Price History (1Y)**")
                if not az_hist.empty:
                    st.line_chart(az_hist[["Close"]], color="#cc0000", height=200)
                else:
                    st.info("No price history available.")

                st.markdown("**Dividend History**")
                if not az_divs.empty:
                    div_df = az_divs.reset_index()
                    div_df.columns = ["Date", "Dividend"]
                    div_df["Date"] = pd.to_datetime(div_df["Date"]).dt.date
                    st.dataframe(div_df.tail(12), use_container_width=True, hide_index=True)
                else:
                    st.info("No dividend history available.")

            # ── Metrics from screener logic ───────────────────────────────────
            with right_col:
                st.markdown("**Screener Metrics**")

                # Compute technical signals inline from hist
                bars_html = []
                if not az_hist.empty and len(az_hist) >= 26:
                    import numpy as np
                    close = az_hist["Close"].dropna()

                    # RSI
                    try:
                        delta = close.diff()
                        gain  = delta.clip(lower=0).rolling(14).mean()
                        loss  = (-delta.clip(upper=0)).rolling(14).mean()
                        rs    = gain / loss.replace(0, np.nan)
                        rsi_s = (100 - (100/(1+rs))).dropna()
                        rsi_v = float(rsi_s.iloc[-1]) if not rsi_s.empty else 50
                        bars_html.append(metric_bar("RSI", rsi_v/100, 1.0, "#2e7d32", ""))
                    except Exception:
                        pass

                    # MACD
                    try:
                        ema12 = close.ewm(span=12, adjust=False).mean()
                        ema26 = close.ewm(span=26, adjust=False).mean()
                        ml    = ema12 - ema26
                        sig   = ml.ewm(span=9, adjust=False).mean()
                        hist_m = (ml - sig).dropna()
                        macd_v = float(hist_m.iloc[-1]) if not hist_m.empty else 0
                        norm   = abs(float(hist_m.abs().max())) or 1
                        bars_html.append(metric_bar("MACD Histogram", (macd_v/norm+1)/2, 1.0, "#1565c0"))
                    except Exception:
                        pass

                    # MA50 vs MA200
                    try:
                        if len(close) >= 200:
                            ma50  = float(close.rolling(50).mean().iloc[-1])
                            ma200 = float(close.rolling(200).mean().iloc[-1])
                            gc    = 1.0 if ma50 > ma200 * 1.02 else (0.5 if ma50 > ma200 * 0.98 else 0.0)
                            bars_html.append(metric_bar("Golden Cross", gc, 1.0, "#827717"))
                            ma50_prox = (close.iloc[-1] - ma50) / ma50
                            prox_score = 1.0 if 0<=ma50_prox<=0.05 else (0.7 if ma50_prox<=0.10 else (0.3 if ma50_prox<=0.20 else 0.0))
                            bars_html.append(metric_bar("MA50 Proximity", prox_score, 1.0, "#388e3c"))
                    except Exception:
                        pass

                if bars_html:
                    st.markdown("".join(bars_html), unsafe_allow_html=True)

                # Fundamental metrics as tagged values
                st.markdown("**Fundamental Signals**")
                fund_rows = [
                    ("Dividend Yield", tag(div_yield, 6, 3, "{:.2f}", "%")),
                    ("Annual Div/Share", ("$" + "{:.4f}".format(div_rate)) if div_rate else "n/a"),
                    ("Monthly/Share",   ("$" + "{:.4f}".format(monthly_pay2)) if monthly_pay2 else "n/a"),
                    ("Payout Ratio",    tag(payout*100 if payout else None, 80, 100, "{:.0f}", "% (inv)") if payout else "n/a"),
                    ("Revenue Growth",  tag(az_info.get("revenueGrowth") or 0, 0.10, 0.03, "{:.1%}")),
                    ("Earnings Growth", tag(az_info.get("earningsGrowth") or 0, 0.10, 0.03, "{:.1%}")),
                    ("Short % Float",   tag(az_info.get("shortPercentOfFloat") or 0, 0.20, 0.10, "{:.1%}")),
                    ("Days to Cover",   tag(az_info.get("shortRatio") or 0, 5, 3, "{:.1f}", "d")),
                    ("52W High",        "$" + "{:.2f}".format(az_info.get("fiftyTwoWeekHigh") or 0)),
                    ("52W Low",         "$" + "{:.2f}".format(az_info.get("fiftyTwoWeekLow") or 0)),
                    ("Beta",            "{:.2f}".format(az_info.get("beta") or 0) if az_info.get("beta") else "n/a"),
                    ("P/E Ratio",       "{:.1f}x".format(pe) if pe else "n/a"),
                ]
                tbl_rows = "".join(
                    "<tr><td style='color:#888;font-size:0.78rem;padding:5px 8px'>" + lbl + "</td>"
                    "<td style='font-size:0.82rem;padding:5px 8px'>" + val + "</td></tr>"
                    for lbl, val in fund_rows
                )
                st.markdown(
                    "<table style='width:100%;border-collapse:collapse'>"
                    "<tbody>" + tbl_rows + "</tbody></table>",
                    unsafe_allow_html=True,
                )

                # Inline investment calculator
                st.markdown("---")
                st.markdown("**Quick Calculator**")
                az_invest = st.number_input(
                    "Investment ($)", min_value=1.0, value=1000.0, step=100.0,
                    format="%.2f", key="az_invest"
                )
                if price > 0 and div_rate > 0:
                    az_shares  = az_invest / price
                    az_annual  = az_shares * div_rate
                    az_monthly = az_annual / 12
                    st.markdown(
                        "<div class='calc-result'>"
                        "<div class='calc-result-row'><span class='calc-label'>Shares</span>"
                        "<span class='calc-value'>" + "{:.3f}".format(az_shares) + "</span></div>"
                        "<div class='calc-result-row'><span class='calc-label'>Monthly income</span>"
                        "<span class='calc-value'>$" + "{:.2f}".format(az_monthly) + "</span></div>"
                        "<div class='calc-result-row'><span class='calc-label'>Annual income</span>"
                        "<span class='calc-value big'>$" + "{:.2f}".format(az_annual) + "</span></div>"
                        "</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.info("No dividend data available for this ticker.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(
    "<hr><p style='font-size:0.7rem;color:#ccc;text-align:center'>"
    "github.com/magicpro33/stock  |  data updated nightly via GitHub Actions  |  "
    "Not financial advice  |  Always verify ex-dates before trading"
    "</p>",
    unsafe_allow_html=True,
)
