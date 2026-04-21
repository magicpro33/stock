"""
dividend_calendar.py — Dividend Capture Calendar
=================================================
Reads pre-computed data from data/stock_data.json.gz
(written nightly by nightly_scan.py via GitHub Actions).

Now reads ExDividendDate directly from scan data — zero live API calls,
loads instantly.

Deploy to Streamlit Cloud:
  1. Push both files to root of magicpro33/stock repo
  2. Go to share.streamlit.io → New app
  3. Repo: magicpro33/stock  Branch: main  File: dividend_calendar.py
  4. Click Deploy — done
"""

import streamlit as st
import pandas as pd
import gzip, json, os, datetime, calendar

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
html,body,[class*="css"]{font-family:'DM Sans',sans-serif}
.main-title{font-family:'DM Serif Display',serif;font-size:2.5rem;color:#cc0000;letter-spacing:-0.02em;line-height:1.1;margin-bottom:0}
.main-sub{font-size:.8rem;color:#aaa;margin-top:4px;letter-spacing:.06em;text-transform:uppercase}
.src-badge{display:inline-flex;align-items:center;gap:6px;border-radius:6px;padding:6px 14px;font-size:.78rem;font-weight:500;margin:10px 0 20px}
.src-ok  {background:#f0f7f0;border:1px solid #b8ddb8;color:#1a6b1a}
.src-warn{background:#fff8e6;border:1px solid #f0d080;color:#7a5a00}
.src-err {background:#fff0f0;border:1px solid #f0b0b0;color:#8b0000}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:4px;margin-top:10px}
.cal-hdr{text-align:center;font-size:.65rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:#ccc;padding:5px 0}
.cal-day{background:#f9f9f7;border:1px solid #efefed;border-radius:7px;min-height:88px;padding:8px 7px}
.cal-day.today{border:2px solid #cc0000}
.cal-day.empty{background:transparent;border:none}
.cal-num{font-size:.7rem;font-weight:500;color:#ccc;margin-bottom:5px}
.cal-day.today .cal-num{color:#cc0000;font-weight:700}
.chip{display:block;border-radius:3px;padding:2px 5px;margin-bottom:3px;font-size:.6rem;font-weight:600;font-family:'DM Mono',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;border-left:2px solid;cursor:default}
.t1{background:rgba(10,61,10,.12);color:#1a6b1a;border-color:#2e7d32}
.t2{background:rgba(30,90,30,.10);color:#2e7d32;border-color:#388e3c}
.t3{background:rgba(100,100,10,.10);color:#827717;border-color:#f9a825}
.t4{background:rgba(180,90,0,.09);color:#e65100;border-color:#ff9800}
.t5{background:rgba(150,40,0,.08);color:#bf360c;border-color:#ff5722}
.stbl{width:100%;border-collapse:collapse}
.stbl th{font-size:.66rem;letter-spacing:.1em;text-transform:uppercase;color:#bbb;border-bottom:1px solid #efefed;padding:8px 10px;text-align:left}
.stbl td{padding:10px;border-bottom:1px solid #f5f5f3;font-size:.82rem}
.stbl tr:last-child td{border-bottom:none}
.mono{font-family:'DM Mono',monospace;font-size:.78rem}
.buy-now{background:#cc0000;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}
.buy-tmr{background:#2e7d32;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}
</style>
""", unsafe_allow_html=True)


# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "stock_data.json.gz")
META_FILE = os.path.join(BASE_DIR, "data", "scan_meta.json")


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def load_dividend_data():
    """
    Load stock_data.json.gz and return only valid dividend-paying stocks.

    Key fixes vs previous version:
    - Reads ExDividendDate directly from scan (stored as Unix timestamp
      by the fixed nightly_scan.py) — no live yfinance calls needed
    - DividendYieldPct is already a percentage (e.g. 4.5 = 4.5%)
      nightly_scan multiplies by 100 before storing
    - Filters yields > 50% (foreign ADR data errors now blocked in
      nightly_scan too, but double-filter here for safety)
    - PayoutRatio already stored as percentage in scan (e.g. 45.2 = 45.2%)
    """
    if not os.path.exists(DATA_FILE):
        return None, (
            "data/stock_data.json.gz not found.\n\n"
            "Run `python nightly_scan.py` once to generate it, "
            "then push data/ to GitHub. GitHub Actions regenerates it nightly."
        )

    try:
        with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        return None, f"Could not read data file: {e}"

    rows = []
    for item in raw:
        if not isinstance(item, dict):
            continue

        ticker = (item.get("Ticker") or "").strip()
        if not ticker:
            continue

        # ── Yield ─────────────────────────────────────────────────────────
        # Stored as percentage already (e.g. 4.5 means 4.5%)
        yield_pct = item.get("DividendYieldPct")
        if yield_pct is None:
            continue
        try:
            yield_pct = float(yield_pct)
        except (TypeError, ValueError):
            continue
        if yield_pct <= 0 or yield_pct > 50:   # 0–50% is the valid range
            continue

        # ── Div rate ───────────────────────────────────────────────────────
        div_rate = item.get("DividendRate")
        try:
            div_rate = float(div_rate) if div_rate is not None else None
        except (TypeError, ValueError):
            div_rate = None

        # ── Payout ratio ───────────────────────────────────────────────────
        # Stored as percentage (e.g. 45.2 = 45.2%)
        payout = item.get("DividendPayoutRatio")
        try:
            payout = float(payout) if payout is not None else None
            if payout is not None and (payout < 0 or payout > 500):
                payout = None
        except (TypeError, ValueError):
            payout = None

        # ── ExDividendDate ─────────────────────────────────────────────────
        # Stored as Unix timestamp integer by nightly_scan.py
        ex_date  = None
        ex_ts    = item.get("ExDividendDate")
        if ex_ts:
            try:
                ts_val = float(ex_ts)
                if ts_val > 1_000_000_000:   # sanity: must be after year 2001
                    ex_date = datetime.datetime.utcfromtimestamp(ts_val).date()
            except (TypeError, ValueError):
                pass

        buy_date = (ex_date - datetime.timedelta(days=1)) if ex_date else None

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
            "buy_date":  buy_date,
        })

    if not rows:
        return None, "No valid dividend stocks found. Check that nightly_scan.py ran successfully."

    df = pd.DataFrame(rows)
    df = df.sort_values("yield_pct", ascending=False).reset_index(drop=True)
    return df, None


@st.cache_data(ttl=1800, show_spinner=False)
def load_meta():
    if not os.path.exists(META_FILE):
        return None
    try:
        with open(META_FILE) as f:
            return json.load(f)
    except Exception:
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────
def tier(y):
    if y >= 8:   return "t1"
    if y >= 6:   return "t2"
    if y >= 4:   return "t3"
    if y >= 2.5: return "t4"
    return "t5"

def ycolor(y):
    return {
        "t1": "#1a6b1a", "t2": "#2e7d32",
        "t3": "#827717", "t4": "#e65100", "t5": "#bf360c"
    }[tier(y)]

def render_calendar(df, year, month):
    today   = datetime.date.today()
    day_map = {}
    for _, r in df.iterrows():
        bd = r.get("buy_date")
        if not isinstance(bd, datetime.date):
            continue
        if bd.year == year and bd.month == month:
            day_map.setdefault(bd.day, []).append(r)

    html = '<div class="cal-grid">'
    for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        html += f'<div class="cal-hdr">{d}</div>'

    for week in calendar.monthcalendar(year, month):
        for day in week:
            if day == 0:
                html += '<div class="cal-day empty"></div>'
                continue
            is_td = (year == today.year and month == today.month and day == today.day)
            cls   = "cal-day today" if is_td else "cal-day"
            html += f'<div class="{cls}"><div class="cal-num">{day}</div>'
            for r in day_map.get(day, []):
                t    = tier(r["yield_pct"])
                dr   = r.get("div_rate") or 0
                px   = r.get("price")    or 0
                ex   = r.get("ex_date")
                freq = r.get("frequency", "—")
                html += (
                    f'<span class="chip {t}" '
                    f'title="BUY {r["ticker"]} before {ex} | '
                    f'Yield: {r["yield_pct"]}% | '
                    f'${dr:.4f}/share | Freq: {freq} | Price: ${px:.2f}">'
                    f'{r["ticker"]} {r["yield_pct"]}%</span>'
                )
            html += '</div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 💰 Dividend Calendar")
    st.markdown("`magicpro33/stock`")
    st.markdown("---")
    min_yield  = st.slider("Min yield (%)",  0.0, 25.0, 0.0, 0.5)
    max_yield  = st.slider("Max yield (%)",  5.0, 50.0, 25.0, 1.0)
    days_ahead = st.slider("Days ahead",     30,  180,  90)
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
        "- Reads `data/stock_data.json.gz`\n"
        "  (updated nightly by GitHub Actions)\n"
        "- Ex-dates stored directly in scan data —\n"
        "  no live API calls, loads instantly\n"
        "- Calendar = day **before** ex-date\n"
        "- Own shares by that day → dividend paid\n"
        "- Chips sorted highest yield first\n"
        "- Hover chip for full details"
    )


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">Dividend Capture Calendar</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="main-sub">'
    'data/stock_data.json.gz · buy 24h before ex-date · highest yield first'
    '</div>',
    unsafe_allow_html=True,
)


# ── Load ──────────────────────────────────────────────────────────────────────
with st.spinner("Loading scan data…"):
    df_all, err = load_dividend_data()
    meta        = load_meta()

if err:
    st.markdown(f'<div class="src-badge src-err">✗ {err}</div>', unsafe_allow_html=True)
    st.info(
        "**Setup:** Run `python nightly_scan.py` once locally, then "
        "`git add data/ && git commit -m 'scan data' && git push`. "
        "GitHub Actions will regenerate it every night after that."
    )
    st.stop()

# ── Sidebar sector filter (needs data) ───────────────────────────────────────
with st.sidebar:
    sectors       = ["All sectors"] + sorted(df_all["sector"].dropna().unique().tolist())
    sector_filter = st.selectbox("Sector", sectors)


# ── Apply filters ─────────────────────────────────────────────────────────────
today  = datetime.date.today()
cutoff = today + datetime.timedelta(days=days_ahead)

df = df_all[
    (df_all["yield_pct"] >= min_yield) &
    (df_all["yield_pct"] <= max_yield)
].copy()

if freq_filter != "All":
    df = df[df["frequency"].str.contains(freq_filter, case=False, na=False)]

if sector_filter != "All sectors":
    df = df[df["sector"] == sector_filter]

# Calendar subset — only stocks with upcoming buy dates in window
df_cal = df[
    df["buy_date"].apply(
        lambda d: isinstance(d, datetime.date) and today <= d <= cutoff
    )
].copy()


# ── Badge ─────────────────────────────────────────────────────────────────────
has_dates  = df_all["ex_date"].notna().sum()
meta_txt   = ""
if meta:
    meta_txt = f" · Last scan: {meta.get('scanned_at_utc','—')} · {meta.get('valid_results','?')} stocks scanned"

badge_cls  = "src-ok"   if has_dates > 0 else "src-warn"
badge_icon = "✓"        if has_dates > 0 else "⚠"
badge_note = (f"{has_dates} with ex-dates in scan data" if has_dates > 0
              else "no ex-dates found — push updated nightly_scan.py and re-run")

st.markdown(
    f'<div class="src-badge {badge_cls}">'
    f'{badge_icon} {len(df_all)} dividend stocks loaded · {badge_note}{meta_txt}'
    f'</div>',
    unsafe_allow_html=True,
)

if has_dates == 0:
    st.warning(
        "**No ex-dividend dates in scan data.** "
        "The updated `nightly_scan.py` now stores `ExDividendDate` for every stock. "
        "Push the fixed file and trigger a new nightly scan (or run manually). "
        "The calendar will populate on the next scan."
    )


# ── Metrics ───────────────────────────────────────────────────────────────────
nxt = df_cal.sort_values("buy_date").iloc[0] if not df_cal.empty else None
c1, c2, c3, c4 = st.columns(4)
c1.metric("Buy signals ahead", len(df_cal))
c2.metric("Avg yield",
          f"{df_cal['yield_pct'].mean():.1f}%" if not df_cal.empty else "—")
c3.metric("Highest yield",
          f"{df_cal['yield_pct'].max():.1f}%" if not df_cal.empty else "—",
          delta=df_cal.iloc[0]["ticker"] if not df_cal.empty else "")
c4.metric("Next buy date",
          nxt["buy_date"].strftime("%b %d") if nxt is not None else "—",
          delta=nxt["ticker"]              if nxt is not None else "")

st.markdown("---")


# ── Calendar nav ──────────────────────────────────────────────────────────────
if "cy" not in st.session_state: st.session_state.cy = today.year
if "cm" not in st.session_state: st.session_state.cm = today.month

cp, cc, cn = st.columns([1, 5, 1])
with cp:
    if st.button("← Prev"):
        if st.session_state.cm == 1:
            st.session_state.cy -= 1; st.session_state.cm = 12
        else:
            st.session_state.cm -= 1
with cn:
    if st.button("Next →"):
        if st.session_state.cm == 12:
            st.session_state.cy += 1; st.session_state.cm = 1
        else:
            st.session_state.cm += 1
with cc:
    st.markdown(
        f"<h3 style='text-align:center;font-family:DM Serif Display,serif;margin:0'>"
        f"{calendar.month_name[st.session_state.cm]} {st.session_state.cy}</h3>",
        unsafe_allow_html=True,
    )

st.markdown(
    "<div style='display:flex;gap:8px;margin:6px 0 4px;flex-wrap:wrap'>"
    "<span style='background:#1a6b1a;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 8%</span>"
    "<span style='background:#2e7d32;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 6%</span>"
    "<span style='background:#827717;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 4%</span>"
    "<span style='background:#e65100;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 2.5%</span>"
    "<span style='background:#bf360c;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>&lt; 2.5%</span>"
    "</div>",
    unsafe_allow_html=True,
)

render_calendar(df_cal, st.session_state.cy, st.session_state.cm)
st.markdown("<br>", unsafe_allow_html=True)


# ── Upcoming table ────────────────────────────────────────────────────────────
st.markdown("### 📋 Upcoming Buy Signals — Ranked by Yield")

if df_cal.empty:
    st.info(
        f"No dividend buy signals found in the next {days_ahead} days. "
        "Try widening the date range, lowering the min yield, or raising the max yield filter."
    )
else:
    df_show = df_cal.sort_values(
        ["buy_date", "yield_pct"], ascending=[True, False]
    ).copy()
    df_show["days_away"] = df_show["buy_date"].apply(lambda d: (d - today).days)

    rows_html = ""
    for _, r in df_show.iterrows():
        yc  = ycolor(r["yield_pct"])
        da  = int(r["days_away"])
        dr  = r.get("div_rate") or 0
        px  = r.get("price")    or 0
        pr  = r.get("payout")
        ex  = r.get("ex_date")
        bd  = r.get("buy_date")
        freq = r.get("frequency", "—")
        alert = ""
        if   da == 0: alert = '<span class="buy-now">BUY TODAY</span>'
        elif da == 1: alert = '<span class="buy-tmr">BUY TOMORROW</span>'

        rows_html += (
            f'<tr>'
            f'<td><strong style="font-family:\'DM Mono\',monospace">{r["ticker"]}</strong></td>'
            f'<td style="color:#aaa;font-size:.75rem">{r["sector"]}</td>'
            f'<td><span style="background:{yc}18;color:{yc};padding:2px 9px;'
            f'border-radius:100px;font-family:\'DM Mono\',monospace;'
            f'font-size:.78rem;font-weight:600">{r["yield_pct"]}%</span></td>'
            f'<td class="mono">${dr:.4f}</td>'
            f'<td class="mono">{f"{pr:.0f}%" if pr else "—"}</td>'
            f'<td style="font-size:.77rem;color:#888">{freq}</td>'
            f'<td class="mono">${px:.2f}</td>'
            f'<td class="mono">{bd.strftime("%b %d, %Y") if bd else "—"}</td>'
            f'<td class="mono" style="color:#bbb">{ex.strftime("%b %d, %Y") if ex else "—"}</td>'
            f'<td>{da}d {alert}</td>'
            f'</tr>'
        )

    st.markdown(
        '<table class="stbl"><thead><tr>'
        '<th>Ticker</th><th>Sector</th><th>Yield</th><th>Div/Share</th>'
        '<th>Payout</th><th>Frequency</th><th>Price</th>'
        '<th>Buy Before</th><th>Ex-Date</th><th>Countdown</th>'
        f'</tr></thead><tbody>{rows_html}</tbody></table>',
        unsafe_allow_html=True,
    )


# ── Full universe ─────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
with st.expander(
    f"📊 Full dividend universe — {len(df)} stocks after filters ({len(df_all)} total)"
):
    st.dataframe(
        df[["ticker","sector","yield_pct","div_rate",
            "payout","frequency","price","ex_date","buy_date"]]
        .rename(columns={
            "ticker":    "Ticker",
            "sector":    "Sector",
            "yield_pct": "Yield %",
            "div_rate":  "Div/Share",
            "payout":    "Payout %",
            "frequency": "Frequency",
            "price":     "Price",
            "ex_date":   "Ex-Date",
            "buy_date":  "Buy Before",
        }),
        use_container_width=True,
        hide_index=True,
    )

st.markdown(
    "<hr><p style='font-size:.7rem;color:#ccc;text-align:center'>"
    "github.com/magicpro33/stock · data updated nightly via GitHub Actions · "
    "Not financial advice · Always verify ex-dates before trading</p>",
    unsafe_allow_html=True,
)"""
dividend_calendar.py — Dividend Capture Calendar
=================================================
Reads pre-computed data from data/stock_data.json.gz
then fetches live ex-dividend dates from yfinance for
stocks that pay dividends (cached 30 min).

Deploy to Streamlit Cloud:
  1. Push this file to root of magicpro33/stock repo
  2. Go to share.streamlit.io → New app
  3. Repo: magicpro33/stock  Branch: main  File: dividend_calendar.py
  4. Click Deploy — done
"""

import streamlit as st
import pandas as pd
import yfinance as yf
import gzip, json, os, datetime, calendar, time

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
html,body,[class*="css"]{font-family:'DM Sans',sans-serif}
.main-title{font-family:'DM Serif Display',serif;font-size:2.5rem;color:#cc0000;letter-spacing:-0.02em;line-height:1.1;margin-bottom:0}
.main-sub{font-size:.8rem;color:#aaa;margin-top:4px;letter-spacing:.06em;text-transform:uppercase}
.src-badge{display:inline-flex;align-items:center;gap:6px;border-radius:6px;padding:6px 14px;font-size:.78rem;font-weight:500;margin:10px 0 20px}
.src-ok  {background:#f0f7f0;border:1px solid #b8ddb8;color:#1a6b1a}
.src-warn{background:#fff8e6;border:1px solid #f0d080;color:#7a5a00}
.src-err {background:#fff0f0;border:1px solid #f0b0b0;color:#8b0000}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:4px;margin-top:10px}
.cal-hdr{text-align:center;font-size:.65rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:#ccc;padding:5px 0}
.cal-day{background:#f9f9f7;border:1px solid #efefed;border-radius:7px;min-height:88px;padding:8px 7px}
.cal-day.today{border:2px solid #cc0000}
.cal-day.empty{background:transparent;border:none}
.cal-num{font-size:.7rem;font-weight:500;color:#ccc;margin-bottom:5px}
.cal-day.today .cal-num{color:#cc0000;font-weight:700}
.chip{display:block;border-radius:3px;padding:2px 5px;margin-bottom:3px;font-size:.6rem;font-weight:600;font-family:'DM Mono',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;border-left:2px solid;cursor:default}
.t1{background:rgba(10,61,10,.12);color:#1a6b1a;border-color:#2e7d32}
.t2{background:rgba(30,90,30,.10);color:#2e7d32;border-color:#388e3c}
.t3{background:rgba(100,100,10,.10);color:#827717;border-color:#f9a825}
.t4{background:rgba(180,90,0,.09);color:#e65100;border-color:#ff9800}
.t5{background:rgba(150,40,0,.08);color:#bf360c;border-color:#ff5722}
.stbl{width:100%;border-collapse:collapse}
.stbl th{font-size:.66rem;letter-spacing:.1em;text-transform:uppercase;color:#bbb;border-bottom:1px solid #efefed;padding:8px 10px;text-align:left}
.stbl td{padding:10px;border-bottom:1px solid #f5f5f3;font-size:.82rem}
.stbl tr:last-child td{border-bottom:none}
.mono{font-family:'DM Mono',monospace;font-size:.78rem}
.buy-now{background:#cc0000;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}
.buy-tmr{background:#2e7d32;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}
</style>
""", unsafe_allow_html=True)


# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "stock_data.json.gz")
META_FILE = os.path.join(BASE_DIR, "data", "scan_meta.json")


# ── Step 1: Load scan data and extract dividend stocks ────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def load_scan_data():
    """
    Load stock_data.json.gz and return only dividend-paying stocks
    with sane yield values.

    FIXES:
    - nightly_scan.py stores DividendYieldPct already as a percentage
      (e.g. 4.5 means 4.5%). No multiplication needed.
    - Filter out absurd yields > 50% (data errors from foreign ADRs)
    - Filter out zero/negative yields
    """
    if not os.path.exists(DATA_FILE):
        return None, "data/stock_data.json.gz not found — run nightly_scan.py first"

    try:
        with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        return None, f"Could not read data file: {e}"

    rows = []
    for item in raw:
        if not isinstance(item, dict):
            continue

        ticker = item.get("Ticker", "").strip()
        if not ticker:
            continue

        # ── Yield: stored as plain percentage already (e.g. 4.5 = 4.5%) ──
        yield_pct = item.get("DividendYieldPct")
        if yield_pct is None:
            continue
        try:
            yield_pct = float(yield_pct)
        except (TypeError, ValueError):
            continue

        # Skip zero, negative, or absurd yields (data errors)
        if yield_pct <= 0 or yield_pct > 50:
            continue

        # ── Dividend rate: stored in dollars per share ─────────────────────
        div_rate = item.get("DividendRate")
        if div_rate is not None:
            try:
                div_rate = float(div_rate)
            except (TypeError, ValueError):
                div_rate = None

        # ── Payout ratio: stored as a percentage (e.g. 45.2 = 45.2%) ───────
        payout = item.get("DividendPayoutRatio")
        if payout is not None:
            try:
                payout = float(payout)
                # Some entries stored as decimal fraction (0.45 not 45)
                if 0 < payout <= 3.0:
                    payout = payout * 100
                # Cap ridiculous values
                if payout > 500 or payout < 0:
                    payout = None
            except (TypeError, ValueError):
                payout = None

        rows.append({
            "ticker":    ticker,
            "sector":    item.get("Sector", "Unknown"),
            "price":     item.get("Price"),
            "yield_pct": round(yield_pct, 2),
            "div_rate":  div_rate,
            "payout":    payout,
            "frequency": item.get("DividendFrequency") or "—",
            "div_score": float(item.get("DividendScore") or 0),
        })

    if not rows:
        return None, "No valid dividend-paying stocks found in data file."

    df = pd.DataFrame(rows)
    df = df.sort_values("yield_pct", ascending=False).reset_index(drop=True)
    return df, None


@st.cache_data(ttl=1800, show_spinner=False)
def load_meta():
    if not os.path.exists(META_FILE):
        return None
    try:
        with open(META_FILE) as f:
            return json.load(f)
    except Exception:
        return None


# ── Step 2: Fetch live ex-dividend dates from yfinance ────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_ex_dates(tickers: tuple) -> dict:
    """
    Fetch current ex-dividend dates for a list of tickers via yfinance.
    Returns dict: {ticker: ex_date_as_date_or_None}

    Only fetches tickers that pay dividends (already filtered).
    Uses batch download for speed, falls back to individual lookups.
    """
    ex_dates = {}
    total    = len(tickers)
    prog     = st.progress(0, text="Fetching ex-dividend dates from yfinance…")

    for i, ticker in enumerate(tickers):
        try:
            info     = yf.Ticker(ticker).info
            ex_ts    = info.get("exDividendDate")
            ex_date  = None
            if ex_ts and isinstance(ex_ts, (int, float)) and ex_ts > 1_000_000_000:
                ex_date = datetime.datetime.utcfromtimestamp(ex_ts).date()
            ex_dates[ticker] = ex_date
        except Exception:
            ex_dates[ticker] = None

        pct = int((i + 1) / total * 100)
        prog.progress(
            (i + 1) / total,
            text=f"Fetching ex-dates… {i+1}/{total} ({pct}%)"
        )
        time.sleep(0.08)   # be polite to Yahoo Finance

    prog.empty()
    return ex_dates


# ── Helpers ───────────────────────────────────────────────────────────────────
def tier(y):
    if y >= 8:   return "t1"
    if y >= 6:   return "t2"
    if y >= 4:   return "t3"
    if y >= 2.5: return "t4"
    return "t5"

def ycolor(y):
    return {
        "t1": "#1a6b1a", "t2": "#2e7d32",
        "t3": "#827717", "t4": "#e65100", "t5": "#bf360c"
    }[tier(y)]

def render_calendar(df, year, month):
    today   = datetime.date.today()
    day_map = {}
    for _, r in df.iterrows():
        bd = r.get("buy_date")
        if bd is None or not isinstance(bd, datetime.date):
            continue
        if bd.year == year and bd.month == month:
            day_map.setdefault(bd.day, []).append(r)

    html = '<div class="cal-grid">'
    for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        html += f'<div class="cal-hdr">{d}</div>'

    for week in calendar.monthcalendar(year, month):
        for day in week:
            if day == 0:
                html += '<div class="cal-day empty"></div>'
                continue
            is_td = (year == today.year and month == today.month and day == today.day)
            cls   = "cal-day today" if is_td else "cal-day"
            html += f'<div class="{cls}"><div class="cal-num">{day}</div>'
            for r in day_map.get(day, []):
                t  = tier(r["yield_pct"])
                dr = r.get("div_rate") or 0
                px = r.get("price")    or 0
                ex = r.get("ex_date")
                freq = r.get("frequency", "—")
                html += (
                    f'<span class="chip {t}" '
                    f'title="BUY {r["ticker"]} before {ex} | '
                    f'Yield: {r["yield_pct"]}% | '
                    f'${dr:.4f}/share | Freq: {freq} | Price: ${px:.2f}">'
                    f'{r["ticker"]} {r["yield_pct"]}%</span>'
                )
            html += '</div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 💰 Dividend Calendar")
    st.markdown("`magicpro33/stock`")
    st.markdown("---")
    min_yield   = st.slider("Min yield (%)",  0.0, 30.0, 0.0, 0.5)
    days_ahead  = st.slider("Days ahead",     30,  180,  90)
    max_yield   = st.slider("Max yield (%) — filter bad data", 5.0, 50.0, 25.0, 1.0)
    freq_filter = st.selectbox(
        "Frequency",
        ["All", "Monthly", "Quarterly", "Semi-Annual", "Annual"]
    )
    st.markdown("---")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown(
        "**How it works:**\n"
        "- Loads dividend stocks from `data/stock_data.json.gz`\n"
        "- Fetches live ex-dates from yfinance (cached 30 min)\n"
        "- Calendar = day **before** ex-date\n"
        "- Own shares before that day → dividend paid to you\n"
        "- Chips = highest yield first\n"
        "- Hover chip for details"
    )


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">Dividend Capture Calendar</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="main-sub">'
    'buy 24h before ex-date · highest yield first · live ex-dates via yfinance'
    '</div>',
    unsafe_allow_html=True,
)


# ── Load scan data ────────────────────────────────────────────────────────────
with st.spinner("Loading scan data…"):
    df_scan, err = load_scan_data()
    meta         = load_meta()

if err:
    st.markdown(f'<div class="src-badge src-err">✗ {err}</div>', unsafe_allow_html=True)
    st.info(
        "**Setup:** Run `python nightly_scan.py` once locally, then "
        "`git add data/ && git commit -m 'scan data' && git push`. "
        "GitHub Actions will update it every night after that."
    )
    st.stop()

# Apply yield range filter immediately to remove garbage data
df_scan = df_scan[
    (df_scan["yield_pct"] >= min_yield) &
    (df_scan["yield_pct"] <= max_yield)
].copy()

if freq_filter != "All":
    df_scan = df_scan[
        df_scan["frequency"].str.contains(freq_filter, case=False, na=False)
    ]

# Sector filter (needs data)
with st.sidebar:
    sectors       = ["All sectors"] + sorted(df_scan["sector"].dropna().unique().tolist())
    sector_filter = st.selectbox("Sector", sectors)

if sector_filter != "All sectors":
    df_scan = df_scan[df_scan["sector"] == sector_filter]

if df_scan.empty:
    st.warning("No dividend stocks match current filters.")
    st.stop()

meta_txt = ""
if meta:
    meta_txt = f" · Last scan: {meta.get('scanned_at_utc','—')} · {meta.get('valid_results','?')} stocks scanned"

st.markdown(
    f'<div class="src-badge src-ok">'
    f'✓ {len(df_scan)} dividend stocks loaded (yield {min_yield}%–{max_yield}%){meta_txt}'
    f'</div>',
    unsafe_allow_html=True,
)


# ── Fetch live ex-dates ───────────────────────────────────────────────────────
# Limit to reasonable yield range and sort by yield to prioritize best stocks
tickers_to_fetch = tuple(df_scan["ticker"].tolist())

with st.spinner(f"Fetching live ex-dividend dates for {len(tickers_to_fetch)} stocks…"):
    ex_date_map = fetch_ex_dates(tickers_to_fetch)

# Merge ex-dates back into dataframe
df_scan["ex_date"]  = df_scan["ticker"].map(ex_date_map)
df_scan["buy_date"] = df_scan["ex_date"].apply(
    lambda d: (d - datetime.timedelta(days=1)) if isinstance(d, datetime.date) else None
)


# ── Filter to upcoming window ─────────────────────────────────────────────────
today  = datetime.date.today()
cutoff = today + datetime.timedelta(days=days_ahead)

df_cal = df_scan[
    df_scan["buy_date"].notna() &
    (df_scan["buy_date"].apply(lambda d: isinstance(d, datetime.date) and today <= d <= cutoff))
].copy()


# ── Metrics ───────────────────────────────────────────────────────────────────
nxt = df_cal.sort_values("buy_date").iloc[0] if not df_cal.empty else None
c1, c2, c3, c4 = st.columns(4)
c1.metric("Buy signals ahead", len(df_cal))
c2.metric("Avg yield",
          f"{df_cal['yield_pct'].mean():.1f}%" if not df_cal.empty else "—")
c3.metric("Highest yield",
          f"{df_cal['yield_pct'].max():.1f}%" if not df_cal.empty else "—",
          delta=df_cal.iloc[0]["ticker"] if not df_cal.empty else "")
c4.metric("Next buy date",
          nxt["buy_date"].strftime("%b %d") if nxt is not None else "—",
          delta=nxt["ticker"]              if nxt is not None else "")

st.markdown("---")


# ── Calendar nav ──────────────────────────────────────────────────────────────
if "cy" not in st.session_state: st.session_state.cy = today.year
if "cm" not in st.session_state: st.session_state.cm = today.month

cp, cc, cn = st.columns([1, 5, 1])
with cp:
    if st.button("← Prev"):
        if st.session_state.cm == 1:
            st.session_state.cy -= 1; st.session_state.cm = 12
        else:
            st.session_state.cm -= 1
with cn:
    if st.button("Next →"):
        if st.session_state.cm == 12:
            st.session_state.cy += 1; st.session_state.cm = 1
        else:
            st.session_state.cm += 1
with cc:
    st.markdown(
        f"<h3 style='text-align:center;font-family:DM Serif Display,serif;margin:0'>"
        f"{calendar.month_name[st.session_state.cm]} {st.session_state.cy}</h3>",
        unsafe_allow_html=True,
    )

# Legend
st.markdown(
    "<div style='display:flex;gap:8px;margin:6px 0 4px;flex-wrap:wrap'>"
    "<span style='background:#1a6b1a;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 8%</span>"
    "<span style='background:#2e7d32;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 6%</span>"
    "<span style='background:#827717;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 4%</span>"
    "<span style='background:#e65100;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>≥ 2.5%</span>"
    "<span style='background:#bf360c;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600'>&lt; 2.5%</span>"
    "</div>",
    unsafe_allow_html=True,
)

render_calendar(df_cal, st.session_state.cy, st.session_state.cm)
st.markdown("<br>", unsafe_allow_html=True)


# ── Upcoming table ────────────────────────────────────────────────────────────
st.markdown("### 📋 Upcoming Buy Signals — Ranked by Yield")

if df_cal.empty:
    st.info(
        "No dividend ex-dates found in the next "
        f"{days_ahead} days for the current filter set. "
        "Try widening the date range or adjusting yield filters."
    )
else:
    df_show = df_cal.sort_values(
        ["buy_date", "yield_pct"], ascending=[True, False]
    ).copy()
    df_show["days_away"] = df_show["buy_date"].apply(
        lambda d: (d - today).days
    )

    rows_html = ""
    for _, r in df_show.iterrows():
        yc    = ycolor(r["yield_pct"])
        da    = int(r["days_away"])
        dr    = r.get("div_rate")   or 0
        px    = r.get("price")      or 0
        pr    = r.get("payout")
        ex    = r.get("ex_date")
        bd    = r.get("buy_date")
        freq  = r.get("frequency", "—")
        alert = ""
        if   da == 0: alert = '<span class="buy-now">BUY TODAY</span>'
        elif da == 1: alert = '<span class="buy-tmr">BUY TOMORROW</span>'

        rows_html += (
            f'<tr>'
            f'<td><strong style="font-family:\'DM Mono\',monospace">{r["ticker"]}</strong></td>'
            f'<td style="color:#aaa;font-size:.75rem">{r["sector"]}</td>'
            f'<td><span style="background:{yc}18;color:{yc};padding:2px 9px;'
            f'border-radius:100px;font-family:\'DM Mono\',monospace;'
            f'font-size:.78rem;font-weight:600">{r["yield_pct"]}%</span></td>'
            f'<td class="mono">${dr:.4f}</td>'
            f'<td class="mono">{f"{pr:.0f}%" if pr else "—"}</td>'
            f'<td style="font-size:.77rem;color:#888">{freq}</td>'
            f'<td class="mono">${px:.2f}</td>'
            f'<td class="mono">{bd.strftime("%b %d, %Y") if bd else "—"}</td>'
            f'<td class="mono" style="color:#bbb">{ex.strftime("%b %d, %Y") if ex else "—"}</td>'
            f'<td>{da}d {alert}</td>'
            f'</tr>'
        )

    st.markdown(
        '<table class="stbl"><thead><tr>'
        '<th>Ticker</th><th>Sector</th><th>Yield</th><th>Div/Share</th>'
        '<th>Payout</th><th>Frequency</th><th>Price</th>'
        '<th>Buy Before</th><th>Ex-Date</th><th>Countdown</th>'
        f'</tr></thead><tbody>{rows_html}</tbody></table>',
        unsafe_allow_html=True,
    )


# ── Full universe ─────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
with st.expander(
    f"📊 Full dividend universe — {len(df_scan)} stocks "
    f"(yield {min_yield}%–{max_yield}%, {days_ahead}d window)"
):
    display_cols = ["ticker", "sector", "yield_pct", "div_rate",
                    "payout", "frequency", "price", "ex_date", "buy_date"]
    st.dataframe(
        df_scan[display_cols].rename(columns={
            "ticker":    "Ticker",
            "sector":    "Sector",
            "yield_pct": "Yield %",
            "div_rate":  "Div/Share",
            "payout":    "Payout %",
            "frequency": "Frequency",
            "price":     "Price",
            "ex_date":   "Ex-Date",
            "buy_date":  "Buy Before",
        }),
        use_container_width=True,
        hide_index=True,
    )

st.markdown(
    "<hr><p style='font-size:.7rem;color:#ccc;text-align:center'>"
    "github.com/magicpro33/stock · data updated nightly via GitHub Actions · "
    "Not financial advice · Always verify ex-dates before trading</p>",
    unsafe_allow_html=True,
)
