"""
dividend_calendar.py — Dividend Capture Calendar
=================================================
Reads pre-computed data from data/stock_data.json.gz
(written nightly by nightly_scan.py via GitHub Actions).

Zero live API calls — loads instantly from cached scan data.

Deploy to Streamlit Cloud:
  1. Push this file to root of magicpro33/stock repo
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
.main-title{font-family:'DM Serif Display',serif;font-size:2.5rem;color:#0a0a0a;letter-spacing:-0.02em;line-height:1.1;margin-bottom:0}
.main-sub{font-size:.8rem;color:#aaa;margin-top:4px;letter-spacing:.06em;text-transform:uppercase}
.src-badge{display:inline-flex;align-items:center;gap:6px;border-radius:6px;padding:6px 14px;font-size:.78rem;font-weight:500;margin:10px 0 20px}
.src-ok  {background:#f0f7f0;border:1px solid #b8ddb8;color:#1a6b1a}
.src-warn{background:#fff8e6;border:1px solid #f0d080;color:#7a5a00}
.src-err {background:#fff0f0;border:1px solid #f0b0b0;color:#8b0000}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:4px;margin-top:10px}
.cal-hdr{text-align:center;font-size:.65rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:#ccc;padding:5px 0}
.cal-day{background:#f9f9f7;border:1px solid #efefed;border-radius:7px;min-height:88px;padding:8px 7px}
.cal-day.today{border:2px solid #1a1a1a}
.cal-day.empty{background:transparent;border:none}
.cal-num{font-size:.7rem;font-weight:500;color:#ccc;margin-bottom:5px}
.cal-day.today .cal-num{color:#1a1a1a;font-weight:700}
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
.buy-now{background:#1a6b1a;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}
.buy-tmr{background:#2e7d32;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "stock_data.json.gz")
META_FILE = os.path.join(BASE_DIR, "data", "scan_meta.json")


@st.cache_data(ttl=1800, show_spinner=False)
def load_dividend_data():
    """
    Parse stock_data.json.gz and return a DataFrame of dividend-paying stocks.
    Each record in the file is one stock dict from nightly_scan.py.
    """
    if not os.path.exists(DATA_FILE):
        return None, (
            "data/stock_data.json.gz not found.\n\n"
            "Run nightly_scan.py locally once to generate it, "
            "then push data/ to GitHub. After that GitHub Actions "
            "will regenerate it every night automatically."
        )

    try:
        with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        return None, f"Could not decompress/parse data file: {e}"

    rows = []
    for item in raw:
        if not isinstance(item, dict):
            continue

        # Only keep stocks that pay dividends
        yield_pct = item.get("DividendYieldPct")
        if not yield_pct or yield_pct <= 0:
            continue

        # Parse ex-dividend date — nightly_scan stores it as a Unix timestamp
        # in the yfinance info dict field "exDividendDate"
        ex_date = None
        for field in ("ExDividendDate", "exDividendDate", "ex_dividend_date",
                      "ExDividendDateTimestamp"):
            raw_val = item.get(field)
            if raw_val is None:
                continue
            try:
                if isinstance(raw_val, (int, float)) and raw_val > 1_000_000_000:
                    ex_date = datetime.datetime.utcfromtimestamp(raw_val).date()
                elif isinstance(raw_val, str) and len(raw_val) >= 8:
                    ex_date = datetime.date.fromisoformat(raw_val[:10])
            except Exception:
                pass
            if ex_date:
                break

        rows.append({
            "ticker":       item.get("Ticker", ""),
            "sector":       item.get("Sector", "Unknown"),
            "price":        item.get("Price"),
            "yield_pct":    round(float(yield_pct), 2),
            "div_rate":     item.get("DividendRate"),
            "payout_ratio": item.get("DividendPayoutRatio"),
            "frequency":    item.get("DividendFrequency") or "—",
            "div_score":    item.get("DividendScore", 0),
            "ex_date":      ex_date,
            "buy_date":     (ex_date - datetime.timedelta(days=1)) if ex_date else None,
        })

    if not rows:
        return None, "No dividend-paying stocks found in the data file."

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


# ── Render helpers ────────────────────────────────────────────────────────────

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
        bd = r["buy_date"]
        if bd is None:
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
                t   = tier(r["yield_pct"])
                dr  = r["div_rate"]
                px  = r["price"]
                ex  = r["ex_date"]
                html += (
                    f'<span class="chip {t}" '
                    f'title="BUY {r["ticker"]} before {ex} ex-date | '
                    f'Yield: {r["yield_pct"]}% | '
                    f'${dr:.4f}/share | '
                    f'Freq: {r["frequency"]} | '
                    f'Price: ${px:.2f}">'
                    f'{r["ticker"]} {r["yield_pct"]}%'
                    f'</span>'
                )
            html += '</div>'

    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 💰 Dividend Calendar")
    st.markdown("`magicpro33/stock`")
    st.markdown("---")
    min_yield  = st.slider("Min yield (%)",  0.0, 15.0, 0.0,  0.5)
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
        "  updated every night by GitHub Actions\n"
        "- Calendar shows the day **before** the ex-date\n"
        "- Own shares by that day → dividend paid to you\n"
        "- Chips sorted highest yield first\n"
        "- Hover a chip to see full details\n\n"
        "**To deploy:** push this file to repo root, "
        "then connect at share.streamlit.io"
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
with st.spinner("Loading..."):
    df_all, err = load_dividend_data()
    meta        = load_meta()

if err:
    st.markdown(f'<div class="src-badge src-err">✗ {err}</div>', unsafe_allow_html=True)

    # Helpful setup instructions
    st.info(
        "**First-time setup:**\n\n"
        "1. Run `python nightly_scan.py` locally — it creates `data/stock_data.json.gz`\n"
        "2. `git add data/stock_data.json.gz && git commit -m 'add initial scan data' && git push`\n"
        "3. GitHub Actions will regenerate it every night automatically after that\n\n"
        "After the data file exists, this calendar populates instantly with no API calls."
    )
    st.stop()

# Badge with scan metadata
has_dates  = df_all["ex_date"].notna().sum()
meta_txt   = ""
if meta:
    meta_txt = f" · Last scan: {meta.get('scanned_at_utc','—')} · {meta.get('valid_results','?')} stocks"
badge_cls  = "src-ok" if has_dates > 0 else "src-warn"
badge_icon = "✓" if has_dates > 0 else "⚠"
badge_note = f"{has_dates} with upcoming ex-dates" if has_dates > 0 else "no upcoming ex-dates found — data may need refresh"
st.markdown(
    f'<div class="src-badge {badge_cls}">'
    f'{badge_icon} Loaded {len(df_all)} dividend stocks ({badge_note}){meta_txt}'
    f'</div>',
    unsafe_allow_html=True,
)

# ── Sector filter (needs data first) ─────────────────────────────────────────
with st.sidebar:
    sectors = ["All sectors"] + sorted(df_all["sector"].dropna().unique().tolist())
    sector_filter = st.selectbox("Sector", sectors)

# ── Apply filters ─────────────────────────────────────────────────────────────
today  = datetime.date.today()
cutoff = today + datetime.timedelta(days=days_ahead)

df = df_all[df_all["yield_pct"] >= min_yield].copy()

if freq_filter != "All":
    df = df[df["frequency"].str.contains(freq_filter, case=False, na=False)]

if sector_filter != "All sectors":
    df = df[df["sector"] == sector_filter]

df_cal = df[
    df["buy_date"].notna() &
    (df["buy_date"] >= today) &
    (df["buy_date"] <= cutoff)
].copy()


# ── Metrics ───────────────────────────────────────────────────────────────────
nxt = df_cal.sort_values("buy_date").iloc[0] if not df_cal.empty else None
c1, c2, c3, c4 = st.columns(4)
c1.metric("Buy signals ahead", len(df_cal))
c2.metric("Avg yield",  f"{df_cal['yield_pct'].mean():.1f}%" if not df_cal.empty else "—")
c3.metric("Highest yield",
          f"{df_cal['yield_pct'].max():.1f}%" if not df_cal.empty else "—",
          delta=df_cal.iloc[0]["ticker"] if not df_cal.empty else "")
c4.metric("Next buy date",
          nxt["buy_date"].strftime("%b %d") if nxt is not None else "—",
          delta=nxt["ticker"]              if nxt is not None else "")

st.markdown("---")


# ── Calendar navigation ───────────────────────────────────────────────────────
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
    st.info("No signals in this window. Try widening the date range or lowering the min yield.")
else:
    df_show = df_cal.sort_values(
        ["buy_date", "yield_pct"], ascending=[True, False]
    ).copy()
    df_show["days_away"] = (df_show["buy_date"] - today).dt.days

    rows_html = ""
    for _, r in df_show.iterrows():
        yc  = ycolor(r["yield_pct"])
        da  = int(r["days_away"])
        dr  = r["div_rate"]  or 0
        px  = r["price"]     or 0
        pr  = r["payout_ratio"]
        alert = ""
        if da == 0: alert = '<span class="buy-now">BUY TODAY</span>'
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
            f'<td style="font-size:.77rem;color:#888">{r["frequency"]}</td>'
            f'<td class="mono">${px:.2f}</td>'
            f'<td class="mono">{r["buy_date"].strftime("%b %d, %Y")}</td>'
            f'<td class="mono" style="color:#bbb">{r["ex_date"].strftime("%b %d, %Y")}</td>'
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
with st.expander(f"📊 Full dividend universe — {len(df)} stocks after filters ({len(df_all)} total)"):
    st.dataframe(
        df[["ticker","sector","yield_pct","div_rate",
            "payout_ratio","frequency","price","ex_date","buy_date"]]
        .rename(columns={
            "ticker":       "Ticker",
            "sector":       "Sector",
            "yield_pct":    "Yield %",
            "div_rate":     "Div/Share",
            "payout_ratio": "Payout %",
            "frequency":    "Frequency",
            "price":        "Price",
            "ex_date":      "Ex-Date",
            "buy_date":     "Buy Before",
        }),
        use_container_width=True,
        hide_index=True,
    )

st.markdown(
    "<hr><p style='font-size:.7rem;color:#ccc;text-align:center'>"
    "github.com/magicpro33/stock · data/stock_data.json.gz updated nightly · "
    "Not financial advice · Verify ex-dates before trading</p>",
    unsafe_allow_html=True,
)
