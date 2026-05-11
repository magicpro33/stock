# -*- coding: utf-8 -*-
# dividend_calendar.py -- Dividend Capture Calendar for magicpro33/stock
# Reads data/stock_data.json.gz written by nightly_scan.py
# Deploy: share.streamlit.io -> magicpro33/stock -> dividend_calendar.py
#
# IMPORTANT: When updating this file on GitHub, REPLACE the entire file.
# Do NOT append or merge with an existing version.
# The file must be exactly as delivered -- no extra copies of functions.

import streamlit as st
import pandas as pd
import yfinance as yf
import gzip, json, os, time, datetime, calendar as cal_module, numpy as np

st.set_page_config(page_title='Dividend Capture Calendar',
    page_icon=':moneybag:', layout='wide', initial_sidebar_state='expanded')

_CSS = (
    '<style>'
    '@import url(\'https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap\');'
    'html,body,[class*=css]{font-family:\'DM Sans\',sans-serif}'
    '.main-title{font-family:\'DM Serif Display\',serif;font-size:2.4rem;color:#cc0000;letter-spacing:-.02em;line-height:1.1;margin-bottom:0}'
    '.main-sub{font-size:.78rem;color:#aaa;margin-top:4px;letter-spacing:.06em;text-transform:uppercase}'
    '.src-badge{display:inline-flex;align-items:center;gap:6px;border-radius:6px;padding:6px 14px;font-size:.78rem;font-weight:500;margin:10px 0 20px}'
    '.src-ok{background:#f0f7f0;border:1px solid #b8ddb8;color:#1a6b1a}'
    '.src-warn{background:#fff8e6;border:1px solid #f0d080;color:#7a5a00}'
    '.src-err{background:#fff0f0;border:1px solid #f0b0b0;color:#8b0000}'
    '.cal-grid{display:grid !important;grid-template-columns:repeat(7,minmax(0,1fr)) !important;gap:4px;margin-top:10px;width:100% !important}'
    '.cal-hdr{text-align:center;font-size:.65rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:#ccc;padding:5px 0}'
    '.cal-day{background:#f9f9f7;border:1px solid #efefed;border-radius:7px;min-height:88px;padding:8px 7px}'
    '.cal-day.today{border:2px solid #cc0000}'
    '.cal-day.empty{background:transparent;border:none}'
    '.cal-num{font-size:.7rem;font-weight:500;color:#ccc;margin-bottom:5px}'
    '.cal-day.today .cal-num{color:#cc0000;font-weight:700}'
    '.chip{display:block;border-radius:3px;padding:2px 5px;margin-bottom:3px;font-size:.6rem;font-weight:600;font-family:\'DM Mono\',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;border-left:2px solid;cursor:default}'
    '.t1{background:rgba(10,61,10,.12);color:#1a6b1a;border-color:#2e7d32}'
    '.t2{background:rgba(30,90,30,.10);color:#2e7d32;border-color:#388e3c}'
    '.t3{background:rgba(100,100,10,.10);color:#827717;border-color:#f9a825}'
    '.t4{background:rgba(180,90,0,.09);color:#e65100;border-color:#ff9800}'
    '.t5{background:rgba(150,40,0,.08);color:#bf360c;border-color:#ff5722}'
    '.tbl-wrap{max-height:480px;overflow-y:auto;border:1px solid #2a2a2a;border-radius:8px;background:#111}'
    '.stbl{width:100%;border-collapse:collapse}'
    '.stbl thead th{position:sticky;top:0;z-index:2;background:#1a1a1a;color:#999;font-size:.66rem;letter-spacing:.1em;text-transform:uppercase;padding:10px;text-align:left;white-space:nowrap;border-bottom:1px solid #333}'
    '.stbl tbody td{padding:10px;border-bottom:1px solid #1e1e1e;font-size:.82rem;color:#d0d0d0;background:#111;transition:background .1s,color .1s}'
    '.stbl tbody tr:last-child td{border-bottom:none}'
    '.stbl tbody tr.tbl-row:hover td{background:#1a2e1a !important}'
    '.td-ticker{font-family:\'DM Mono\',monospace;font-weight:700;color:#fff}'
    '.td-sector{font-size:.74rem;color:#888}'
    '.td-num{font-family:\'DM Mono\',monospace;color:#c8c8c8}'
    '.td-freq{font-size:.76rem;color:#888}'
    '.td-date{font-family:\'DM Mono\',monospace;color:#777}'
    '.td-count{font-family:\'DM Mono\',monospace;color:#aaa;white-space:nowrap}'
    '.stbl tbody tr.tbl-row:hover .td-ticker{color:#fff !important}'
    '.stbl tbody tr.tbl-row:hover .td-sector{color:#a0c8a0 !important}'
    '.stbl tbody tr.tbl-row:hover .td-num{color:#e8e8e8 !important}'
    '.stbl tbody tr.tbl-row:hover .td-freq{color:#a0c8a0 !important}'
    '.stbl tbody tr.tbl-row:hover .td-date{color:#c8c8c8 !important}'
    '.stbl tbody tr.tbl-row:hover .td-count{color:#e8e8e8 !important}'
    '.yield-badge{padding:2px 9px;border-radius:100px;font-family:\'DM Mono\',monospace;font-size:.78rem;font-weight:600;background:rgba(255,255,255,.07);color:var(--yc);border:1px solid var(--yc);opacity:.9}'
    '.stbl tbody tr.tbl-row:hover .yield-badge{opacity:1}'
    '.mono{font-family:\'DM Mono\',monospace;font-size:.78rem}'
    '.buy-now{background:#cc0000;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}'
    '.buy-tmr{background:#2e7d32;color:#fff;padding:2px 8px;border-radius:4px;font-size:.67rem;font-weight:700}'
    '.calc-card{background:#f9f9f7;border:1px solid #e8e8e4;border-radius:10px;padding:20px 24px;margin-bottom:20px}'
    '.calc-result{background:#0a2a0a;border-radius:8px;padding:16px 20px;margin-top:12px}'
    '.calc-result-row{display:flex;justify-content:space-between;align-items:baseline;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.06)}'
    '.calc-result-row:last-child{border-bottom:none}'
    '.calc-label{font-size:.78rem;color:#88aa88}'
    '.calc-value{font-family:\'DM Mono\',monospace;font-size:1rem;font-weight:600;color:#7fff7f}'
    '.calc-value.big{font-size:1.3rem;color:#39ff4a}'
    '.section-hdr{font-family:\'DM Serif Display\',serif;font-size:1.2rem;color:#cc0000;margin:24px 0 12px;padding-bottom:6px;border-bottom:2px solid #cc0000}'
    '.mrow{border-bottom:1px solid #1e1e1e}'
    '.mrow:last-child{border-bottom:none}'
    '.mrow-label{padding:8px 10px;font-size:.78rem;color:#888;vertical-align:middle;white-space:nowrap}'
    '.mrow-val{padding:8px 10px;font-size:.84rem;font-weight:500}'
    '.az-section{font-size:.65rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#cc0000;padding:10px 0 5px;margin-top:8px;border-bottom:1px solid #2a2a2a}'
    '.signal-pill{display:inline-block;padding:2px 9px;border-radius:100px;font-size:.72rem;font-weight:600;margin:2px 2px}'
    '.pill-bull{background:#0a2a0a;color:#7fff7f;border:1px solid #2e7d32}'
    '.pill-bear{background:#2a0a0a;color:#ff9999;border:1px solid #7d2e2e}'
    '.pill-neut{background:#1a1a0a;color:#ffe066;border:1px solid #7d7020}'
    '.tag-good{background:#e8f5e9;color:#1b5e20;padding:2px 8px;border-radius:100px;font-size:.68rem;font-weight:600}'
    '.tag-ok{background:#fff8e1;color:#e65100;padding:2px 8px;border-radius:100px;font-size:.68rem;font-weight:600}'
    '.tag-bad{background:#ffebee;color:#b71c1c;padding:2px 8px;border-radius:100px;font-size:.68rem;font-weight:600}'
    '</style>'
)
st.markdown(_CSS, unsafe_allow_html=True)

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, 'data', 'stock_data.json.gz')
META_FILE = os.path.join(BASE_DIR, 'data', 'scan_meta.json')

def safe_date(v):
    if v is None: return None
    if isinstance(v, datetime.datetime): return v.date()
    if isinstance(v, datetime.date): return v
    try:
        if pd.isna(v): return None
    except Exception: pass
    return None

def tier(y):
    if y >= 8: return 't1'
    if y >= 6: return 't2'
    if y >= 4: return 't3'
    if y >= 2.5: return 't4'
    return 't5'

def ycolor(y):
    return {'t1':'#1a6b1a','t2':'#2e7d32','t3':'#827717','t4':'#e65100','t5':'#bf360c'}[tier(y)]

def tag(v, good, ok, fmt='{:.1f}', sfx=''):
    if v is None: return '<span style="color:#ccc">n/a</span>'
    s = fmt.format(v) + sfx
    if v >= good: return '<span class="tag-good">' + s + '</span>'
    if v >= ok:   return '<span class="tag-ok">'   + s + '</span>'
    return '<span class="tag-bad">' + s + '</span>'

def tip(label, text):
    safe = text.replace("'", '&#39;').replace('"', '&quot;')
    # Use <details>/<summary> -- works in Streamlit's HTML sandbox
    # no external CSS needed, click ? to expand
    return (
        '<span style="display:inline-flex;align-items:flex-start;gap:4px">' + label +
        '<details style="display:inline;position:relative">'
        '<summary style="display:inline-flex;align-items:center;justify-content:center;'
        'width:15px;height:15px;border-radius:50%;background:#555;color:#ddd;'
        'font-size:9px;font-weight:700;cursor:pointer;list-style:none;'
        'border:1px solid #777;flex-shrink:0;line-height:1">?</summary>'
        '<div style="position:absolute;left:0;top:20px;z-index:9999;'
        'background:#1e1e1e;color:#e8e8e8;border:1px solid #555;'
        'border-radius:8px;padding:12px 14px;font-size:.76rem;line-height:1.6;'
        'width:280px;max-width:80vw;word-wrap:break-word;white-space:normal;'
        'box-shadow:0 8px 32px rgba(0,0,0,.75);min-width:200px">' + safe + '</div>'
        '</details></span>')

def mrow(label, tip_text, val_html):
    safe = tip_text.replace("'", '&#39;').replace('"', '&quot;')
    lbl = (
        '<span style="display:inline-flex;align-items:flex-start;gap:4px">'
        + label +
        '<details style="display:inline;position:relative">'
        '<summary style="display:inline-flex;align-items:center;justify-content:center;'
        'width:15px;height:15px;border-radius:50%;background:#555;color:#ddd;'
        'font-size:9px;font-weight:700;cursor:pointer;list-style:none;'
        'border:1px solid #777;flex-shrink:0;line-height:1">?</summary>'
        '<div style="position:absolute;left:0;top:20px;z-index:9999;'
        'background:#1e1e1e;color:#e8e8e8;border:1px solid #555;'
        'border-radius:8px;padding:12px 14px;font-size:.76rem;line-height:1.6;'
        'width:280px;max-width:80vw;word-wrap:break-word;white-space:normal;'
        'box-shadow:0 8px 32px rgba(0,0,0,.75)">'
        + safe +
        '</div></details></span>')
    return ('<tr class="mrow"><td class="mrow-label">' + lbl +
            '</td><td class="mrow-val">' + val_html + '</td></tr>')

def pill(label, bull):
    cls = 'pill-bull' if bull is True else ('pill-bear' if bull is False else 'pill-neut')
    return '<span class="signal-pill ' + cls + '">' + label + '</span>'

@st.cache_data(ttl=1800, show_spinner=False)
def load_scan_data():
    if not os.path.exists(DATA_FILE):
        return None, 'data/stock_data.json.gz not found -- run nightly_scan.py first', False
    try:
        with gzip.open(DATA_FILE, 'rt', encoding='utf-8') as f:
            raw = json.load(f)
    except Exception as e:
        return None, 'Could not read data file: ' + str(e), False
    rows = []
    ex_count = 0
    for item in raw:
        if not isinstance(item, dict): continue
        ticker = (item.get('Ticker') or '').strip()
        if not ticker: continue
        try: yp = float(item.get('DividendYieldPct') or 0)
        except (TypeError, ValueError): continue
        if yp <= 0 or yp > 50: continue
        try: dr = float(item.get('DividendRate') or 0) or None
        except (TypeError, ValueError): dr = None
        try:
            pr = float(item.get('DividendPayoutRatio') or 0) or None
            if pr and (pr < 0 or pr > 500): pr = None
        except (TypeError, ValueError): pr = None
        freq = item.get('DividendFrequency') or '--'
        mp = round(dr / 12, 4) if dr else None
        ex_date = None
        ex_ts = item.get('ExDividendDate')
        if ex_ts:
            try:
                ts = float(ex_ts)
                if ts > 1e9:
                    ex_date = datetime.datetime.utcfromtimestamp(ts).date()
                    ex_count += 1
            except (TypeError, ValueError): pass
        rows.append({'ticker': ticker, 'sector': item.get('Sector') or 'Unknown',
            'price': item.get('Price'), 'yield_pct': round(yp, 2),
            'div_rate': dr, 'monthly_pay': mp, 'payout': pr,
            'frequency': freq, 'div_score': float(item.get('DividendScore') or 0),
            'ex_date': ex_date})
    if not rows: return None, 'No valid dividend stocks found.', False
    df = pd.DataFrame(rows).sort_values('yield_pct', ascending=False).reset_index(drop=True)
    return df, None, ex_count > 0

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ex_dates_live(tickers_tuple):
    result = {}
    tickers = list(tickers_tuple)
    prog = st.progress(0, text='Fetching ex-dividend dates...')
    for i, tk in enumerate(tickers):
        try:
            info = yf.Ticker(tk).info
            ex_ts = info.get('exDividendDate')
            if ex_ts and isinstance(ex_ts, (int, float)) and float(ex_ts) > 1e9:
                result[tk] = datetime.datetime.utcfromtimestamp(float(ex_ts)).date()
            else: result[tk] = None
        except Exception: result[tk] = None
        prog.progress((i + 1) / len(tickers), text='Fetching ex-dates... ' + str(i+1) + '/' + str(len(tickers)))
        time.sleep(0.08)
    prog.empty()
    return result

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stock_analysis(sym):
    sym = sym.upper().strip()
    for attempt in range(2):
        try:
            t    = yf.Ticker(sym)
            info = t.info or {}
            # yfinance can return {} on rate limit -- retry once
            if not info and attempt == 0:
                time.sleep(1.5)
                continue
            hist = t.history(period='1y')
            divs = t.dividends
            return info, hist, divs, None
        except Exception as e:
            if attempt == 0: time.sleep(1.5)
            else: return {}, pd.DataFrame(), pd.Series(dtype=float), str(e)
    return {}, pd.DataFrame(), pd.Series(dtype=float), 'No data returned'

@st.cache_data(ttl=1800, show_spinner=False)
def load_meta():
    if not os.path.exists(META_FILE): return None
    try:
        with open(META_FILE) as f: return json.load(f)
    except Exception: return None

def render_calendar(df, year, month):
    today = datetime.date.today()
    day_map = {}
    for row in df.to_dict('records'):
        bd = safe_date(row.get('buy_date'))
        if bd and bd.year == year and bd.month == month:
            day_map.setdefault(bd.day, []).append(row)
    parts = ['<div class="cal-grid">']
    for d in ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']:
        parts.append('<div class="cal-hdr">' + d + '</div>')
    for week in cal_module.monthcalendar(year, month):
        for day in week:
            if day == 0: parts.append('<div class="cal-day empty"></div>'); continue
            is_td = (year == today.year and month == today.month and day == today.day)
            parts.append('<div class="' + ('cal-day today' if is_td else 'cal-day') + '">')
            parts.append('<div class="cal-num">' + str(day) + '</div>')
            for row in day_map.get(day, []):
                t = tier(row['yield_pct'])
                dr = row.get('div_rate') or 0
                px = row.get('price') or 0
                ex = safe_date(row.get('ex_date'))
                mp = row.get('monthly_pay')
                tip_txt = ('BUY ' + row['ticker'] + ' before ' + str(ex) +
                    ' | Yield: ' + str(row['yield_pct']) + '%' +
                    ' | Monthly: $' + ('{:.4f}'.format(mp) if mp else 'n/a') +
                    ' | Price: $' + '{:.2f}'.format(px))
                parts.append('<span class="chip ' + t + '" title="' + tip_txt + '">' +
                    row['ticker'] + ' ' + str(row['yield_pct']) + '%</span>')
            parts.append('</div>')
    parts.append('</div>')
    st.markdown(''.join(parts), unsafe_allow_html=True)

with st.sidebar:
    st.markdown('### Dividend Calendar')
    st.markdown('`magicpro33/stock`')
    st.markdown('---')
    min_yield  = st.slider('Min yield (%)',  0.0, 25.0, 0.0,  0.5, key='sb_min_yield')
    max_yield  = st.slider('Max yield (%)',  5.0, 50.0, 25.0, 1.0, key='sb_max_yield')
    days_ahead = st.slider('Days ahead',     30,  180,  90, key='sb_days_ahead')
    max_price  = st.number_input('Max stock price ($)', min_value=1,
        max_value=100000, value=1000, step=1, key='sb_max_price',
        help='Type any dollar amount - only shows stocks at or below this price')
    freq_filter = st.selectbox('Frequency', ['All','Monthly','Quarterly','Semi-Annual','Annual'], key='sb_freq')
    st.markdown('---')
    if st.button('Refresh', key='sb_refresh'):
        st.cache_data.clear()
        st.rerun()
    st.markdown('---')
    st.markdown('**How it works:**\n'
        '- Reads data/stock_data.json.gz (nightly)\n'
        '- Calendar = day before ex-date\n'
        '- Own shares before that day = dividend paid\n'
        '- Calculator tab to model returns\n'
        '- Analyzer tab for full stock deep-dive')

st.markdown('<div class="main-title">Dividend Capture Calendar</div>', unsafe_allow_html=True)
st.markdown('<div class="main-sub">buy 24h before ex-date | highest yield first | magicpro33/stock</div>', unsafe_allow_html=True)

with st.spinner('Loading scan data...'):
    scan_result = load_scan_data()
    meta = load_meta()

if scan_result[0] is None:
    st.markdown('<div class="src-badge src-err">&#x2717; ' + scan_result[1] + '</div>', unsafe_allow_html=True)
    st.info('Run python nightly_scan.py and push data/ to GitHub. GitHub Actions regenerates it nightly.')
    st.stop()

df_all, _err, has_ex_dates = scan_result

with st.sidebar:
    sectors = ['All sectors'] + sorted(df_all['sector'].dropna().unique().tolist())
    sector_filter = st.selectbox('Sector', sectors, key='sb_sector')

today  = datetime.date.today()
df = df_all[
    (df_all['yield_pct'] >= min_yield) &
    (df_all['yield_pct'] <= max_yield) &
    (df_all['price'].fillna(0) <= max_price)
].copy()
if freq_filter != 'All':
    df = df[df['frequency'].str.contains(freq_filter, case=False, na=False)]
if sector_filter != 'All sectors':
    df = df[df['sector'] == sector_filter]

if not has_ex_dates:
    st.info('Fetching live ex-dates for ' + str(len(df)) + ' stocks from Yahoo Finance...')
    with st.spinner('Fetching ex-dates...'):
        live_map = fetch_ex_dates_live(tuple(df['ticker'].tolist()))
    df = df.copy()
    df['ex_date'] = df['ticker'].map(live_map)
    src_label, badge_cls = 'live Yahoo Finance', 'src-warn'
else:
    src_label, badge_cls = 'scan data', 'src-ok'
df['buy_date'] = df['ex_date'].apply(
    lambda d: (safe_date(d) - datetime.timedelta(days=1)) if safe_date(d) else None)

cutoff = today + datetime.timedelta(days=days_ahead)
# Vectorized window filter -- no apply() overhead
_bd = pd.to_datetime(df['buy_date'].apply(safe_date), errors='coerce')
df_cal = df[_bd.notna() & (_bd.dt.date >= today) & (_bd.dt.date <= cutoff)].copy()
df_cal = df_cal.sort_values('yield_pct', ascending=False)

meta_txt = ('  |  Last scan: ' + str(meta.get('scanned_at_utc','--'))) if meta else ''
ex_found = df['ex_date'].notna().sum()
st.markdown('<div class="src-badge ' + badge_cls + '">&#x2713; ' +
    str(len(df_all)) + ' dividend stocks | ' + str(ex_found) + ' with ex-dates (' + src_label + ')' +
    meta_txt + '</div>', unsafe_allow_html=True)

tab_cal, tab_calc, tab_az = st.tabs(['Calendar', 'Calculator', 'Stock Analyzer'])

with tab_cal:
    # Sort once by (buy_date asc, yield desc) -- used for both nxt and table
    df_cal_sorted = df_cal.sort_values(['buy_date','yield_pct'], ascending=[True,False]).reset_index(drop=True) if not df_cal.empty else df_cal
    nxt = df_cal_sorted.iloc[0] if not df_cal.empty else None
    c1,c2,c3,c4 = st.columns(4)
    c1.metric('Buy signals ahead', len(df_cal))
    c2.metric('Avg yield', '{:.1f}%'.format(df_cal['yield_pct'].mean()) if not df_cal.empty else '--')
    c3.metric('Highest yield',
        '{:.1f}%'.format(df_cal['yield_pct'].max()) if not df_cal.empty else '--',
        delta=str(df_cal.iloc[0]['ticker']) if not df_cal.empty else '')
    nd = safe_date(nxt['buy_date']) if nxt is not None else None
    c4.metric('Next buy date', nd.strftime('%b %d') if nd else '--',
        delta=str(nxt['ticker']) if nxt is not None else '')
    st.markdown('---')

    if 'cy' not in st.session_state: st.session_state.cy = today.year
    if 'cm' not in st.session_state: st.session_state.cm = today.month
    cp,cc,cn = st.columns([1,5,1])
    with cp:
        if st.button('Prev', key='cal_prev'):
            if st.session_state.cm == 1: st.session_state.cy -= 1; st.session_state.cm = 12
            else: st.session_state.cm -= 1
    with cn:
        if st.button('Next', key='cal_next'):
            if st.session_state.cm == 12: st.session_state.cy += 1; st.session_state.cm = 1
            else: st.session_state.cm += 1
    with cc:
        st.markdown('<h3 style="text-align:center;font-family:DM Serif Display,serif;margin:0">' +
            cal_module.month_name[st.session_state.cm] + ' ' + str(st.session_state.cy) +
            '</h3>', unsafe_allow_html=True)
    st.markdown(
        '<div style="display:flex;gap:8px;margin:6px 0 4px;flex-wrap:wrap">'
        '<span style="background:#1a6b1a;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600">8%+</span>'
        '<span style="background:#2e7d32;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600">6%+</span>'
        '<span style="background:#827717;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600">4%+</span>'
        '<span style="background:#e65100;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600">2.5%+</span>'
        '<span style="background:#bf360c;color:#fff;padding:2px 10px;border-radius:3px;font-size:.7rem;font-weight:600">below 2.5%</span>'
        '</div>', unsafe_allow_html=True)
    render_calendar(df_cal, st.session_state.cy, st.session_state.cm)
    st.markdown('<br>', unsafe_allow_html=True)

    st.markdown('<div class="section-hdr">Upcoming Buy Signals &mdash; Ranked by Yield</div>', unsafe_allow_html=True)
    if df_cal.empty:
        st.info('No buy signals in the next ' + str(days_ahead) + ' days. Try widening the date range.')
    else:
        df_show = df_cal_sorted  # already sorted, no copy needed
        _dsa = pd.to_datetime(df_show['buy_date'].apply(safe_date), errors='coerce').dt.date
        df_show = df_show.assign(days_away=(_dsa - today).apply(lambda x: x.days if pd.notna(x) else 0))
        rows_html = []
        for row in df_show.to_dict('records'):
            yc  = ycolor(row['yield_pct'])
            bd  = safe_date(row.get('buy_date'))
            ex  = safe_date(row.get('ex_date'))
            da  = int(row['days_away'])
            dr  = float(row.get('div_rate') or 0)
            mp  = row.get('monthly_pay')
            px  = float(row.get('price') or 0)
            pr  = row.get('payout')
            freq = str(row.get('frequency') or '--')
            mp_str = ('$' + '{:.4f}'.format(mp)) if mp else '--'
            pr_str = '{:.0f}%'.format(pr) if pr else '--'
            bd_str = bd.strftime('%b %d, %Y') if bd else '--'
            ex_str = ex.strftime('%b %d, %Y') if ex else '--'
            alert = ''
            if da == 0:   alert = '<span class="buy-now">BUY TODAY</span>'
            elif da == 1: alert = '<span class="buy-tmr">BUY TOMORROW</span>'
            rows_html.append(
                '<tr class="tbl-row">'
                '<td class="td-ticker"><strong>' + row['ticker'] + '</strong></td>'
                '<td class="td-sector">' + str(row['sector']) + '</td>'
                '<td class="td-yield"><span class="yield-badge" style="--yc:' + yc + '">' + str(row['yield_pct']) + '%</span></td>'
                '<td class="td-num">$' + '{:.4f}'.format(dr) + '</td>'
                '<td class="td-num">' + mp_str + '</td>'
                '<td class="td-num">' + pr_str + '</td>'
                '<td class="td-freq">' + freq + '</td>'
                '<td class="td-num">$' + '{:.2f}'.format(px) + '</td>'
                '<td class="td-date">' + bd_str + '</td>'
                '<td class="td-date">' + ex_str + '</td>'
                '<td class="td-count">' + str(da) + 'd ' + alert + '</td>'
                '</tr>')
        tbl = ('<div class="tbl-wrap"><table class="stbl"><thead><tr>'
            '<th>Ticker</th><th>Sector</th><th>Yield</th><th>Div/Share</th>'
            '<th>Monthly/Share</th><th>Payout</th><th>Frequency</th>'
            '<th>Price</th><th>Buy Before</th><th>Ex-Date</th><th>Countdown</th>'
            '</tr></thead><tbody>' + ''.join(rows_html) + '</tbody></table></div>')
        st.markdown(tbl, unsafe_allow_html=True)

    st.markdown('<br>', unsafe_allow_html=True)
    with st.expander('Full dividend universe -- ' + str(len(df)) + ' stocks (' + str(len(df_all)) + ' total)'):
        st.dataframe(df[['ticker','sector','yield_pct','div_rate','monthly_pay','payout','frequency','price','ex_date','buy_date']]
            .rename(columns={'ticker':'Ticker','sector':'Sector','yield_pct':'Yield %',
                'div_rate':'Div/Share','monthly_pay':'Monthly/Share','payout':'Payout %',
                'frequency':'Frequency','price':'Price','ex_date':'Ex-Date','buy_date':'Buy Before'}),
            use_container_width=True, hide_index=True)

with tab_calc:
    st.markdown('<div class="section-hdr">Dividend Investment Calculator</div>', unsafe_allow_html=True)
    calc_mode = st.radio('Stock source', ['Pick from dividend list','Enter any ticker'], horizontal=True, key='calc_mode')
    calc_info = {}
    if calc_mode == 'Pick from dividend list':
        if df.empty: st.warning('No dividend stocks match current filters.'); st.stop()
        opts = [r['ticker'] + '  --  ' + str(r['yield_pct']) + '% yield  |  ' + str(r['frequency'])
            for r in df.sort_values('yield_pct', ascending=False).to_dict('records')]
        sel = st.selectbox('Select stock', opts, key='calc_sel')
        sel_tk = sel.split('  --  ')[0].strip()
        sel_row = df[df['ticker'] == sel_tk].iloc[0]
        calc_info = {'ticker': sel_tk, 'price': float(sel_row['price'] or 0),
            'yield_pct': float(sel_row['yield_pct']), 'div_rate': float(sel_row['div_rate'] or 0),
            'monthly_pay': sel_row.get('monthly_pay'), 'frequency': sel_row['frequency'],
            'payout': sel_row.get('payout'), 'sector': sel_row['sector']}
    else:
        ctk = st.text_input('Enter ticker symbol', placeholder='e.g. ET, EPD, DOC', key='calc_ticker')
        if ctk:
            with st.spinner('Fetching ' + ctk.upper() + '...'):
                li, _, _, le = fetch_stock_analysis(ctk)
            if le or not li: st.error('Could not fetch ' + ctk.upper() + '. Check ticker.')
            else:
                ry = li.get('trailingAnnualDividendYield') or li.get('dividendYield') or 0
                if ry > 0.50: ry = 0
                rr = float(li.get('trailingAnnualDividendRate') or li.get('dividendRate') or 0)
                calc_info = {'ticker': ctk.upper().strip(),
                    'price': float(li.get('currentPrice') or li.get('regularMarketPrice') or 0),
                    'yield_pct': round(ry * 100, 2), 'div_rate': rr,
                    'monthly_pay': round(rr / 12, 4) if rr else None,
                    'frequency': 'Quarterly (est)',
                    'payout': round((li.get('payoutRatio') or 0) * 100, 1) or None,
                    'sector': li.get('sector') or 'Unknown'}
    if calc_info and calc_info.get('price', 0) > 0:
        st.markdown('---')
        ci1, ci2 = st.columns([1, 1])
        with ci1:
            st.markdown('<div class="calc-card">', unsafe_allow_html=True)
            st.markdown('**' + calc_info['ticker'] + '** -- ' + calc_info['sector'])
            st.markdown('Annual yield: **' + str(calc_info['yield_pct']) + '%**  |  '
                'Price: **$' + '{:.2f}'.format(calc_info['price']) + '**  |  '
                'Frequency: **' + calc_info['frequency'] + '**')
            st.markdown('---')
            inv = st.number_input('Investment amount ($)', min_value=1.0,
                max_value=10000000.0, value=1000.0, step=100.0, format='%.2f', key='calc_inv')
            st.markdown('</div>', unsafe_allow_html=True)
        with ci2:
            px   = calc_info['price']
            dr   = calc_info['div_rate'] or 0
            shrs = inv / px if px > 0 else 0
            annd = shrs * dr
            mthd = annd / 12
            wkd  = annd / 52
            fl   = calc_info['frequency'].lower()
            if 'month' in fl:  spay, slbl = annd/12,  'Per monthly payment'
            elif 'semi'  in fl: spay, slbl = annd/2,   'Per semi-annual payment'
            elif 'annual' in fl and 'semi' not in fl: spay, slbl = annd, 'Per annual payment'
            else: spay, slbl = annd/4, 'Per quarterly payment'
            st.markdown(
                '<div class="calc-result">'
                '<div class="calc-result-row"><span class="calc-label">Shares purchased</span>'
                '<span class="calc-value">' + '{:.4f}'.format(shrs) + ' shares</span></div>'
                '<div class="calc-result-row"><span class="calc-label">' + slbl + '</span>'
                '<span class="calc-value">$' + '{:.2f}'.format(spay) + '</span></div>'
                '<div class="calc-result-row"><span class="calc-label">Monthly dividend income</span>'
                '<span class="calc-value">$' + '{:.2f}'.format(mthd) + '</span></div>'
                '<div class="calc-result-row"><span class="calc-label">Annual dividend income</span>'
                '<span class="calc-value big">$' + '{:.2f}'.format(annd) + '</span></div>'
                '<div class="calc-result-row"><span class="calc-label">Yield on investment</span>'
                '<span class="calc-value">' + str(calc_info['yield_pct']) + '%</span></div>'
                '<div class="calc-result-row"><span class="calc-label">Weekly income</span>'
                '<span class="calc-value">$' + '{:.2f}'.format(wkd) + '</span></div>'
                '</div>', unsafe_allow_html=True)
        st.markdown('---')
        st.markdown('#### Holding Period Projections')
        proj = [{'Hold': str(m) + ' month' + ('s' if m > 1 else ''),
            'Total Dividends': '$' + '{:.2f}'.format(mthd * m),
            'Return': '{:.2f}%'.format((mthd * m / inv) * 100),
            'Monthly Income': '$' + '{:.2f}'.format(mthd)}
            for m in [1,3,6,12,24,36,60]]
        st.dataframe(pd.DataFrame(proj), use_container_width=True, hide_index=True)
        st.markdown('#### Monthly Income Over 12 Months')
        st.line_chart(pd.DataFrame({'Month': range(1,13), 'Cumulative Dividends ($)': [mthd*m for m in range(1,13)]}).set_index('Month'), color='#cc0000')

with tab_az:
    st.markdown('<div class="section-hdr">Stock Analyzer</div>', unsafe_allow_html=True)
    st.markdown('Enter any ticker for a full breakdown with metric explanations.')
    az1, az2 = st.columns([2,3])
    with az1:
        az_ticker = st.text_input('Ticker symbol', placeholder='e.g. ET, WPM, DOC', key='az_ticker')
        az_btn = st.button('Analyze', type='primary', key='az_analyze')
    if az_btn and az_ticker:
        with st.spinner('Fetching ' + az_ticker.upper() + '...'):
            ai, ah, ad, ae = fetch_stock_analysis(az_ticker)
        if ae or not ai:
            st.error('Could not load data for ' + az_ticker.upper() + '. ' +
                'This may be a rate limit, invalid ticker, or yfinance outage. ' +
                'Try again in 30 seconds. Error: ' + str(ae or 'empty response'))
        else:
            sym   = az_ticker.upper().strip()
            name  = ai.get('longName') or ai.get('shortName') or sym
            sec   = ai.get('sector') or 'Unknown'
            ind   = ai.get('industry') or 'Unknown'
            px    = float(ai.get('currentPrice') or ai.get('regularMarketPrice') or
                         ai.get('navPrice') or ai.get('previousClose') or 0)
            mcap  = ai.get('marketCap') or 0
            pe    = ai.get('trailingPE')
            fwpe  = ai.get('forwardPE')
            pb    = ai.get('priceToBook')
            ps    = ai.get('priceToSalesTrailing12Months')
            ry    = ai.get('trailingAnnualDividendYield') or ai.get('dividendYield') or 0
            if ry > 0.50: ry = 0
            dy    = round(ry * 100, 2)
            dr    = float(ai.get('trailingAnnualDividendRate') or ai.get('dividendRate') or 0)
            pout  = ai.get('payoutRatio')
            ex_ts = ai.get('exDividendDate')
            ex_dt = None
            if ex_ts:
                try: ex_dt = datetime.datetime.utcfromtimestamp(float(ex_ts)).date()
                except Exception: pass
            hi52  = ai.get('fiftyTwoWeekHigh') or 0
            lo52  = ai.get('fiftyTwoWeekLow') or 0
            rng   = ((px - lo52)/(hi52 - lo52)*100) if (hi52 and lo52 and hi52 != lo52) else None
            beta  = ai.get('beta')
            spf   = ai.get('shortPercentOfFloat')
            sratio= ai.get('shortRatio')
            am    = ai.get('targetMeanPrice')
            al    = ai.get('targetLowPrice')
            ahigh = ai.get('targetHighPrice')
            nana  = ai.get('numberOfAnalystOpinions') or 0
            recky = ai.get('recommendationKey') or ''
            rg    = ai.get('revenueGrowth')
            eg    = ai.get('earningsGrowth')
            pm    = ai.get('profitMargins')
            om    = ai.get('operatingMargins')
            roe   = ai.get('returnOnEquity')
            roa   = ai.get('returnOnAssets')
            deq   = ai.get('debtToEquity')
            cr    = ai.get('currentRatio')
            aus   = ((am - px) / px * 100) if (am and px > 0) else None
            mp2   = dr / 12 if dr else 0
            pays_yr = 4; freq2 = 'Quarterly'
            if not ad.empty:
                oyr = pd.Timestamp.now(tz='UTC') - pd.DateOffset(years=1)
                rec = ad[ad.index >= oyr]; n = len(rec)
                if n >= 10:  pays_yr=12; freq2='Monthly'
                elif n >= 3: pays_yr=4;  freq2='Quarterly'
                elif n == 2: pays_yr=2;  freq2='Semi-Annual'
                elif n == 1: pays_yr=1;  freq2='Annual'
            rsi_v=ma50_v=ma200_v=macd_v=macd_s=vol_avg=vol_td=obv_tr=None
            pct1d=pct5d=pct1m=pct3m=None
            if not ah.empty and len(ah) >= 26:
                cl = ah['Close'].dropna(); vl = ah['Volume'].dropna()
                try:
                    dlt=cl.diff(); g=dlt.clip(lower=0).rolling(14).mean()
                    ls=(-dlt.clip(upper=0)).rolling(14).mean()
                    rs2=g/ls.replace(0,np.nan)
                    rs3=(100-(100/(1+rs2))).dropna()
                    rsi_v=float(rs3.iloc[-1]) if not rs3.empty else None
                except Exception: pass
                try:
                    e12=cl.ewm(span=12,adjust=False).mean()
                    e26=cl.ewm(span=26,adjust=False).mean()
                    ml=e12-e26; sl=ml.ewm(span=9,adjust=False).mean()
                    macd_v=float(ml.iloc[-1]); macd_s=float(sl.iloc[-1])
                except Exception: pass
                try:
                    if len(cl)>=50:  ma50_v=float(cl.rolling(50).mean().iloc[-1])
                    if len(cl)>=200: ma200_v=float(cl.rolling(200).mean().iloc[-1])
                except Exception: pass
                try:
                    if len(vl)>=20: vol_avg=float(vl.iloc[-20:].mean()); vol_td=float(vl.iloc[-1])
                except Exception: pass
                try:
                    obv=(np.sign(cl.diff().fillna(0))*vl).cumsum()
                    obv_tr='rising' if np.polyfit(range(20),obv.iloc[-20:].values,1)[0]>0 else 'falling'
                except Exception: pass
                try:
                    if len(cl)>=2:  pct1d=(float(cl.iloc[-1])-float(cl.iloc[-2]))/float(cl.iloc[-2])*100
                    if len(cl)>=6:  pct5d=(float(cl.iloc[-1])-float(cl.iloc[-6]))/float(cl.iloc[-6])*100
                    if len(cl)>=22: pct1m=(float(cl.iloc[-1])-float(cl.iloc[-22]))/float(cl.iloc[-22])*100
                    if len(cl)>=66: pct3m=(float(cl.iloc[-1])-float(cl.iloc[-66]))/float(cl.iloc[-66])*100
                except Exception: pass

            pills = []
            if rsi_v is not None:
                if rsi_v < 30: pills.append(pill('RSI Oversold', True))
                elif rsi_v > 70: pills.append(pill('RSI Overbought', False))
                elif 45 < rsi_v < 65: pills.append(pill('RSI Sweet Spot', True))
                else: pills.append(pill('RSI Neutral', None))
            if macd_v is not None and macd_s is not None:
                pills.append(pill('MACD Bullish' if macd_v > macd_s else 'MACD Bearish', macd_v > macd_s))
            if ma50_v and ma200_v:
                pills.append(pill('Golden Cross' if ma50_v>ma200_v else 'Death Cross', ma50_v>ma200_v))
            if ma50_v and px:
                ab=(px-ma50_v)/ma50_v*100
                if 0<ab<5: pills.append(pill('Near MA50 Support', True))
                elif ab<0: pills.append(pill('Below MA50', False))
            if vol_avg and vol_td:
                if vol_td>vol_avg*1.5: pills.append(pill('High Volume', True))
                elif vol_td<vol_avg*0.5: pills.append(pill('Low Volume', None))
            if spf and spf>0.15: pills.append(pill('High Short Interest', None))
            if aus and aus>15: pills.append(pill('Analyst Upside '+'{:.0f}'.format(aus)+'%', True))
            if pout and pout>1: pills.append(pill('Payout > Earnings', False))
            if beta and beta>1.5: pills.append(pill('High Volatility', None))
            if beta and beta<0.6: pills.append(pill('Low Volatility', True))

            h1,h2,h3,h4,h5,h6 = st.columns(6)
            h1.metric('Price', '$'+'{:.2f}'.format(px))
            h2.metric('Div Yield', str(dy)+'%')
            h3.metric('Monthly/Share', '$'+'{:.4f}'.format(mp2) if mp2 else '--')
            h4.metric('Ex-Date', ex_dt.strftime('%b %d, %Y') if ex_dt else '--')
            h5.metric('Frequency', freq2)
            h6.metric('Analyst Target', '$'+'{:.2f}'.format(am) if am else '--',
                delta=('{:.1f}%'.format(aus) if aus else None))
            st.markdown('<div style="margin:6px 0 4px"><strong>' + name + '</strong>'
                '  <span style="color:#666;font-size:.82rem">' + sec + ' / ' + ind + '</span></div>',
                unsafe_allow_html=True)
            if pills:
                st.markdown('<div style="margin:6px 0 14px">' + ''.join(pills) + '</div>', unsafe_allow_html=True)
            st.markdown('---')
            colA, colB, colC = st.columns(3)

            with colA:
                st.markdown('<div class="az-section">Price History (1Y)</div>', unsafe_allow_html=True)
                if not ah.empty:
                    cd = ah[['Close']].copy()
                    if ma50_v and len(ah)>=50:  cd['MA50']  = ah['Close'].rolling(50).mean()
                    if ma200_v and len(ah)>=200: cd['MA200'] = ah['Close'].rolling(200).mean()
                    st.line_chart(cd, height=200)
                else: st.info('No price history.')
                st.markdown('<div class="az-section">Price Performance</div>', unsafe_allow_html=True)
                def pf(v):
                    if v is None: return '--'
                    c='#7fff7f' if v>=0 else '#ff9999'
                    return '<span style="color:'+c+';font-family:DM Mono,monospace">'+('+'if v>=0 else '')+'{:.2f}%'.format(v)+'</span>'
                pr_rows = [('1 Day','1 Day = one trading day. How much the stock price moved today compared to yesterday. A positive number means the stock went up; negative means it went down.',pf(pct1d)),
                    ('5 Day','5 Day = five trading days (roughly one calendar week). Shows short-term price momentum. If positive and rising, buyers are in control over the near term.',pf(pct5d)),
                    ('1 Month','1 Month = approximately 22 trading days. Shows the near-term trend. A stock that is up over one month but down on the day may just be pulling back within a larger uptrend.',pf(pct1m)),
                    ('3 Month','3 Month = approximately 66 trading days (one quarter). Shows the medium-term trend direction. This is the timeframe most institutional investors use to evaluate recent performance.',pf(pct3m))]
                pr_html = '<table style="width:100%;border-collapse:collapse"><tbody>'
                for lbl,tt,val in pr_rows:
                    pr_html += '<tr class="mrow"><td class="mrow-label">' + tip(lbl,tt) + '</td><td class="mrow-val">' + val + '</td></tr>'
                pr_html += '</tbody></table>'
                st.markdown(pr_html, unsafe_allow_html=True)
                st.markdown('<div class="az-section">Dividend History (Last 12)</div>', unsafe_allow_html=True)
                if not ad.empty:
                    ddf = ad.reset_index(); ddf.columns=['Date','Dividend']
                    ddf['Date'] = pd.to_datetime(ddf['Date']).dt.date
                    st.dataframe(ddf.tail(12), use_container_width=True, hide_index=True)
                else: st.info('No dividend history.')

            with colB:
                st.markdown('<div class="az-section">Technical Signals</div>', unsafe_allow_html=True)
                tech_rows = []
                def vo(v, fmt='{:.2f}', fb='--'): return fmt.format(v) if v is not None else fb
                if rsi_v is not None:
                    if rsi_v<30: ri='Oversold -- potential bounce coming'; rc='#7fff7f'
                    elif rsi_v<45: ri='Weak -- losing momentum'; rc='#ff9999'
                    elif rsi_v<55: ri='Neutral -- no clear direction'; rc='#ccc'
                    elif rsi_v<70: ri='Strong -- uptrend confirmed'; rc='#7fff7f'
                    else: ri='Overbought -- pullback possible'; rc='#ff9999'
                    tech_rows.append(mrow('RSI (14-day)',
                        'RSI = Relative Strength Index. A momentum indicator scored 0-100. Below 30 = oversold, the stock has fallen too far too fast and may bounce. Above 70 = overbought, may have risen too fast and a pullback is likely. 45-70 is the sweet spot -- strong upward momentum without being overheated.',
                        '<span style="font-family:DM Mono,monospace;color:'+rc+'">'+vo(rsi_v,'{:.1f}')+'</span> <span style="font-size:.72rem;color:#888">'+ri+'</span>'))
                if macd_v is not None:
                    mi = 'Bullish -- momentum building' if macd_v>macd_s else 'Bearish -- momentum fading'
                    mc2 = '#7fff7f' if macd_v>macd_s else '#ff9999'
                    tech_rows.append(mrow('MACD',
                        'MACD = Moving Average Convergence Divergence. Compares a 12-day and 26-day exponential moving average (EMA) of the price. When the MACD line is above its 9-day signal line, buyers are in control. Crossing above the signal line is a classic buy signal. Crossing below is a sell signal.',
                        '<span style="font-family:DM Mono,monospace;color:'+mc2+'">'+vo(macd_v,'{:.4f}')+'</span> <span style="font-size:.72rem;color:#888">'+mi+'</span>'))
                if ma50_v:
                    pvs=(px-ma50_v)/ma50_v*100
                    if pvs>5: m5i='Extended above -- may be overbought'; m5c='#ccc'
                    elif pvs>0: m5i='Just above -- ideal entry zone'; m5c='#7fff7f'
                    elif pvs>-5: m5i='Just below -- watch for reclaim'; m5c='#ffe066'
                    else: m5i='Well below -- downtrend'; m5c='#ff9999'
                    tech_rows.append(mrow('50-Day MA',
                        'MA = Moving Average. The 50-Day MA is the average closing price over the past 50 trading days (~2.5 months). It smooths out daily noise to show the short-to-medium trend. When price is just above the 50-Day MA, it often acts as a floor of support -- this is frequently the ideal low-risk entry point for an uptrending stock.',
                        '$'+vo(ma50_v)+' <span style="color:'+m5c+';font-size:.72rem">('+('+'if pvs>=0 else '')+'{:.1f}%'.format(pvs)+')</span> <span style="font-size:.72rem;color:#888">'+m5i+'</span>'))
                if ma200_v:
                    m2i='Golden Cross -- long-term uptrend' if (ma50_v and ma50_v>ma200_v) else 'Death Cross -- long-term downtrend'
                    m2c='#7fff7f' if (ma50_v and ma50_v>ma200_v) else '#ff9999'
                    tech_rows.append(mrow('200-Day MA',
                        'MA = Moving Average. The 200-Day MA is the average closing price over the past 200 trading days (~10 months). It is the most widely watched long-term trend line. When the 50-Day MA crosses above the 200-Day MA, that is called the Golden Cross -- a major bullish signal used by large institutions to open long positions. The reverse crossing is called the Death Cross -- a bearish signal.',
                        '$'+vo(ma200_v)+' <span style="font-size:.72rem;color:'+m2c+'">'+m2i+'</span>'))
                if vol_avg and vol_td:
                    vr=vol_td/vol_avg
                    if vr>1.5: vi='High volume -- strong conviction'; vc='#7fff7f'
                    elif vr>1: vi='Above average -- buyers engaged'; vc='#ccc'
                    elif vr>0.5: vi='Below average -- quiet session'; vc='#888'
                    else: vi='Very low -- no conviction'; vc='#888'
                    tech_rows.append(mrow('Volume',
                        'Volume = the total number of shares bought and sold in a given trading session. Shown here as a multiple of the 20-day average volume (e.g. 1.5x means 50% more shares than usual traded today). High volume confirms a price move has conviction behind it -- many investors agree. Low volume moves are unreliable and often reverse. Volume spikes frequently precede or confirm major breakouts.',
                        '<span style="color:'+vc+';font-family:DM Mono,monospace">'+'{:.1f}x avg'.format(vr)+'</span> <span style="font-size:.72rem;color:#888">'+vi+'</span>'))
                if obv_tr:
                    oc='#7fff7f' if obv_tr=='rising' else '#ff9999'
                    tech_rows.append(mrow('OBV Trend',
                        'OBV = On-Balance Volume. A cumulative indicator that adds volume on up-days and subtracts it on down-days. A rising OBV means more shares are trading on days the stock goes up -- this signals that large institutions are quietly accumulating (buying) the stock even if the price has not moved much yet. A falling OBV means distribution -- big money is selling into strength, which often precedes a price decline.',
                        '<span style="color:'+oc+';font-family:DM Mono,monospace">'+obv_tr.capitalize()+'</span>'))
                if rng is not None:
                    if rng<25: rni='Near 52W low -- historically cheap'; rnc='#7fff7f'
                    elif rng<50: rni='Lower half -- value zone'; rnc='#ccc'
                    elif rng<75: rni='Upper half -- momentum zone'; rnc='#ccc'
                    else: rni='Near 52W high -- extended or breakout'; rnc='#ffe066'
                    tech_rows.append(mrow('52W Range Position',
                        '52W = 52-Week (one full year). Shows where the current price sits within its yearly high-low range. 0% = trading at the 52-week low. 100% = trading at the 52-week high. Stocks near the low end often offer better value and a higher effective dividend yield on your purchase price. Stocks near the high end may be breaking out to new highs or may be overextended and due for a pullback.',
                        '<span style="color:'+rnc+';font-family:DM Mono,monospace">'+'{:.0f}% of range'.format(rng)+'</span><span style="font-size:.72rem;color:#888;display:block">$'+'{:.2f}'.format(lo52)+' -- $'+'{:.2f}'.format(hi52)+'</span><span style="font-size:.72rem;color:#888">'+rni+'</span>'))
                if tech_rows:
                    st.markdown('<table style="width:100%;border-collapse:collapse"><tbody>' + ''.join(tech_rows) + '</tbody></table>', unsafe_allow_html=True)
                else: st.info('Not enough price history for technical signals.')

            with colC:
                st.markdown('<div class="az-section">Dividend Metrics</div>', unsafe_allow_html=True)
                div_rows = [
                    mrow('Annual Yield',
                        'Annual Yield = Dividend Yield. The total annual dividend payments divided by the current stock price, expressed as a percentage. A 6% yield means for every $100 you invest, you receive $6 per year in dividends. Very high yields above 15% can signal the dividend is at risk of being cut -- often called a yield trap.',
                        tag(dy,6,3,'{:.2f}','%')),
                    mrow('Annual Div/Share',
                        'Annual Div/Share = Annual Dividend Per Share. The total dollar amount paid in dividends for each share you own over the past 12 months (trailing twelve months or TTM). Multiply this by your number of shares to get your total annual dividend income.',
                        ('$'+'{:.4f}'.format(dr)) if dr else '--'),
                    mrow('Monthly/Share',
                        'Monthly/Share = Monthly Dividend Per Share. The annual dividend rate divided by 12, giving you the equivalent monthly income per share. Useful for monthly income planning regardless of whether the stock actually pays monthly, quarterly, or annually. Multiply by your share count to get total monthly income.',
                        ('$'+'{:.4f}'.format(mp2)) if mp2 else '--'),
                    mrow('Payment Frequency',
                        'How often you receive a dividend payment. Monthly=12 payments/year. Quarterly=4 payments/year. Less frequent means longer gaps between income.',
                        freq2),
                    mrow('Ex-Dividend Date',
                        'Ex-Dividend Date (also called Ex-Date). The cutoff date set by the company. You must own the stock BEFORE this date to qualify for the upcoming dividend payment. If you buy ON or AFTER the ex-date, you miss that payment. The stock price typically drops by approximately the dividend amount on this date as the value of that payment leaves the stock.',
                        ex_dt.strftime('%b %d, %Y') if ex_dt else '--'),
                    mrow('Payout Ratio',
                        'Payout Ratio = Dividend Payout Ratio. What percentage of the company&apos;s net earnings (EPS) is paid out as dividends. Under 60% is generally sustainable -- the company keeps plenty of earnings to reinvest and grow. 60-80% is a yellow flag. Over 100% means the company is paying MORE in dividends than it earns -- this is unsustainable and a dividend cut is likely.',
                        tag((pout or 0)*100,80,100,'{:.0f}','%') if pout else '--'),
                ]
                st.markdown('<table style="width:100%;border-collapse:collapse"><tbody>' + ''.join(div_rows) + '</tbody></table>', unsafe_allow_html=True)

                st.markdown('<div class="az-section">Valuation</div>', unsafe_allow_html=True)
                mcstr = ('$'+'{:.1f}B'.format(mcap/1e9) if mcap>=1e9 else '$'+'{:.0f}M'.format(mcap/1e6) if mcap>=1e6 else '--')
                val_rows = [
                    mrow('Market Cap','Market Cap = Market Capitalization. The total dollar value of all outstanding shares (share price x total shares). Large-cap above $10B = established, stable companies. Mid-cap $2-10B = growing companies with moderate risk. Small-cap below $2B = higher growth potential but also higher risk and volatility.',mcstr),
                    mrow('P/E (Trailing)','P/E = Price-to-Earnings Ratio. Trailing P/E uses actual earnings from the past 12 months (TTM = Trailing Twelve Months). Calculated as: Stock Price / EPS (Earnings Per Share). A P/E of 15x means you pay $15 for every $1 the company earns annually. Lower P/E can mean the stock is cheap relative to profits. Very high P/E means investors expect strong future growth -- or the stock is overvalued.','{:.1f}x'.format(pe) if pe else '--'),
                    mrow('P/E (Forward)','Forward P/E uses analyst-estimated earnings for the next 12 months instead of past earnings. If forward P/E is lower than trailing P/E, earnings are expected to grow -- a bullish signal. If forward P/E is higher than trailing, earnings are expected to shrink -- a warning sign. NTM = Next Twelve Months is another term for forward P/E.','{:.1f}x'.format(fwpe) if fwpe else '--'),
                    mrow('Price/Book','P/B = Price-to-Book Ratio. Compares the stock price to the company&apos;s book value (net assets = total assets minus total liabilities). Under 1x means the stock is trading below the value of what the company actually owns -- potentially very undervalued. 1-3x is typical for most healthy companies. Very high P/B means the market values intangibles like brand, patents, or future growth.','{:.2f}x'.format(pb) if pb else '--'),
                    mrow('Price/Sales','P/S = Price-to-Sales Ratio (also P/Rev). Compares the stock price to revenue per share. Useful when earnings are negative since every company has revenue. Under 1x = generally cheap. Under 2x = reasonable. Above 10x = investors are paying a huge premium for future growth potential -- common in high-growth tech but risky if growth slows.','{:.2f}x'.format(ps) if ps else '--'),
                    mrow('Beta','Beta measures a stock&apos;s price volatility relative to the overall market (S&P 500). Beta 1.0 = moves exactly with the market. Beta 1.5 = moves 50% more than the market in both directions -- bigger gains AND bigger drops. Beta 0.5 = moves half as much as the market -- stable, defensive. Low-beta stocks (utilities, REITs, consumer staples) are favored during recessions and bear markets.','{:.2f}'.format(beta) if beta else '--'),
                ]
                st.markdown('<table style="width:100%;border-collapse:collapse"><tbody>' + ''.join(val_rows) + '</tbody></table>', unsafe_allow_html=True)

                st.markdown('<div class="az-section">Financial Health</div>', unsafe_allow_html=True)
                hlth_rows = [
                    mrow('Profit Margin','Net Profit Margin = Net Income / Revenue. How many cents of profit the company keeps for every dollar of revenue after ALL expenses including taxes and interest. A 20% margin means the company pockets $20 profit from every $100 in sales. Higher margins signal strong pricing power and operational efficiency. Shrinking margins over time are a warning sign of rising costs or increasing competition.',tag((pm or 0)*100,15,5,'{:.1f}','%') if pm else '--'),
                    mrow('Operating Margin','EBIT Margin = Operating Income / Revenue. Similar to profit margin but measured BEFORE interest payments and taxes -- shows how efficient the core business is at turning revenue into profit. If operating margin is high but net profit margin is low, the company likely carries heavy debt (high interest costs eating into profits). Useful for comparing companies with different debt levels.',tag((om or 0)*100,15,5,'{:.1f}','%') if om else '--'),
                    mrow('Return on Equity','ROE = Return on Equity = Net Income / Shareholders Equity. Measures how effectively the company uses shareholder money to generate profit. A 20% ROE means for every $100 of equity, the company earns $20 in profit. Above 15% is considered strong. Warren Buffett looks for consistently high ROE as a sign of durable competitive advantage (a moat). Watch for ROE inflated by high debt.',tag((roe or 0)*100,15,8,'{:.1f}','%') if roe else '--'),
                    mrow('Return on Assets','ROA = Return on Assets = Net Income / Total Assets. Shows how much profit the company generates relative to everything it owns (cash, factories, equipment, intellectual property). Unlike ROE, ROA is not inflated by debt. Above 5% is solid. Asset-heavy industries like utilities and manufacturing typically have lower ROA than software or consumer brands.',tag((roa or 0)*100,8,3,'{:.1f}','%') if roa else '--'),
                    mrow('D/E Ratio','D/E = Debt-to-Equity Ratio = Total Debt / Shareholders Equity. Shows how much the company relies on borrowed money vs its own capital. A D/E of 200% means the company has $2 of debt for every $1 of equity. High D/E is dangerous when earnings fall because interest payments are fixed -- it amplifies losses. Some capital-intensive industries (utilities, pipelines, REITs) routinely carry high D/E because their predictable cash flows can service the debt.','{:.1f}%'.format(deq) if deq else '--'),
                    mrow('Current Ratio','Current Ratio = Current Assets / Current Liabilities. A liquidity measure answering: can the company pay its bills due within the next 12 months? Current assets include cash, receivables, and inventory. Current liabilities include payables and short-term debt. Above 1.5 = comfortable -- plenty of buffer. 1.0-1.5 = manageable but watch closely. Below 1.0 = a warning sign -- the company may struggle to meet near-term financial obligations.',tag(cr or 0,1.5,1.0,'{:.2f}') if cr else '--'),
                ]
                st.markdown('<table style="width:100%;border-collapse:collapse"><tbody>' + ''.join(hlth_rows) + '</tbody></table>', unsafe_allow_html=True)

                st.markdown('<div class="az-section">Short Interest &amp; Growth</div>', unsafe_allow_html=True)
                si_rows = [
                    mrow('Short % Float','Short % of Float = the percentage of a company&apos;s freely tradeable shares (float) that are currently sold short. Short sellers borrow and sell shares betting the price will fall. Above 10% = significant bearish conviction. Above 20% = very heavily shorted -- but this also sets up a potential short squeeze: if the stock rises, short sellers must buy to cover losses, which forces the price even higher. Often called short interest as a percentage of float.',tag((spf or 0)*100,20,10,'{:.1f}','%') if spf else '--'),
                    mrow('Days to Cover','Days to Cover = Short Interest / Average Daily Volume. Also called the Short Ratio. Estimates how many average trading days it would take ALL short sellers to buy back their shares and exit their positions. High days-to-cover (above 5) means short sellers are effectively trapped -- if good news hits and the stock starts rising, they are forced to buy to limit losses. This forced buying pushes the price higher still, which is the mechanism of a short squeeze.',tag(sratio or 0,5,3,'{:.1f}','d') if sratio else '--'),
                    mrow('Revenue Growth','YoY Revenue Growth = Year-over-Year change in total revenue (the top line of the income statement). A growing top line means the company is selling more products or services and expanding its business. This is the foundation for long-term stock appreciation. Consistent double-digit revenue growth is highly attractive. Negative growth means the business is shrinking.',tag((rg or 0)*100,10,3,'{:.1f}','%') if rg else '--'),
                    mrow('Earnings Growth','YoY EPS Growth = Year-over-Year change in Earnings Per Share (EPS = Net Income / Shares Outstanding). If earnings grow faster than revenue, the company is becoming more efficient and profitable -- a sign of a strengthening business. If earnings shrink while revenue grows, rising costs are eating into profits. EPS growth is what ultimately drives dividend increases and stock price appreciation over time.',tag((eg or 0)*100,10,3,'{:.1f}','%') if eg else '--'),
                ]
                st.markdown('<table style="width:100%;border-collapse:collapse"><tbody>' + ''.join(si_rows) + '</tbody></table>', unsafe_allow_html=True)

                if am:
                    st.markdown('<div class="az-section">Analyst Consensus</div>', unsafe_allow_html=True)
                    rdisp = recky.replace('_',' ').title() if recky else '--'
                    rcol = '#7fff7f' if 'buy' in recky.lower() else ('#ff9999' if 'sell' in recky.lower() else '#ffe066')
                    an_rows = [
                        mrow('Recommendation','Wall Street Consensus Rating. Aggregates the buy/sell/hold ratings from all analysts who cover the stock. Strong Buy = most analysts expect the stock to significantly outperform the market. Buy = expected to outperform. Hold = expected to match the market. Underperform/Sell = expected to lag. Note: analysts employed by banks often have conflicts of interest -- their buy ratings outnumber sells by a large ratio. Use as one data point, not gospel.',
                            '<span style="color:'+rcol+';font-weight:600">'+rdisp+'</span> <span style="font-size:.72rem;color:#888">('+str(nana)+' analysts)</span>'),
                        mrow('Price Target (Mean)','Mean (Average) Price Target = the average 12-month price target across all analysts covering the stock. Represents the consensus expectation for where the stock price will be in one year. The percentage shown is the implied upside or downside from the current price. Treat this as directional guidance -- analysts frequently revise targets and are often wrong on timing.',
                            '$'+'{:.2f}'.format(am)+(
                            ' <span style="font-size:.72rem;color:'+('#7fff7f' if aus and aus>0 else '#ff9999')+'">'+('+'if aus and aus>=0 else '')+'{:.1f}%'.format(aus)+' from current</span>' if aus is not None else '')),
                        mrow('Target Range','Analyst Price Target Range = the spread from the most bearish analyst&apos;s low target to the most bullish analyst&apos;s high target. A wide range (e.g. $10 to $50) means analysts fundamentally disagree about the company&apos;s prospects -- high uncertainty. A narrow range (e.g. $20 to $24) means there is strong consensus and the outlook is well-understood. Wide ranges often occur around companies undergoing major change.',
                            ('$'+'{:.2f}'.format(al)+' -- $'+'{:.2f}'.format(ahigh)) if (al and ahigh) else '--'),
                    ]
                    st.markdown('<table style="width:100%;border-collapse:collapse"><tbody>' + ''.join(an_rows) + '</tbody></table>', unsafe_allow_html=True)

            st.markdown('---')
            st.markdown('<div class="section-hdr" style="font-size:1rem">Quick Calculator</div>', unsafe_allow_html=True)
            qc1, qc2 = st.columns([1,2])
            with qc1:
                azinv = st.number_input('Investment ($)', min_value=1.0, value=1000.0, step=100.0, format='%.2f', key='az_invest')
            with qc2:
                if px > 0 and dr > 0:
                    azsh=azinv/px; aza=azsh*dr; azm=aza/12; azw=aza/52
                    azup=(azinv*aus/100) if aus else None
                    st.markdown(
                        '<div class="calc-result">'
                        '<div class="calc-result-row"><span class="calc-label">Shares purchased</span><span class="calc-value">'+'{:.4f}'.format(azsh)+'</span></div>'
                        '<div class="calc-result-row"><span class="calc-label">Monthly dividend income</span><span class="calc-value">$'+'{:.2f}'.format(azm)+'</span></div>'
                        '<div class="calc-result-row"><span class="calc-label">Annual dividend income</span><span class="calc-value big">$'+'{:.2f}'.format(aza)+'</span></div>'
                        '<div class="calc-result-row"><span class="calc-label">Weekly income</span><span class="calc-value">$'+'{:.2f}'.format(azw)+'</span></div>'
                        +((
                        '<div class="calc-result-row"><span class="calc-label">Analyst price upside ($)</span><span class="calc-value">$'+'{:.2f}'.format(azup)+'</span></div>'
                        ) if azup else '')+
                        '</div>', unsafe_allow_html=True)
                else: st.info('No dividend data available for this ticker.')

st.markdown(
    '<hr><p style="font-size:.7rem;color:#ccc;text-align:center">'
    'github.com/magicpro33/stock | data updated nightly via GitHub Actions | '
    'Not financial advice | Always verify ex-dates before trading'
    '</p>', unsafe_allow_html=True)
