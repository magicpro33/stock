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
APP_VERSION = '2026-06-01b'  # bump when deploying -- verify in sidebar footer

def safe_date(v):
    if v is None:
        return None
    if isinstance(v, datetime.datetime):
        return v.date()
    if isinstance(v, datetime.date):
        return v
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        if isinstance(v, pd.Timestamp):
            return v.date()
    except Exception:
        pass
    try:
        ts = pd.to_datetime(v, errors='coerce')
        if ts is not pd.NaT and not pd.isna(ts):
            return ts.date()
    except Exception:
        pass
    return None

def _in_buy_window(bd, start, end):
    d = safe_date(bd)
    return d is not None and start <= d <= end

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

# ══ Dividend / date math helpers ═══════════════════════════════════════════
# yfinance changed dividendYield from decimal (0.0314) to percent (3.14) in
# 2025. trailingAnnualDividendYield is still decimal. Normalize both to decimal.
def _norm_yield(v):
    try: v = float(v)
    except (TypeError, ValueError): return None
    if v <= 0: return None
    if v > 1.0: v = v / 100.0          # value was percent-scaled
    return v if 0 < v <= 0.60 else None

# Scan stores growth as a decimal already (0.124 = 12.4%). Some feeds send
# percent (12.4). Disambiguate by magnitude -- >150% growth is vanishingly rare.
def _norm_growth(v):
    try: v = float(v)
    except (TypeError, ValueError): return None
    if v == 0: return None
    return v / 100.0 if abs(v) > 1.5 else v

def _pct_to_dec(v):
    try: v = float(v)
    except (TypeError, ValueError): return None
    return v / 100.0 if v else None

# Yield the UI shows must equal Div/Share divided by Price, or users see a
# contradiction. Prefer that; fall back to the reported yield field.
def _resolve_yield(info, price):
    try: rate = float(info.get('trailingAnnualDividendRate') or info.get('dividendRate') or 0)
    except (TypeError, ValueError): rate = 0.0
    from_rate = (rate / price) if (rate > 0 and price and price > 0) else None
    reported = (_norm_yield(info.get('trailingAnnualDividendYield'))
                or _norm_yield(info.get('dividendYield')))
    if from_rate and 0 < from_rate <= 0.60:
        return from_rate, reported
    return reported, reported

def _epoch_to_date(ts):
    try: ts = float(ts)
    except (TypeError, ValueError): return None
    if ts < 1e9: return None
    try:
        return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).date()
    except Exception:
        return None

# ── NYSE trading calendar (settlement is T+1 since May 2024, so the last day
# ── to buy and still receive the dividend is the prior TRADING day) ─────────
def _easter(y):
    a=y%19; b=y//100; c=y%100; d=b//4; e=b%4; f=(b+8)//25; g=(b-f+1)//3
    h=(19*a+b-d-g+15)%30; i=c//4; k=c%4; l=(32+2*e+2*i-h-k)%7
    m=(a+11*h+22*l)//451
    return datetime.date(y, (h+l-7*m+114)//31, ((h+l-7*m+114)%31)+1)

def _nth_weekday(y, month, weekday, n):
    d = datetime.date(y, month, 1)
    return d + datetime.timedelta(days=(weekday - d.weekday()) % 7 + 7*(n-1))

def _last_weekday(y, month, weekday):
    nxt = datetime.date(y+1,1,1) if month == 12 else datetime.date(y, month+1, 1)
    d = nxt - datetime.timedelta(days=1)
    while d.weekday() != weekday: d -= datetime.timedelta(days=1)
    return d

_HOL_CACHE = {}
def _market_holidays(y):
    if y in _HOL_CACHE: return _HOL_CACHE[y]
    def obs(d):
        if d.weekday() == 5: return d - datetime.timedelta(days=1)
        if d.weekday() == 6: return d + datetime.timedelta(days=1)
        return d
    hs = {
        obs(datetime.date(y,1,1)),                        # New Year
        _nth_weekday(y,1,0,3),                            # MLK
        _nth_weekday(y,2,0,3),                            # Presidents
        _easter(y) - datetime.timedelta(days=2),          # Good Friday
        _last_weekday(y,5,0),                             # Memorial
        obs(datetime.date(y,6,19)),                       # Juneteenth
        obs(datetime.date(y,7,4)),                        # Independence
        _nth_weekday(y,9,0,1),                            # Labor
        _nth_weekday(y,11,3,4),                           # Thanksgiving
        obs(datetime.date(y,12,25)),                      # Christmas
    }
    _HOL_CACHE[y] = hs
    return hs

def _prev_trading_day(d):
    if d is None: return None
    d = d - datetime.timedelta(days=1)
    for _ in range(10):
        if d.weekday() < 5 and d not in _market_holidays(d.year):
            return d
        d -= datetime.timedelta(days=1)
    return d

def _freq_to_pays(freq_label):
    f = (freq_label or '').lower()
    if 'month' in f: return 12
    if 'semi'  in f: return 2
    if 'annual' in f: return 1
    if 'quarter' in f: return 4
    return 4

# The scan emits Monthly / Quarterly / Quarterly (est) / Semi-Annual / Annual /
# Irregular / Unknown / None. Collapse them into five pickable buckets so the
# filter is exact -- substring matching wrongly counted Semi-Annual as Annual.
_FREQ_BUCKETS = ['Monthly', 'Quarterly', 'Semi-Annual', 'Annual', 'Other']

def _freq_bucket(freq_label):
    f = (freq_label or '').lower()
    if 'month' in f:   return 'Monthly'
    if 'semi' in f:    return 'Semi-Annual'
    if 'quarter' in f: return 'Quarterly'
    if 'annual' in f:  return 'Annual'
    return 'Other'

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

def _parse_ex_date(ex_ts):
    if not ex_ts:
        return None
    try:
        ts = float(ex_ts)
        if ts > 1e9:
            return _epoch_to_date(ts)
    except (TypeError, ValueError):
        pass
    return None

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
        ex_date = _parse_ex_date(item.get('ExDividendDate'))
        if ex_date:
            ex_count += 1
        rows.append({'ticker': ticker, 'sector': item.get('Sector') or 'Unknown',
            'price': item.get('Price'), 'yield_pct': round(yp, 2),
            'div_rate': dr, 'monthly_pay': mp, 'payout': pr,
            'frequency': freq, 'freq_bucket': _freq_bucket(freq),
            'div_score': float(item.get('DividendScore') or 0),
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
                result[tk] = _epoch_to_date(ex_ts)
            else: result[tk] = None
        except Exception: result[tk] = None
        prog.progress((i + 1) / len(tickers), text='Fetching ex-dates... ' + str(i+1) + '/' + str(len(tickers)))
        time.sleep(0.08)
    prog.empty()
    return result

@st.cache_data(ttl=1800, show_spinner=False)
def _load_scan_dict():
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with gzip.open(DATA_FILE, 'rt', encoding='utf-8') as f:
            raw = json.load(f)
        return {x['Ticker']: x for x in raw if isinstance(x, dict) and x.get('Ticker')}
    except Exception:
        return {}

def _hist_to_df(hist_dict):
    if not hist_dict or not isinstance(hist_dict, dict):
        return pd.DataFrame()
    try:
        df = pd.DataFrame({
            'Open':   hist_dict.get('open', []),
            'High':   hist_dict.get('high', []),
            'Low':    hist_dict.get('low', []),
            'Close':  hist_dict.get('close', []),
            'Volume': hist_dict.get('volume', []),
        }, index=pd.to_datetime(hist_dict.get('dates', [])))
        return df.dropna(subset=['Close'])
    except Exception:
        return pd.DataFrame()

# ══ Data providers ═════════════════════════════════════════════════════════
# Field-level waterfall. Each provider fills only the fields still empty, so a
# single flaky source never blanks the page. Provenance is tracked per field.
#
# Optional keys in Streamlit secrets (Settings -> Secrets). All are optional;
# every one you add raises the fill rate:
#   APCA_API_KEY_ID / APCA_API_SECRET_KEY   Alpaca  - price, bars, dividends
#   FMP_API_KEY                             Financial Modeling Prep - ratios
#   FINNHUB_API_KEY                         Finnhub - metrics, analyst targets
#   ANTHROPIC_API_KEY                       Claude web search - last resort

_ALPACA_DATA = 'https://data.alpaca.markets'
_ALPACA_TRADE = 'https://api.alpaca.markets'
_ALPACA_PAPER = 'https://paper-api.alpaca.markets'

# Every field the analyzer renders.
_YF_FIELDS = [
    'longName', 'sector', 'industry',
    'currentPrice', 'marketCap',
    'trailingPE', 'forwardPE',
    'priceToBook', 'priceToSalesTrailing12Months',
    'beta', 'profitMargins', 'operatingMargins',
    'returnOnEquity', 'returnOnAssets',
    'debtToEquity', 'currentRatio',
    'revenueGrowth', 'earningsGrowth',
    'trailingAnnualDividendYield', 'trailingAnnualDividendRate',
    'payoutRatio', 'exDividendDate',
    'fiftyTwoWeekHigh', 'fiftyTwoWeekLow',
    'shortPercentOfFloat', 'shortRatio',
    'targetMeanPrice', 'targetLowPrice', 'targetHighPrice',
    'numberOfAnalystOpinions', 'recommendationKey',
]

# Fields that genuinely do not exist for funds/ETFs/trusts. Reporting these as
# "missing" would be dishonest -- an ETF has no return on equity.
_EQUITY_ONLY = {
    'trailingPE', 'forwardPE', 'priceToBook', 'priceToSalesTrailing12Months',
    'profitMargins', 'operatingMargins', 'returnOnEquity', 'returnOnAssets',
    'debtToEquity', 'currentRatio', 'revenueGrowth', 'earningsGrowth',
    'payoutRatio', 'targetMeanPrice', 'targetLowPrice', 'targetHighPrice',
    'numberOfAnalystOpinions', 'recommendationKey',
}

# Secrets get named a dozen different ways in the wild, and Streamlit's
# secrets.toml is often written with a [section] header. Accept all of it.
_SECRET_ALIASES = {
    'APCA_API_KEY_ID': ['APCA_API_KEY_ID', 'ALPACA_API_KEY_ID', 'ALPACA_API_KEY',
                        'ALPACA_KEY_ID', 'ALPACA_KEY', 'APCA_KEY_ID', 'api_key',
                        'key_id', 'APCA-API-KEY-ID'],
    'APCA_API_SECRET_KEY': ['APCA_API_SECRET_KEY', 'ALPACA_API_SECRET_KEY',
                            'ALPACA_SECRET_KEY', 'ALPACA_SECRET', 'APCA_SECRET_KEY',
                            'secret_key', 'api_secret', 'APCA-API-SECRET-KEY'],
    'FMP_API_KEY':       ['FMP_API_KEY', 'FMP_KEY', 'FINANCIAL_MODELING_PREP_KEY'],
    'FINNHUB_API_KEY':   ['FINNHUB_API_KEY', 'FINNHUB_KEY', 'FINNHUB_TOKEN'],
    'ANTHROPIC_API_KEY': ['ANTHROPIC_API_KEY', 'CLAUDE_API_KEY', 'ANTHROPIC_KEY'],
}
_SECRET_SECTIONS = ['alpaca', 'ALPACA', 'Alpaca', 'apca', 'APCA', 'api', 'keys',
                    'fmp', 'FMP', 'finnhub', 'FINNHUB', 'anthropic', 'ANTHROPIC']

def _secret(name):
    aliases = _SECRET_ALIASES.get(name, [name])
    try:
        sec = st.secrets
    except Exception:
        return ''
    def _grab(container, key):
        try:
            v = container.get(key)
        except Exception:
            return ''
        # A pasted key often carries a trailing newline or space
        return str(v).strip() if v not in (None, '') else ''
    for a in aliases:
        v = _grab(sec, a)
        if v:
            return v
    for section in _SECRET_SECTIONS:
        try:
            sub = sec.get(section)
        except Exception:
            sub = None
        if sub is None or not hasattr(sub, 'get'):
            continue
        for a in aliases:
            v = _grab(sub, a)
            if v:
                return v
    return ''

def _tz_naive(obj):
    # Providers disagree on timezones: yfinance returns tz-aware dividend
    # dates (America/New_York), Alpaca returns naive ones. Comparing a naive
    # DatetimeIndex against an aware Timestamp raises TypeError on pandas 3.x,
    # so everything is flattened to naive at a single choke point.
    try:
        if obj is None or len(obj) == 0:
            return obj
        idx = obj.index
        if getattr(idx, 'tz', None) is not None:
            obj = obj.copy()
            obj.index = idx.tz_convert(None) if hasattr(idx, 'tz_convert') \
                else idx.tz_localize(None)
    except Exception:
        pass
    return obj

def _creds_fp():
    # Included in cache keys so editing a secret invalidates cached failures
    # instead of serving an empty result until the TTL expires.
    k = _secret('APCA_API_KEY_ID')
    return (k[:4] + ':' + str(len(k))) if k else 'none'

def _http_json(url, headers=None, timeout=12):
    import urllib.request as ur, ssl
    req = ur.Request(url, headers=headers or {'User-Agent': 'Mozilla/5.0'})
    with ur.urlopen(req, timeout=timeout, context=ssl.create_default_context()) as r:
        return json.loads(r.read().decode('utf-8'))

def _http_probe(url, headers=None, timeout=12):
    # Never raises. Returns (status_code, parsed_json_or_None, error_text).
    import urllib.request as ur, urllib.error as ue, ssl
    req = ur.Request(url, headers=headers or {'User-Agent': 'Mozilla/5.0'})
    try:
        with ur.urlopen(req, timeout=timeout,
                        context=ssl.create_default_context()) as r:
            body = r.read().decode('utf-8', 'ignore')
            try:
                return r.status, json.loads(body), ''
            except Exception:
                return r.status, None, body[:200]
    except ue.HTTPError as e:
        try:
            detail = e.read().decode('utf-8', 'ignore')[:300]
        except Exception:
            detail = ''
        return e.code, None, detail
    except Exception as e:
        return 0, None, str(e)[:200]

def _fnum(v):
    # Coerce to float, rejecting None/''/NaN/inf so a bad value never
    # masquerades as a real one and blocks a later provider from filling it.
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float('inf'), float('-inf')):
        return None
    return v

def _put(info, sources, provider, field, value):
    # Fill a field only if still empty. Returns True when it was filled.
    if value is None or info.get(field) is not None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    info[field] = value
    sources.setdefault(provider, []).append(field)
    return True

# ── Provider 1: Alpaca ─────────────────────────────────────────────────────
def _alpaca_headers():
    kid, sec = _secret('APCA_API_KEY_ID'), _secret('APCA_API_SECRET_KEY')
    if not kid or not sec:
        return None
    return {'APCA-API-KEY-ID': kid, 'APCA-API-SECRET-KEY': sec,
            'User-Agent': 'Mozilla/5.0'}

@st.cache_data(ttl=900, show_spinner=False)
def _fetch_alpaca(sym, _fp=''):
    # Returns (info_fragment, hist_df, dividends_series).
    # Alpaca is the most reliable source for price and bars from cloud IPs --
    # it authenticates by key, so there is no shared-IP rate limiting.
    h = _alpaca_headers()
    out, hist, divs = {}, pd.DataFrame(), pd.Series(dtype=float)
    if not h:
        return out, hist, divs
    # Asset name / class
    # Paper-trading keys are rejected by the live trading host and vice versa,
    # so try both before giving up on the asset record.
    for _host in (_ALPACA_TRADE, _ALPACA_PAPER):
        try:
            a = _http_json(_host + '/v2/assets/' + sym, h)
            if a.get('name'):
                out['longName'] = a['name']
            out['_asset_class'] = a.get('class') or ''
            break
        except Exception:
            continue
    # Latest snapshot -> current price
    # Free Alpaca plans are limited to the IEX feed; the default (SIP) returns
    # 403 for them, so fall back rather than losing the price entirely.
    for _suffix in ('', '?feed=iex'):
        try:
            s = _http_json(_ALPACA_DATA + '/v2/stocks/' + sym + '/snapshot' + _suffix, h)
            px = (((s.get('latestTrade') or {}).get('p'))
                  or ((s.get('dailyBar') or {}).get('c'))
                  or ((s.get('prevDailyBar') or {}).get('c')))
            if _fnum(px):
                out['currentPrice'] = _fnum(px)
                break
        except Exception:
            continue
    # Daily bars -> 1y history
    try:
        start = (datetime.date.today() - datetime.timedelta(days=400)).isoformat()
        url = (_ALPACA_DATA + '/v2/stocks/' + sym + '/bars?timeframe=1Day'
               '&adjustment=all&limit=1000&start=' + start)
        j = None
        for suffix in ('', '&feed=iex'):
            try:
                j = _http_json(url + suffix, h)
                if j.get('bars'):
                    break
            except Exception:
                j = None
        bars = (j or {}).get('bars') or []
        if bars:
            hist = pd.DataFrame({
                'Open':   [_fnum(b.get('o')) for b in bars],
                'High':   [_fnum(b.get('h')) for b in bars],
                'Low':    [_fnum(b.get('l')) for b in bars],
                'Close':  [_fnum(b.get('c')) for b in bars],
                'Volume': [_fnum(b.get('v')) for b in bars],
            }, index=pd.to_datetime([b.get('t') for b in bars], utc=True, errors='coerce'))
            hist = hist.dropna(subset=['Close'])
            hist.index = hist.index.tz_localize(None)
    except Exception:
        hist = pd.DataFrame()
    # Cash dividends -> rate, ex-date, payment history
    try:
        start = (datetime.date.today() - datetime.timedelta(days=420)).isoformat()
        end   = (datetime.date.today() + datetime.timedelta(days=90)).isoformat()
        j = _http_json(_ALPACA_DATA + '/v1/corporate-actions?symbols=' + sym +
                       '&types=cash_dividend&start=' + start + '&end=' + end +
                       '&limit=200', h)
        cds = ((j.get('corporate_actions') or {}).get('cash_dividends')) or []
        rows = []
        for c in cds:
            amt = _fnum(c.get('rate'))
            d = c.get('ex_date') or c.get('process_date')
            if amt and d:
                rows.append((pd.to_datetime(d, errors='coerce'), amt))
        rows = [r for r in rows if pd.notna(r[0])]
        if rows:
            rows.sort(key=lambda r: r[0])
            divs = pd.Series([r[1] for r in rows],
                             index=pd.DatetimeIndex([r[0] for r in rows]))
            today_ts = pd.Timestamp(datetime.date.today())
            # Next upcoming ex-date if Alpaca has announced one
            future = [r[0] for r in rows if r[0] >= today_ts]
            if future:
                out['exDividendDate'] = int(future[0].timestamp())
            # Trailing 12-month cash total = the annual rate
            past12 = [r[1] for r in rows
                      if today_ts - pd.DateOffset(years=1) <= r[0] <= today_ts]
            if past12:
                out['trailingAnnualDividendRate'] = round(sum(past12), 4)
    except Exception:
        pass
    return out, hist, divs

@st.cache_data(ttl=3600, show_spinner=False)
def _alpaca_benchmark(_fp=''):
    # SPY daily closes, used to derive beta when no provider reports one.
    h = _alpaca_headers()
    if not h:
        return pd.Series(dtype=float)
    try:
        start = (datetime.date.today() - datetime.timedelta(days=400)).isoformat()
        url = (_ALPACA_DATA + '/v2/stocks/SPY/bars?timeframe=1Day'
               '&adjustment=all&limit=1000&start=' + start)
        j = None
        for suffix in ('', '&feed=iex'):
            try:
                j = _http_json(url + suffix, h)
                if j.get('bars'):
                    break
            except Exception:
                j = None
        bars = (j or {}).get('bars') or []
        if not bars:
            return pd.Series(dtype=float)
        s = pd.Series([_fnum(b.get('c')) for b in bars],
                      index=pd.to_datetime([b.get('t') for b in bars],
                                           utc=True, errors='coerce'))
        s.index = s.index.tz_localize(None)
        return s.dropna()
    except Exception:
        return pd.Series(dtype=float)

# ── Provider 2: yfinance ───────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_yfinance(sym):
    try:
        import yfinance as yf
        t = yf.Ticker(sym)
        info = t.info or {}
        if info.get('currentPrice') or info.get('regularMarketPrice'):
            try:
                hist = t.history(period='1y')
            except Exception:
                hist = pd.DataFrame()
            try:
                divs = t.dividends
            except Exception:
                divs = pd.Series(dtype=float)
            return info, hist, divs, True
    except Exception:
        pass
    return {}, pd.DataFrame(), pd.Series(dtype=float), False

# ── Provider 3: Financial Modeling Prep (optional key) ─────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_fmp(sym):
    key = _secret('FMP_API_KEY')
    out = {}
    if not key:
        return out
    base = 'https://financialmodelingprep.com/api/v3/'
    try:
        p = _http_json(base + 'profile/' + sym + '?apikey=' + key)
        p = p[0] if isinstance(p, list) and p else {}
        for src_k, dst_k in [('companyName','longName'), ('sector','sector'),
                             ('industry','industry')]:
            if p.get(src_k):
                out[dst_k] = p[src_k]
        for src_k, dst_k in [('price','currentPrice'), ('mktCap','marketCap'),
                             ('beta','beta'), ('lastDiv','trailingAnnualDividendRate')]:
            if _fnum(p.get(src_k)):
                out[dst_k] = _fnum(p[src_k])
    except Exception:
        pass
    try:
        r = _http_json(base + 'ratios-ttm/' + sym + '?apikey=' + key)
        r = r[0] if isinstance(r, list) and r else {}
        pairs = [
            ('peRatioTTM','trailingPE'), ('priceToBookRatioTTM','priceToBook'),
            ('priceToSalesRatioTTM','priceToSalesTrailing12Months'),
            ('netProfitMarginTTM','profitMargins'),
            ('operatingProfitMarginTTM','operatingMargins'),
            ('returnOnEquityTTM','returnOnEquity'),
            ('returnOnAssetsTTM','returnOnAssets'),
            ('debtEquityRatioTTM','debtToEquity'),
            ('currentRatioTTM','currentRatio'),
            ('payoutRatioTTM','payoutRatio'),
            ('dividendYielTTM','trailingAnnualDividendYield'),
        ]
        for src_k, dst_k in pairs:
            v = _fnum(r.get(src_k))
            if v is not None:
                out[dst_k] = v
    except Exception:
        pass
    return out

# ── Provider 4: Finnhub (optional key) ─────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_finnhub(sym):
    key = _secret('FINNHUB_API_KEY')
    out = {}
    if not key:
        return out
    base = 'https://finnhub.io/api/v1/'
    try:
        j = _http_json(base + 'stock/metric?symbol=' + sym + '&metric=all&token=' + key)
        m = j.get('metric') or {}
        pairs = [
            ('52WeekHigh','fiftyTwoWeekHigh'), ('52WeekLow','fiftyTwoWeekLow'),
            ('beta','beta'), ('peTTM','trailingPE'), ('pbAnnual','priceToBook'),
            ('psTTM','priceToSalesTrailing12Months'),
            ('netProfitMarginTTM','profitMargins'),
            ('operatingMarginTTM','operatingMargins'),
            ('roeTTM','returnOnEquity'), ('roaTTM','returnOnAssets'),
            ('totalDebt/totalEquityAnnual','debtToEquity'),
            ('currentRatioAnnual','currentRatio'),
            ('revenueGrowthTTMYoy','revenueGrowth'),
            ('epsGrowthTTMYoy','earningsGrowth'),
            ('dividendYieldIndicatedAnnual','trailingAnnualDividendYield'),
            ('payoutRatioTTM','payoutRatio'),
        ]
        for src_k, dst_k in pairs:
            v = _fnum(m.get(src_k))
            if v is None:
                continue
            # Finnhub reports margins/ROE/growth in percent, not decimal
            if dst_k in ('profitMargins','operatingMargins','returnOnEquity',
                         'returnOnAssets','revenueGrowth','earningsGrowth',
                         'payoutRatio'):
                v = v / 100.0
            if dst_k == 'trailingAnnualDividendYield':
                v = _norm_yield(v)
            if v is not None:
                out[dst_k] = v
    except Exception:
        pass
    try:
        j = _http_json(base + 'stock/price-target?symbol=' + sym + '&token=' + key)
        for src_k, dst_k in [('targetMean','targetMeanPrice'),
                             ('targetLow','targetLowPrice'),
                             ('targetHigh','targetHighPrice')]:
            if _fnum(j.get(src_k)):
                out[dst_k] = _fnum(j[src_k])
    except Exception:
        pass
    try:
        j = _http_json(base + 'stock/recommendation?symbol=' + sym + '&token=' + key)
        r = j[0] if isinstance(j, list) and j else {}
        sb, b = int(r.get('strongBuy') or 0), int(r.get('buy') or 0)
        h_, s_, ss = (int(r.get('hold') or 0), int(r.get('sell') or 0),
                      int(r.get('strongSell') or 0))
        n = sb + b + h_ + s_ + ss
        if n:
            out['numberOfAnalystOpinions'] = n
            score = (sb*1 + b*2 + h_*3 + s_*4 + ss*5) / n
            out['recommendationKey'] = ('strong_buy' if score <= 1.5 else
                                        'buy' if score <= 2.5 else
                                        'hold' if score <= 3.5 else
                                        'sell' if score <= 4.5 else 'strong_sell')
    except Exception:
        pass
    return out

# ── Provider 5: Stooq (no key, EOD history fallback) ───────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_stooq_hist(sym):
    # Free end-of-day OHLCV. Last-resort price history when every keyed
    # provider is unavailable.
    try:
        import urllib.request as ur, ssl, csv, io as _io
        url = 'https://stooq.com/q/d/l/?s=' + sym.lower() + '.us&i=d'
        req = ur.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with ur.urlopen(req, timeout=12,
                        context=ssl.create_default_context()) as r:
            txt = r.read().decode('utf-8', 'ignore')
        rows = list(csv.DictReader(_io.StringIO(txt)))
        if not rows or 'Close' not in (rows[0] or {}):
            return pd.DataFrame()
        rows = rows[-260:]
        df = pd.DataFrame({
            'Open':   [_fnum(r.get('Open')) for r in rows],
            'High':   [_fnum(r.get('High')) for r in rows],
            'Low':    [_fnum(r.get('Low')) for r in rows],
            'Close':  [_fnum(r.get('Close')) for r in rows],
            'Volume': [_fnum(r.get('Volume')) for r in rows],
        }, index=pd.to_datetime([r.get('Date') for r in rows], errors='coerce'))
        return df.dropna(subset=['Close'])
    except Exception:
        return pd.DataFrame()

# ── Derivation layer ───────────────────────────────────────────────────────
def _derive_fields(info, hist, divs, sources, benchmark=None):
    # Compute whatever can be computed from data already in hand. This is what
    # turns a half-empty page into a full one without another network call.
    close = hist['Close'].dropna() if (not hist.empty and 'Close' in hist) else None

    if close is not None and len(close) > 1:
        _put(info, sources, 'derived', 'currentPrice', _fnum(close.iloc[-1]))
        win = close.iloc[-252:] if len(close) >= 252 else close
        hi = hist['High'].dropna().iloc[-252:] if 'High' in hist else win
        lo = hist['Low'].dropna().iloc[-252:] if 'Low' in hist else win
        _put(info, sources, 'derived', 'fiftyTwoWeekHigh',
             _fnum(max(hi.max(), win.max())))
        _put(info, sources, 'derived', 'fiftyTwoWeekLow',
             _fnum(min(lo.min(), win.min())))
        # Beta vs SPY, 1-year daily returns (vendors often use 5-year monthly,
        # so this can differ from a published figure -- it is labelled derived)
        if info.get('beta') is None and benchmark is not None and len(benchmark) > 30:
            try:
                a = close.pct_change().dropna()
                b = benchmark.pct_change().dropna()
                j = pd.concat([a, b], axis=1, join='inner').dropna()
                if len(j) > 60:
                    var = float(j.iloc[:, 1].var())
                    # Guard against a degenerate benchmark series: near-zero
                    # variance makes the ratio explode into nonsense.
                    if var > 1e-8:
                        b_ = float(j.cov().iloc[0, 1]) / var
                        # Real-world equity betas essentially never sit outside
                        # this band; anything beyond it is a data artifact.
                        if -4.0 <= b_ <= 4.0:
                            _put(info, sources, 'derived', 'beta', round(b_, 3))
            except Exception:
                pass

    px = _fnum(info.get('currentPrice'))

    # Dividend rate from the actual payment stream
    if divs is not None and len(divs) and info.get('trailingAnnualDividendRate') is None:
        try:
            divs = _tz_naive(divs)
            cutoff = pd.Timestamp(datetime.date.today()) - pd.DateOffset(years=1)
            recent = divs[divs.index >= cutoff]
            if len(recent):
                _put(info, sources, 'derived', 'trailingAnnualDividendRate',
                     round(float(recent.sum()), 4))
        except Exception:
            pass

    rate = _fnum(info.get('trailingAnnualDividendRate'))
    if rate and px and px > 0:
        _put(info, sources, 'derived', 'trailingAnnualDividendYield',
             round(rate / px, 6))

    # Valuation ratios from raw statement figures when the ratio is absent
    eps  = _fnum(info.get('trailingEps'))
    feps = _fnum(info.get('forwardEps'))
    bvps = _fnum(info.get('bookValue'))
    shares = _fnum(info.get('sharesOutstanding')) or _fnum(info.get('impliedSharesOutstanding'))
    rev  = _fnum(info.get('totalRevenue'))
    ni   = _fnum(info.get('netIncomeToCommon'))

    if px and shares:
        _put(info, sources, 'derived', 'marketCap', round(px * shares))
    if px and eps and eps > 0:
        _put(info, sources, 'derived', 'trailingPE', round(px / eps, 2))
    if px and feps and feps > 0:
        _put(info, sources, 'derived', 'forwardPE', round(px / feps, 2))
    if px and bvps and bvps > 0:
        _put(info, sources, 'derived', 'priceToBook', round(px / bvps, 2))
    mc = _fnum(info.get('marketCap'))
    if mc and rev and rev > 0:
        _put(info, sources, 'derived', 'priceToSalesTrailing12Months',
             round(mc / rev, 2))
    if ni is not None and rev:
        _put(info, sources, 'derived', 'profitMargins', round(ni / rev, 4))
    if rate and eps and eps > 0:
        _put(info, sources, 'derived', 'payoutRatio', round(rate / eps, 4))

    # Short interest ratios
    ss  = _fnum(info.get('sharesShort'))
    flt = _fnum(info.get('floatShares'))
    adv = _fnum(info.get('averageVolume10days')) or _fnum(info.get('averageVolume'))
    if ss and flt and flt > 0:
        _put(info, sources, 'derived', 'shortPercentOfFloat', round(ss / flt, 4))
    if ss and adv and adv > 0:
        _put(info, sources, 'derived', 'shortRatio', round(ss / adv, 2))

    # Most recent past ex-date, when no upcoming one was announced
    if info.get('exDividendDate') is None and divs is not None and len(divs):
        try:
            _put(info, sources, 'derived', 'exDividendDate',
                 int(pd.Timestamp(divs.index[-1]).timestamp()))
        except Exception:
            pass
    return info

def _alpaca_diagnose(sym='AAPL'):
    # Live end-to-end check of the Alpaca setup. Returns a list of
    # (step, ok, detail) tuples. Never raises, never prints a secret.
    steps = []
    kid = _secret('APCA_API_KEY_ID')
    sec = _secret('APCA_API_SECRET_KEY')

    if not kid or not sec:
        found = []
        try:
            found = [k for k in list(st.secrets.keys())]
        except Exception:
            pass
        steps.append(('Credentials found in secrets', False,
            'Missing ' + ('key id' if not kid else '') +
            (' and ' if (not kid and not sec) else '') +
            ('secret key' if not sec else '') +
            '. Top-level secret names detected: ' +
            (', '.join(found) if found else 'none') +
            '. Expected APCA_API_KEY_ID and APCA_API_SECRET_KEY (aliases such as '
            'ALPACA_API_KEY / ALPACA_SECRET_KEY and [alpaca] sections also work).'))
        return steps

    steps.append(('Credentials found in secrets', True,
        'Key id ' + kid[:4] + '...' + kid[-2:] + ' (' + str(len(kid)) +
        ' chars), secret ' + str(len(sec)) + ' chars.'))

    if len(kid) < 15 or len(sec) < 30:
        steps.append(('Key format looks plausible', False,
            'Alpaca key ids are ~20 chars and secrets ~40. These look truncated '
            '-- check for a missing character or an accidental line break.'))
    else:
        steps.append(('Key format looks plausible', True, 'Lengths are in range.'))

    h = {'APCA-API-KEY-ID': kid, 'APCA-API-SECRET-KEY': sec,
         'User-Agent': 'Mozilla/5.0'}

    # 1. Authentication against the market data host
    code, body, err = _http_probe(_ALPACA_DATA + '/v2/stocks/AAPL/snapshot', h)
    if code == 200:
        steps.append(('Market data auth (AAPL snapshot)', True, 'HTTP 200, SIP feed allowed.'))
        feed_ok = True
    elif code in (401, 403):
        c2, b2, e2 = _http_probe(_ALPACA_DATA + '/v2/stocks/AAPL/snapshot?feed=iex', h)
        if c2 == 200:
            steps.append(('Market data auth (AAPL snapshot)', True,
                'HTTP ' + str(code) + ' on the default SIP feed but 200 on IEX -- '
                'normal for the free plan. The app falls back to IEX automatically.'))
            feed_ok = True
        else:
            steps.append(('Market data auth (AAPL snapshot)', False,
                'HTTP ' + str(code) + ' on SIP and ' + str(c2) + ' on IEX. '
                'A 401 means the key or secret is wrong (or swapped). A 403 on '
                'both feeds means the key is valid but has no market data '
                'entitlement. Detail: ' + (err or e2 or 'none')))
            feed_ok = False
    else:
        steps.append(('Market data auth (AAPL snapshot)', False,
            'HTTP ' + str(code) + '. ' + (err or 'No response.')))
        feed_ok = False

    if not feed_ok:
        return steps

    # 2. Trading host -- identifies paper vs live keys
    live_code, live_b, _ = _http_probe(_ALPACA_TRADE + '/v2/account', h)
    pap_code, pap_b, _   = _http_probe(_ALPACA_PAPER + '/v2/account', h)
    if live_code == 200:
        steps.append(('Account type', True, 'Live trading keys.'))
    elif pap_code == 200:
        steps.append(('Account type', True,
            'Paper trading keys. Market data still works; the app queries both hosts.'))
    else:
        steps.append(('Account type', True,
            'Data-only key (HTTP ' + str(live_code) + '/' + str(pap_code) +
            ' on the trading hosts). Fine -- only company names come from there.'))

    # 3. Daily bars for the requested symbol
    start = (datetime.date.today() - datetime.timedelta(days=400)).isoformat()
    burl = (_ALPACA_DATA + '/v2/stocks/' + sym + '/bars?timeframe=1Day'
            '&adjustment=all&limit=1000&start=' + start)
    n_bars, used_feed, bcode = 0, None, None
    for suffix, label in (('', 'SIP'), ('&feed=iex', 'IEX')):
        c, b, e = _http_probe(burl + suffix, h)
        bcode = c
        if c == 200 and (b or {}).get('bars'):
            n_bars, used_feed = len((b or {}).get('bars') or []), label
            break
    if n_bars:
        steps.append((sym + ' daily bars', True,
            str(n_bars) + ' bars via the ' + used_feed + ' feed.'))
    else:
        steps.append((sym + ' daily bars', False,
            'HTTP ' + str(bcode) + ', no bars returned. A 404 means the symbol is '
            'not in Alpaca coverage (it lists US equities and ETFs only -- no '
            'OTC, no foreign listings, no mutual funds).'))

    # 4. Corporate actions -- the dividend source
    ca_start = (datetime.date.today() - datetime.timedelta(days=420)).isoformat()
    ca_end   = (datetime.date.today() + datetime.timedelta(days=90)).isoformat()
    c, b, e = _http_probe(_ALPACA_DATA + '/v1/corporate-actions?symbols=' + sym +
                          '&types=cash_dividend&start=' + ca_start +
                          '&end=' + ca_end + '&limit=200', h)
    if c == 200:
        cds = ((b or {}).get('corporate_actions') or {}).get('cash_dividends') or []
        steps.append(('Cash dividend history', True,
            str(len(cds)) + ' dividend records in the last 12 months plus any '
            'announced upcoming ex-date.' if cds else
            'Endpoint reachable, but no cash dividends on record for ' + sym + '.'))
    else:
        steps.append(('Cash dividend history', False,
            'HTTP ' + str(c) + '. ' + (e or 'No response.')))

    # 5. What the full pipeline actually produced
    try:
        a_info, a_hist, a_divs = _fetch_alpaca(sym, _creds_fp())
        got = sorted(k for k in a_info if not k.startswith('_'))
        steps.append(('Fields Alpaca supplied for ' + sym, bool(got or len(a_hist)),
            ('Fields: ' + ', '.join(got) if got else 'No fields') +
            ' | history rows: ' + str(len(a_hist)) +
            ' | dividend rows: ' + str(len(a_divs))))
    except Exception as ex:
        steps.append(('Fields Alpaca supplied for ' + sym, False, str(ex)[:200]))

    return steps

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stock_analysis(sym, live=False):
    sym = sym.upper().strip()
    scan_dict = _load_scan_dict()
    rec = scan_dict.get(sym, {})

    info, sources = {}, {}
    hist = pd.DataFrame()
    divs = pd.Series(dtype=float)
    hist_source = 'none'
    providers_tried, providers_ok = [], []

    def absorb(provider, frag):
        for k, v in (frag or {}).items():
            if k.startswith('_'):
                info.setdefault(k, v)
                continue
            if k in _YF_FIELDS:
                _put(info, sources, provider, k, v)
        if frag:
            providers_ok.append(provider)

    # 1. Alpaca runs on EVERY lookup, not just after Refresh. It authenticates
    #    by key, so it is fast and immune to the shared-IP throttling that makes
    #    the other live sources unreliable -- there is no reason to gate it.
    if _alpaca_headers():
        providers_tried.append('alpaca')
        a_info, a_hist, a_divs = _fetch_alpaca(sym, _creds_fp())
        absorb('alpaca', a_info)
        if not a_hist.empty:
            hist, hist_source = a_hist, 'alpaca'
        if len(a_divs):
            divs = _tz_naive(a_divs)

    if live:
        # 2. yfinance -- broadest fundamentals
        providers_tried.append('yfinance')
        y_info, y_hist, y_divs, y_ok = _fetch_yfinance(sym)
        if y_ok:
            absorb('yfinance', y_info)
            # Keep raw statement figures for the derivation layer
            for k in ('trailingEps', 'forwardEps', 'bookValue', 'sharesOutstanding',
                      'impliedSharesOutstanding', 'totalRevenue', 'netIncomeToCommon',
                      'sharesShort', 'floatShares', 'averageVolume10days',
                      'averageVolume', 'quoteType'):
                if y_info.get(k) is not None:
                    info.setdefault(k, y_info[k])
            if hist.empty and not y_hist.empty:
                hist, hist_source = y_hist, 'yfinance'
            if not len(divs) and len(y_divs):
                divs = _tz_naive(y_divs)

    # 3/4. Keyed fundamentals providers -- also key-authenticated, always on
    if _secret('FMP_API_KEY'):
        providers_tried.append('fmp')
        absorb('fmp', _fetch_fmp(sym))
    if _secret('FINNHUB_API_KEY'):
        providers_tried.append('finnhub')
        absorb('finnhub', _fetch_finnhub(sym))

    # 5. Nightly scan dump
    if rec:
        scan_map = {
            'longName':                   rec.get('Ticker', sym),
            'sector':                     rec.get('Sector'),
            'industry':                   rec.get('Sector'),
            'currentPrice':               _fnum(rec.get('Price')),
            'marketCap':                  _fnum(rec.get('MarketCap')),
            'trailingPE':                 _fnum(rec.get('P/E')),
            'fiftyTwoWeekHigh':           _fnum(rec.get('RangeHigh')),
            'fiftyTwoWeekLow':            _fnum(rec.get('RangeLow')),
            'trailingAnnualDividendYield': _pct_to_dec(rec.get('DividendYieldPct')),
            'trailingAnnualDividendRate': _fnum(rec.get('DividendRate')),
            'payoutRatio':                _pct_to_dec(rec.get('DividendPayoutRatio')),
            'exDividendDate':             rec.get('ExDividendDate'),
            'shortPercentOfFloat':        _pct_to_dec(rec.get('ShortPctFloatRaw')),
            'shortRatio':                 _fnum(rec.get('DaysToCover')),
            'revenueGrowth':              _norm_growth(rec.get('RevenueGrowth')),
            'earningsGrowth':             _norm_growth(rec.get('EarningsGrowth')),
        }
        for field, val in scan_map.items():
            _put(info, sources, 'scan', field, val)
        if rec.get('DividendFrequency'):
            info.setdefault('dividendFrequency', rec['DividendFrequency'])
        if hist.empty:
            sh = _hist_to_df(rec.get('_hist'))
            if not sh.empty:
                hist, hist_source = sh, 'scan'

    # 6. Stooq -- free EOD history when nothing else supplied bars
    if live and hist.empty:
        providers_tried.append('stooq')
        s_hist = _fetch_stooq_hist(sym)
        if not s_hist.empty:
            hist, hist_source = s_hist, 'stooq'
            providers_ok.append('stooq')

    # 7. Derive everything computable from what we already have
    bench = _alpaca_benchmark(_creds_fp()) if _alpaca_headers() else None
    info = _derive_fields(info, hist, divs, sources, bench)

    # 8. Claude web search -- last resort, only for what is still empty and
    #    only for fields that actually exist for this security type
    is_fund = str(info.get('quoteType') or info.get('_asset_class') or '').upper() in (
        'ETF', 'MUTUALFUND', 'FUND', 'INDEX')
    still = [f for f in _YF_FIELDS
             if info.get(f) is None and not (is_fund and f in _EQUITY_ONLY)]
    if live and still and _secret('ANTHROPIC_API_KEY'):
        providers_tried.append('claude')
        for k, v in (_fetch_claude_live(sym, still) or {}).items():
            if k in _YF_FIELDS:
                _put(info, sources, 'claude', k, v)
        # Derive again -- Claude may have supplied an input that unlocks a ratio
        info = _derive_fields(info, hist, divs, sources, bench)

    # Screener signals always come from the scan (pre-computed, most accurate)
    if rec:
        info.update({
            '_rsi_score':     rec.get('RSI'),
            '_macd_score':    rec.get('MACD'),
            '_obv_score':     rec.get('OBV'),
            '_gc_score':      rec.get('GoldenCross'),
            '_ma50_val':      rec.get('MA50'),
            '_mfi_score':     rec.get('MFI'),
            '_piotroski':     rec.get('Piotroski'),
            '_range_pos':     rec.get('RangePct'),
            '_short_squeeze': rec.get('ShortSqueeze'),
        })

    if not info.get('currentPrice'):
        return {}, pd.DataFrame(), _tz_naive(divs), (
            sym + ' not found in any source. Check the ticker symbol.'), {}

    missing = [f for f in _YF_FIELDS if info.get(f) is None]
    na_type = [f for f in missing if is_fund and f in _EQUITY_ONLY]
    truly   = [f for f in missing if f not in na_type]
    applicable = len(_YF_FIELDS) - len(na_type)

    source_summary = {
        'by_provider':  {k: len(v) for k, v in sources.items() if v},
        'field_source': {f: p for p, fl in sources.items() for f in fl},
        'hist_source':  hist_source,
        'scan_in_repo': bool(rec),
        'is_fund':      is_fund,
        'applicable':   applicable,
        'filled':       applicable - len(truly),
        'na_fields':    na_type,
        'none_fields':  truly,
        'providers_tried': providers_tried,
        'providers_ok':    sorted(set(providers_ok)),
        # legacy keys used elsewhere in the app
        'yf_ok':        'yfinance' in providers_ok,
        'yf_count':     len(sources.get('yfinance', [])),
        'scan_count':   len(sources.get('scan', [])),
        'claude_count': len(sources.get('claude', [])),
        'none_count':   len(truly),
        'scan_fields':  sources.get('scan', []),
        'claude_fields': sources.get('claude', []),
    }
    return info, hist, _tz_naive(divs), None, source_summary

@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_claude_live(sym, missing_fields):
    # Step 3 (only called when yfinance missing data): Claude web search
    # Only requests the specific fields that are still missing
    try:
        api_key = st.secrets.get('ANTHROPIC_API_KEY', '')
        if not api_key:
            return {}
        import urllib.request as ur, ssl
        ctx  = ssl.create_default_context()
        flds = ', '.join('"' + f + '": number_or_str_or_null' for f in missing_fields)
        prompt = (
            'Get the latest stock data for ' + sym + '. '
            'Return ONLY a JSON object with these fields (null if unavailable): '
            '{' + flds + '} '
            'Return ONLY raw JSON, no markdown, no explanation. '
            'Decimals for ratios: revenueGrowth 0.15 not 15%. '
            'shortPercentOfFloat 0.05 not 5%. payoutRatio 0.45 not 45%.'
        )
        body = json.dumps({
            'model': 'claude-haiku-4-5-20251001',
            'max_tokens': 600,
            'tools': [{'type': 'web_search_20250305', 'name': 'web_search'}],
            'messages': [{'role': 'user', 'content': prompt}]
        }).encode('utf-8')
        req = ur.Request(
            'https://api.anthropic.com/v1/messages',
            data=body,
            headers={
                'Content-Type': 'application/json',
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
                'anthropic-beta': 'web-search-2025-03-05',
            },
            method='POST'
        )
        with ur.urlopen(req, timeout=30, context=ctx) as r:
            resp = json.loads(r.read())
        text = ''
        for block in resp.get('content', []):
            if block.get('type') == 'text':
                text += block.get('text', '')
        text = text.strip()
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        data = json.loads(text.strip())
        # Convert exDividendDate string to Unix timestamp
        if data.get('exDividendDate') and isinstance(data['exDividendDate'], str):
            try:
                for fmt in ('%Y-%m-%d', '%B %d, %Y', '%b %d, %Y', '%m/%d/%Y'):
                    try:
                        dt = datetime.datetime.strptime(data['exDividendDate'], fmt)
                        data['exDividendDate'] = int(dt.timestamp())
                        break
                    except ValueError:
                        continue
            except Exception:
                data['exDividendDate'] = None
        return data
    except Exception:
        return {}

def _calc_info_from_scan_row(row):
    return {
        'ticker': row['ticker'],
        'price': float(row.get('price') or 0),
        'yield_pct': float(row['yield_pct']),
        'div_rate': float(row.get('div_rate') or 0),
        'monthly_pay': row.get('monthly_pay'),
        'frequency': row.get('frequency') or '--',
        'payout': row.get('payout'),
        'sector': row.get('sector') or 'Unknown',
    }

def _calc_info_from_scan_rec(sym, rec):
    try:
        yp = float(rec.get('DividendYieldPct') or 0)
    except (TypeError, ValueError):
        return None
    if yp <= 0:
        return None
    dr = float(rec.get('DividendRate') or 0)
    return {
        'ticker': sym,
        'price': float(rec.get('Price') or 0),
        'yield_pct': round(yp, 2),
        'div_rate': dr,
        'monthly_pay': round(dr / 12, 4) if dr else None,
        'frequency': rec.get('DividendFrequency') or '--',
        'payout': rec.get('DividendPayoutRatio'),
        'sector': rec.get('Sector') or 'Unknown',
    }

def _render_dividend_table(df_show, today, show_buy_cols=True):
    rows_html = []
    for row in df_show.to_dict('records'):
        yc  = ycolor(row['yield_pct'])
        bd  = safe_date(row.get('buy_date'))
        ex  = safe_date(row.get('ex_date'))
        da  = int(row.get('days_away') or 0)
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
        if show_buy_cols:
            if da == 0:   alert = '<span class="buy-now">BUY TODAY</span>'
            elif da == 1: alert = '<span class="buy-tmr">BUY TOMORROW</span>'
        buy_cols = ''
        if show_buy_cols:
            buy_cols = (
                '<td class="td-date">' + bd_str + '</td>'
                '<td class="td-date">' + ex_str + '</td>'
                '<td class="td-count">' + str(da) + 'd ' + alert + '</td>')
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
            + buy_cols +
            '</tr>')
    hdr_buy = ''
    if show_buy_cols:
        hdr_buy = '<th>Buy Before</th><th>Ex-Date</th><th>Countdown</th>'
    tbl = ('<div class="tbl-wrap"><table class="stbl"><thead><tr>'
        '<th>Ticker</th><th>Sector</th><th>Yield</th><th>Div/Share</th>'
        '<th title="Annual dividend divided by 12 -- a monthly equivalent, not necessarily an actual monthly payment">Monthly/Share</th><th>Payout</th><th>Frequency</th>'
        '<th>Price</th>' + hdr_buy +
        '</tr></thead><tbody>' + ''.join(rows_html) + '</tbody></table></div>')
    st.markdown(tbl, unsafe_allow_html=True)

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
    freq_filter = st.multiselect(
        'Payout frequency', _FREQ_BUCKETS, default=_FREQ_BUCKETS, key='sb_freq',
        help='All frequencies are shown by default. Deselect any you do not '
             'want. "Other" covers irregular and unclassified payers.')
    st.markdown('---')
    if st.button('Refresh', key='sb_refresh',
                 help='Reload scan file and fetch live ex-dividend dates from Yahoo'):
        st.session_state['live_mode'] = True
        st.session_state['fetch_ex_dates'] = True
        st.cache_data.clear()
        st.rerun()
    st.markdown('---')
    st.markdown('**How it works:**\n'
        '- Loads data/stock_data.json.gz only (no live API on startup)\n'
        '- Calendar = day before ex-date\n'
        '- Hit **Refresh** to fetch live ex-dates + enable live analyzer\n'
        '- Calculator tab to model returns\n'
        '- Analyzer tab for full stock deep-dive')
    st.caption('v' + APP_VERSION)

st.markdown('<div class="main-title">Dividend Capture Calendar</div>', unsafe_allow_html=True)
st.markdown('<div class="main-sub">buy 1 trading day before ex-date | highest yield first | magicpro33/stock</div>', unsafe_allow_html=True)

with st.spinner('Loading scan data...'):
    scan_result = load_scan_data()
    meta = load_meta()

if scan_result[0] is None:
    st.markdown('<div class="src-badge src-err">&#x2717; ' + scan_result[1] + '</div>', unsafe_allow_html=True)
    st.info('Run python nightly_scan.py and push data/ to GitHub. GitHub Actions regenerates it nightly.')
    st.stop()

df_all, _err, has_ex_dates = scan_result
_scan_dict = _load_scan_dict()
live_mode = bool(st.session_state.get('live_mode'))

# On Refresh only: fetch live ex-dates; persist in session for this visit
if st.session_state.get('fetch_ex_dates'):
    with st.spinner('Fetching ex-dividend dates (Refresh)...'):
        st.session_state['ex_date_overrides'] = fetch_ex_dates_live(
            tuple(df_all['ticker'].tolist()))
    st.session_state['fetch_ex_dates'] = False

ex_overrides = st.session_state.get('ex_date_overrides') or {}
if ex_overrides:
    df_all = df_all.copy()
    df_all['ex_date'] = df_all.apply(
        lambda row: ex_overrides.get(row['ticker']) or row.get('ex_date')
            or _parse_ex_date((_scan_dict.get(row['ticker']) or {}).get('ExDividendDate')),
        axis=1)
    has_ex_dates = df_all['ex_date'].notna().any()

with st.sidebar:
    sectors = ['All sectors'] + sorted(df_all['sector'].dropna().unique().tolist())
    sector_filter = st.selectbox('Sector', sectors, key='sb_sector')

today  = datetime.date.today()
df = df_all[
    (df_all['yield_pct'] >= min_yield) &
    (df_all['yield_pct'] <= max_yield) &
    (df_all['price'].fillna(0) <= max_price)
].copy()
# Empty selection is treated as no filter rather than an empty screen
if freq_filter and len(freq_filter) < len(_FREQ_BUCKETS):
    df = df[df['freq_bucket'].isin(freq_filter)]
if sector_filter != 'All sectors':
    df = df[df['sector'] == sector_filter]

if not has_ex_dates:
    src_label, badge_cls = 'scan data (no ex-dates -- click Refresh)', 'src-warn'
    st.warning(
        'No ex-dividend dates in scan data. Dividend stocks are listed below by yield. '
        'Click **Refresh** in the sidebar to fetch live ex-dates for the calendar.'
    )
else:
    src_label = 'scan data + live ex-dates' if live_mode else 'scan data'
    badge_cls = 'src-ok'
# Last day you can buy and still be on the books: previous TRADING day
# (T+1 settlement). A calendar -1 day lands on weekends/holidays.
df['buy_date'] = df['ex_date'].apply(lambda d: _prev_trading_day(safe_date(d)))

cutoff = today + datetime.timedelta(days=days_ahead)
# Pure Python dates only -- pandas datetime comparisons break on Python 3.14
df_cal = df[df['buy_date'].apply(lambda bd: _in_buy_window(bd, today, cutoff))].copy()
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
    c2.metric('Avg yield', '{:.1f}%'.format(df_cal['yield_pct'].mean()) if not df_cal.empty else (
        '{:.1f}%'.format(df['yield_pct'].mean()) if not df.empty else '--'))
    c3.metric('Highest yield',
        '{:.1f}%'.format(df_cal['yield_pct'].max()) if not df_cal.empty else (
            '{:.1f}%'.format(df['yield_pct'].max()) if not df.empty else '--'),
        delta=str((df_cal if not df_cal.empty else df.sort_values('yield_pct', ascending=False)).iloc[0]['ticker'])
            if not (df_cal.empty and df.empty) else '')
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

    with st.expander('How dividend capture actually works -- read before trading'):
        st.markdown(
            '- **The price drops on the ex-date.** On the ex-dividend morning a stock '
            'typically opens lower by roughly the dividend amount. The dividend is not '
            'free money -- it is a transfer from share price to cash.\n'
            '- **You must own it before the ex-date.** With T+1 settlement the last day '
            'to buy is the previous trading day, which is the Buy Before column here.\n'
            '- **Taxes decide whether capture is profitable.** A dividend is only '
            '*qualified* (lower tax rate) if you hold the shares more than 60 days '
            'during the 121-day window around the ex-date. Buy-and-sell-immediately '
            'capture produces ordinary-income dividends plus short-term capital '
            'gains or losses.\n'
            '- **Yields above roughly 12% deserve scrutiny.** They often signal a '
            'closed-end fund returning capital, a distribution about to be cut, or a '
            'price that has already collapsed.\n'
            '- Trading costs, bid-ask spread, and price drift usually exceed the '
            'edge on a pure capture trade.'
        )
    st.markdown('<div class="section-hdr">Upcoming Buy Signals &mdash; Ranked by Yield</div>', unsafe_allow_html=True)
    if df_cal.empty:
        if df.empty:
            st.info('No dividend stocks match current filters.')
        else:
            st.info(
                'No buy signals in the next ' + str(days_ahead) + ' days. '
                + ('Click **Refresh** to fetch ex-dates, or widen the date range. '
                   if not has_ex_dates else 'Try widening the date range. ')
                + 'Showing all matching dividend stocks below.')
            df_show = df.sort_values('yield_pct', ascending=False).reset_index(drop=True)
            _render_dividend_table(df_show, today, show_buy_cols=False)
    else:
        df_show = df_cal_sorted.assign(days_away=df_cal_sorted['buy_date'].apply(
            lambda bd: (safe_date(bd) - today).days if safe_date(bd) else 0))
        _render_dividend_table(df_show, today, show_buy_cols=True)

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
        calc_info = _calc_info_from_scan_row(sel_row)
    else:
        ctk = st.text_input('Enter ticker symbol', placeholder='e.g. ET, EPD, DOC', key='calc_ticker')
        if ctk:
            sym = ctk.upper().strip()
            rec = _scan_dict.get(sym)
            if rec:
                calc_info = _calc_info_from_scan_rec(sym, rec)
            elif live_mode:
                with st.spinner('Fetching ' + sym + '...'):
                    _calc_res = fetch_stock_analysis(sym, live=True)
                    li, le = _calc_res[0], _calc_res[3]
                if le or not li:
                    st.error('Could not fetch ' + sym + '. Check ticker.')
                else:
                    _lpx = float(li.get('currentPrice') or li.get('regularMarketPrice') or 0)
                    rr = float(li.get('trailingAnnualDividendRate') or li.get('dividendRate') or 0)
                    ry, _ = _resolve_yield(li, _lpx)
                    _lfreq = li.get('dividendFrequency') or 'Quarterly (assumed)'
                    calc_info = {'ticker': sym,
                        'price': _lpx,
                        'yield_pct': round((ry or 0) * 100, 2), 'div_rate': rr,
                        'monthly_pay': round(rr / 12, 4) if rr else None,
                        'frequency': _lfreq,
                        'payout': round((li.get('payoutRatio') or 0) * 100, 1) or None,
                        'sector': li.get('sector') or 'Unknown'}
            else:
                st.warning(sym + ' not in scan data. Click **Refresh** for live lookup, or pick from the list.')
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
            _n   = _freq_to_pays(calc_info['frequency'])
            spay = annd / _n
            slbl = {12:'Per monthly payment', 4:'Per quarterly payment',
                    2:'Per semi-annual payment', 1:'Per annual payment'}[_n]
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
    if not live_mode:
        st.caption('Using scan data only. Click **Refresh** in the sidebar for live Yahoo/Claude data.')
    # Show setup tip if no API key configured
    _keys = {
        'Alpaca (price, bars, dividends)': bool(_secret('APCA_API_KEY_ID') and
                                                _secret('APCA_API_SECRET_KEY')),
        'Financial Modeling Prep (ratios)': bool(_secret('FMP_API_KEY')),
        'Finnhub (metrics, analyst targets)': bool(_secret('FINNHUB_API_KEY')),
        'Claude web search (last resort)': bool(_secret('ANTHROPIC_API_KEY')),
    }
    _alp_on = _keys['Alpaca (price, bars, dividends)']
    with st.expander(('Data sources -- ' + str(sum(_keys.values()))
                      + ' of 4 connected') + ('' if _alp_on else '  (Alpaca NOT detected)'),
                     expanded=not _alp_on):
        _dcol1, _dcol2 = st.columns([1, 2])
        with _dcol1:
            _diag_sym = st.text_input('Test symbol', value='AAPL', key='alp_diag_sym')
            _run_diag = st.button('Verify Alpaca connection', key='alp_diag_btn')
        with _dcol2:
            st.caption('Runs a live end-to-end check: credentials, market-data '
                       'auth, feed entitlement, account type, daily bars and the '
                       'dividend feed. Nothing is written and no key is displayed.')
        if _run_diag:
            with st.spinner('Testing Alpaca...'):
                _steps = _alpaca_diagnose((_diag_sym or 'AAPL').upper().strip())
            for _label, _ok, _detail in _steps:
                st.markdown(('**PASS** -- ' if _ok else '**FAIL** -- ') + _label)
                st.caption(_detail)
            if all(s[1] for s in _steps):
                st.success('Alpaca is working. If fields are still blank, they are '
                           'fundamentals Alpaca does not carry -- add FMP_API_KEY or '
                           'FINNHUB_API_KEY for ratios, margins and analyst targets.')
            else:
                st.warning('Fix the failing step above, then press Refresh in the '
                           'sidebar to clear cached results.')
        st.markdown('---')
        for _label, _on in _keys.items():
            st.markdown(('- **Connected** -- ' if _on else '- Not set -- ') + _label)
        st.markdown(
            'Add keys under the 3-dot menu -> Settings -> Secrets. Note that '
            'secrets.toml uses `=` and quoted values, and a key pasted with a '
            'trailing space or line break will fail auth:\n'
            '```\n'
            'APCA_API_KEY_ID = "PK..."\n'
            'APCA_API_SECRET_KEY = "..."\n'
            'FMP_API_KEY = "..."\n'
            'FINNHUB_API_KEY = "..."\n'
            'ANTHROPIC_API_KEY = "sk-ant-..."\n'
            '```\n'
            'Alpaca and Finnhub have free tiers and are the biggest wins: both '
            'authenticate by key, so neither is affected by the shared-IP rate '
            'limiting that blocks Yahoo Finance on Streamlit Cloud.'
        )
    az1, az2 = st.columns([2,3])
    with az1:
        az_ticker = st.text_input('Ticker symbol', placeholder='e.g. ET, WPM, DOC', key='az_ticker')
        az_btn = st.button('Analyze', type='primary', key='az_analyze')
    # Trigger fetch on button click, store in session_state so
    # the number_input below doesn't reset the whole analysis
    if az_btn and az_ticker:
        _has_api = bool(st.secrets.get('ANTHROPIC_API_KEY', ''))
        _spin_msg = 'Querying all sources for ' + az_ticker.upper() + '...'
        with st.spinner(_spin_msg):
            # Analyze is an explicit, single-ticker action -- run every source.
            # (live_mode only ever gated this to keep app STARTUP fast, which
            #  is irrelevant here; results are cached for an hour anyway.)
            _res = fetch_stock_analysis(az_ticker, live=True)
        # fetch returns 5 values now (added source_summary)
        ai, ah, ad, ae = _res[0], _res[1], _res[2], _res[3]
        _src = _res[4] if len(_res) > 4 else {}
        if ae or not ai:
            st.error('Could not load ' + az_ticker.upper() + ': ' + str(ae or 'no data returned'))
            st.session_state.pop('az_result', None)
        else:
            st.session_state['az_result'] = (ai, ah, ad, _src)
    # Render from session_state -- survives reruns caused by number_input
    if 'az_result' in st.session_state:
        _stored = st.session_state['az_result']
        ai, ah, ad = _stored[0], _stored[1], _stored[2]
        _src = _stored[3] if len(_stored) > 3 else {}
        ae = None
    if 'az_result' in st.session_state:
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
            dr    = float(ai.get('trailingAnnualDividendRate') or ai.get('dividendRate') or 0)
            # Yield is derived from rate/price so it always agrees with the
            # Div/Share figure shown below it. _resolve_yield also repairs
            # yfinance's percent-vs-decimal inconsistency.
            ry, _reported_y = _resolve_yield(ai, px)
            dy    = round((ry or 0) * 100, 2)
            _y_mismatch = (ry and _reported_y and abs(ry - _reported_y) > 0.005)
            # EPS-based payout ratio is not meaningful for REITs (use FFO/AFFO),
            # closed-end funds (distributions come from NAV/ROC) or MLPs (DCF).
            pout  = ai.get('payoutRatio')
            _pass_thru = sec in ('Real Estate', 'Financial Services', 'Energy')
            _payout_flag = ' *' if (_pass_thru and pout and pout > 0.9) else ''
            ex_ts = ai.get('exDividendDate')
            ex_dt = None
            if ex_ts:
                ex_dt = _epoch_to_date(ex_ts)
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
            # Frequency priority: counted actual payments > scan field > default
            _scan_freq = ai.get('dividendFrequency') or ''
            if not ad.empty:
                ad = _tz_naive(ad)
                oyr = pd.Timestamp.now().normalize() - pd.DateOffset(years=1)
                try:
                    _recent = ad[ad.index >= oyr]; n = len(_recent)
                except TypeError:
                    n = len(ad)
                if n >= 10:  pays_yr=12; freq2='Monthly'
                elif n >= 3: pays_yr=4;  freq2='Quarterly'
                elif n == 2: pays_yr=2;  freq2='Semi-Annual'
                elif n == 1: pays_yr=1;  freq2='Annual'
                else:        pays_yr=4;  freq2='Quarterly'
            elif _scan_freq and _scan_freq != '--':
                freq2 = _scan_freq; pays_yr = _freq_to_pays(_scan_freq)
            else:
                pays_yr = 4; freq2 = 'Quarterly (assumed)'
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
            _notes = []
            if _y_mismatch:
                _notes.append('Yield shown is Div/Share divided by Price ('
                    + '{:.2f}'.format(dy) + '%). The data feed reports '
                    + '{:.2f}'.format((_reported_y or 0) * 100)
                    + '% -- the gap usually means the feed mixes a forward rate '
                      'with a trailing yield, or a special dividend is included.')
            if _payout_flag:
                _notes.append('* Payout ratio above 100% is normal for this sector. '
                    'REITs distribute from FFO, closed-end funds from NAV and return '
                    'of capital, MLPs from distributable cash flow -- none of which '
                    'are earnings per share, so the EPS-based ratio overstates risk.')
            if freq2.endswith('(assumed)'):
                _notes.append('Payment frequency was not reported, so quarterly is '
                    'assumed. Per-payment amounts below are estimates.')
            for _n_ in _notes:
                st.caption(_n_)

            # ── Data source readout ─────────────────────────────────────
            _PROV = [
                ('alpaca',   'Alpaca',      '#22d3a6', 'Price, daily bars, cash dividends and company name. Alpaca does not publish fundamentals -- a low count here is normal, not a failure.'),
                ('yfinance', 'Yahoo',       '#4ac4ff', 'Broadest fundamentals set, but frequently rate-limited on Streamlit Cloud shared IPs.'),
                ('fmp',      'FMP',         '#ff9f4a', 'Financial Modeling Prep: TTM valuation and profitability ratios.'),
                ('finnhub',  'Finnhub',     '#7ee081', 'Finnhub: ratios, growth, 52-week range and analyst price targets.'),
                ('scan',     'Nightly Scan','#ffe066', 'Your own nightly screener dump from the repo.'),
                ('derived',  'Computed',    '#9ecbff', 'Calculated from data already retrieved (e.g. P/E from price and EPS, beta from returns vs SPY).'),
                ('claude',   'Claude Web',  '#b388ff', 'Claude web search, used last and only for fields still empty.'),
            ]
            def _src_badge(label, count, color, why=''):
                if not count: return ''
                return (
                    '<span title="' + why.replace('"', '&quot;') + '" '
                    'style="display:inline-flex;align-items:center;gap:5px;'
                    'background:rgba(255,255,255,0.05);border:1px solid ' + color + ';'
                    'border-radius:5px;padding:3px 10px;font-size:0.72rem;'
                    'font-family:DM Mono,monospace;color:' + color + ';'
                    'margin:0 6px 4px 0">' + label + ' ' + str(count) + '</span>'
                )
            _byp   = _src.get('by_provider', {}) or {}
            _appl  = _src.get('applicable', len(_YF_FIELDS)) or len(_YF_FIELDS)
            _fill  = _src.get('filled', 0)
            _navs  = _src.get('na_fields', []) or []
            _gone  = _src.get('none_fields', []) or []
            _hs    = _src.get('hist_source', 'unknown')
            _hist_label = {'alpaca':'Alpaca', 'yfinance':'Yahoo Finance',
                           'scan':'Nightly Scan', 'stooq':'Stooq',
                           'none':'unavailable'}.get(_hs, _hs)
            _badges = ''.join(_src_badge(lbl, _byp.get(k, 0), col, why)
                              for k, lbl, col, why in _PROV)
            if _gone:
                _badges += _src_badge('Not found', len(_gone), '#ff6666',
                                      'No connected source publishes these fields.')
            _pct = int(_fill / _appl * 100) if _appl else 0
            _bar_col = '#22d3a6' if _pct >= 90 else ('#ffe066' if _pct >= 70 else '#ff9f4a')
            if _src:
                st.markdown(
                    '<div style="margin:4px 0 6px">'
                    '<span style="font-size:0.65rem;color:#666;text-transform:uppercase;'
                    'letter-spacing:0.1em;font-family:DM Mono,monospace">Field coverage </span>'
                    '<span style="font-family:DM Mono,monospace;font-size:0.78rem;color:'
                    + _bar_col + ';font-weight:600">' + str(_fill) + '/' + str(_appl)
                    + ' (' + str(_pct) + '%)</span>'
                    '<span style="font-size:0.68rem;color:#666;font-family:DM Mono,monospace">'
                    '  |  Price chart: ' + _hist_label
                    + ('  |  In nightly scan' if _src.get('scan_in_repo')
                       else '  |  Not in nightly scan') + '</span></div>'
                    '<div style="margin:0 0 10px">' + _badges + '</div>',
                    unsafe_allow_html=True
                )
                if _navs:
                    st.caption('Not applicable to this security type ('
                        + str(len(_navs)) + ' fields): ' + ', '.join(_navs[:8])
                        + ('...' if len(_navs) > 8 else '')
                        + '. Funds and ETFs have no earnings, equity or analyst coverage.')
                if _gone:
                    _want = []
                    if not _secret('FINNHUB_API_KEY'):
                        _want.append('**FINNHUB_API_KEY** (free tier) -- covers P/E, '
                                     'P/B, P/S, margins, ROE, ROA, debt/equity, '
                                     'current ratio, growth and analyst targets')
                    if not _secret('FMP_API_KEY'):
                        _want.append('**FMP_API_KEY** (free tier) -- a second source '
                                     'for the same ratio set')
                    if not _secret('ANTHROPIC_API_KEY'):
                        _want.append('**ANTHROPIC_API_KEY** -- web-search fallback for '
                                     'anything the APIs miss')
                    st.caption('No source returned ' + str(len(_gone)) + ' field(s): '
                        + ', '.join(_gone[:8]) + ('...' if len(_gone) > 8 else ''))
                    if _want:
                        st.markdown('These are fundamentals. Alpaca does not carry '
                                    'them -- it is a price, bars and corporate-actions '
                                    'feed. To fill them, add:')
                        for _w in _want:
                            st.markdown('- ' + _w)
                    else:
                        st.caption('All sources are connected; these fields are simply '
                                   'not published for this security.')
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
                    mrow('Payout Ratio' + _payout_flag,
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
