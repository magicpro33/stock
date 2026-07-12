"""
Sector Money Flow tab for the magicpro33/stock Streamlit app.

v2: the sector-flow table is computed LIVE from data/stock_data.json.gz with a
user-adjustable lookback (5-63 trading days). The candidate pick list below it
still comes from the nightly Money Flow Engine run (fixed 21d lookback — the
backtest-validated setting).

Wire-up in app.py:

    from money_flow_tab import render_money_flow_tab
    tab_screener, tab_analyze, tab_money = st.tabs(
        ["📊 Screener", "🔍 Analyze a Stock", "💸 Sector Money Flow"])
    with tab_money:
        render_money_flow_tab()
"""
import gzip
import json
import os

import numpy as np
import pandas as pd
import streamlit as st

RAW_BASE = "https://raw.githubusercontent.com/magicpro33/stock/main/data"
LOCAL_DIR = os.path.join(os.path.dirname(__file__), "data")
DUMP_NAME = "stock_data.json.gz"
CSV_NAME = "money_flow_picks.csv"
JSON_NAME = "money_flow_picks_sectors.json"

MIN_PRICE = 3.0
MIN_MED_DOLLAR_VOL = 2e6
MIN_SECTOR_NAMES = 20


# ────────────────────────────────────────────────────────────────────
# Compact price/volume panel, parsed once and cached (~11 MB resident)
# ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner="Loading price history for flow analysis…")
def _load_flow_panel():
    path = os.path.join(LOCAL_DIR, DUMP_NAME)
    if os.path.exists(path):
        with gzip.open(path, "rt") as f:
            data = json.load(f)
    else:  # stale checkout fallback
        import io
        import urllib.request
        with urllib.request.urlopen(f"{RAW_BASE}/{DUMP_NAME}", timeout=60) as r:
            data = json.load(io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(r.read())), encoding="utf-8"))

    rows = [r for r in data if len(r.get("_hist", {}).get("dates", [])) >= 30]
    all_dates = sorted({d for r in rows for d in r["_hist"]["dates"]})
    dix = {d: i for i, d in enumerate(all_dates)}
    T, N = len(all_dates), len(rows)
    close = np.full((T, N), np.nan, dtype=np.float32)
    vol = np.zeros((T, N), dtype=np.float32)
    sectors, tickers = [], []
    for j, r in enumerate(rows):
        h = r["_hist"]
        ix = [dix[d] for d in h["dates"]]
        close[ix, j] = h["close"]
        vol[ix, j] = h["volume"]
        sectors.append(r.get("Sector") or "Unknown")
        tickers.append(r["Ticker"])
    close = pd.DataFrame(close).ffill(limit=5).values.astype(np.float32)
    del data, rows
    return close, vol, np.array(sectors), np.array(tickers), np.array(all_dates)


def _sector_flows(lookback: int):
    """Net signed dollar flow per sector over the trailing `lookback` days."""
    close, vol, sectors, _, dates = _load_flow_panel()
    T = close.shape[0]
    lb = int(min(lookback, T - 2))
    dvol = close * vol
    ret = np.full_like(close, np.nan)
    ret[1:] = close[1:] / close[:-1] - 1.0
    signed = np.sign(np.nan_to_num(ret)) * dvol

    last_close = close[-1]
    med_dvol = np.nanmedian(dvol[-21:], axis=0)
    ok = np.isfinite(last_close) & (last_close >= MIN_PRICE) & (med_dvol >= MIN_MED_DOLLAR_VOL)

    win_signed, win_dvol = signed[-lb:], dvol[-lb:]
    out = []
    for sc in sorted(set(sectors) - {"Unknown"}):
        m = ok & (sectors == sc)
        if m.sum() < MIN_SECTOR_NAMES:
            continue
        denom = np.nansum(win_dvol[:, m])
        if denom <= 0:
            continue
        out.append((sc, float(np.nansum(win_signed[:, m]) / denom), int(m.sum())))
    flows = pd.DataFrame(out, columns=["Sector", "NetFlow", "Names"]).sort_values(
        "NetFlow", ascending=False, ignore_index=True)
    return flows, str(dates[-1])


@st.cache_data(ttl=1800)
def _load_picks():
    csv_local = os.path.join(LOCAL_DIR, CSV_NAME)
    json_local = os.path.join(LOCAL_DIR, JSON_NAME)
    try:
        if os.path.exists(csv_local):
            picks = pd.read_csv(csv_local)
            with open(json_local) as f:
                meta = json.load(f)
        else:
            import urllib.request
            picks = pd.read_csv(f"{RAW_BASE}/{CSV_NAME}")
            with urllib.request.urlopen(f"{RAW_BASE}/{JSON_NAME}", timeout=15) as r:
                meta = json.load(r)
        return picks, meta, None
    except Exception as e:
        return None, None, str(e)


def _flow_color(v):
    # red -> yellow -> green ramp over [-0.2, +0.2], no matplotlib needed
    x = max(-0.2, min(0.2, float(v))) / 0.2
    if x >= 0:
        r, g = int(255 * (1 - x)), 200
    else:
        r, g = 255, int(200 * (1 + x))
    return f"background-color: rgb({r},{g},110); color: black"


def render_money_flow_tab():
    st.subheader("💸 Sector Money Flow")

    lookback = st.slider(
        "Flow lookback (trading days)",
        min_value=5, max_value=63, value=21, step=1,
        help="Days of net signed dollar flow used to rank sectors. Backtest notes: "
        "21d was the validated setting (IC 0.10 full-year, 0.199 out-of-sample); "
        "10d was weaker but positive; 42d+ showed no edge. Shorter = faster, noisier.",
    )

    try:
        flows, as_of = _sector_flows(lookback)
    except Exception as e:
        st.error(f"Could not compute live sector flows: {e}")
        flows, as_of = None, None

    if flows is not None and not flows.empty:
        top3 = set(flows.head(3)["Sector"])
        flows["Leading"] = flows["Sector"].isin(top3)
        st.caption(
            f"Net signed dollar flow, trailing {lookback} trading days — data through "
            f"{as_of}. Positive = money moving in. Filter: price ≥ ${MIN_PRICE:.0f}, "
            f"median $vol ≥ ${MIN_MED_DOLLAR_VOL/1e6:.0f}M."
        )
        st.dataframe(
            flows.style
            .map(_flow_color, subset=["NetFlow"])
            .format({"NetFlow": "{:+.3f}"}),
            width="stretch",
            hide_index=True,
        )
        st.bar_chart(flows.set_index("Sector")["NetFlow"])
        if lookback != 21:
            st.caption(
                "⚠️ Sector table reflects your custom lookback. The pick list below is "
                "generated nightly with the validated 21d setting and does not change "
                "with the slider."
            )

    # ── nightly pick list (fixed 21d engine output) ──────────────────
    picks, meta, err = _load_picks()
    if err:
        st.info(
            "Nightly pick list not found — the Money Flow Engine Action hasn't "
            "committed `data/money_flow_picks.csv` yet. Trigger it manually in "
            "Actions, then reload."
        )
        return
    if picks is None or picks.empty or not meta.get("sector_flows"):
        st.warning("Pick files exist but are empty — check the last Money Flow Engine run in Actions.")
        return

    st.subheader(
        f"Top candidates in leading sectors ({', '.join(meta['top_sectors'])})"
    )
    st.caption(
        f"Nightly engine output as of {meta['as_of']} (21d lookback). Score = 40% "
        "intermediate momentum + 25% relative volume + 20% range position + 15% base "
        "tightness, halved below the 50d MA. SuggestedStop = entry −8%. Weekly refresh "
        "cadence; research tool, not investment advice."
    )
    show = picks.copy()
    if "Score" in show:
        show = show.sort_values("Score", ascending=False)
    st.dataframe(
        show.style.format(
            {c: "{:.2f}" for c in ("Price", "RVOL", "SuggestedStop") if c in show}
            | {c: "{:.1%}" for c in ("Mom63", "OE_Yield", "ROIC", "ShortPctFloat") if c in show}
            | ({"Score": "{:.3f}"} if "Score" in show else {})
        ),
        width="stretch",
        hide_index=True,
        height=600,
    )
    st.download_button(
        "Download picks CSV",
        picks.to_csv(index=False).encode(),
        file_name=CSV_NAME,
        mime="text/csv",
    )
