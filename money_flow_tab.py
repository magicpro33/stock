"""
Sector Money Flow tab — v3 (fully live).

Layout: diverging flow leaderboard (Option B) + dropdown lookback (Option C).
Both the sector ranking AND the ticker picks recompute from the selected
lookback: the lookback chooses which sectors lead, picks are then scored
inside those sectors with the backtest-validated windows (63d momentum,
5/63d relative volume, 63d range position, 21d base tightness, 50d MA gate).

No dependency on the nightly CSV — everything computes from
data/stock_data.json.gz, parsed once per hour and cached compactly.

Wire-up in app.py (unchanged from v2):
    from money_flow_tab import render_money_flow_tab
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

MIN_PRICE = 3.0
MIN_MED_DOLLAR_VOL = 2e6
MIN_SECTOR_NAMES = 20
TOP_SECTORS = 3
STOP_PCT = -0.08

LOOKBACK_CHOICES = {
    "5 days — very fast, noisy": 5,
    "10 days — fast": 10,
    "21 days — validated default": 21,
    "42 days — slow": 42,
    "63 days — quarter": 63,
}

POS = ["#9FE1CB", "#5DCAA5", "#1D9E75"]          # teal ramp, light -> strong
NEG = ["#F09595", "#E24B4A", "#A32D2D"]          # red ramp


# ────────────────────────────────────────────────────────────────────
# One-time parse of the dump into compact float32 panels (~22 MB)
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
            data = json.load(io.TextIOWrapper(
                gzip.GzipFile(fileobj=io.BytesIO(r.read())), encoding="utf-8"))

    rows = [r for r in data if len(r.get("_hist", {}).get("dates", [])) >= 70]
    all_dates = sorted({d for r in rows for d in r["_hist"]["dates"]})
    dix = {d: i for i, d in enumerate(all_dates)}
    T, N = len(all_dates), len(rows)
    close = np.full((T, N), np.nan, dtype=np.float32)
    high = np.full((T, N), np.nan, dtype=np.float32)
    low = np.full((T, N), np.nan, dtype=np.float32)
    vol = np.zeros((T, N), dtype=np.float32)
    sectors, tickers, pio, spf = [], [], [], []
    for j, r in enumerate(rows):
        h = r["_hist"]
        ix = [dix[d] for d in h["dates"]]
        close[ix, j] = h["close"]
        high[ix, j] = h["high"]
        low[ix, j] = h["low"]
        vol[ix, j] = h["volume"]
        sectors.append(r.get("Sector") or "Unknown")
        tickers.append(r["Ticker"])
        pio.append(r.get("Piotroski") if r.get("Piotroski") is not None else np.nan)
        spf.append(r.get("ShortPctFloat") if r.get("ShortPctFloat") is not None else np.nan)
    close = pd.DataFrame(close).ffill(limit=5).values.astype(np.float32)
    del data, rows
    return (close, high, low, vol, np.array(sectors), np.array(tickers),
            np.array(pio, dtype=np.float32), np.array(spf, dtype=np.float32),
            np.array(all_dates))


# ────────────────────────────────────────────────────────────────────
# Lookback-independent scoring (cached) — validated windows never move
# ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _score_universe():
    close, high, low, vol, sectors, tickers, pio, spf, dates = _load_flow_panel()
    T = close.shape[0]
    t = T - 1
    last = close[t]
    dvol = close * vol
    med_dvol = np.nanmedian(dvol[-21:], axis=0)
    ok = np.isfinite(last) & (last >= MIN_PRICE) & (med_dvol >= MIN_MED_DOLLAR_VOL)

    mom = close[t - 5] / close[t - 63] - 1.0
    rvol = vol[t - 4:t + 1].mean(0) / np.where(
        vol[t - 62:t + 1].mean(0) == 0, np.nan, vol[t - 62:t + 1].mean(0))
    hi21 = np.nanmax(high[t - 20:t + 1], 0)
    lo21 = np.nanmin(low[t - 20:t + 1], 0)
    base = -np.where(lo21 > 0, (hi21 - lo21) / lo21, np.nan)
    hi63 = np.nanmax(high[t - 62:t + 1], 0)
    lo63 = np.nanmin(low[t - 62:t + 1], 0)
    rangepos = (last - lo63) / np.where(hi63 - lo63 == 0, np.nan, hi63 - lo63)
    sma50 = np.nanmean(close[t - 49:t + 1], 0)
    above = last > sma50

    df = pd.DataFrame({
        "Ticker": tickers, "Sector": sectors, "Price": last,
        "Mom63": mom, "RVOL": rvol, "Base": base, "RangePos": rangepos,
        "AboveMA50": above, "Piotroski": pio, "ShortPctFloat": spf,
        "Tradeable": ok,
    })
    for col, w in (("Mom63", 0.40), ("RVOL", 0.25), ("Base", 0.15), ("RangePos", 0.20)):
        df[f"_r_{col}"] = df.loc[df.Tradeable, col].rank(pct=True)
    rank_cols = [c for c in df.columns if c.startswith("_r_")]
    df["Score"] = (0.40 * df["_r_Mom63"] + 0.25 * df["_r_RVOL"]
                   + 0.15 * df["_r_Base"] + 0.20 * df["_r_RangePos"])
    df.loc[~df.AboveMA50, "Score"] *= 0.5
    df["Stop"] = df["Price"] * (1 + STOP_PCT)
    return df.drop(columns=rank_cols), str(dates[-1])


def _sector_flows(lookback: int):
    close, high, low, vol, sectors, tickers, pio, spf, dates = _load_flow_panel()
    T = close.shape[0]
    lb = int(min(lookback, T - 2))
    dvol = close * vol
    ret = np.full_like(close, np.nan)
    ret[1:] = close[1:] / close[:-1] - 1.0
    signed = np.sign(np.nan_to_num(ret)) * dvol
    last = close[-1]
    med_dvol = np.nanmedian(dvol[-21:], axis=0)
    ok = np.isfinite(last) & (last >= MIN_PRICE) & (med_dvol >= MIN_MED_DOLLAR_VOL)
    win_s, win_d = signed[-lb:], dvol[-lb:]
    out = []
    for sc in sorted(set(sectors) - {"Unknown"}):
        m = ok & (sectors == sc)
        if m.sum() < MIN_SECTOR_NAMES:
            continue
        denom = np.nansum(win_d[:, m])
        if denom > 0:
            out.append((sc, float(np.nansum(win_s[:, m]) / denom), int(m.sum())))
    return pd.DataFrame(out, columns=["Sector", "NetFlow", "Names"]).sort_values(
        "NetFlow", ascending=False, ignore_index=True)


def _shade(v, vmax):
    ramp = POS if v >= 0 else NEG
    x = min(abs(v) / vmax, 1.0) if vmax > 0 else 0
    return ramp[2] if x > 0.66 else (ramp[1] if x > 0.33 else ramp[0])


def _flow_bars_html(flows: pd.DataFrame) -> str:
    """Diverging horizontal bars, zero-line anchored, pure inline HTML."""
    maxpos = max(flows.NetFlow.max(), 1e-9)
    maxneg = max(-flows.NetFlow.min(), 0.0)
    zero = maxneg / (maxneg + maxpos) if maxneg > 0 else 0.0
    vmax = max(maxpos, maxneg)
    rows = []
    for _, r in flows.iterrows():
        v, color = r.NetFlow, _shade(r.NetFlow, vmax)
        if v >= 0:
            w = (v / maxpos) * (1 - zero) * 100
            bar = (f'<div style="margin-left:{zero*100:.1f}%;width:{max(w,0.8):.1f}%;'
                   f'height:15px;background:{color};border-radius:0 4px 4px 0;"></div>')
            val_color = "#5DCAA5"
        else:
            w = (-v / maxneg) * zero * 100
            bar = (f'<div style="margin-left:{(zero*100 - w):.1f}%;width:{max(w,0.8):.1f}%;'
                   f'height:15px;background:{color};border-radius:4px 0 0 4px;"></div>')
            val_color = "#F09595"
        rows.append(
            '<div style="display:flex;align-items:center;margin-bottom:6px;font-size:13px;">'
            f'<span style="width:110px;flex-shrink:0;opacity:.75;overflow:hidden;'
            f'text-overflow:ellipsis;white-space:nowrap;">{r.Sector}</span>'
            f'<div style="flex:1;position:relative;">{bar}</div>'
            f'<span style="width:56px;flex-shrink:0;text-align:right;font-weight:600;'
            f'color:{val_color};">{v:+.3f}</span></div>')
    zline = (f'<div style="position:relative;height:0;"><div style="position:absolute;'
             f'left:calc(110px + {zero:.3f} * (100% - 166px));top:0;width:1px;height:2px;"></div></div>')
    return zline + "".join(rows)


def render_money_flow_tab():
    st.subheader("💸 Sector Money Flow")

    label = st.selectbox(
        "Flow lookback",
        list(LOOKBACK_CHOICES),
        index=2,
        help="Days of net signed dollar flow used to rank sectors — the picks "
        "below follow the sectors this lookback selects. Backtest: 21d was the "
        "validated setting (IC 0.199 out-of-sample, 88% weekly hit rate); 10d "
        "weaker but positive; 42d+ showed no edge.",
    )
    lookback = LOOKBACK_CHOICES[label]

    try:
        flows = _sector_flows(lookback)
        scored, as_of = _score_universe()
    except Exception as e:
        st.error(f"Could not compute sector flows: {e}")
        return
    if flows.empty:
        st.warning("No sectors passed the liquidity filter — check the nightly dump.")
        return

    st.caption(
        f"Net signed dollar flow, trailing {lookback} trading days — data through "
        f"{as_of}. Positive = money moving in. Filter: price ≥ ${MIN_PRICE:.0f}, "
        f"median $vol ≥ ${MIN_MED_DOLLAR_VOL/1e6:.0f}M."
    )
    st.markdown(_flow_bars_html(flows), unsafe_allow_html=True)

    leaders = list(flows.head(TOP_SECTORS)["Sector"])
    st.subheader(f"Top candidates · {', '.join(leaders)}")
    st.caption(
        f"Live picks inside the top-{TOP_SECTORS} flow sectors at {lookback}d. "
        "Score = 40% momentum (63d, skip last wk) + 25% relative volume + 20% "
        "range position + 15% base tightness, halved below the 50d MA. "
        "Stop = entry −8%. Research tool, not investment advice."
    )

    picks = (scored[scored.Tradeable & scored.Sector.isin(leaders) & scored.Score.notna()]
             .sort_values("Score", ascending=False))
    n_show = 30 if st.toggle("Show 30 picks", value=False) else 12
    for _, r in picks.head(n_show).iterrows():
        title = f"{r.Ticker}  ·  ${r.Price:,.2f}  ·  score {r.Score:.2f}"
        with st.expander(title):
            c1, c2 = st.columns(2)
            c1.metric("Momentum 63d", f"{r.Mom63:+.1%}" if np.isfinite(r.Mom63) else "—")
            c2.metric("Relative volume", f"{r.RVOL:.2f}x" if np.isfinite(r.RVOL) else "—")
            c3, c4 = st.columns(2)
            c3.metric("Range position", f"{r.RangePos:.0%}" if np.isfinite(r.RangePos) else "—")
            c4.metric("Suggested stop", f"${r.Stop:,.2f}")
            bits = [f"Sector: {r.Sector}"]
            if np.isfinite(r.Piotroski):
                q = "strong" if r.Piotroski >= 6 else ("ok" if r.Piotroski >= 4 else "weak")
                bits.append(f"Piotroski {int(r.Piotroski)} ({q})")
            if np.isfinite(r.ShortPctFloat):
                bits.append(f"Short float {r.ShortPctFloat:.1%}")
            if not r.AboveMA50:
                bits.append("⚠️ below 50d MA — score halved")
            st.caption(" · ".join(bits))

    st.download_button(
        "Download picks CSV",
        picks.head(n_show).drop(columns=["Tradeable", "Base"]).to_csv(index=False).encode(),
        file_name=f"money_flow_picks_{lookback}d.csv",
        mime="text/csv",
    )
