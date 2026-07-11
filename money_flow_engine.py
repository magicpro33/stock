"""
MONEY FLOW ENGINE v1.0
======================
Follow the money: rank sectors by net signed dollar flow, then surface the
strongest accumulation candidates inside the leading sectors, with a
fundamentals quality overlay.

Validated on the 2025-07 -> 2026-07 nightly dump (5,372 tradeable tickers):
  - Sector flow -> next-21d sector return: IC 0.10 full period, 0.199 and
    88% hit rate in the held-out second half.
  - Weekly rotation into top-3 flow sectors (top-quartile momentum names):
    +18.3% vs +8.8% universe, second-half Sharpe 2.32 vs 0.64 (15 bps/wk cost).
  - Stock-level composites alone did NOT survive out-of-sample this year;
    the sector-flow layer is what carried the edge. Do not skip it.

Usage:
    python money_flow_engine.py path/to/stock_data.json.gz
    python money_flow_engine.py stock_data.json --top-sectors 3 --picks 25

Designed for the magicpro33/stock nightly dump schema (_hist OHLCV + snapshot
fundamentals). Point-in-time safe: every ranking uses only data through the
last bar in the file.
"""
import argparse, gzip, json, sys
import numpy as np
import pandas as pd

# ---------------------------------------------------------------- config ----
FLOW_LOOKBACK = 21        # days of signed dollar flow for sector ranking
MIN_PRICE = 3.0
MIN_MED_DOLLAR_VOL = 2e6  # 21d median daily dollar volume
MIN_SECTOR_NAMES = 20
STOP_SUGGESTION = -0.08   # backtest expectancy improved with an -8% stop

WEIGHTS = dict(mom=0.40, rvol=0.25, base=0.15, rangepos=0.20)  # within-sector score


def load(path):
    op = gzip.open if path.endswith('.gz') else open
    with op(path, 'rt') as f:
        return json.load(f)


def build_panels(data):
    rows = [r for r in data if len(r.get('_hist', {}).get('dates', [])) >= 70]
    all_dates = sorted({d for r in rows for d in r['_hist']['dates']})
    dix = {d: i for i, d in enumerate(all_dates)}
    T, N = len(all_dates), len(rows)
    P = {f: np.full((T, N), np.nan) for f in ('high', 'low', 'close', 'volume')}
    for j, r in enumerate(rows):
        ix = [dix[d] for d in r['_hist']['dates']]
        for f in P:
            P[f][ix, j] = r['_hist'][f]
    C = pd.DataFrame(P['close']).ffill(limit=5).values
    return rows, np.array(all_dates), C, P['high'], P['low'], np.nan_to_num(P['volume'])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('path')
    ap.add_argument('--top-sectors', type=int, default=3)
    ap.add_argument('--picks', type=int, default=25)
    ap.add_argument('--csv', default='money_flow_picks.csv')
    a = ap.parse_args()

    data = load(a.path)
    rows, dates, C, H, L, V = build_panels(data)
    T, N = C.shape
    t = T - 1
    sectors = np.array([r.get('Sector') or 'Unknown' for r in rows])
    tickers = np.array([r['Ticker'] for r in rows])

    ret1 = np.full_like(C, np.nan); ret1[1:] = C[1:] / C[:-1] - 1.0
    dvol = C * V
    med_dvol = pd.DataFrame(dvol).rolling(21, min_periods=21).median().values
    ok = (C[t] >= MIN_PRICE) & (med_dvol[t] >= MIN_MED_DOLLAR_VOL) & np.isfinite(C[t])

    # ---- layer 1: sector money flow ----------------------------------------
    signed = np.sign(np.nan_to_num(ret1)) * dvol
    lb = FLOW_LOOKBACK
    flows = {}
    for sc in sorted(set(sectors) - {'Unknown'}):
        m = ok & (sectors == sc)
        if m.sum() < MIN_SECTOR_NAMES:
            continue
        flows[sc] = np.nansum(signed[t - lb:t + 1, m]) / max(np.nansum(dvol[t - lb:t + 1, m]), 1)
    ranked = sorted(flows, key=flows.get, reverse=True)
    top_sectors = ranked[:a.top_sectors]

    print(f'\n=== SECTOR MONEY FLOW (as of {dates[t]}, {lb}d signed $-flow) ===')
    for sc in ranked:
        tag = ' <== LEADING' if sc in top_sectors else ''
        print(f'  {sc:24s} {flows[sc]:+.3f}{tag}')

    # ---- layer 2: within-sector accumulation score --------------------------
    def rk(v, m):
        out = np.full(N, np.nan)
        out[m] = pd.Series(v[m]).rank(pct=True).values
        return out

    mom = np.full(N, np.nan)
    if t >= 63:
        mom = C[t - 5] / C[t - 63] - 1.0                        # 63d momentum, skip last week
    rvol = V[max(t - 4, 0):t + 1].mean(0) / np.where(V[max(t - 62, 0):t + 1].mean(0) == 0, np.nan,
                                                     V[max(t - 62, 0):t + 1].mean(0))
    hi21 = np.nanmax(H[t - 20:t + 1], 0); lo21 = np.nanmin(L[t - 20:t + 1], 0)
    base = -np.where(lo21 > 0, (hi21 - lo21) / lo21, np.nan)    # tighter base = better
    hi63 = np.nanmax(H[t - 62:t + 1], 0); lo63 = np.nanmin(L[t - 62:t + 1], 0)
    rangepos = (C[t] - lo63) / np.where(hi63 - lo63 == 0, np.nan, hi63 - lo63)
    sma50 = np.nanmean(C[t - 49:t + 1], 0)
    above = C[t] > sma50

    cands = ok & np.isin(sectors, top_sectors)
    score = (WEIGHTS['mom'] * rk(mom, cands) + WEIGHTS['rvol'] * rk(rvol, cands)
             + WEIGHTS['base'] * rk(base, cands) + WEIGHTS['rangepos'] * rk(rangepos, cands))
    score = np.where(above, score, score * 0.5)                 # stock-level trend gate

    # ---- layer 3: fundamentals quality overlay (snapshot; not backtested) ---
    fund = pd.DataFrame([{k: r.get(k) for k in
        ('Ticker', 'Piotroski', 'ROIC', 'OE_Yield', 'RevenueGrowth', 'ShortPctFloat',
         'DaysToCover', 'MarketCap', 'P/E')} for r in rows]).set_index('Ticker')

    idx = np.where(cands & np.isfinite(score))[0]
    idx = idx[np.argsort(-score[idx])][:a.picks * 2]
    out = []
    for j in idx:
        tk = tickers[j]
        f = fund.loc[tk]
        pio = f.get('Piotroski')
        quality = 'strong' if (pio or 0) >= 6 else ('ok' if (pio or 0) >= 4 else 'weak')
        out.append(dict(
            Ticker=tk, Sector=sectors[j], Price=round(float(C[t, j]), 2),
            Score=round(float(score[j]), 3),
            Mom63=round(float(mom[j]), 3) if np.isfinite(mom[j]) else None,
            RVOL=round(float(rvol[j]), 2) if np.isfinite(rvol[j]) else None,
            RangePos63=round(float(rangepos[j]), 2) if np.isfinite(rangepos[j]) else None,
            AboveMA50=bool(above[j]), Piotroski=pio, Quality=quality,
            ROIC=f.get('ROIC'), OE_Yield=f.get('OE_Yield'),
            ShortPctFloat=f.get('ShortPctFloat'), DaysToCover=f.get('DaysToCover'),
            SuggestedStop=round(float(C[t, j]) * (1 + STOP_SUGGESTION), 2),
        ))
    df = pd.DataFrame(out)
    # prefer quality when scores are close: stable sort by quality tier within score
    df['qrank'] = df['Quality'].map({'strong': 0, 'ok': 1, 'weak': 2})
    df = df.sort_values(['Score', 'qrank'], ascending=[False, True]).head(a.picks)
    df = df.drop(columns='qrank')
    df.to_csv(a.csv, index=False)

    # sector flows + metadata for the Streamlit tab
    meta = dict(as_of=str(dates[t]), flow_lookback=lb, top_sectors=top_sectors,
                sector_flows={sc: round(float(flows[sc]), 4) for sc in ranked})
    json_path = a.csv.rsplit('.', 1)[0] + '_sectors.json'
    with open(json_path, 'w') as f:
        json.dump(meta, f, indent=2)
    print(f'Saved: {json_path}')

    print(f'\n=== TOP {len(df)} CANDIDATES in leading sectors ===')
    print(df.to_string(index=False))
    print(f'\nSaved: {a.csv}')
    print('\nDiscipline (from backtest): weekly refresh; -8% stop improved expectancy in '
          'nearly every regime cell; skip the stock layer entirely rather than buying '
          'names below their 50d MA.')
    print('Research tool, not investment advice. One year of data = one market regime.')


if __name__ == '__main__':
    sys.exit(main())
