# Hybrid Stock Screener

Screens S&P 500, NYSE, or NASDAQ for individual companies. ETFs and funds
are excluded. The Analyze tab is a full company profile (overview, valuation,
statements, price history).

**Run locally:** `streamlit run app.py` (Python 3.10+)

**Deploy**
1. Push this repo and connect it on [share.streamlit.io](https://share.streamlit.io). Main file: `app.py`.
2. Optional Cloud secrets (top-level, not nested): `GITHUB_REPO` (defaults to `magicpro33/stock`), `AV_API_KEY`. See `.streamlit/secrets.toml.example`.
3. First visit downloads the nightly dump from GitHub (`data/stock_data.json.gz`) — can take up to ~2 minutes — then caches it. Do not ship the gz in the Cloud repo if it is stale; the Action writes a fresh one.
4. Ship together: `app.py`, `alpha_vantage_fallback.py`, `requirements.txt`, `.streamlit/config.toml`, `assets/aiupscale_logo.png`. Nightly scan lives in `.github/workflows/nightly_scan.yml`.
5. In the GitHub repo, add `AV_API_KEY` as an Actions secret if you want the nightly Yahoo fallback.

Research tool. Not investment advice.
