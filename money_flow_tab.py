"""
Money Flow tab for the magicpro33/stock Streamlit app.

Wire-up (2 lines in your main app):

    from money_flow_tab import render_money_flow_tab
    ...
    tab_mf, = st.tabs(["💸 Money Flow"])        # or add to your existing st.tabs(...)
    with tab_mf:
        render_money_flow_tab()

Reads the files committed by the money_flow.yml GitHub Action. Uses local repo
files when present (Streamlit Cloud auto-redeploys on commit); falls back to
raw.githubusercontent so it also works if the app runs from a stale checkout.
"""
import json
import os

import pandas as pd
import streamlit as st

RAW_BASE = "https://raw.githubusercontent.com/magicpro33/stock/main/data"
LOCAL_DIR = os.path.join(os.path.dirname(__file__), "data")
CSV_NAME = "money_flow_picks.csv"
JSON_NAME = "money_flow_picks_sectors.json"


@st.cache_data(ttl=1800)  # refresh every 30 min, same cadence as the rest of the app
def _load():
    csv_local = os.path.join(LOCAL_DIR, CSV_NAME)
    json_local = os.path.join(LOCAL_DIR, JSON_NAME)
    try:
        if os.path.exists(csv_local):
            picks = pd.read_csv(csv_local)
            with open(json_local) as f:
                meta = json.load(f)
        else:
            picks = pd.read_csv(f"{RAW_BASE}/{CSV_NAME}")
            import urllib.request
            with urllib.request.urlopen(f"{RAW_BASE}/{JSON_NAME}", timeout=15) as r:
                meta = json.load(r)
        return picks, meta, None
    except Exception as e:  # first run before the Action has committed anything
        return None, None, str(e)


def render_money_flow_tab():
    picks, meta, err = _load()
    if err:
        st.info(
            "Money-flow output not found yet — the nightly Action hasn't committed "
            "`data/money_flow_picks.csv` on this branch. Trigger the *Money Flow Engine* "
            "workflow manually once, then reload."
        )
        st.caption(f"Loader detail: {err}")
        return

    st.subheader("💸 Sector Money Flow")
    st.caption(
        f"Net signed dollar flow, trailing {meta['flow_lookback']} trading days — "
        f"as of {meta['as_of']}. Positive = money moving in."
    )

    flows = pd.DataFrame(
        [(k, v) for k, v in meta["sector_flows"].items()],
        columns=["Sector", "NetFlow"],
    )
    flows["Leading"] = flows["Sector"].isin(meta["top_sectors"])

    def _flow_color(v):
        # red -> yellow -> green ramp over [-0.2, +0.2], no matplotlib needed
        x = max(-0.2, min(0.2, float(v))) / 0.2          # -1..1
        if x >= 0:
            r, g = int(255 * (1 - x)), 200
        else:
            r, g = 255, int(200 * (1 + x))
        return f"background-color: rgb({r},{g},110); color: black"

    st.dataframe(
        flows.style
        .map(_flow_color, subset=["NetFlow"])
        .format({"NetFlow": "{:+.3f}"}),
        use_container_width=True,
        hide_index=True,
    )
    st.bar_chart(flows.set_index("Sector")["NetFlow"])

    st.subheader(f"Top candidates in leading sectors ({', '.join(meta['top_sectors'])})")
    st.caption(
        "Score = 40% intermediate momentum + 25% relative volume + 20% range position "
        "+ 15% base tightness, halved below the 50d MA. SuggestedStop = entry −8%. "
        "Weekly refresh cadence; research tool, not investment advice."
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
        use_container_width=True,
        hide_index=True,
        height=600,
    )

    st.download_button(
        "Download picks CSV",
        picks.to_csv(index=False).encode(),
        file_name=CSV_NAME,
        mime="text/csv",
    )
