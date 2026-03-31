# .github/workflows/nightly_scan.yml
#
# Runs nightly_scan.py every night at 2:00 AM Eastern Time (07:00 UTC).
# Saves the output to the data/ folder and commits it back to the repo.
# The Streamlit app reads from data/stock_data.json.gz on startup.
#
# To trigger manually: go to Actions → Nightly Stock Scan → Run workflow

name: Nightly Stock Scan

on:
  schedule:
    # 07:00 UTC = 02:00 AM EST (03:00 AM EDT during daylight saving)
    - cron: "0 7 * * 1-5"   # weekdays only — markets are closed on weekends

  workflow_dispatch:          # allows manual trigger from GitHub Actions UI
    inputs:
      reason:
        description: "Reason for manual run"
        required: false
        default: "Manual trigger"

jobs:
  scan:
    name: Download & compute stock data
    runs-on: ubuntu-latest
    timeout-minutes: 240      # 4 hour hard cap — kills stuck jobs

    steps:
      # ── 1. Check out the repo ────────────────────────────────────────────
      - name: Checkout repository
        uses: actions/checkout@v4

      # ── 2. Set up Python ─────────────────────────────────────────────────
      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      # ── 3. Install dependencies ──────────────────────────────────────────
      - name: Install dependencies
        run: |
          pip install --upgrade pip
          # lxml + html5lib required by pandas.read_html (S&P 500 Wikipedia table)
          pip install yfinance pandas numpy requests lxml html5lib

      # ── 4. Create data directory if it doesn't exist ─────────────────────
      - name: Ensure data directory exists
        run: mkdir -p data

      # ── 5. Run the nightly scan ───────────────────────────────────────────
      - name: Run nightly scan
        run: python nightly_scan.py
        timeout-minutes: 210

      # ── 6. Commit only if output files were actually produced ─────────────
      - name: Commit data files
        run: |
          if [ -f data/stock_data.json.gz ] && [ -f data/scan_meta.json ]; then
            git config user.name  "github-actions[bot]"
            git config user.email "github-actions[bot]@users.noreply.github.com"
            git add data/stock_data.json.gz data/scan_meta.json
            git diff --staged --quiet || git commit -m "chore: nightly scan $(date -u '+%Y-%m-%d %H:%M UTC')"
            git push
            echo "Data files committed successfully."
          else
            echo "ERROR: data files not found — scan failed. Check logs above."
            exit 1
          fi
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      # ── 7. Upload as artifact (backup, visible in Actions UI) ─────────────
      - name: Upload scan results as artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: stock-data-${{ github.run_id }}
          path: |
            data/stock_data.json.gz
            data/scan_meta.json
          retention-days: 7
          if-no-files-found: warn
