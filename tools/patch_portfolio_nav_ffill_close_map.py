# tools/patch_portfolio_nav_ffill_close_map.py
from __future__ import annotations

from pathlib import Path
import re

PATCH = """
def _get_close_map(store: DuckDBStore, trade_date: date, tickers: List[str]) -> Dict[str, float]:
    '''
    Return close price map for given tickers on trade_date.

    IMPORTANT:
    If prices_daily has missing rows for some tickers on that date (row missing, not just NaN),
    we forward-fill (ffill) using the most recent available close <= trade_date.

    This keeps mark-to-market stable under sparse/missing price rows.
    '''
    if not tickers:
        return {}

    rows = store.fetchall(
        '''
        SELECT
          p.ticker,
          p.close
        FROM (
          SELECT
            ticker,
            close,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
          FROM prices_daily
          WHERE trade_date <= ?
            AND ticker = ANY(?)
            AND close IS NOT NULL
        ) p
        WHERE p.rn = 1
        ''',
        (trade_date, tickers),
    )
    return {str(t): float(c) for t, c in rows}
""".strip("\n")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    target = root / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py"
    if not target.exists():
        raise FileNotFoundError(f"Not found: {target}")

    text = target.read_text(encoding="utf-8", errors="ignore")

    # Replace existing _get_close_map definition block.
    # Match from "def _get_close_map" until the next "\n\ndef " OR end-of-file.
    pattern = re.compile(r"def _get_close_map\([\s\S]*?\n(?=\ndef\s|\Z)", re.MULTILINE)
    m = pattern.search(text)
    if not m:
        raise RuntimeError("Could not find _get_close_map() block to patch.")

    new_text = text[: m.start()] + PATCH + "\n\n" + text[m.end() :]

    # Basic sanity check
    if "forward-fill (ffill)" not in new_text:
        raise RuntimeError("Patch sanity check failed: expected marker not found.")

    target.write_text(new_text, encoding="utf-8")
    print(f"[OK] patched: {target}")


if __name__ == "__main__":
    main()
