"""Convenience entrypoint.

Run from repo root:
  python run_daily.py --date 2026-01-14

This simply forwards to: alpha_tracker2.pipelines.run_daily
"""

from alpha_tracker2.pipelines.run_daily import main


if __name__ == "__main__":
    main()
