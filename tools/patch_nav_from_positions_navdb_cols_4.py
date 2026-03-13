from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src" / "alpha_tracker2" / "pipelines" / "nav_from_positions.py"

OLD = 'pd.DataFrame(rows, columns=["trade_date", "version", "nav"])'
NEW = 'pd.DataFrame(rows, columns=["trade_date", "version", "nav", "nav_gross"])'

def main():
    text = TARGET.read_text(encoding="utf-8")
    if OLD not in text:
        raise RuntimeError("[ERROR] target DataFrame columns line not found; nav_from_positions.py changed?")
    text = text.replace(OLD, NEW, 1)
    TARGET.write_text(text, encoding="utf-8")
    print(f"[OK] patched DataFrame columns to 4 cols: {TARGET}")

if __name__ == "__main__":
    main()
