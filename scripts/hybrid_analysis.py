#!/usr/bin/env python3
import csv
from pathlib import Path

RESULTS = Path(__file__).resolve().parent.parent / "results"


def read_mapreduce_views(path: Path):
    """Parse Hadoop Streaming reducer output: product_id<TAB>count."""
    views = {}
    with open(path, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            pid, cnt = line.split("\t")
            views[pid] = int(cnt)
    return views


def read_warehouse(path: Path):
    """Parse the DW product sales CSV."""
    rows = {}
    with open(path, "r", newline="") as fh:
        for row in csv.DictReader(fh):
            rows[row["product_id"]] = {
                "product_name": row["product_name"],
                "category": row["category"],
                "purchases": int(row["purchases"]),
                "revenue_usd": float(row["revenue_usd"]),
            }
    return rows


def main():
    views = read_mapreduce_views(RESULTS / "mapreduce_product_views.txt")
    sales = read_warehouse(RESULTS / "warehouse_product_summary.csv")

    merged = []
    for pid, cnt in views.items():
        s = sales.get(
            pid,
            {"purchases": 0, "revenue_usd": 0.0, "product_name": pid, "category": "?"},
        )
        conversion = (s["purchases"] / cnt) * 100 if cnt else 0.0
        merged.append(
            {
                "product_id": pid,
                "product_name": s["product_name"],
                "category": s["category"],
                "web_views": cnt,
                "purchases": s["purchases"],
                "revenue_usd": round(s["revenue_usd"], 2),
                "conversion_rate_pct": round(conversion, 2),
            }
        )

    merged.sort(key=lambda r: -r["web_views"])

    # Write combined CSV for the report appendix
    out_csv = RESULTS / "conversion_analysis.csv"
    with open(out_csv, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(merged[0].keys()))
        writer.writeheader()
        writer.writerows(merged)
    print(f"wrote {out_csv}")

    # Human-readable report
    print("\n================ COMBINED ANALYSIS (DW + Hadoop) ================")
    print(
        f"{'Product':6}{'View Count':>11}{'Purchases':>10}{'Revenue $':>12}"
        f"{'Conv %':>8}  Product"
    )
    print("-" * 80)
    for r in merged:
        print(
            f"{r['product_id']:6}{r['web_views']:>11}{r['purchases']:>10}"
            f"{r['revenue_usd']:>12.2f}{r['conversion_rate_pct']:>8.2f}  "
            f"{r['product_name']}"
        )

    # Insight: high views, low purchases
    flagged = [
        r for r in merged if r["web_views"] >= 100 and r["conversion_rate_pct"] < 5.0
    ]
    flagged.sort(key=lambda r: r["conversion_rate_pct"])
    print("\n--- Products with high views but low purchases (conversion < 5%) ---")
    for r in flagged:
        print(
            f"  {r['product_id']} {r['product_name']}: "
            f"{r['web_views']} views, {r['purchases']} purchases, "
            f"{r['conversion_rate_pct']}% conversion"
        )


if __name__ == "__main__":
    main()

