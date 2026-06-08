#!/usr/bin/env python3
"""Basic ETL example: CSV → Transform → Parquet."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datapilot import Pipeline
from datapilot.connectors import CSVSource, ParquetSink
from datapilot import transforms as T

# Sample data (normally loaded from a file)
SAMPLE_CSV = Path(__file__).parent.parent / "data" / "sales.csv"
SAMPLE_CSV.parent.mkdir(exist_ok=True)

# Create sample data if it doesn't exist
if not SAMPLE_CSV.exists():
    SAMPLE_CSV.write_text(
        "id,region,product,amount,quantity\n"
        "1,APAC,Widget A,150.00,3\n"
        "2,EMEA,Widget B,200.00,1\n"
        "3,APAC,Widget A,450.00,9\n"
        "4,AMER,Widget C,75.00,5\n"
        "5,EMEA,Widget A,300.00,6\n"
        "6,AMER,Widget B,100.00,2\n"
        "7,APAC,Widget C,225.00,3\n"
        "8,EMEA,Widget C,150.00,10\n"
    )

OUTPUT = Path(__file__).parent.parent / "output" / "sales_summary.parquet"
OUTPUT.parent.mkdir(exist_ok=True)


def main():
    print("Running basic ETL pipeline...")

    pipeline = (
        Pipeline(source=CSVSource(SAMPLE_CSV), name="sales-etl")
        | T.filter(lambda r: float(r["amount"]) > 0)
        | T.add_field("unit_price", lambda r: float(r["amount"]) / max(int(r["quantity"]), 1))
        | T.group_by("region")
        | T.aggregate({
            "total_revenue": ("amount", "sum"),
            "total_quantity": ("quantity", "sum"),
            "order_count": ("id", "count"),
        })
    )

    sink = ParquetSink(OUTPUT)
    sink.write(pipeline.run())

    print(f"✅ ETL complete. Output: {OUTPUT}")
    print(f"   Processed {pipeline.count()} regional summaries.")


if __name__ == "__main__":
    main()
