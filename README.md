# PSI Data Engineering Intern Take-Home Assessment

## Project summary

This project builds a modular PySpark ETL pipeline for messy e-commerce CSV data. It performs explicit schema enforcement, rejected-record handling, cleaning, customer and item enrichment, window-based analytics, return analysis, data quality gates, and idempotent output writes to Parquet and CSV.

## Dataset description

The pipeline expects four CSV files under `data/`:

| File | Description |
| --- | --- |
| `customers.csv` | Customer dimension containing `customer_id`, signup date, country, tier, and email. |
| `orders.csv` | Order facts containing `order_id`, `customer_id`, order date, status, gross amount, and discount percentage. |
| `order_items.csv` | Item-level order lines containing product, quantity, unit price, and category. |
| `returns.csv` | Return facts containing return date, reason, and refund amount by order. |

## Data quality issues handled

- Exact duplicate rows are removed with `dropDuplicates()`.
- Mixed date formats (`yyyy-MM-dd` and `dd/MM/yyyy`) are normalized with Spark date functions.
- Orders missing `customer_id` or `order_id` are rejected/dropped from clean orders.
- Orders with missing `total_amount` are written to rejected records rather than silently ignored.
- Negative `total_amount` values are flagged with `is_negative_amount` and separated before final DQ output.
- Orphaned `order_items` are isolated with a left anti join and written separately.
- `customer_tier` values are trimmed and lowercased.
- Refund anomalies are detected when `refund_amount > net_amount`.

## Project structure

```text
.
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   └── returns.csv
├── output/
│   ├── enriched_orders/
│   ├── summaries/
│   ├── rejected/
│   └── orphaned_order_items/
├── src/
│   ├── __init__.py
│   ├── schemas.py
│   ├── spark_session.py
│   ├── ingestion.py
│   ├── cleaning.py
│   ├── transformations.py
│   ├── aggregations.py
│   ├── returns_analysis.py
│   ├── quality_gates.py
│   └── writer.py
├── tests/
│   └── test_transformations.py
├── pipeline.py
├── requirements.txt
└── README.md
```

## Setup instructions

```bash
python -m venv venv
```

Windows:

```bash
venv\Scripts\activate
```

Mac/Linux:

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## How to run

```bash
python pipeline.py --input data --output output
```

The `--input` and `--output` arguments are optional. They default to `data` and `output`.

## How to run tests

```bash
pytest
```

The tests create small synthetic Spark DataFrames and validate date normalization, tier standardization, negative amount flags, net amount calculations, and orphaned item detection.

## Output locations

| Output path | Format | Description |
| --- | --- | --- |
| `output/enriched_orders/` | Parquet | Final item-level enriched analytical table partitioned by `order_year` and `order_month`. |
| `output/orphaned_order_items/` | CSV | Order items that reference an order missing from cleaned orders. |
| `output/rejected/` | CSV | Rejected or suspicious records, including source-table rejects and negative net amount rows. |
| `output/summaries/customer_lifetime_rankings/` | CSV | Customers ranked by lifetime net spend within each country. |
| `output/summaries/customer_7_day_rolling_orders/` | CSV | Customer order counts over rolling seven-day windows. |
| `output/summaries/category_monthly_revenue_share/` | CSV | Product category share of total monthly revenue. |
| `output/summaries/return_rate_by_category/` | CSV | Return rate by category using distinct order counts. |
| `output/summaries/return_rate_by_customer_tier/` | CSV | Return rate by customer tier using distinct order counts. |
| `output/summaries/top_10_refund_customers/` | CSV | Top ten customers by total refund amount. |
| `output/summaries/refund_anomalies/` | CSV | Returns where refund amount exceeds order net amount. |

## Design decisions

- Explicit `StructType` schemas are used instead of `inferSchema` so input types are controlled and reproducible.
- Rejected-record detection reads raw strings in parallel with schema enforcement so invalid numerics are not lost when Spark casts them to `NULL`.
- Dates are normalized with `coalesce(to_date(..., "yyyy-MM-dd"), to_date(..., "dd/MM/yyyy"))`.
- `customer_tier` is standardized to lowercase after trimming whitespace.
- Missing `discount_pct` is treated as `0.0`, assuming a missing discount means no discount.
- Missing `category` is filled with `unknown` so downstream category aggregations retain those rows.
- Negative order amounts are flagged first, then negative `net_amount` rows are written to `output/rejected/negative_net_amount/` before the final DQ gate. This preserves suspicious records while ensuring the final analytical output has non-negative net revenue.
- The orders-to-customers join is a left join so valid orders are preserved before quality validation.
- The customers table is broadcast because it is a small dimension table compared with orders and order items.
- Orphaned order items are detected with a left anti join instead of being silently dropped.
- The final enriched dataset is written as Parquet partitioned by `order_year` and `order_month` for efficient date-based reads.
- Summary outputs are CSV folders generated by Spark, so they contain `part-*` files rather than single flat CSV files.

## Explain plan bonus

The pipeline prints `df.explain(mode="formatted")` for the final item-level enriched dataset. This transformation is the most complex because it combines the broadcast customer dimension join, item-level inner join, derived columns, and final projection. In the physical plan, review whether Spark uses a broadcast join for customers and where exchanges/shuffles occur for joins and downstream aggregations.

## Known limitations

- Rejected-record handling focuses on required IDs, accepted date formats, numeric cast validity, and missing `total_amount` as required by the assignment.
- The dataset is synthetic, so assumptions are documented for missing `discount_pct` and missing `category` values.
- Spark writes CSV outputs as directories containing part files rather than a single CSV file.
- The DQ gate intentionally validates only the final analytical output; suspicious negative net amount records are preserved separately.

## Bonus tasks completed

- Pytest unit tests with synthetic Spark DataFrames.
- Broadcast join for the customers dimension table.
- DQ gate requiring customer information, non-null `net_amount`, and non-negative `net_amount`.
- Formatted Spark explain plan printed during the pipeline run.

## Submission email section

Subject:
`[Data Engineering Intern] Take-Home Submission — [Your Full Name]`

Body:

```text
Hello,

Please find my submission for the Data Engineering Intern Take-Home Assessment below:

GitHub repository: [insert repo link]

Summary:
I built a modular PySpark ETL pipeline that ingests four messy e-commerce CSV files, enforces schemas, handles data quality issues, enriches orders with customer and item data, performs window-based analytics, analyzes returns, and writes idempotent Parquet/CSV outputs.

Known limitations:
[Briefly mention any incomplete item, if any.]

Approximate time spent:
[Insert time spent.]

Kind regards,
[Your Name]
```
