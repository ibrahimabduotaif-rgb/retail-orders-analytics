# Retail Orders Analytics — End-to-End Data Pipeline

## Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline for analyzing retail order data sourced from Kaggle. It moves raw transactional data through a structured pipeline into a relational database, then applies SQL-based analytical queries to extract business insights across products, regions, time periods, and customer segments.

The pipeline is built with production-grade practices: centralized configuration, structured logging, comprehensive error handling, and data validation at every stage.

---

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐     ┌──────────────────────┐
│   Kaggle     │━━━━▶━━━━│  Extract     │━━━━▶━━━━│  Transform   │━━━━▶━━━━│    Load      │
│   Dataset    │     │  (ZIP → CSV) │     │  (Pandas)    │     │  (SQLAlchemy)│
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘     └─────━━━━━━━━┘
                                                                    │
                                                                    ▼
                                                            ┌───────────────┐
                                                            │  SQL Analysis│
                                                            │  (8 Queries) │
                                                            └───────────────┘
```

---

## Tech Stack

| Layer         | Technology                          |
|---------------|-------------------------------------|
| Data Source   | Kaggle API                          |
| Processing    | Python 3, Pandas, NumPy             |
| Database      | SQLite (default) / SQL Server       |
| ORM           | SQLAlchemy                          |
| Logging       | Python `logging` module             |
| Analysis      | SQL (compatible with SQLite, SQL Server, PostgreSQL) |

---

## Pipeline Walkthrough

### Phase 1 — Extract

The extraction layer downloads a compressed dataset from Kaggle via the Kaggle CLI and decompresses it using Python's `zipfile` module inside a context manager, which guarantees the file handle is released even if an error occurs mid-extraction.

Key design decisions:

- **Fail-fast behavior:** If the download fails or the ZIP file is corrupted, the pipeline raises an exception immediately with a descriptive log message rather than silently proceeding with missing or partial data.
- **Separation of concerns:** The file path, dataset identifier, and all other parameters live in a centralized `CONFIG` dictionary at the top of the script, making it trivial to swap datasets or change paths without touching the extraction logic.

### Phase 2 — Transform

This is the core of the pipeline, where raw CSV data becomes analysis-ready. The transformation happens in five sequential steps:

**Step 1 — Ingestion with null handling:**  
The CSV is read with a predefined list of null markers (`'Not Available'`, `'unknown'`, `'N/A'`, `'NA'`, `'null'`, `'none'`, and empty strings). Handling these at read-time is more efficient and less error-prone than post-hoc replacement, because Pandas applies the correct `NaN` dtype from the start.

**Step 2 — Column name standardization:**  
All column names are converted to lowercase, stripped of leading/trailing whitespace, spaces are replaced with underscores, and any special characters are removed via regex. This ensures consistent, SQL-friendly column names like `order_date`, `ship_mode`, and `sub_category`.

**Step 3 — Derived columns:**  
Three new financial columns are computed:

```
discount   = list_price × discount_percent × 0.01
sale_price = list_price − discount
profit     = sale_price − cost_price
```

The multiplication by `0.01` converts the discount percentage (stored as a whole number like `20` for 20%) into a decimal multiplier. After computation, a validation check logs warnings if any rows have negative sale prices (which would indicate data quality issues) or negative profits (which may be legitimate loss-leaders but deserve attention).

**Step 4 — Date parsing:**  
The `order_date` column is converted from string to `datetime64`. The parser first attempts the expected `YYYY-MM-DD` format; if that fails, it falls back to automatic format inference with a logged warning.

**Step 5 — Column pruning:**  
The intermediate columns (`list_price`, `cost_price`, `discount_percent`) are dropped since their information now lives in the derived columns. The drop operation checks for column existence first to avoid errors on re-runs.

After all five steps, the pipeline logs a summary: row count, column list, date range, total sales, total profit, and overall profit margin.

### Phase 3 — Load

The processed DataFrame is written to a relational database via SQLAlchemy. The design supports any SQLAlchemy-compatible backend through a single connection string stored in the `CONFIG` dictionary (or overridden by an environment variable `DB_CONNECTION_STRING`).

Key design decisions:

- **`if_exists='replace'`:** The table is recreated on each run. This is intentional for a batch pipeline processing the full dataset — it guarantees idempotency (running the pipeline twice produces the same result, not duplicate rows).
- **Chunked inserts:** Data is written in batches of 1,000 rows using multi-row `INSERT` statements, which significantly reduces round-trip overhead compared to row-by-row insertion.
- **Connection lifecycle:** The engine is created, used, and disposed within a `try/finally` block, ensuring connections are released even on failure.

### Orchestration

All three phases are coordinated by a `main()` function that:

1. Records the start time  
2. Executes Extract → Transform → Load in sequence  
3. Logs the total elapsed time on success  
4. Catches and logs any exception on failure, then re-raises it  

Running the script directly (`python retail_orders_etl_improved.py`) executes the full pipeline. All operations are logged to both the console and a file (`etl_pipeline.log`).

---

## SQL Analysis Layer

The analysis layer consists of eight queries, each targeting a distinct business question. All queries use Common Table Expressions (CTEs) for readability and are annotated with the rationale behind each design choice.

### Query 1 — Top 10 Revenue-Generating Products

Returns the ten products with the highest total revenue, along with order count, average order value, total profit, and profit margin percentage. Including profit margin alongside revenue is essential because a high-revenue product with thin margins may be less strategically valuable than a moderate-revenue product with strong margins.

The query uses `NULLIF` in the margin calculation to protect against division by zero in edge cases where a product's total sale price is zero (e.g., fully discounted items).

### Query 2 — Top 5 Products per Region

Ranks products within each region by total sales using `DENSE_RANK()` as the window function. The choice of `DENSE_RANK` over `ROW_NUMBER` is deliberate: if two products are tied in sales, both receive the same rank, and neither is arbitrarily excluded. With `ROW_NUMBER`, a tie at position 5 would arbitrarily include one product and exclude the other.

The query also includes total profit and order count for each product-region combination, enabling a multi-dimensional view without additional subqueries.

### Query 3 — Month-over-Month Growth: 2022 vs. 2023

Pivots monthly sales data into a side-by-side comparison of 2022 and 2023, then computes both the absolute growth and the percentage growth for each month.

Design considerations:

- A `WHERE` clause filters to only 2022 and 2023 data before aggregation, reducing the working set.  
- `COALESCE` wraps each pivoted sum to convert `NULL` (months with no orders in one year) to `0`, preventing misleading blanks.  
- The percentage growth denominator uses `NULLIF(..., 0)` to return `NULL` instead of raising a division error when 2022 sales are zero.

### Query 4 — Peak Sales Month per Category

Identifies the single month with the highest sales for each product category, and contextualizes it by showing what percentage of that category's total sales occurred in that peak month.

This concentration metric is a proxy for seasonality: if one month accounts for 40% of a category's annual sales, inventory planning and marketing campaigns should be timed accordingly. The query achieves this through a three-CTE structure: monthly aggregation → category totals → ranking with percentage.

### Query 5 — Sub-Category Profit Growth (2022 → 2023)

Computes year-over-year profit growth for every sub-category, showing 2022 profit, 2023 profit, absolute growth, and percentage growth. The percentage calculation uses `ABS()` in the denominator to handle cases where a sub-category had a net loss in 2022 — without this, a sub-category moving from -$100 to +$200 would show a misleading negative growth percentage.

Results are ordered by absolute profit growth descending, showing the full spectrum from highest growth to steepest decline, rather than limiting to only the top result.

### Query 6 — Day-of-Week Seasonality

Analyzes order volume, average sale value, and total profit by day of the week. This reveals operational patterns: are weekends stronger than weekdays? Does average order value change by day? The results inform staffing, ad scheduling, and logistics planning.

### Query 7 — Shipping Mode Performance

Breaks down orders by shipping mode (Standard, Express, etc.), showing order count, average sale price, total profit, and profit margin for each. This analysis answers whether faster (and presumably more expensive) shipping modes correlate with higher or lower profitability — a key input for shipping strategy decisions.

### Query 8 — Simplified RFM Segmentation

Implements a basic RFM (Recency, Frequency, Monetary) analysis using `NTILE(4)` to divide customers into quartiles along each dimension:

- **Recency:** How recently a customer last ordered  
- **Frequency:** How many orders they've placed  
- **Monetary:** How much they've spent in total  

Each customer receives a score from 1 to 4 on each dimension. A customer scoring (1, 1, 1) is the most valuable: they ordered recently, order frequently, and spend heavily. This segmentation enables targeted marketing strategies for retention, reactivation, and growth.

---

## Database Compatibility

The SQL queries are written primarily for SQLite (the default database backend), but include inline comments showing the SQL Server equivalents where syntax differs:

| Operation | SQLite | SQL Server |
|-----------|--------|------------|
| Year extraction | `EXTRACT(YEAR FROM col)` | `YEAR(col)` |
| Month extraction | `EXTRACT(MONTH FROM col)` | `MONTH(col)` |
| Date formatting | `STRFTIME('%Y%m', col)` | `FORMAT(col, 'yyyyMM')` |
| Row limiting | `LIMIT N` | `TOP N` |

---

## Project Structure

```
├── retail_orders_etl_improved.py       # Python ETL pipeline
├── retail_orders_analysis_improved.sql # SQL analytical queries
├── etl_pipeline.log                    # Auto-generated execution log
├── retail_orders.db                    # SQLite database (generated)
└── orders.csv                          # Raw data (extracted)
```

---

## How to Run

### Prerequisites

```bash
pip install pandas numpy sqlalchemy kaggle
```

Ensure your Kaggle API credentials are configured (`~/.kaggle/kaggle.json`).

### Execution

```bash
# Run the full ETL pipeline
python retail_orders_etl_improved.py

# Then run queries against the generated database
sqlite3 retail_orders.db < retail_orders_analysis_improved.sql
```

### Using SQL Server Instead

Set the connection string via environment variable:

```bash
export DB_CONNECTION_STRING="mssql://USER:PASS@HOST/DB?driver=ODBC+DRIVER+17+FOR+SQL+SERVER"
python retail_orders_etl_improved.py
```

---

## Design Principles

1. **Idempotency:** The pipeline can be run multiple times safely without creating duplicate data.  
2. **Fail-fast:** Errors surface immediately with descriptive messages rather than propagating silently.  
3. **Portability:** No hardcoded machine names or paths; everything is configurable.  
4. **Observability:** Every stage logs its inputs, outputs, and any anomalies detected.  
5. **Defensive computation:** Division-by-zero protection, null handling, and column existence checks throughout.  
