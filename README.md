# 🎬 MovieLens Analytics Engineering Pipeline

![dbt](https://img.shields.io/badge/dbt-1.x-FF694B?logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-232F3E?logo=amazonaws&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Data%20Ready%20for%20BI-brightgreen)

> A production-grade Analytics Engineering pipeline built on **dbt + Snowflake + AWS S3**, transforming the [MovieLens 20M dataset](https://grouplens.org/datasets/movielens/20m/) from raw CSVs into a fully-tested, documented, and BI-ready dimensional model — covering every core dbt feature: materialization strategies, incremental models, SCD Type 2 snapshots, seeds, macros, custom tests, and packages.

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Data Model](#-data-model)
- [Tech Stack](#-tech-stack)
- [Design Decisions & Trade-offs](#-design-decisions--trade-offs)
- [Prerequisites](#-prerequisites)
- [Setup & Installation](#-setup--installation)
- [Usage Guide](#-usage-guide)
- [Testing & Data Quality](#-testing--data-quality)
- [Folder Structure](#-folder-structure)
- [Known Limitations & Future Work](#-known-limitations--future-work)

---

## 🔍 Overview

Most dbt tutorials stop at `dbt run`. This project goes further.

The pipeline ingests six raw MovieLens CSV tables from S3 into Snowflake, then applies a **four-layer dbt transformation architecture** (staging → dim → fct → mart) that makes deliberate materialization decisions at every layer — not as a default, but as a calculated trade-off between query performance and compute cost.

Three features distinguish this from a standard tutorial project:

1. **Conformed Dimension Design** — `dim_users` is built by `UNION`-ing user IDs from both the ratings and tags sources, ensuring no user is lost to a single-sided join. This implements Kimball's Conformed Dimension principle against a real dataset.
2. **SCD Type 2 via dbt Snapshot** — `snap_tags` tracks historical changes to user-generated tag data with `dbt_valid_from` / `dbt_valid_to` metadata, verified against a manually simulated data update.
3. **Secure S3 Integration** — The infrastructure layer is designed around AWS IAM Role-based Storage Integration (not long-lived access keys), reflecting production security standards.

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Data Flow Overview                         │
│                                                                 │
│  MovieLens 20M Dataset (CSV)                                    │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────┐    IAM Role Trust    ┌──────────────────────┐  │
│  │  AWS S3     │◄────────────────────►│  Snowflake Storage   │  │
│  │  Bucket     │   (Storage Int.)     │  Integration         │  │
│  └─────────────┘                      └──────────┬───────────┘  │
│                                                  │              │
│                                    COPY INTO     ▼              │
│                                       ┌──────────────────────┐  │
│                                       │  MOVIELENS.RAW       │  │
│                                       │  (6 raw tables)      │  │
│                                       └──────────┬───────────┘  │
│                                                  │              │
│                              dbt Transformations │              │
│                                                  ▼              │
│                    ┌─────────────────────────────────────────┐  │
│                    │  staging/  (view → table for perf.)     │  │
│                    │  src_movies, src_ratings*, src_tags*,   │  │
│                    │  src_genome_scores, src_links           │  │
│                    └──────────────────┬──────────────────────┘  │
│                                       │                         │
│          ┌────────────────────────────┼──────────────────┐      │
│          ▼                            ▼                   ▼     │
│  ┌──────────────┐          ┌──────────────────┐  ┌────────────┐ │
│  │  dim/ (table)│          │  fct/ (table /   │  │ snapshots/ │ │
│  │              │          │   incremental)   │  │            │ │
│  │ dim_movies   │          │                  │  │ snap_tags  │ │
│  │ dim_users    │          │ fct_ratings      │  │ (SCD Type2)│ │
│  │ dim_genome_  │          │ (incremental ✦)  │  └────────────┘ │
│  │   tags       │          │ fct_genome_scores│                 │
│  │ dim_movies_  │          │ ep_movies_w_tags │                 │
│  │  with_tags   │          │  (ephemeral ◆)   │                 │
│  │  (ephemeral) │          └────────┬─────────┘                 │
│  └──────────────┘                   │                           │
│                                     ▼                           │
│                          ┌──────────────────────-┐              │
│                          │  mart/ (table)        │              │
│                          │  mart_movies_releases │              │
│                          │  (fct + seed join)    │              │
│                          └──────────────────────-┘              │
│                                                                 │
│  ✦ incremental: only processes new records by rating_timestamp  │
│  ◆ ephemeral: compiled as CTE, no physical object in Snowflake  │
└─────────────────────────────────────────────────────────────────┘
```

> [建議在此處插入 dbt docs lineage graph 截圖]

---

## 📊 Data Model

### Source Dataset: MovieLens 20M

| Raw Table | Rows (approx.) | Description |
|---|---|---|
| `raw_movies` | 27,278 | Movie metadata with title and genres |
| `raw_ratings` | 20,000,263 | User ratings (0.5–5.0 stars) |
| `raw_tags` | 465,564 | User-generated text tags |
| `raw_genome_scores` | 11,709,768 | Tag relevance scores per movie (0–1) |
| `raw_genome_tags` | 1,128 | Genome tag label definitions |
| `raw_links` | 27,278 | IMDb / TMDb crosswalk IDs |

### dbt Model Layers

```
staging/          rename columns, cast types, no business logic
    │
    ├── dim/      conformed dimensions (slowly changing or stable)
    │
    ├── fct/      grain-level fact tables (one row per event)
    │
    ├── mart/     aggregated, BI-ready tables joining fct + seeds
    │
    └── snapshots/ SCD Type 2 history tables
```

**Key design decision — `dim_users` Conformed Dimension:**

```sql
-- Users can appear in ratings WITHOUT appearing in tags, and vice versa.
-- A single-source SELECT would silently drop users from the other side.
SELECT DISTINCT user_id FROM src_ratings
UNION
SELECT DISTINCT user_id FROM src_tags
```

This `UNION` pattern implements Kimball's Conformed Dimension principle: a dimension table must represent the complete entity population across all fact tables that reference it.

---

## 🛠 Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Storage** | AWS S3 | Raw CSV landing zone |
| **Data Warehouse** | Snowflake | Query engine + storage |
| **S3 Integration** | Snowflake Storage Integration (IAM Role) | Secure, keyless S3 access |
| **Transformation** | dbt Core | Model DAG, tests, docs, snapshots |
| **Package** | dbt-labs/dbt_utils 1.3.0 | Surrogate key generation |
| **Orchestration** | Manual / dbt CLI | `dbt run`, `dbt test`, `dbt snapshot` |

---

## ⚖️ Design Decisions & Trade-offs

### 1. S3 Integration: IAM Role vs. Direct Access Keys

**Two approaches exist for connecting Snowflake to S3:**

| Approach | How It Works | Risk |
|---|---|---|
| `STAGE` with Access Keys | `AWS_KEY_ID` / `AWS_SECRET_KEY` stored in Snowflake config | Key exposure in plaintext; breaks on key rotation |
| `STORAGE INTEGRATION` (IAM Role) | Trust relationship via `sts:AssumeRole`; no keys stored | Zero standing credentials in Snowflake |

**Decision:** This project implements Storage Integration for all documentation and production-path setup, even though the initial prototype used direct keys for speed. The trust relationship setup requires three steps across AWS (IAM Policy → IAM Role → Trust Policy update) and two steps in Snowflake (`CREATE STORAGE INTEGRATION` → `DESC INTEGRATION` to retrieve `STORAGE_AWS_IAM_USER_ARN`).

**Rule:** Never store AWS credentials inside a data warehouse. Key rotation schedules and accidental config exposure are both production failure modes.

> [建議在此處插入 Storage Integration vs Stage with Keys 對比架構圖]

---

### 2. Materialization Strategy — The Decision Tree

The project's `dbt_project.yml` sets `view` as the default, then overrides per layer. Each override is deliberate:

```
Is this a source-aligned rename/cast with no business logic?
    └─ Is it referenced by multiple downstream models OR used as
       incremental/snapshot source?
           Yes → table  (src_ratings, src_tags)
           No  → view   (src_movies, src_links, src_genome_*)

Is this a conformed dimension that BI tools query directly?
    └─ table (dim_movies, dim_users, dim_genome_tags)

Is this purely transitional logic needed by other models but
unnecessary to persist in Snowflake?
    └─ ephemeral (dim_movies_with_tags)

Is this a high-volume fact table with an append-only pattern?
    └─ incremental (fct_ratings) — filtered by rating_timestamp

Is this a final BI-facing mart with a static join pattern?
    └─ table (mart_movies_releases)
```

**The core principle:** Compute cost scales with query complexity and frequency. A `view` re-executes its full query on every access. A `table` pays the compute cost once per `dbt run` and serves all subsequent queries from storage. For a 20M-row ratings table referenced by multiple downstream models, `table` is not a convenience — it's a cost control decision.

---

### 3. Incremental Model — `fct_ratings`

```sql
{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}

{% if is_incremental() %}
    AND rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}
```

Two decisions worth noting:

- **`on_schema_change = 'fail'`**: The model fails loudly if the source schema changes rather than silently proceeding. This enforces a contract between the raw layer and the transformation layer — schema drift is a bug, not a warning.
- **Timestamp-based incremental filter**: `this` refers to the existing `fct_ratings` table. The model only processes records newer than the current maximum timestamp, making each run additive rather than full-refresh.

---

### 4. SCD Type 2 Snapshot — `snap_tags`

```sql
{% snapshot snap_tags %}
{{
    config(
        unique_key=['user_id', 'movie_id', 'tag'],
        strategy='timestamp',
        updated_at='tag_timestamp',
        invalidate_hard_deletes=True
    )
}}
```

The snapshot uses a **composite `unique_key`** (`user_id + movie_id + tag`) because no single column uniquely identifies a tag event. When dbt detects a change in `tag_timestamp` for an existing key combination, it:

1. Sets `dbt_valid_to` on the old record to the change timestamp
2. Inserts a new record with `dbt_valid_from = change_timestamp` and `dbt_valid_to = NULL`

This was verified by manually running `UPDATE src_tags SET tag = '...' WHERE user_id = 18` in Snowflake, then executing `dbt snapshot` to confirm the history table captured both the old and new state.

> [建議在此處插入 snap_tags 執行前後的 SELECT 結果截圖，展示 dbt_valid_from / dbt_valid_to 欄位]

---

### 5. The `FALSE` Sentinel in the Custom Macro

```sql
{% macro no_nulls_in_columns(model) %}
    SELECT * FROM {{ model }} WHERE
    {% for col in adapter.get_columns_in_relation(model) %}
        {{ col.column }} IS NULL OR
    {% endfor %}
    FALSE
{% endmacro %}
```

The `FALSE` at the end is not a placeholder — it is syntactically required. The Jinja loop generates `col IS NULL OR` for every column, leaving a trailing `OR` after the last column. Without `FALSE` as a terminator, the SQL is invalid. `FALSE` does not affect the logical result (`any_condition OR FALSE` is equivalent to `any_condition`) but allows the loop to be written uniformly without special-casing the last iteration.

---

## 📋 Prerequisites

- **Snowflake account** with `ACCOUNTADMIN` access for initial setup
- **AWS account** with permissions to create IAM Policies and Roles
- **dbt Core** installed (`pip install dbt-snowflake`)
- **Python 3.9+**
- **dbt_utils package**: installed via `dbt deps`

---

## 🚀 Setup & Installation

### Step 1: Load Raw Data into Snowflake

#### Option A: Stage with Access Keys (quick start)

```sql
USE ROLE ACCOUNTADMIN;

-- Create role, warehouse, and dbt user
CREATE ROLE IF NOT EXISTS TRANSFORM;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='MOVIELENS.RAW';

GRANT ROLE TRANSFORM TO USER dbt;

-- Create database and schema
CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;

-- Grant permissions
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;

-- Create stage pointing to S3
CREATE STAGE netflixstage
  URL='s3://your-bucket-name/'
  CREDENTIALS=(AWS_KEY_ID='...' AWS_SECRET_KEY='...');
```

#### Option B: Storage Integration with IAM Role (production recommended)

```sql
-- Step 1: Create storage integration (run as ACCOUNTADMIN)
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account-id>:role/snowflake_s3_integration_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/');

-- Step 2: Retrieve the Snowflake-generated IAM identity
DESC INTEGRATION s3_int;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Update the IAM Role's Trust Policy in AWS with these values

-- Step 3: Create stage using the integration (no keys stored)
CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://your-bucket-name/'
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);
```

> ⚠️ **S3 URL Note:** Do not append `*` to the Stage URL. Snowflake treats it as a literal path prefix, not a wildcard. Use `PATTERN` in `COPY INTO` to filter files: `PATTERN = '.*movies.*\.csv'`

#### Load raw tables

```sql
COPY INTO raw_movies FROM '@my_s3_stage' PATTERN = '.*movies.*\.csv'
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO raw_ratings FROM '@my_s3_stage' PATTERN = '.*ratings.*\.csv'
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Repeat for raw_tags, raw_genome_scores, raw_genome_tags, raw_links
```

### Step 2: Configure dbt Profile

Create `~/.dbt/profiles.yml`:

```yaml
netflix:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-snowflake-account>
      user: dbt
      password: dbtPassword123
      role: TRANSFORM
      database: MOVIELENS
      warehouse: COMPUTE_WH
      schema: DEV
      threads: 4
```

### Step 3: Install dbt packages

```bash
cd netflix/
dbt deps
```

### Step 4: Run the pipeline

```bash
# Full run
dbt run

# Run specific layer
dbt run --select staging
dbt run --select dim
dbt run --select fct

# Run tests
dbt test

# Run snapshot
dbt snapshot

# Load seed data
dbt seed

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

---

## 📖 Usage Guide

### Running the full pipeline in order

```bash
dbt seed          # Load seed_movie_release_dates.csv into Snowflake
dbt run           # Execute all models in dependency order
dbt snapshot      # Capture SCD Type 2 history for snap_tags
dbt test          # Validate data quality across all models
dbt docs generate && dbt docs serve  # Browse the lineage graph
```

### Simulating an incremental load

```sql
-- Insert a new rating record into src_ratings
INSERT INTO MOVIELENS.DEV.SRC_RATINGS (user_id, movie_id, rating, rating_timestamp)
VALUES (87587, 7151, 4.0, '2025-03-31 23:40:02.000 -0700');
```

Then run `dbt run --select fct_ratings`. Only the new record (newer than the current `MAX(rating_timestamp)`) will be appended.

### Simulating SCD Type 2 history capture

```sql
-- Update a tag in the source table
UPDATE MOVIELENS.DEV.SRC_TAGS
SET tag = 'Updated Tag Name',
    tag_timestamp = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)
WHERE user_id = 18;
```

Then run `dbt snapshot`. Query `snapshots.snap_tags ORDER BY user_id, dbt_valid_from DESC` to see the old record with `dbt_valid_to` populated and the new record with `dbt_valid_to = NULL`.

---

## 🧪 Testing & Data Quality

### Built-in dbt tests (`schema.yml`)

| Model | Column | Tests Applied |
|---|---|---|
| `dim_movies` | `movie_id` | `not_null` |
| `dim_movies` | `movie_title` | `not_null` |
| `dim_users` | `user_id` | `not_null` |
| `fct_ratings` | `movie_id` | `not_null`, `relationships` → `dim_movies` |
| `fct_ratings` | `rating` | `not_null` |
| `fct_genome_scores` | `relevance_score` | `not_null` |

### Custom macro test

```sql
-- tests/relevance_score_test.sql
{{ no_nulls_in_columns(ref('fct_genome_scores')) }}
```

This calls the `no_nulls_in_columns` macro, which dynamically generates a `IS NULL OR` chain across all columns in the target model. Any row with a null in any column will surface as a test failure.

---

## 📁 Folder Structure

```
adamxiang-dbt-netflix-project/
└── netflix/
    ├── dbt_project.yml             # Project config + materialization defaults
    ├── packages.yml                # dbt-labs/dbt_utils dependency
    ├── models/
    │   ├── sources.yml             # Raw table declarations + alias mapping
    │   ├── schema.yml              # Column-level tests and descriptions
    │   ├── staging/                # Rename, cast — no business logic
    │   │   ├── src_movies.sql
    │   │   ├── src_ratings.sql     # materialized as table (incremental source)
    │   │   ├── src_tags.sql        # materialized as table (snapshot source)
    │   │   └── ...
    │   ├── dim/                    # Conformed dimensions (materialized: table)
    │   │   ├── dim_movies.sql
    │   │   ├── dim_users.sql       # UNION of ratings + tags user IDs
    │   │   ├── dim_genome_tags.sql
    │   │   └── dim_movies_with_tags.sql  # ephemeral — CTE only
    │   ├── fct/                    # Fact tables
    │   │   ├── fct_ratings.sql     # incremental by rating_timestamp
    │   │   └── fct_genome_scores.sql
    │   └── mart/                   # BI-ready output
    │       └── mart_movies_releases.sql  # fct_ratings JOIN seed
    ├── seeds/
    │   └── seed_movie_release_dates.csv  # Static lookup (10 movies)
    ├── snapshots/
    │   └── snap_tags.sql           # SCD Type 2 — composite unique_key
    ├── macros/
    │   └── no_nulls_in_columns.sql # Custom null-check macro
    └── tests/
        └── relevance_score_test.sql
```

---

## 🔭 Known Limitations & Future Work

| Area | Current State | Next Step |
|---|---|---|
| **BI Layer** | `mart_movies_releases` is pipeline terminal | Connect to Superset or Metabase; build genre-by-year rating dashboard |
| **Seed coverage** | Only 10 movies have `release_date` in seed | Expand seed from a complete reference dataset or IMDb API |
| **Orchestration** | Manual `dbt run` via CLI | Schedule with Airflow or dbt Cloud |
| **Monitoring** | No alerting on test failures | Route `dbt test` failures to Slack via dbt Cloud or custom CI step |
| **Unique key tests** | Commented out in `schema.yml` | Investigate and resolve duplicate `movie_id` / `user_id` sources before enabling |
| **`ep_movies_with_tags`** | Duplicates `dim_movies_with_tags` — unclear purpose | Evaluate if this model is needed or can be removed |

---

🙏 Acknowledgments

**Darshil Parmar** provides this useful and insight data engineering project - [Netflix Data Analysis](https://www.youtube.com/watch?v=zZVQluYDwYY)
