# ETL to Insights

End-to-end HR analytics platform with a medallion ETL pipeline (Bronze/Silver/Gold), KPI reporting, and a JWT-secured REST API.

## What This Project Does

- Ingests employee and timesheet data from local files or MinIO.
- Transforms and validates data into relational analytics tables in PostgreSQL.
- Computes derived attendance behavior metrics (late, early departure, overtime).
- Generates KPI CSV outputs and interactive HTML dashboards.
- Exposes employee CRUD and read-only timesheet APIs with role-based access control.

## Engineering Decisions

### Technology Choices

- **Python** for ETL + API consistency.
- **PostgreSQL** for relational integrity and analytics-friendly querying.
- **Apache Airflow** for orchestration, retries, and operational visibility.
- **Pandas + SQLAlchemy** for transformation and chunked loading.
- **FastAPI** for typed API contracts and auto-generated docs.
- **JWT (PyJWT)** with PBKDF2 password hashing for stateless authentication.
- **Docker Compose** for reproducible local infrastructure.

### Architecture Decisions

- **Medallion architecture:**
  - **Bronze:** `staging_employee`, `staging_timesheet`
  - **Silver:** `employee`, `timesheet`
  - **Gold:** `timesheet_derived`
- **Separation of concerns:**
  - `src/etl/` for ingestion + transforms + derivations
  - `src/analytics/visualizations.py` for KPI/report generation
  - `api/` for authentication, authorization, and REST endpoints
- **Security model:**
  - `users` table with salted PBKDF2 hashes
  - JWT contains `sub` + `role`
  - roles: `admin`, `viewer`

### Trade-offs

- Pandas-first transforms improve readability/flexibility but use more memory than pure SQL pipelines.
- Truncate-and-reload Silver tables is simple and repeatable, but not optimized for CDC/incremental loads.
- JWT keeps auth stateless and scalable, but revocation depends on expiry and user-state checks.

## Project Structure

```text
.
├── api/
│   ├── main.py
│   ├── db.py
│   ├── security.py
│   ├── schemas.py
│   └── README.md
├── dags/
│   └── etl_dag.py
├── migrations/
├── reports/
├── sql/analytics_queries/
├── src/
│   ├── etl/
│   ├── analytics/
│   └── migrate.py
├── docker-compose.yml
└── requirements.txt
```

## Setup Instructions

### 1) Prerequisites

- Python 3.10+
- Docker + Docker Compose
- Git

### 2) Clone Repository

```bash
git clone https://github.com/matinatula/ETL_to_Insights.git
cd ETL_to_Insights
```

### 3) Configure Environment

Create or update `.env`:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Airflow metadata DB
POSTGRES_DB=airflow_metadata

# ETL/API domain DB
ETL_POSTGRES_DB=etl_db

AIRFLOW_FERNET_KEY=<fernet_key>
AIRFLOW_ADMIN_PASSWORD=<airflow_admin_password>

SOURCE_TYPE=local
EMPLOYEE_CSV=data/employee_202510161125.csv
TIMESHEETS_FOLDER=data/timesheets

# Optional MinIO settings
MINIO_ENDPOINT=
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=
MINIO_BUCKET=
MINIO_EMPLOYEE_OBJECT=
MINIO_TIMESHEETS_PREFIX=

# API auth
JWT_SECRET_KEY=<strong_secret>
JWT_EXPIRES_MINUTES=60
```

If `etl_db` does not exist yet, create it:

```bash
docker exec -it etl-postgres psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE etl_db;"
```

### 4) Install Dependencies

```bash
pip install -r requirements.txt
```

### 5) Start Infrastructure

```bash
docker compose up -d db airflow-init airflow-webserver airflow-scheduler
docker compose ps
```

### 6) Run Migrations

```bash
python src/migrate.py
```

This creates staging/core/derived/auth tables and indexes.

## Usage Guide

### Run ETL Pipeline (Airflow)

1. Open `http://localhost:8080`
2. Login to Airflow
3. Trigger DAG `etl_pipeline`

Pipeline flow:
- `extract_employee`, `extract_timesheets`
- `transform_employee`, `transform_timesheet`
- `derive_gold`

### Generate Reports

```bash
python -m src.analytics.visualizations
```

Outputs:
- `reports/csv/`
- `reports/interactive/`
- `reports/index.html`

### Run API

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

API docs:
- `http://localhost:8000/docs`
- `http://localhost:8000/openapi.json`

## API Guide

### Authentication Flow

1. `POST /auth/bootstrap-admin` (one-time first admin)
2. `POST /auth/login` to obtain JWT
3. Pass `Authorization: Bearer <token>`
4. `POST /auth/register` to create users (admin only)

### Endpoints

- `GET /health`
- `POST /auth/bootstrap-admin`
- `POST /auth/register` (admin)
- `POST /auth/login`

Employee API (CRUD):
- `POST /employees` (admin)
- `GET /employees` (authenticated)
- `GET /employees/{employee_id}` (authenticated)
- `PUT /employees/{employee_id}` (admin)
- `DELETE /employees/{employee_id}` (admin)

Timesheet API (read-only):
- `GET /timesheets?employee_id=<id>&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
- `GET /timesheets/employee/{employee_id}?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

## Schema Documentation

### Core Tables

- `employee` (dimension)
  - PK: `client_employee_id`
- `timesheet` (fact)
  - PK: `timesheet_id`
  - FK: `client_employee_id -> employee.client_employee_id` (`ON DELETE CASCADE`)
- `timesheet_derived` (gold)
  - derived metrics: `late_flag`, `early_departure_flag`, `overtime_flag`
- `users` (API auth)
  - PK: `user_id`, unique `username`, `role`, `is_active`

### Staging Tables

- `staging_employee`
- `staging_timesheet`

### Entity Relationships

- `employee` 1 --- N `timesheet`
- `timesheet` feeds `timesheet_derived`
- `users` is an independent auth domain used by API security

ER diagram image:
- `ER_Diagram.png`

## Integrity and Performance

- FK on `timesheet.client_employee_id` ensures employee ownership integrity.
- Indexes:
  - `idx_timesheet_employee`
  - `idx_timesheet_date`
  - `idx_employee_department`
  - `idx_users_username`

## Operational Notes

- Airflow logs: `logs/`
- Reports: `reports/`
- Migrations are applied in lexical order from `migrations/`
- Current mode is deterministic batch ETL, not streaming

## Additional Docs

- Detailed documentation: `DOCUMENTATION.md`
- API-only quickstart: `api/README.md`
