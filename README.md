# Wanderbricks FastAPI — Databricks App

A REST API built on Databricks Apps that serves data from `samples.wanderbricks`
(properties, reviews, destinations) with optional booking support via Lakebase Postgres.

Interactive API docs available at `/api/docs` once deployed.

---

## Prerequisites

Install these once on your local machine:

| Tool | Install | Why |
|------|---------|-----|
| [Databricks CLI v1.x](https://docs.databricks.com/dev-tools/cli/install.html) | Download from GitHub releases | Deploy bundles |
| [uv](https://docs.astral.sh/uv/getting-started/installation/) | `pip install uv` | Build Python wheel |
| Python 3.10–3.13 | [python.org](https://www.python.org/downloads/) | Local dev / tests |

Your **Databricks workspace** must have:

- [ ] **Unity Catalog** enabled (provides the `samples` catalog automatically)
- [ ] **Databricks Apps** enabled — ask your workspace admin if not available
- [ ] A **SQL Warehouse** running (Serverless or Classic)
- [ ] *(Optional — for bookings)* A **Lakebase Postgres** instance created

---

## Deploy to a New Workspace

### Step 1 — Authenticate the CLI

```bash
databricks configure
# Enter your workspace URL: https://your-workspace.azuredatabricks.net
# Enter your token: (create one in User Settings → Developer → Access Tokens)
```

### Step 2 — Set your workspace values in `databricks.yml`

Open `databricks.yml` and update the `dev` target:

```yaml
targets:
  dev:
    workspace:
      host: https://YOUR-WORKSPACE-URL.azuredatabricks.net  # ← change this
    variables:
      warehouse_id: "YOUR_WAREHOUSE_ID"     # ← required
      lakebase_host: ""                     # ← optional (for bookings)
      lakebase_endpoint: ""                 # ← optional (for bookings)
```

**Finding your Warehouse ID:**
> Databricks UI → Compute → SQL Warehouses → click your warehouse → Connection details → copy the ID from the JDBC URL (the last path segment after the final `/`)

**Finding your Lakebase details** *(only needed for `/api/bookings`)*:
> Databricks UI → Compute → Database instances → your instance → Connection details

### Step 3 — Deploy

```bash
databricks bundle deploy -t dev
databricks bundle run -t dev wanderbricks_app
```

The CLI will print the app URL when ready.

### Step 4 — Open the API

```
https://<your-app-url>/api/docs     # Swagger UI — try every endpoint here
https://<your-app-url>/health       # Health check
https://<your-app-url>/api/tables   # Lists all wanderbricks tables
```

---

## Endpoints

### Properties (`samples.wanderbricks.properties`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/properties` | Paginated list |
| GET | `/api/properties/{id}` | Single property |
| GET | `/api/properties/search` | Filter by type, price, bedrooms |
| GET | `/api/properties/types` | Property types with counts |

### Reviews
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/reviews` | Paginated, filter by `property_id` |

### Destinations
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/destinations` | All destinations |

### Bookings (Lakebase Postgres — requires `lakebase_host` and `lakebase_endpoint`)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/bookings` | Create a booking |
| GET | `/api/bookings` | List, filter by `property_id` or `user_id` |
| GET | `/api/bookings/{id}` | Single booking |
| DELETE | `/api/bookings/{id}` | Cancel a booking |

### Utility
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness check |
| GET | `/api/me` | Logged-in user identity |
| GET | `/api/tables` | Schema discovery |
| GET | `/api/debug` | Environment / auth diagnostics |
| GET | `/api/debug/warehouse` | SQL warehouse connectivity test |

---

## Stop & Restart

```bash
# Stop (no compute cost while stopped)
databricks apps stop wanderbricks-api

# Restart later
databricks bundle run -t dev wanderbricks_app
```

---

## Project Structure

```
databricks.yml                        # Bundle config — edit your variables here
resources/
  wanderbricks_app.app.yml            # Databricks App definition
  sample_job.job.yml                  # Sample batch job
  dbx_app_etl.pipeline.yml            # DLT pipeline
src/
  requirements.txt                    # App dependencies (auto-installed on deploy)
  start.sh                            # App startup script
  dbx_app/
    api.py                            # FastAPI routes
    wanderbricks.py                   # Unity Catalog data layer
    lakebase.py                       # Lakebase Postgres data layer
    main.py                           # Batch job entry point
tests/
  conftest.py                         # Pytest + Databricks Connect setup
```

---

## Local Development

```bash
pip install uv
uv sync --dev
uv run pytest
```

Set these environment variables to point at your workspace:

```bash
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=your-pat-token
export DATABRICKS_WAREHOUSE_ID=your-warehouse-id
export LAKEBASE_HOST=your-lakebase-host           # optional
export LAKEBASE_ENDPOINT=projects/.../endpoints/primary  # optional
```
