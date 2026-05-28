import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = "samples"
SCHEMA = "wanderbricks"


def _warehouse_id() -> str:
    wh = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not wh:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID env var is not set. "
            "Set it to a SQL warehouse ID from your Databricks workspace."
        )
    return wh


def _client() -> WorkspaceClient:
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return WorkspaceClient(host=host) if host else WorkspaceClient()


def _run(sql: str) -> list[dict]:
    w = _client()
    stmt = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=_warehouse_id(),
        wait_timeout="50s",
    )
    try:
        state_str = stmt.status.state.value  # enum path
    except AttributeError:
        state_str = str(stmt.status.state)   # plain string path
    if state_str != "SUCCEEDED":
        try:
            error_msg = stmt.status.error.message
        except AttributeError:
            error_msg = str(stmt.status.error) if stmt.status.error else state_str
        raise RuntimeError(f"Query failed ({state_str}): {error_msg}")
    cols = [c.name for c in stmt.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (stmt.result.data_array or [])]


def list_tables() -> list[dict]:
    return _run(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")


# ── Properties ────────────────────────────────────────────────────────────────
# Columns: property_id, host_id, destination_id, title, description,
#          base_price, property_type, max_guests, bedrooms, bathrooms,
#          property_latitude, property_longitude, created_at

def get_properties(limit: int = 20, offset: int = 0) -> list[dict]:
    return _run(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.properties LIMIT {limit} OFFSET {offset}"
    )


def get_property(property_id: int) -> dict | None:
    rows = _run(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.properties WHERE property_id = {property_id}"
    )
    return rows[0] if rows else None


def search_properties(
    property_type: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    min_bedrooms: int | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    filters = []
    if property_type:
        safe = property_type.replace("'", "''")
        filters.append(f"property_type = '{safe}'")
    if min_price is not None:
        filters.append(f"base_price >= {min_price}")
    if max_price is not None:
        filters.append(f"base_price <= {max_price}")
    if min_bedrooms is not None:
        filters.append(f"bedrooms >= {min_bedrooms}")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    return _run(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.properties {where} LIMIT {limit} OFFSET {offset}"
    )


def get_property_types() -> list[dict]:
    return _run(
        f"SELECT property_type, COUNT(*) AS count "
        f"FROM {CATALOG}.{SCHEMA}.properties "
        f"GROUP BY property_type ORDER BY count DESC"
    )


# ── Reviews ───────────────────────────────────────────────────────────────────
# Columns: review_id, booking_id, property_id, user_id, rating,
#          comment, is_deleted, created_at, updated_at

def get_reviews(
    property_id: int | None = None, limit: int = 20, offset: int = 0
) -> list[dict]:
    where = f"WHERE property_id = {property_id} AND is_deleted = false" if property_id is not None else "WHERE is_deleted = false"
    return _run(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.reviews {where} LIMIT {limit} OFFSET {offset}"
    )


def get_destinations(limit: int = 20, offset: int = 0) -> list[dict]:
    return _run(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.destinations LIMIT {limit} OFFSET {offset}"
    )
