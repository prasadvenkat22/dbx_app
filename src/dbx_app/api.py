import os
import traceback
from datetime import date
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel
from . import wanderbricks as wb
from . import lakebase as lb

app = FastAPI(
    title="Wanderbricks API",
    description="REST API over samples.wanderbricks in Databricks Unity Catalog",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)


@app.exception_handler(Exception)
async def all_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": type(exc).__name__},
    )


# ── Root & Health ─────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/api/docs")


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}


@app.get("/api/debug", tags=["health"])
def debug():
    """Show environment info and test SDK auth."""
    from databricks.sdk import WorkspaceClient
    info = {
        "DATABRICKS_HOST": os.environ.get("DATABRICKS_HOST", "NOT SET"),
        "DATABRICKS_WAREHOUSE_ID": os.environ.get("DATABRICKS_WAREHOUSE_ID", "NOT SET"),
        "DATABRICKS_AUTH_TYPE": os.environ.get("DATABRICKS_AUTH_TYPE", "NOT SET"),
        "DATABRICKS_CLIENT_ID": "SET" if os.environ.get("DATABRICKS_CLIENT_ID") else "NOT SET",
        "DATABRICKS_CLIENT_SECRET": "SET" if os.environ.get("DATABRICKS_CLIENT_SECRET") else "NOT SET",
        "DATABRICKS_TOKEN": "SET" if os.environ.get("DATABRICKS_TOKEN") else "NOT SET",
    }
    try:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        w = WorkspaceClient(host=host)
        me = w.current_user.me()
        info["sdk_auth"] = "SUCCESS"
        info["sdk_user"] = me.user_name
    except Exception as e:
        info["sdk_auth"] = f"FAILED: {type(e).__name__}: {e}"
    return info


# ── Identity ──────────────────────────────────────────────────────────────────

@app.get("/api/me", tags=["identity"])
def me(request: Request):
    return {
        "user": request.headers.get("X-Forwarded-User"),
        "email": request.headers.get("X-Forwarded-Email"),
        "username": request.headers.get("X-Forwarded-Preferred-Username"),
    }


# ── Schema discovery ──────────────────────────────────────────────────────────

@app.get("/api/debug/warehouse", tags=["health"])
def debug_warehouse():
    """Deep diagnostic — inspect every field type from the SQL statement response."""
    import databricks.sdk as _sdk
    from databricks.sdk import WorkspaceClient
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    info: dict = {"sdk_version": getattr(_sdk, "__version__", "unknown")}
    try:
        w = WorkspaceClient(host=host)
        stmt = w.statement_execution.execute_statement(
            statement="SELECT 1 AS test",
            warehouse_id=warehouse_id,
            wait_timeout="30s",
        )
        state = stmt.status.state
        info["state_type"] = type(state).__name__
        info["state_repr"] = repr(state)
        info["state_str"] = str(state)
        info["state_has_value"] = hasattr(state, "value")
        if hasattr(state, "value"):
            info["state_value"] = state.value
        info["result"] = stmt.result.data_array if stmt.result else None
    except Exception as e:
        import traceback as tb
        info["error"] = f"{type(e).__name__}: {e}"
        info["traceback"] = tb.format_exc()
    return info


@app.get("/api/tables", tags=["schema"])
def tables():
    """List all tables in samples.wanderbricks."""
    return wb.list_tables()


# ── Properties ────────────────────────────────────────────────────────────────

@app.get("/api/properties", tags=["properties"])
def properties(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Paginated list of all properties."""
    return wb.get_properties(limit=limit, offset=offset)


@app.get("/api/properties/search", tags=["properties"])
def search(
    property_type: str | None = Query(None, description="e.g. Hotel, Apartment, Villa"),
    min_price: float | None = Query(None, ge=0),
    max_price: float | None = Query(None, ge=0),
    min_bedrooms: int | None = Query(None, ge=0),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Search properties by type, price range, and bedrooms."""
    return wb.search_properties(
        property_type=property_type,
        min_price=min_price,
        max_price=max_price,
        min_bedrooms=min_bedrooms,
        limit=limit,
        offset=offset,
    )


@app.get("/api/properties/types", tags=["properties"])
def property_types():
    """List all property types with counts."""
    return wb.get_property_types()


@app.get("/api/properties/{property_id}", tags=["properties"])
def property_detail(property_id: int):
    """Get a single property by ID."""
    row = wb.get_property(property_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    return row


# ── Reviews ───────────────────────────────────────────────────────────────────

@app.get("/api/reviews", tags=["reviews"])
def reviews(
    property_id: int | None = Query(None, description="Filter by property ID"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Paginated reviews, optionally filtered by property."""
    return wb.get_reviews(property_id=property_id, limit=limit, offset=offset)


# ── Destinations ──────────────────────────────────────────────────────────────

@app.get("/api/destinations", tags=["destinations"])
def destinations(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List travel destinations."""
    return wb.get_destinations(limit=limit, offset=offset)


# ── Bookings (Lakebase Postgres) ──────────────────────────────────────────────

class BookingRequest(BaseModel):
    property_id: int
    user_id: int
    check_in: date
    check_out: date
    guests: int = 1
    total_price: float | None = None


@app.post("/api/bookings", tags=["bookings"], status_code=201)
def create_booking(body: BookingRequest):
    """Create a new booking — stored in Lakebase Postgres."""
    if body.check_out <= body.check_in:
        raise HTTPException(status_code=400, detail="check_out must be after check_in")
    return lb.create_booking(
        property_id=body.property_id,
        user_id=body.user_id,
        check_in=str(body.check_in),
        check_out=str(body.check_out),
        guests=body.guests,
        total_price=body.total_price,
    )


@app.get("/api/bookings", tags=["bookings"])
def list_bookings(
    property_id: int | None = Query(None),
    user_id: int | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List bookings, optionally filtered by property or user."""
    return lb.list_bookings(property_id=property_id, user_id=user_id, limit=limit, offset=offset)


@app.get("/api/bookings/{booking_id}", tags=["bookings"])
def get_booking(booking_id: int):
    """Get a single booking by ID."""
    row = lb.get_booking(booking_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Booking {booking_id} not found")
    return row


@app.delete("/api/bookings/{booking_id}", tags=["bookings"])
def cancel_booking(booking_id: int):
    """Cancel a booking."""
    row = lb.cancel_booking(booking_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Booking {booking_id} not found")
    return row
