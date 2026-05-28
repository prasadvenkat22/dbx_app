import os
import psycopg2
from psycopg2.extras import RealDictCursor
from databricks.sdk import WorkspaceClient

LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "ep-long-wildflower-d8ew0atm.database.us-east-2.cloud.databricks.com",
)
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "postgres")
LAKEBASE_ENDPOINT = "projects/dbx-lbase/branches/production/endpoints/primary"


def _connect() -> psycopg2.extensions.connection:
    w = WorkspaceClient()

    # Get the current identity to use as the Postgres username
    me = w.current_user.me()
    db_user = me.user_name or (me.emails[0].value if me.emails else "unknown")

    # Generate a short-lived OAuth token used as the Postgres password
    cred = w.lakebase.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)

    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB,
        user=db_user,
        password=cred.token,
        sslmode="require",
        cursor_factory=RealDictCursor,
    )


def ensure_bookings_table() -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bookings (
                    booking_id  SERIAL        PRIMARY KEY,
                    property_id BIGINT        NOT NULL,
                    user_id     BIGINT        NOT NULL,
                    check_in    DATE          NOT NULL,
                    check_out   DATE          NOT NULL,
                    guests      INT           NOT NULL DEFAULT 1,
                    status      VARCHAR(20)   NOT NULL DEFAULT 'CONFIRMED',
                    total_price NUMERIC(10,2),
                    created_at  TIMESTAMP     NOT NULL DEFAULT NOW()
                )
            """)
            conn.commit()


def create_booking(
    property_id: int,
    user_id: int,
    check_in: str,
    check_out: str,
    guests: int,
    total_price: float | None = None,
) -> dict:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bookings (property_id, user_id, check_in, check_out, guests, total_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (property_id, user_id, check_in, check_out, guests, total_price),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row)


def list_bookings(
    property_id: int | None = None,
    user_id: int | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    filters, params = [], []
    if property_id is not None:
        filters.append("property_id = %s")
        params.append(property_id)
    if user_id is not None:
        filters.append("user_id = %s")
        params.append(user_id)
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    params.extend([limit, offset])
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM bookings {where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                params,
            )
            return [dict(r) for r in cur.fetchall()]


def get_booking(booking_id: int) -> dict | None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bookings WHERE booking_id = %s", (booking_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def cancel_booking(booking_id: int) -> dict | None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bookings SET status = 'CANCELLED' WHERE booking_id = %s RETURNING *",
                (booking_id,),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
