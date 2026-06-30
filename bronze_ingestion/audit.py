from sqlalchemy import create_engine, text

from config import JDBC_PROPERTIES

ENGINE = create_engine(
    f"postgresql+psycopg2://"
    f"{JDBC_PROPERTIES['user']}:"
    f"{JDBC_PROPERTIES['password']}"
    f"@127.0.0.1:5432/24online"
)

# ==========================================================
# UPDATE AUDIT
# ==========================================================

def update_audit(
    run_id,
    table_name,
    row_count,
    status,
    last_watermark=None
):

    with ENGINE.begin() as conn:

        conn.execute(
            text("""
                INSERT INTO ingestion_audit
                (
                    run_id,
                    table_name,
                    last_run_time,
                    last_watermark,
                    row_count,
                    status,
                    load_date
                )
                VALUES
                (
                    :run_id,
                    :table_name,
                    NOW(),
                    :last_watermark,
                    :row_count,
                    :status,
                    CURRENT_DATE
                )
            """),
            {
                "run_id": run_id,
                "table_name": table_name,
                "last_watermark": last_watermark,
                "row_count": row_count,
                "status": status
            }
        )

    print("Audit Updated")