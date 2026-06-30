from sqlalchemy import create_engine, text

from config import JDBC_PROPERTIES

ENGINE = create_engine(
    f"postgresql+psycopg2://"
    f"{JDBC_PROPERTIES['user']}:"
    f"{JDBC_PROPERTIES['password']}"
    f"@127.0.0.1:5432/24online"
)

# ==========================================================
# START PIPELINE
# ==========================================================

def start_pipeline():

    with ENGINE.begin() as conn:

        run_id = conn.execute(
            text("""
                INSERT INTO pipeline_run
                (
                    pipeline_name,
                    start_time,
                    status
                )
                VALUES
                (
                    'Bronze Pipeline',
                    NOW(),
                    'RUNNING'
                )
                RETURNING run_id
            """)
        ).scalar()

    return run_id


# ==========================================================
# FINISH PIPELINE
# ==========================================================

def finish_pipeline(run_id, status):

    with ENGINE.begin() as conn:

        conn.execute(
            text("""
                UPDATE pipeline_run
                SET
                    end_time = NOW(),
                    status = :status
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "status": status
            }
        )