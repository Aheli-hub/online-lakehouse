from sqlalchemy import create_engine, text

from config import JDBC_PROPERTIES

# ==========================================================
# SQLALCHEMY CONNECTION
# ==========================================================

ENGINE = create_engine(
    f"postgresql+psycopg2://"
    f"{JDBC_PROPERTIES['user']}:"
    f"{JDBC_PROPERTIES['password']}"
    f"@127.0.0.1:5432/24online"
)

# ==========================================================
# LOG PIPELINE ERROR
# ==========================================================

def log_error(
    run_id,
    table_name,
    stage,
    error_message,
    stack_trace
):

    with ENGINE.begin() as conn:

        conn.execute(
            text("""
                INSERT INTO pipeline_errors
                (
                    run_id,
                    table_name,
                    stage,
                    error_message,
                    stack_trace
                )
                VALUES
                (
                    :run_id,
                    :table_name,
                    :stage,
                    :error_message,
                    :stack_trace
                )
            """),
            {
                "run_id": run_id,
                "table_name": table_name,
                "stage": stage,
                "error_message": error_message,
                "stack_trace": stack_trace
            }
        )

    print("Pipeline Error Logged")