from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import text

from config import JDBC_PROPERTIES

# ==========================================================
# POSTGRES CONNECTION
# ==========================================================

ENGINE = create_engine(

    f"postgresql+psycopg2://"

    f"{JDBC_PROPERTIES['user']}:"

    f"{JDBC_PROPERTIES['password']}"

    "@127.0.0.1:5432/24online"

)

# ==========================================================
# UPDATE SILVER AUDIT
# ==========================================================

def update_silver_audit(

    table_name,
    row_count,
    status

):

    with ENGINE.begin() as conn:

        conn.execute(

            text("""

            INSERT INTO silver_audit
            (

                table_name,

                row_count,

                status,

                load_time

            )

            VALUES

            (

                :table_name,

                :row_count,

                :status,

                NOW()

            )

            """),

            {

                "table_name": table_name,

                "row_count": row_count,

                "status": status

            }

        )

    print("Silver Audit Updated")