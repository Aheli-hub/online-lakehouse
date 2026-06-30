from config import JDBC_URL
from config import JDBC_PROPERTIES
from config import FETCH_SIZE
from retry import retry

# ==========================================================
# READ TABLE FROM POSTGRESQL
# ==========================================================

@retry()
def read_table(spark, sql):

    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", JDBC_PROPERTIES["driver"])
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("dbtable", f"({sql}) src")
        .option("fetchsize", FETCH_SIZE)
        .load()
    )

    return df