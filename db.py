import pymssql

import config


def connect(database: str = config.MSSQL_DB, autocommit: bool = False):
    return pymssql.connect(
        server=config.MSSQL_HOST,
        port=config.MSSQL_PORT,
        user=config.MSSQL_USER,
        password=config.MSSQL_PASSWORD,
        database=database,
        autocommit=autocommit,
    )


def reset_tables():
    with connect(autocommit=True) as conn:
        conn.cursor().execute("TRUNCATE TABLE sensor_readings; TRUNCATE TABLE rejected_messages;")


def counts() -> dict:
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT (SELECT COUNT(*) FROM sensor_readings), (SELECT COUNT(*) FROM rejected_messages)")
        stored, rejected = cur.fetchone()
        cur.execute("SELECT reason, COUNT(*) FROM rejected_messages GROUP BY reason ORDER BY reason")
        return {"stored": stored, "rejected": rejected, "by_reason": dict(cur.fetchall())}
