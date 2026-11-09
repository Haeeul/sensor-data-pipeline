import re
from pathlib import Path

import db

SQL_FILE = Path(__file__).parent / "db" / "init.sql"


def main():
    sql = SQL_FILE.read_text(encoding="utf-8")
    # GO는 sqlcmd 전용 구분자라 드라이버로 보낼 땐 직접 나눠서 실행
    batches = [b.strip() for b in re.split(r"^\s*GO\s*$", sql, flags=re.M | re.I) if b.strip()]

    # CREATE DATABASE는 트랜잭션 안에서 실행 불가 → autocommit
    with db.connect(database="master", autocommit=True) as conn:
        cur = conn.cursor()
        for batch in batches:
            cur.execute(batch)

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.name, i.name, i.is_unique
            FROM sys.tables t JOIN sys.indexes i ON i.object_id = t.object_id
            WHERE i.name IS NOT NULL ORDER BY t.name, i.index_id
        """)
        print("table               | index                              | unique")
        for table, index, unique in cur.fetchall():
            print(f"{table:<19} | {index:<34} | {bool(unique)}")


if __name__ == "__main__":
    main()
