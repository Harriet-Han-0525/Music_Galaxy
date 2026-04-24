#!/usr/bin/env python3
"""Quick PostgreSQL connectivity check (defaults + env overrides)."""

import os
import sys

try:
    import psycopg2
except ImportError:
    print(
        "缺少 psycopg2。在 conda 环境中执行：\n"
        "  conda install -c conda-forge psycopg2\n"
        "或：pip install psycopg2-binary",
        file=sys.stderr,
    )
    sys.exit(1)


def main() -> None:
    host = os.environ.get("PGHOST", "127.0.0.1")
    port = int(os.environ.get("PGPORT", "5432"))
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "hyaya2002")
    dbname = os.environ.get("PGDATABASE", "postgres")

    print(f"尝试连接: host={host} port={port} user={user} dbname={dbname}")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=5,
        )
    except psycopg2.Error as e:
        print("连接失败:", e, file=sys.stderr)
        sys.exit(2)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
        print("连接成功。")
        print("服务器版本:", version.splitlines()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
