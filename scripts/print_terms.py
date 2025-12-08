from __future__ import annotations

import os
import psycopg2

from dotenv import load_dotenv
load_dotenv()

def db_creds_from_env(prefix: str) -> str:
    host = os.environ[f"{prefix}_PGHOST"]
    user = os.environ[f"{prefix}_PGUSER"]
    pwd  = os.environ[f"{prefix}_PGPASSWORD"]
    port = os.environ.get(f"{prefix}_PGPORT", "5432")
    db   = os.environ.get(f"{prefix}_PGDATABASE", "postgres")
    ssl  = os.environ.get(f"{prefix}_PGSSLMODE", "require")

    return (
        f"host={host} port={port} dbname={db} user={user} "
        f"password={pwd} sslmode={ssl}"
    )


def connect_from_prefix(prefix: str) -> psycopg2.extensions.connection:
    dsn = db_creds_from_env(prefix)
    return psycopg2.connect(dsn)
    
    
conn = connect_from_prefix("DEV")
cur = conn.cursor()
cur.execute("""select name from taxonomy.vaccine_term""")
r = cur.fetchall()
for i in r:
    print(i[0])
cur.close()
conn.close()