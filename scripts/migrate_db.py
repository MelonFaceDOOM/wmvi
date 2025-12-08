from db.db import init_pool, close_pool, getcursor
from db.migrations_runner import run_migrations

init_pool()

# the checksum of the 001 sql file changed
# because i added some comments to items
# i'll delete this code after i've applied the new
# checksum value to prod and dev permanently
with getcursor(commit=True) as cur:
    cur.execute("""
        UPDATE schema_migrations
        SET checksum = %s
        WHERE version = %s
        """,
       ("27f89496ea0b419ea274eaf98c31fcb48596f951672cb2bb3da68486517e1b47",
        "001_base.sql")
    )
   
    
applied = run_migrations(migrations_dir="db/migrations")
print("Applied migrations:", applied)
close_pool()