from db.db import 1, getcursor, close_pool
from dotenv import load_dotenv
from db.migrations_runner import run_migrations
load_dotenv()

pool = init_pool(prefix="PROD")
applied = run_migrations(migrations_dir="db/migrations")