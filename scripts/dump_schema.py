import os
import sys
import subprocess
from dotenv import load_dotenv


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {"dev", "prod"}:
        print("usage: python dump_schema.py [dev|prod]")
        sys.exit(1)

    env = sys.argv[1].upper()
    prefix = f"{env}_"

    load_dotenv()

    def req(name: str) -> str:
        val = os.getenv(prefix + name)
        if not val:
            raise RuntimeError(f"Missing env var: {prefix}{name}")
        return val

    db = req("PGDATABASE")
    user = req("PGUSER")
    host = req("PGHOST")
    port = os.getenv(prefix + "PGPORT", "5432")
    os.environ["PGPASSWORD"] = req("PGPASSWORD")
    cmd = [
        "pg_dump",
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "-h", host,
        "-p", port,
        "-U", user,
        db,
    ]

    outfile = f"schema_{env.lower()}.sql"

    print(f"Dumping {env} schema → {outfile}")
    print("Password prompt may appear if not set in env")

    with open(outfile, "w") as f:
        subprocess.run(cmd, stdout=f, check=True)

    print("Done.")


if __name__ == "__main__":
    main()
