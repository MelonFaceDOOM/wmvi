import os
import subprocess
from dotenv import load_dotenv
import argparse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--prod",
        action="store_true",
        help="Apply dump PROD schema (DEV by default).",
    )
    args = ap.parse_args()
    env = "PROD" if args.prod else "DEV"
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
