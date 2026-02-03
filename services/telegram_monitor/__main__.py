import argparse
import asyncio
from .telegram_monitor import main, login_once

"""
TO RUN ON DEV:
python -m services.telegram_monitor

TO RUN ON PROD:
python -m services.telegram_monitor --prod

calls must come from root dir (wmvi):
"""


def _parse_args():
    ap = argparse.ArgumentParser(prog="python -m services.telegram_monitor")
    ap.add_argument("--prod", action="store_true")
    ap.add_argument(
        "--login",
        action="store_true",
        help="Interactive login to create/refresh the Telethon session, then exit.",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.login:
        asyncio.run(login_once(prod=args.prod))
    else:
        asyncio.run(main(prod=args.prod))
