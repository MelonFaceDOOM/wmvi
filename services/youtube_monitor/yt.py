import os
from typing import Optional
from googleapiclient.discovery import build
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

YT_API_KEY = os.getenv("YT_API_KEY")


def youtube_client(api_key: Optional[str] = None):
    key = api_key or YT_API_KEY
    if not key:
        raise RuntimeError("Missing YT_API_KEY")
    return build("youtube", "v3", developerKey=key)
