from __future__ import annotations

from typing import Type

from ingestion.row_model import InsertableRow
from ingestion.youtube.video import YoutubeVideoRow
from ingestion.youtube.comment import YoutubeCommentRow
from ingestion.reddit.submission import RedditSubmissionRow
from ingestion.reddit.comment import RedditCommentRow
from ingestion.telegram import TelegramPostRow
from ingestion.podcast import PodcastEpisodeRow
# tweet + news: add when you have dataclasses for them

PLATFORM_ROW: dict[str, Type[InsertableRow]] = {
    "reddit_submission": RedditSubmissionRow,
    "reddit_comment": RedditCommentRow,
    "telegram_post": TelegramPostRow,
    "youtube_video": YoutubeVideoRow,
    "youtube_comment": YoutubeCommentRow,
    "podcast_episode": PodcastEpisodeRow,
    # "tweet": TweetRow,
    # "news_article": NewsArticleRow,
}