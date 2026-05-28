from db.migrations_runner import _migration_number


def test_migration_number() -> None:
    assert _migration_number("001_base.sql") == 1
    assert _migration_number("021_drop_yt_comment_raw.sql") == 21
    assert _migration_number("022_podcast_transcript_import_state.sql") == 22
