import pytest

from transcription.transcription_checklist import (
    CHECKS,
    GROUP_EXPANSIONS,
    all_check_ids,
    ids_for_groups,
    parse_selection,
)


def test_all_check_ids_count():
    assert len(all_check_ids()) == 15
    assert all_check_ids() == {c.id for c in CHECKS}


def test_parse_selection_zero():
    assert parse_selection("0") == all_check_ids()


def test_parse_selection_comma_list():
    assert parse_selection("1,3,7") == {1, 3, 7}


def test_parse_selection_zero_in_list_means_all():
    assert parse_selection("1,0,3") == all_check_ids()


def test_parse_selection_invalid_token():
    with pytest.raises(ValueError, match="invalid"):
        parse_selection("1,foo")


def test_parse_selection_unknown_id():
    with pytest.raises(ValueError, match="unknown test id"):
        parse_selection("99")


def test_ids_for_group_podcast():
    ids = ids_for_groups(["podcast"])
    assert 10 not in ids  # youtube-only
    assert 15 in ids
    assert 1 in ids  # core
    assert 7 in ids  # db


def test_ids_for_group_youtube():
    ids = ids_for_groups(["youtube"])
    assert 10 in ids
    assert 15 not in ids


def test_ids_for_group_all():
    assert ids_for_groups(["all"]) == all_check_ids()


def test_ids_for_unknown_group():
    with pytest.raises(ValueError, match="unknown group"):
        ids_for_groups(["nope"])


def test_group_expansions_keys():
    assert set(GROUP_EXPANSIONS) == {"core", "db", "youtube", "podcast", "all"}
