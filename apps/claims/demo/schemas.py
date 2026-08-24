"""Structured-output JSON schemas for hierarchy title massage."""

from __future__ import annotations

from typing import Any

_ITEM = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "id": {"type": "integer"},
        "title": {"type": "string"},
        "blurb": {"type": "string"},
    },
    "required": ["id", "title", "blurb"],
}

NARRATIVE_NAMES_SCHEMA: dict[str, Any] = {
    "name": "narrative_names",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {"narratives": {"type": "array", "items": _ITEM}},
        "required": ["narratives"],
    },
}

LEAF_NAMES_SCHEMA: dict[str, Any] = {
    "name": "leaf_names",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {"leaves": {"type": "array", "items": _ITEM}},
        "required": ["leaves"],
    },
}

_ASSIGN = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "leaf_id": {"type": "integer"},
        "narrative_id": {"type": "integer"},
    },
    "required": ["leaf_id", "narrative_id"],
}

REASSIGN_SCHEMA: dict[str, Any] = {
    "name": "leaf_reassign",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {"assignments": {"type": "array", "items": _ASSIGN}},
        "required": ["assignments"],
    },
}

NARRATIVE_NAMES_SCHEMA = NARRATIVE_NAMES_SCHEMA
LEAF_NAMES_SCHEMA = LEAF_NAMES_SCHEMA
REASSIGN_SCHEMA = REASSIGN_SCHEMA

