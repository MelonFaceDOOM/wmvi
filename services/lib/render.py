from __future__ import annotations

from pathlib import Path


def load_template(path: Path) -> str:
    return path.read_text()


def render_template(template: str, replacements: dict[str, str]) -> str:
    """
    Replaces {{KEY}} with replacements["KEY"].
    """
    for k, v in replacements.items():
        template = template.replace(f"{{{{{k}}}}}", v)
    return template


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
