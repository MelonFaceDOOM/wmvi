from __future__ import annotations

import sys
from pathlib import Path

from faster_whisper import WhisperModel
from deepmultilingualpunctuation import PunctuationModel


def transcribe_raw(model: WhisperModel, audio_path: Path) -> str:
    """
    Route 1:
    Mimics existing production behavior as closely as possible.
    """
    segments, info = model.transcribe(
        str(audio_path),
        beam_size=1,
        vad_filter=True,
    )

    parts: list[str] = []
    for seg in segments:
        if seg.text:
            parts.append(seg.text.strip())

    return " ".join(parts)


def restore_punctuation(text: str) -> str:
    """
    Route 2:
    Post-process punctuation restoration.
    """
    punct_model = PunctuationModel()
    return punct_model.restore_punctuation(text)


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: python whisper_punct_compare.py <audio_file>")
        sys.exit(1)

    audio_path = Path(sys.argv[1])
    if not audio_path.exists():
        print(f"file not found: {audio_path}")
        sys.exit(1)

    print("[whisper] loading model (cpu, tiny)...")
    model = WhisperModel(
        "tiny",
        device="cpu",
        compute_type="int8",
    )

    print("[route 1] transcribing (raw, no punctuation)...")
    raw_text = transcribe_raw(model, audio_path)

    print("[route 2] restoring punctuation...")
    punct_text = restore_punctuation(raw_text)

    print("\n" + "=" * 80)
    print("ROUTE 1: RAW WHISPER OUTPUT (first 1000 chars)")
    print("=" * 80)
    print(raw_text[:1000])

    print("\n" + "=" * 80)
    print("ROUTE 2: PUNCTUATION RESTORED (first 1000 chars)")
    print("=" * 80)
    print(punct_text[:1000])

    print("\n[ok] comparison complete")


if __name__ == "__main__":
    main()
