from __future__ import annotations

import os
from functools import lru_cache

import torch
from faster_whisper import WhisperModel
from faster_whisper.transcribe import Segment
from deepmultilingualpunctuation import PunctuationModel

# ----------------------------
# Model loading
# ----------------------------


@lru_cache(maxsize=1)
def load_whisper_model(
    model_name: str | None = None,
) -> WhisperModel:
    """
    Load faster-whisper model once per process.

    model_name:
      - defaults to env WHISPER_MODEL or "tiny"
      - examples: tiny, base, small, medium, large-v3
    """
    model_name = model_name or os.getenv("WHISPER_MODEL", "tiny")

    if torch.cuda.is_available():
        device = "cuda"
        compute_type = "float16"
    else:
        device = "cpu"
        compute_type = "int8"

    model = WhisperModel(
        model_name,
        device=device,
        compute_type=compute_type,
    )

    return model


# ----------------------------
# Transcription
# ----------------------------


def transcribe_audio_file(
    model: WhisperModel,
    audio_path: str,
    *,
    language: str | None = None,
    word_level: bool = False,
) -> tuple[list[Segment], str]:
    segments, info = model.transcribe(
        audio_path,
        language=language,
        vad_filter=True,
        word_timestamps=word_level,
    )

    parts = [seg.text.strip() for seg in segments if seg.text]
    full_text = " ".join(parts)

    return list(segments), full_text


def restore_punctuation(text: str) -> str:
    """
    This supposedly will fix punctuation on a given input text block.
    TODO: Assess if this is needed. I noticed some transcription outputs lack
    good punctuation. This post-processing might help, but I need to investigate more.
    - create a detector for bad punct transcripts
    - run this on them and
    - analyse output and see what it looks like
    """
    punct_model = PunctuationModel()
    return punct_model.restore_punctuation(text)
