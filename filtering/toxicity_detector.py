"""
- Uses Detoxify models (PyTorch) under the hood.
- Returns per-dimension toxicity scores (toxicity, insult, threat, etc.).
- Provides a simple boolean "is this toxic?" classifier based on configurable
  thresholds over one or more labels.

- Plan is to use this to help remove toxic/irrelevant posts,
- But a second step of measuring "irrelevance" will be needed
- It casts too wide of a net for now

Typical usage:

    from toxicity_detector import score_toxicity, is_text_toxic

    scores = score_toxicity("You are terrible.")
    is_toxic, scores = is_text_toxic("You are terrible.", threshold=0.7)
"""

from __future__ import annotations

from functools import lru_cache
from typing import Dict, Iterable, List, Tuple

from detoxify import Detoxify


# Default model name for Detoxify.
# Options include: "original", "unbiased", "multilingual", "original-small", "unbiased-small".
DEFAULT_MODEL_NAME = "unbiased"

# Labels that we consider when deciding "is this toxic?" by default.
# For "unbiased" and "multilingual" models these are:
#   toxicity, severe_toxicity, obscene, threat, insult, identity_attack, sexual_explicit
DEFAULT_TOXICITY_LABELS = (
    "toxicity",
    "severe_toxicity",
    "insult",
    "threat",
    "identity_attack",
)


@lru_cache(maxsize=4)
def _get_model(model_name: str = DEFAULT_MODEL_NAME) -> Detoxify:
    """
    Lazily construct and cache Detoxify models by name.

    Detoxify API expects the model name as a positional argument, e.g.:
        Detoxify("unbiased")
    """
    return Detoxify(model_name)


def score_toxicity(
    text: str,
    *,
    model_name: str = DEFAULT_MODEL_NAME,
) -> Dict[str, float]:
    """
    Return Detoxify toxicity scores for a single text.

    Parameters
    ----------
    text:
        Input text to score.
    model_name:
        Detoxify model name (e.g. "original", "unbiased", "multilingual").

    Returns
    -------
    scores : dict[str, float]
        Mapping from label -> score in [0.0, 1.0], e.g.:

            {
                "toxicity": 0.87,
                "severe_toxicity": 0.12,
                "insult": 0.90,
                "threat": 0.02,
                ...
            }

        If the input text is empty/whitespace, returns an empty dict.
    """
    if not text or not text.strip():
        return {}

    model = _get_model(model_name)
    raw = model.predict(text)  # Detoxify returns a dict[label -> score]
    # Ensure plain Python floats
    return {label: float(score) for label, score in raw.items()}


def score_toxicity_batch(
    texts: Iterable[str],
    *,
    model_name: str = DEFAULT_MODEL_NAME,
) -> List[Dict[str, float]]:
    """
    Score a batch of texts with Detoxify.

    Parameters
    ----------
    texts:
        Iterable of input strings.
    model_name:
        Detoxify model name.

    Returns
    -------
    scores_list : list[dict[str, float]]
        A list of per-text score dicts, in the same order as `texts`.
        Empty/whitespace texts yield {} for that position.
    """
    texts = list(texts)
    if not texts:
        return []

    model = _get_model(model_name)
    # Detoxify's predict can take a list[str] and returns dict[label -> list[score]]
    raw = model.predict(texts)  # type: ignore[arg-type]

    # raw looks like: {"toxicity": [0.1, 0.9, ...], "insult": [0.0, 0.8, ...]}
    labels = list(raw.keys())
    n = len(texts)
    scores_list: List[Dict[str, float]] = [dict() for _ in range(n)]

    for label in labels:
        scores_for_label = raw[label]
        for i in range(n):
            if texts[i] and texts[i].strip():
                scores_list[i][label] = float(scores_for_label[i])

    return scores_list


def is_text_toxic(
    text: str,
    *,
    model_name: str = DEFAULT_MODEL_NAME,
    threshold: float = 0.7,
    labels_to_consider: Iterable[str] = DEFAULT_TOXICITY_LABELS,
) -> Tuple[bool, Dict[str, float]]:
    """
    Convenience function: classify text as toxic / non-toxic with scores.

    Parameters
    ----------
    text:
        Input text to score.
    model_name:
        Detoxify model name.
    threshold:
        Threshold in [0.0, 1.0]. If any selected label's score >= threshold,
        we classify the text as toxic (True).
    labels_to_consider:
        Iterable of label names to check when deciding toxicity. By default:
        toxicity, severe_toxicity, insult, threat, identity_attack.

    Returns
    -------
    is_toxic : bool
        True if any selected label score >= threshold, False otherwise.
    scores : dict[str, float]
        Full label -> score mapping from Detoxify.
    """
    scores = score_toxicity(text, model_name=model_name)
    if not scores:
        return False, scores

    labels_to_consider = tuple(labels_to_consider)

    is_toxic_flag = any(
        (label in scores) and (scores[label] >= threshold)
        for label in labels_to_consider
    )

    return is_toxic_flag, scores


def is_text_toxic_multi_threshold(
    text: str,
    *,
    model_name: str = DEFAULT_MODEL_NAME,
    thresholds: Dict[str, float] | None = None,
) -> Tuple[bool, Dict[str, float]]:
    """
    Classify text as toxic / non-toxic with per-label thresholds.

    This is useful if you want, for example, a higher threshold for "toxicity"
    but a lower threshold for "threat" or "identity_attack".

    Parameters
    ----------
    text:
        Input text.
    model_name:
        Detoxify model name.
    thresholds:
        Mapping label -> threshold. If None, a reasonable default is used for
        common labels. Labels not present in `thresholds` are ignored when
        deciding toxicity.

    Returns
    -------
    is_toxic : bool
        True if any label's score >= its configured threshold.
    scores : dict[str, float]
        Full label -> score mapping from Detoxify.
    """
    scores = score_toxicity(text, model_name=model_name)
    if not scores:
        return False, scores

    if thresholds is None:
        thresholds = {
            "toxicity": 0.7,
            "severe_toxicity": 0.6,
            "insult": 0.7,
            "threat": 0.4,
            "identity_attack": 0.5,
            "obscene": 0.7,
        }

    is_toxic_flag = False
    for label, thr in thresholds.items():
        if label in scores and scores[label] >= thr:
            is_toxic_flag = True
            break

    return is_toxic_flag, scores


def test() -> None:
    """
    Quick manual smoke test for toxicity detection.

    Prints text, scores, and boolean toxicity decisions.
    No assertions; visual inspection only.
    """
    examples = [
        "Vaccines are safe and effective when evaluated in clinical trials.",
        "I disagree with the government policies, but violence is not the answer.",
        "You are so stupid, nobody should listen to you.",
        "Kill yourself.",
        "We should kill all those people.",
        "I hope you get sick from that poison shot.",
        "I hate those idiots, they are ruining everything.",
        "Let's calmly discuss the risks and benefits of this vaccine.",
        "You are a worthless piece of garbage.",
        "This is a neutral statement about data and statistics.",
    ]

    print("=" * 80)
    print(f"Using Detoxify model: {DEFAULT_MODEL_NAME}")
    print("=" * 80)

    for i, text in enumerate(examples, start=1):
        print("-" * 80)
        print(f"Example {i}")
        print("TEXT:")
        print(text)
        is_tox, scores = is_text_toxic(text, threshold=0.7)
        print("\nScores:")
        for label, score in sorted(scores.items()):
            print(f"  {label:18s}: {score:.3f}")
        print(f"\nis_text_toxic(threshold=0.7): {is_tox}")

        is_tox_multi, _ = is_text_toxic_multi_threshold(text)
        print(f"is_text_toxic_multi_threshold(defaults): {is_tox_multi}")
        print()

    print("=" * 80)
    print("Done.")


if __name__ == "__main__":
    test()
