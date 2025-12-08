"""
wrapper around Microsoft Presidio to strip common PII from text.
- Replaces detected PII with simple typed placeholders, e.g. [REDACTED_EMAIL].
- Main knobs:
    - `skip_entity_types`: entity types to ignore entirely (e.g. PERSON, DATE, DATE_TIME).
   
"""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable, List, Tuple, Union, Collection

import logging

from presidio_analyzer import AnalyzerEngine, RecognizerResult
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig



# These will not be redacted
SKIPPED_ENTITIES = {"PERSON", "DATE", "DATE_TIME", "LOCATION", "URL"}
# reasoning:
#  - person: names of public figures provide important context and can't be filtered out
#  - date/date_time: no reason to remove date
#  - location: specific locations would be good to removed,
#      but i don't know if they can be separated from city names, which are important to keep
#  - url: no reason to remove urls

_ANALYZER = AnalyzerEngine()       # built once at import time
_ANONYMIZER = AnonymizerEngine()   # same

# Turn down all Presidio noise
for name in list(logging.Logger.manager.loggerDict.keys()):
    if "presidio" in name:
        logging.getLogger(name).setLevel(logging.WARNING)


# Default anonymization behavior:
DEFAULT_OPERATORS = {
    "DEFAULT": OperatorConfig("replace", {"new_value": "[REDACTED]"}),
    "PERSON": OperatorConfig("replace", {"new_value": "[REDACTED_NAME]"}),
    "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "[REDACTED_PHONE]"}),
    "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "[REDACTED_EMAIL]"}),
    "IP_ADDRESS": OperatorConfig("replace", {"new_value": "[REDACTED_IP]"}),
    "URL": OperatorConfig("replace", {"new_value": "[REDACTED_URL]"}),
    "CREDIT_CARD": OperatorConfig("replace", {"new_value": "[REDACTED_CREDIT_CARD]"}),
    "IBAN_CODE": OperatorConfig("replace", {"new_value": "[REDACTED_IBAN]"}),
    "LOCATION": OperatorConfig("replace", {"new_value": "[REDACTED_LOCATION]"}),
}


def _filter_entities(
    results: Iterable[RecognizerResult],
    *,
    skip_entity_types: Collection[str] | None,
) -> List[RecognizerResult]:
    """
    Filter analyzer results before anonymization.
    - If `skip_entity_types` is provided, do not anonymize entities of those types.
    """
    skip = set(skip_entity_types or ())
    out: List[RecognizerResult] = []

    for r in results:
        # Drop anything explicitly skipped
        if r.entity_type in skip:
            continue
        out.append(r)

    return out


def redact_pii(
    text: str,
    *,
    language: str = "en",
    score_threshold: float | None = 0.35,
    skip_entity_types: Collection[str] | None = None,
    return_analyzer_results: bool = False,
) -> Union[str, Tuple[str, List[RecognizerResult]]]:
    """
    Detect and replace PII in `text` using Presidio.
    return_analyzer_results:
        If False (default), return only the redacted text (str).
        If True, return a tuple: (redacted_text, analyzer_results).
    Returns
    -------
    Either:
        - redacted_text : str
      or, if `return_analyzer_results=True`:
        - (redacted_text, analyzer_results) where analyzer_results is a
          list[RecognizerResult].
    """
    if skip_entity_types is None:
        skip_entity_types = SKIPPED_ENTITIES
    
    if not text:
        empty_results: List[RecognizerResult] = []
        if return_analyzer_results:
            return "", empty_results
        return ""

    analyzer_results: List[RecognizerResult] = _ANALYZER.analyze(
        text=text,
        language=language,
        score_threshold=score_threshold,
    )

    analyzer_results = _filter_entities(
        analyzer_results,
        skip_entity_types=skip_entity_types,
    )

    anonymized = _ANONYMIZER.anonymize(
        text=text,
        analyzer_results=analyzer_results,
        operators=DEFAULT_OPERATORS,
    )

    if return_analyzer_results:
        return anonymized.text, list(analyzer_results)

    return anonymized.text



def test() -> None:
    """
    Quick manual smoke test for redact_pii().

    Prints before/after for a small set of representative examples.
    No assertions; just visual inspection.
    """
    examples = [
        # Basic emails
        ("Email only", "Contact me at john.doe@example.com"),
        # Phone numbers
        ("US-style phone", "My number is (555) 123-4567."),
        ("Intl phone", "Call me at +1-416-555-7890 tomorrow."),
        # IP address and URL
        ("IP address", "The server is at 192.168.0.1 right now."),
        ("URL", "See https://example.org for more info."),
        # Credit card / IBAN-like
        (
            "Credit card",
            "Card 4111 1111 1111 1111 expires 10/28.",
        ),
        (
            "IBAN",
            "My IBAN is DE89 3704 0044 0532 0130 00, please send the refund.",
        ),
        # Names
        ("Person name", "I spoke with Alice Johnson about the trial."),
        ("Multiple names", "Bob and Charlie met Dr. Emily Smith at lunch."),
        # Location / address-ish
        (
            "Simple address",
            "I live at 123 Main St, Toronto, ON M5V 2T6.",
        ),
        (
            "Address with name",
            "John Doe lives at 456 Queen Street West, Toronto.",
        ),
        # Mixed PII
        (
            "Mixed PII",
            "Jane's email is jane99@example.com and her phone is 555-987-6543.",
        ),
        # Organization vs person
        (
            "Org vs person",
            "I visited the World Health Organization with Dr. Robert Smith.",
        ),  
        # No PII
        (
            "No PII",
            "Vaccines are safe and effective when evaluated in clinical trials.",
        ),
        (
            "No PII, names-like words",
            "We discussed COVID-19, Pfizer, and Moderna.",
        ),
        (
            "date test",
            "the date is 10/28",
        ),
    ]

    for i, (label, text) in enumerate(examples, start=1):
        print("=" * 80)
        print(f"Example {i}: {label}")
        print("INPUT : ", text)
        try:
            redacted = redact_pii(text)
        except Exception as e:
            print("ERROR  : ", repr(e))
            continue
        print("OUTPUT: ", redacted)
        print()

    print("=" * 80)
    print("Done.")

if __name__ == "__main__":
    test()
