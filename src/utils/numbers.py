"""Locale-tolerant number parsing for CSV exports and postback payouts."""

import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Optional

_NUMBER_CHUNK_RE = re.compile(r"[-+]?\d[\d.,\s ']*")


def parse_decimal(value: Any) -> Optional[Decimal]:
    """'1,234.56' / '1 234,56' / '1.234,56' / '12,5' -> Decimal; None for empty or not a number.

    Both separators present: the last one is decimal. One kind repeated: thousands.
    A single ',' or '.' is decimal (FB exports in EU locale use '12,5').
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    text = str(value).replace(" ", "").replace(" ", "").replace("'", "")
    if not text:
        return None
    if "," in text and "." in text:
        decimal_sep = "," if text.rfind(",") > text.rfind(".") else "."
        thousands_sep = "." if decimal_sep == "," else ","
        text = text.replace(thousands_sep, "").replace(decimal_sep, ".")
    elif text.count(",") > 1 or text.count(".") > 1:
        text = text.replace(",", "").replace(".", "")
    else:
        text = text.replace(",", ".")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def extract_decimal(value: Any) -> Optional[Decimal]:
    """First number inside free text: '233.70 USD' -> 233.70."""
    if value is None or isinstance(value, (bool, int, float, Decimal)):
        return parse_decimal(value)
    match = _NUMBER_CHUNK_RE.search(str(value))
    return parse_decimal(match.group(0).strip()) if match else None


def round_money(value: Any) -> int:
    """Whole units, half up (2.5 -> 3; round() would give 2). Floats go through str to avoid 0.1-style noise."""
    return int(Decimal(str(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))


if __name__ == "__main__":
    assert [round_money(v) for v in (2.5, 3.5, "252.5", 0.4999, -2.5)] == [3, 4, 253, 0, -3]
    cases = {
        "1,234.56": "1234.56", "1 234,56": "1234.56", "1.234,56": "1234.56", "12,5": "12.5",
        "1,234,567": "1234567", "0.7": "0.7", "-5": "-5", "": None, "abc": None,
    }
    for raw, expected in cases.items():
        got = parse_decimal(raw)
        assert (str(got) if got is not None else None) == expected, (raw, got)
    assert extract_decimal("233.70 USD") == Decimal("233.70")
    assert extract_decimal("{conversion.revenue}") is None
    print("ok")
