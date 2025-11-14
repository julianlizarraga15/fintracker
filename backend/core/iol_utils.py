from __future__ import annotations

import unicodedata
from typing import Optional, Any

_CURRENCY_ALIASES = {
    "peso_argentino": "ARS",
    "pesos_argentinos": "ARS",
    "peso": "ARS",
    "pesos": "ARS",
    "ars": "ARS",
    "dolar_estadounidense": "USD",
    "dolares_estadounidenses": "USD",
    "dolar": "USD",
    "dolares": "USD",
    "usd": "USD",
    "usd_oficial": "USD",
    "usd_mep": "USD",
    "usd_ccl": "USD",
    "usd_cable": "USD",
    "dolar_mep": "USD",
    "dolar_ccl": "USD",
    "dolar_bolsa": "USD",
    "dolar_cable": "USD",
    "euro": "EUR",
    "eur": "EUR",
    "real_brasileno": "BRL",
    "brl": "BRL",
}


def _normalize_currency(value: str) -> str:
    if not value:
        return value

    stripped = value.strip()
    if not stripped:
        return stripped

    normalized = unicodedata.normalize("NFKD", stripped)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = (
        normalized.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )

    alias = _CURRENCY_ALIASES.get(slug)
    if alias:
        return alias

    if len(stripped) == 3 and stripped.isalpha():
        return stripped.upper()

    return stripped


def resolve_currency(item: Any) -> Optional[str]:
    """
    Best-effort currency resolver for raw IOL payloads.
    Finds 'moneda', 'divisa', or 'currency' fields on both the
    top-level dict and nested `titulo` entries, returning a normalized code.
    """
    if not isinstance(item, dict):
        return None

    for key in ("moneda", "divisa", "currency"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return _normalize_currency(value)

    titulo = item.get("titulo")
    if isinstance(titulo, dict):
        for key in ("moneda", "divisa", "currency"):
            value = titulo.get(key)
            if isinstance(value, str) and value.strip():
                return _normalize_currency(value)

    return None
