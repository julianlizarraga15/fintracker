import logging
from datetime import date, datetime
from typing import Any, List, Optional

import requests

from ..valuation import models as valuation_models

BLUE_DOLLAR_URL = "https://dolarapi.com/v1/dolares/blue"
FX_TIMEOUT_SECONDS = 15
LOG = logging.getLogger(__name__)


def get_fx_rates() -> List[valuation_models.FXRate]:
    """Fetch FX rates (currently the blue dollar) and return validated models."""

    try:
        resp = requests.get(BLUE_DOLLAR_URL, timeout=FX_TIMEOUT_SECONDS)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        LOG.warning("Unable to fetch FX rates from dolarapi: %s", exc)
        return []
    except ValueError:
        LOG.warning("dolarapi returned a non-JSON payload")
        return []

    if not isinstance(payload, dict):
        LOG.warning("dolarapi payload is not an object: %r", payload)
        return []

    asof_dt = date.today()
    updated_at = payload.get("fechaActualizacion") or payload.get("last_update")
    if isinstance(updated_at, str):
        try:
            # Replace trailing Z (if present) to keep fromisoformat happy
            asof_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).date()
        except ValueError:
            LOG.debug("Unable to parse dolarapi updated_at=%s", updated_at)

    def _coerce_float(value: Any) -> Optional[float]:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            cleaned = cleaned.replace(".", "").replace(",", ".") if "," in cleaned and cleaned.count(",") == 1 else cleaned
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    rate_value: Optional[float] = None
    for key in ("venta", "sell", "value", "precio"):
        rate_value = _coerce_float(payload.get(key))
        if rate_value:
            break

    if rate_value is None or rate_value <= 0:
        LOG.warning("dolarapi payload missing usable rate: %s", payload)
        return []

    fx = valuation_models.FXRate(
        asof_dt=asof_dt,
        from_ccy="USD",
        to_ccy="ARS",
        rate=rate_value,
        source="dolarapi_blue_venta",
    )

    return [fx]
