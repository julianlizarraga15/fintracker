from fastapi import APIRouter, Depends, HTTPException
from pydantic import ValidationError

from backend.app.manual_crypto import (
    ManualCryptoHoldingsResponse,
    ManualCryptoHoldingsUpdate,
    load_manual_crypto_holdings,
    save_manual_crypto_holdings,
)
from backend.app.security import require_jwt

router = APIRouter()


@router.get("/manual/crypto-holdings", response_model=ManualCryptoHoldingsResponse)
def get_manual_crypto_holdings(_: dict = Depends(require_jwt)):
    try:
        return load_manual_crypto_holdings()
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.put("/manual/crypto-holdings", response_model=ManualCryptoHoldingsResponse)
def update_manual_crypto_holdings(
    payload: ManualCryptoHoldingsUpdate,
    _: dict = Depends(require_jwt),
):
    try:
        return save_manual_crypto_holdings(payload)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
