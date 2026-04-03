# ──────────────────────────────────────────────────────────
# V5.0 Backend — Stripe Checkout & Webhook Routes
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Request

from api.middleware.auth import get_current_user
from auth.models import get_user_by_id
from auth.stripe_service import StripeService
from auth.tiers import TIERS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stripe", tags=["Stripe"])
_stripe = StripeService()


def _default_app_base(request: Request) -> str:
    configured = (os.getenv("APP_BASE_URL") or os.getenv("NEXTAUTH_URL") or "").strip().rstrip("/")
    if configured:
        return configured
    origin = request.headers.get("origin")
    if origin:
        return origin.rstrip("/")
    return str(request.base_url).rstrip("/")


@router.post(
    "/create-checkout",
    summary="Create Stripe checkout session",
    description="Start a Stripe Checkout flow for a tier/billing combo.",
)
async def create_checkout(
    request: Request,
    user: Annotated[dict[str, Any], Depends(get_current_user)],
    tier: str = Body(..., embed=True),
    billing: str = Body("monthly", embed=True),
    billing_period: str | None = Body(None, embed=True),
    success_url: str | None = Body(None, embed=True),
    cancel_url: str | None = Body(None, embed=True),
):
    billing = billing_period or billing
    if tier not in TIERS or tier == "free":
        raise HTTPException(status_code=400, detail=f"Invalid tier: {tier}")
    if billing not in ("monthly", "yearly"):
        raise HTTPException(status_code=400, detail="Billing must be 'monthly' or 'yearly'")

    app_base = _default_app_base(request)
    resolved_success_url = success_url or f"{app_base}/account?checkout=success"
    resolved_cancel_url = cancel_url or f"{app_base}/pricing?checkout=cancelled"

    db_user = await get_user_by_id(user["sub"])
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    try:
        result = await _stripe.create_checkout_session(
            user_id=db_user.id,
            email=db_user.email,
            tier=tier,
            billing=billing,
            success_url=resolved_success_url,
            cancel_url=resolved_cancel_url,
            stripe_customer_id=db_user.stripe_customer_id,
        )
        return {"success": True, "data": result}
    except Exception as e:
        logger.error("Stripe checkout error: %s", e)
        raise HTTPException(status_code=502, detail="Payment service unavailable")


@router.post(
    "/webhook",
    summary="Stripe webhook handler",
    description="Receives Stripe webhook events. Not meant to be called directly.",
    include_in_schema=False,
)
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("Stripe-Signature", "")
    try:
        result = await _stripe.handle_webhook(payload, sig)
        return result
    except Exception as e:
        logger.error("Webhook processing error: %s", e)
        raise HTTPException(status_code=400, detail="Webhook processing failed")


@router.get(
    "/portal",
    summary="Create Stripe customer portal session",
    description="Returns a URL to the Stripe customer portal for managing subscriptions.",
)
async def create_portal(
    request: Request,
    user: Annotated[dict[str, Any], Depends(get_current_user)],
    return_url: str | None = None,
):
    db_user = await get_user_by_id(user["sub"])
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    if not db_user.stripe_customer_id:
        raise HTTPException(status_code=400, detail="No Stripe customer found. Subscribe first.")

    try:
        result = await _stripe.create_portal_session(
            stripe_customer_id=db_user.stripe_customer_id,
            return_url=return_url or f"{_default_app_base(request)}/account",
        )
        return {"success": True, "data": result}
    except Exception as e:
        logger.error("Stripe portal error: %s", e)
        raise HTTPException(status_code=502, detail="Payment service unavailable")
