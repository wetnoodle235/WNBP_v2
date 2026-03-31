# ──────────────────────────────────────────────────────────
# V5.0 Backend — Stripe Integration Service
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import stripe

from .models import (
    create_referral_reward,
    create_subscription,
    get_active_subscription,
    get_user_by_id,
    update_subscription_period,
    update_subscription_status,
    update_user_keys_tier,
    update_user_stripe_id,
    update_user_tier,
)
from .tiers import TIERS, tier_level

logger = logging.getLogger(__name__)


class StripeService:
    """Manages Stripe checkout, webhooks, and subscription lifecycle."""

    def __init__(self) -> None:
        stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
        self.webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET", "")

    # ── Tier × Billing → Stripe price mapping ────────────
    _TIER_PRICE_IDS: dict[str, dict[str, str]] = {
        "starter": {
            "monthly": os.getenv("STRIPE_PRICE_STARTER_MONTHLY", "price_starter_monthly_placeholder"),
            "yearly": os.getenv("STRIPE_PRICE_STARTER_YEARLY", "price_starter_yearly_placeholder"),
        },
        "pro": {
            "monthly": os.getenv("STRIPE_PRICE_PRO_MONTHLY", "price_pro_monthly_placeholder"),
            "yearly": os.getenv("STRIPE_PRICE_PRO_YEARLY", "price_pro_yearly_placeholder"),
        },
        "enterprise": {
            "monthly": os.getenv("STRIPE_PRICE_ENTERPRISE_MONTHLY", "price_enterprise_monthly_placeholder"),
            "yearly": os.getenv("STRIPE_PRICE_ENTERPRISE_YEARLY", "price_enterprise_yearly_placeholder"),
        },
    }

    def _get_price_id(self, tier: str, billing: str = "monthly") -> str:
        tier_prices = self._TIER_PRICE_IDS.get(tier)
        if not tier_prices:
            raise ValueError(f"No Stripe price configured for tier '{tier}'")
        price_id = tier_prices.get(billing, tier_prices.get("monthly"))
        if not price_id:
            raise ValueError(f"No Stripe price for tier '{tier}' billing '{billing}'")
        return price_id

    async def create_checkout_session(
        self,
        user_id: str,
        email: str,
        tier: str,
        billing: str = "monthly",
        success_url: str = "https://sportstock.dev/success",
        cancel_url: str = "https://sportstock.dev/cancel",
        stripe_customer_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a Stripe Checkout Session for a subscription."""
        price_id = self._get_price_id(tier, billing)

        params: dict[str, Any] = {
            "mode": "subscription",
            "line_items": [{"price": price_id, "quantity": 1}],
            "success_url": success_url + "?session_id={CHECKOUT_SESSION_ID}",
            "cancel_url": cancel_url,
            "metadata": {"user_id": user_id, "tier": tier},
        }

        if stripe_customer_id:
            params["customer"] = stripe_customer_id
        else:
            params["customer_email"] = email

        session = stripe.checkout.Session.create(**params)
        return {"checkout_url": session.url, "session_id": session.id}

    async def create_portal_session(
        self,
        stripe_customer_id: str,
        return_url: str = "https://sportstock.dev/account",
    ) -> dict[str, str]:
        """Create a Stripe customer portal session."""
        session = stripe.billing_portal.Session.create(
            customer=stripe_customer_id,
            return_url=return_url,
        )
        return {"portal_url": session.url}

    async def handle_webhook(self, payload: bytes, sig_header: str) -> dict[str, str]:
        """Process Stripe webhook events."""
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, self.webhook_secret
            )
        except (stripe.error.SignatureVerificationError, ValueError) as e:
            logger.warning("Webhook signature verification failed: %s", e)
            raise

        event_type = event["type"]
        data = event["data"]["object"]

        if event_type == "checkout.session.completed":
            await self._on_checkout_completed(data)
        elif event_type == "customer.subscription.updated":
            await self._on_subscription_updated(data)
        elif event_type == "customer.subscription.deleted":
            await self._on_subscription_deleted(data)
        else:
            logger.debug("Unhandled webhook event: %s", event_type)

        return {"status": "ok", "event": event_type}

    async def cancel_subscription(self, stripe_subscription_id: str) -> dict[str, str]:
        """Cancel a Stripe subscription."""
        stripe.Subscription.delete(stripe_subscription_id)
        await update_subscription_status(stripe_subscription_id, "canceled")
        return {"status": "canceled", "subscription_id": stripe_subscription_id}

    async def upgrade_subscription(
        self, stripe_subscription_id: str, new_tier: str
    ) -> dict[str, str]:
        """Upgrade a subscription to a higher tier (prorated)."""
        price_id = self._get_price_id(new_tier)
        sub = stripe.Subscription.retrieve(stripe_subscription_id)
        stripe.Subscription.modify(
            stripe_subscription_id,
            items=[
                {
                    "id": sub["items"]["data"][0]["id"],
                    "price": price_id,
                }
            ],
            proration_behavior="create_prorations",
        )
        return {"status": "upgraded", "new_tier": new_tier}

    async def downgrade_to_free(self, user_id: str) -> None:
        """Cancel subscription and downgrade user's keys to free tier."""
        tier_info = TIERS["free"]
        await update_user_tier(user_id, "free")
        await update_user_keys_tier(
            user_id,
            tier="free",
            sport_access=tier_info["sports"],
            rate_limit=tier_info["rate_limit"],
        )

    # ── Private webhook handlers ─────────────────────────

    async def _on_checkout_completed(self, session: dict) -> None:
        user_id = session.get("metadata", {}).get("user_id")
        tier = session.get("metadata", {}).get("tier", "starter")
        stripe_sub_id = session.get("subscription")
        stripe_customer_id = session.get("customer")

        if not user_id or not stripe_sub_id:
            logger.warning("Checkout session missing user_id or subscription")
            return

        # Save Stripe customer ID
        if stripe_customer_id:
            await update_user_stripe_id(user_id, stripe_customer_id)

        tier_info = TIERS.get(tier, TIERS["starter"])
        sports = tier_info["sports"] if isinstance(tier_info["sports"], list) else ["all"]

        # Cancel lower-tier active subscriptions
        existing_sub = await get_active_subscription(user_id)
        if existing_sub and tier_level(existing_sub["tier"]) < tier_level(tier):
            try:
                if existing_sub.get("stripe_subscription_id"):
                    await self.cancel_subscription(existing_sub["stripe_subscription_id"])
            except Exception as e:
                logger.warning("Could not cancel old subscription: %s", e)

        await create_subscription(
            user_id=user_id,
            stripe_subscription_id=stripe_sub_id,
            tier=tier,
            sports=sports,
        )
        await update_user_tier(user_id, tier)
        await update_user_keys_tier(
            user_id, tier=tier, sport_access=sports, rate_limit=tier_info["rate_limit"]
        )
        logger.info("User %s subscribed to %s tier", user_id, tier)

        # Handle referral rewards
        user = await get_user_by_id(user_id)
        if user and user.referred_by:
            reward = await create_referral_reward(
                referrer_id=user.referred_by,
                referred_user_id=user_id,
                tier_purchased=tier,
            )
            if reward:
                logger.info("Referral reward granted to %s: %s days of %s",
                            user.referred_by, reward["days_granted"], tier)

    async def _on_subscription_updated(self, sub: dict) -> None:
        stripe_sub_id = sub.get("id")
        if not stripe_sub_id:
            return
        start = sub.get("current_period_start")
        end = sub.get("current_period_end")
        if start and end:
            from datetime import datetime, timezone

            start_iso = datetime.fromtimestamp(start, tz=timezone.utc).isoformat()
            end_iso = datetime.fromtimestamp(end, tz=timezone.utc).isoformat()
            await update_subscription_period(stripe_sub_id, start_iso, end_iso)

    async def _on_subscription_deleted(self, sub: dict) -> None:
        stripe_sub_id = sub.get("id")
        if not stripe_sub_id:
            return
        await update_subscription_status(stripe_sub_id, "canceled")

        # Find the user and downgrade to free
        from .database import get_db
        async with get_db() as db:
            cursor = await db.execute(
                "SELECT user_id FROM subscriptions WHERE stripe_subscription_id = ?",
                (stripe_sub_id,),
            )
            row = await cursor.fetchone()
            if row:
                await self.downgrade_to_free(row["user_id"])

        logger.info("Subscription %s canceled via webhook", stripe_sub_id)
