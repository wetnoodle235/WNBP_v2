from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from auth.models import (
    has_subscription_notification,
    list_lifecycle_subscriptions,
    record_subscription_notification,
    update_subscription_status_by_id,
    update_user_keys_tier,
    update_user_tier,
)
from auth.tiers import TIERS
from services.email_service import EmailService

logger = logging.getLogger(__name__)

TRIAL_REMINDER_EVENT = "trial_reminder_3d"
TRIAL_DOWNGRADE_EVENT = "trial_downgraded"
RENEWAL_REMINDER_EVENT = "renewal_reminder_3d"


class SubscriptionLifecycleService:
    def __init__(self, email_service: EmailService | None = None) -> None:
        self.email = email_service or EmailService()

    async def run_cycle(self, now: datetime | None = None) -> dict[str, int]:
        now = now or datetime.now(timezone.utc)
        processed = {"trial_reminders": 0, "trial_downgrades": 0, "renewal_reminders": 0}
        subscriptions = await list_lifecycle_subscriptions(("trial", "active"))

        for sub in subscriptions:
            period_end = _parse_iso(sub.get("current_period_end"))
            if period_end is None:
                continue

            if sub.get("status") == "trial":
                if period_end <= now:
                    if await self._expire_trial(sub):
                        processed["trial_downgrades"] += 1
                elif period_end - now <= timedelta(days=3):
                    if await self._send_trial_reminder(sub):
                        processed["trial_reminders"] += 1
            elif sub.get("status") == "active" and sub.get("source") == "stripe":
                delta = period_end - now
                if timedelta(0) < delta <= timedelta(days=3):
                    if await self._send_renewal_reminder(sub):
                        processed["renewal_reminders"] += 1

        return processed

    async def _send_trial_reminder(self, sub: dict) -> bool:
        period_end = sub.get("current_period_end")
        if await has_subscription_notification(sub["id"], TRIAL_REMINDER_EVENT, period_end):
            return False
        result = await self.email.send_trial_reminder(
            email=sub["email"],
            display_name=sub.get("display_name") or sub["email"],
            trial_ends_at=period_end,
        )
        if not result.sent:
            return False
        await record_subscription_notification(sub["user_id"], sub["id"], TRIAL_REMINDER_EVENT, period_end)
        return True

    async def _expire_trial(self, sub: dict) -> bool:
        await update_subscription_status_by_id(sub["id"], "expired")
        await update_user_tier(sub["user_id"], "free")
        free_tier = TIERS["free"]
        sports = free_tier["sports"] if isinstance(free_tier["sports"], list) else ["all"]
        await update_user_keys_tier(sub["user_id"], "free", sports, free_tier["rate_limit"])

        period_end = sub.get("current_period_end")
        if not await has_subscription_notification(sub["id"], TRIAL_DOWNGRADE_EVENT, period_end):
            result = await self.email.send_trial_downgraded(
                email=sub["email"],
                display_name=sub.get("display_name") or sub["email"],
            )
            if result.sent:
                await record_subscription_notification(sub["user_id"], sub["id"], TRIAL_DOWNGRADE_EVENT, period_end)
        return True

    async def _send_renewal_reminder(self, sub: dict) -> bool:
        period_end = sub.get("current_period_end")
        if await has_subscription_notification(sub["id"], RENEWAL_REMINDER_EVENT, period_end):
            return False
        result = await self.email.send_renewal_reminder(
            email=sub["email"],
            display_name=sub.get("display_name") or sub["email"],
            tier=sub["tier"],
            renewal_at=period_end,
        )
        if not result.sent:
            return False
        await record_subscription_notification(sub["user_id"], sub["id"], RENEWAL_REMINDER_EVENT, period_end)
        return True


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
