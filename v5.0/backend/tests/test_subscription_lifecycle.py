from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from auth import database
from auth.database import close_pool, init_db
from auth.models import (
    create_trial_subscription,
    create_user,
    get_active_subscription,
    get_user_by_id,
    update_user_tier,
)
from auth.subscription_lifecycle import SubscriptionLifecycleService


class StubEmailService:
    def __init__(self) -> None:
        self.trial_reminders: list[str] = []
        self.trial_downgrades: list[str] = []
        self.renewal_reminders: list[str] = []

    async def send_trial_reminder(self, email: str, display_name: str, trial_ends_at: str):
        self.trial_reminders.append(email)
        return type("Result", (), {"sent": True})()

    async def send_trial_downgraded(self, email: str, display_name: str):
        self.trial_downgrades.append(email)
        return type("Result", (), {"sent": True})()

    async def send_renewal_reminder(self, email: str, display_name: str, tier: str, renewal_at: str):
        self.renewal_reminders.append(email)
        return type("Result", (), {"sent": True})()


@pytest.fixture
def isolated_auth_db(tmp_path, monkeypatch):
    asyncio.run(close_pool())
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "users.db")
    asyncio.run(init_db())
    yield
    asyncio.run(close_pool())


def test_trial_reminder_sent_once(isolated_auth_db):
    async def scenario():
        user = await create_user("trial1@example.com", "hash", "Trial User")
        await update_user_tier(user.id, "pro")
        trial = await create_trial_subscription(user.id, "pro", ["all"], duration_days=7)

        email = StubEmailService()
        service = SubscriptionLifecycleService(email_service=email)
        reminder_time = datetime.fromisoformat(trial.current_period_end) - timedelta(days=3)

        stats1 = await service.run_cycle(reminder_time)
        stats2 = await service.run_cycle(reminder_time)

        assert stats1["trial_reminders"] == 1
        assert stats2["trial_reminders"] == 0
        assert email.trial_reminders == ["trial1@example.com"]

    asyncio.run(scenario())


def test_expired_trial_downgrades_to_free(isolated_auth_db):
    async def scenario():
        user = await create_user("trial2@example.com", "hash", "Trial User")
        await update_user_tier(user.id, "pro")
        trial = await create_trial_subscription(user.id, "pro", ["all"], duration_days=1)

        email = StubEmailService()
        service = SubscriptionLifecycleService(email_service=email)
        after_expiry = datetime.fromisoformat(trial.current_period_end) + timedelta(minutes=1)

        stats = await service.run_cycle(after_expiry)
        refreshed_user = await get_user_by_id(user.id)
        active_sub = await get_active_subscription(user.id)

        assert stats["trial_downgrades"] == 1
        assert refreshed_user is not None
        assert refreshed_user.tier == "free"
        assert active_sub is None
        assert email.trial_downgrades == ["trial2@example.com"]

    asyncio.run(scenario())
