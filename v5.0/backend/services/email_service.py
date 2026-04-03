from __future__ import annotations

import asyncio
import logging
import os
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class EmailDeliveryResult:
    sent: bool
    reason: str | None = None


class EmailService:
    def __init__(self) -> None:
        self.enabled = os.getenv("EMAIL_ENABLED", "false").lower() in {"1", "true", "yes"}
        self.host = os.getenv("SMTP_HOST", "").strip()
        self.port = int(os.getenv("SMTP_PORT", "587") or "587")
        self.username = os.getenv("SMTP_USERNAME", "").strip()
        self.password = os.getenv("SMTP_PASSWORD", "")
        self.use_tls = os.getenv("SMTP_USE_TLS", "true").lower() in {"1", "true", "yes"}
        self.from_email = os.getenv("EMAIL_FROM", "no-reply@wnbp.local").strip()
        self.app_base_url = (os.getenv("APP_BASE_URL") or os.getenv("NEXTAUTH_URL") or "https://wnbp.vercel.app").rstrip("/")

    def _ready(self) -> tuple[bool, str | None]:
        if not self.enabled:
            return False, "email_disabled"
        if not self.host:
            return False, "smtp_host_missing"
        if not self.from_email:
            return False, "email_from_missing"
        return True, None

    async def send_trial_reminder(self, email: str, display_name: str, trial_ends_at: str) -> EmailDeliveryResult:
        subject = "Your Pro trial ends in 3 days"
        body = (
            f"Hi {display_name},\n\n"
            "Your 7-day Pro trial is ending in 3 days. If you want to keep Pro access, upgrade before your trial ends.\n\n"
            f"Manage your plan: {self.app_base_url}/pricing\n"
            f"Trial end: {trial_ends_at}\n\n"
            "If you do nothing, your account will be downgraded to the free tier automatically.\n"
        )
        return await self._send(email, subject, body)

    async def send_trial_downgraded(self, email: str, display_name: str) -> EmailDeliveryResult:
        subject = "Your account has been downgraded to Free"
        body = (
            f"Hi {display_name},\n\n"
            "Your 7-day Pro trial has ended, and your account has been downgraded to the free tier.\n\n"
            f"Upgrade any time: {self.app_base_url}/pricing\n"
            f"Account: {self.app_base_url}/account\n"
        )
        return await self._send(email, subject, body)

    async def send_renewal_reminder(self, email: str, display_name: str, tier: str, renewal_at: str) -> EmailDeliveryResult:
        subject = f"Your {tier.title()} plan renews in 3 days"
        body = (
            f"Hi {display_name},\n\n"
            f"Your {tier.title()} subscription is scheduled to renew in 3 days. If you stay subscribed, you will be charged automatically on or after {renewal_at}.\n\n"
            f"Manage billing: {self.app_base_url}/account\n"
        )
        return await self._send(email, subject, body)

    async def _send(self, to_email: str, subject: str, body: str) -> EmailDeliveryResult:
        ready, reason = self._ready()
        if not ready:
            logger.info("Skipping email to %s: %s", to_email, reason)
            return EmailDeliveryResult(sent=False, reason=reason)

        message = EmailMessage()
        message["From"] = self.from_email
        message["To"] = to_email
        message["Subject"] = subject
        message["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
        message.set_content(body)

        def _deliver() -> None:
            with smtplib.SMTP(self.host, self.port, timeout=20) as smtp:
                if self.use_tls:
                    smtp.starttls()
                if self.username:
                    smtp.login(self.username, self.password)
                smtp.send_message(message)

        try:
            await asyncio.to_thread(_deliver)
            return EmailDeliveryResult(sent=True)
        except Exception as exc:
            logger.exception("Email delivery failed for %s", to_email)
            return EmailDeliveryResult(sent=False, reason=str(exc))
