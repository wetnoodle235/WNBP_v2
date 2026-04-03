from __future__ import annotations

import hashlib
import io
import importlib
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from config import get_settings

logger = logging.getLogger(__name__)

_CURATED_LEAGUE_BADGES: dict[str, dict[str, str]] = {
    "lol": {"label": "LoL", "background": "#0a42d4", "foreground": "#f5c542"},
    "csgo": {"label": "CS2", "background": "#1f2937", "foreground": "#f59e0b"},
    "dota2": {"label": "DOTA", "background": "#7f1d1d", "foreground": "#f8fafc"},
    "valorant": {"label": "VAL", "background": "#ff4655", "foreground": "#f8fafc"},
}


@dataclass
class MediaTarget:
    sport: str
    entity_type: str
    entity_id: str
    field_name: str
    source_url: str


class MediaMirrorService:
    """Mirrors remote media assets to local PNG files with catalog metadata."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._media_dir: Path = self._settings.media_dir
        self._public_base = self._settings.media_public_base_path.rstrip("/")
        self._catalog_path = self._media_dir / "media_catalog.duckdb"
        self._stale_warning_hours = max(1, int(getattr(self._settings, "media_stale_warning_hours", 48) or 48))
        self._stale_error_hours = max(
            self._stale_warning_hours,
            int(getattr(self._settings, "media_stale_error_hours", 168) or 168),
        )
        self._lock = threading.Lock()
        self._conn = None
        self._ready = False

        self._media_dir.mkdir(parents=True, exist_ok=True)
        self._init_catalog()

    def _init_catalog(self) -> None:
        try:
            duckdb = importlib.import_module("duckdb")
            self._conn = duckdb.connect(str(self._catalog_path))
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS media_assets (
                    media_id VARCHAR,
                    sport VARCHAR NOT NULL,
                    entity_type VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    field_name VARCHAR NOT NULL,
                    source_url VARCHAR NOT NULL,
                    source_etag VARCHAR,
                    source_last_modified VARCHAR,
                    source_content_hash VARCHAR,
                    stored_format VARCHAR,
                    stored_rel_path VARCHAR,
                    width INTEGER,
                    height INTEGER,
                    fetched_at TIMESTAMP,
                    status VARCHAR,
                    error_message VARCHAR,
                    PRIMARY KEY (media_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_media_assets_entity
                ON media_assets (sport, entity_type, entity_id, field_name)
                """
            )
            self._ready = True
        except Exception as exc:
            logger.warning("Media mirror catalog init failed: %s", exc)
            self._ready = False

    @staticmethod
    def _safe_segment(value: str) -> str:
        text = (value or "").strip().lower()
        if not text:
            return "unknown"
        out = []
        for ch in text:
            if ch.isalnum() or ch in {"-", "_"}:
                out.append(ch)
            else:
                out.append("-")
        compact = "".join(out).strip("-")
        while "--" in compact:
            compact = compact.replace("--", "-")
        return compact or "unknown"

    def _media_id(self, target: MediaTarget) -> str:
        return "::".join(
            [
                target.sport.lower(),
                target.entity_type.lower(),
                str(target.entity_id),
                target.field_name.lower(),
            ]
        )

    def _relative_path(self, target: MediaTarget) -> str:
        sport = self._safe_segment(target.sport)
        entity_type = self._safe_segment(target.entity_type)
        entity_id = self._safe_segment(str(target.entity_id))
        field_name = self._safe_segment(target.field_name)
        return f"{sport}/{entity_type}/{entity_id}/{field_name}.png"

    def _local_url(self, rel_path: str) -> str:
        return f"{self._public_base}/{rel_path}"

    def _get_existing(self, target: MediaTarget) -> dict[str, Any] | None:
        if not self._ready or self._conn is None:
            return None
        row = self._conn.execute(
            """
            SELECT media_id, source_url, source_etag, source_last_modified, stored_rel_path, status
            FROM media_assets
            WHERE sport = ? AND entity_type = ? AND entity_id = ? AND field_name = ?
            """,
            [
                target.sport.lower(),
                target.entity_type.lower(),
                str(target.entity_id),
                target.field_name.lower(),
            ],
        ).fetchone()
        if not row:
            return None
        return {
            "media_id": row[0],
            "source_url": row[1],
            "source_etag": row[2],
            "source_last_modified": row[3],
            "stored_rel_path": row[4],
            "status": row[5],
        }

    def _upsert_asset(
        self,
        target: MediaTarget,
        *,
        rel_path: str,
        etag: str | None,
        last_modified: str | None,
        content_hash: str | None,
        width: int | None,
        height: int | None,
        status: str,
        error_message: str | None = None,
    ) -> None:
        if not self._ready or self._conn is None:
            return
        media_id = self._media_id(target)
        now = datetime.now(timezone.utc)
        self._conn.execute(
            """
            INSERT INTO media_assets (
                media_id, sport, entity_type, entity_id, field_name,
                source_url, source_etag, source_last_modified, source_content_hash,
                stored_format, stored_rel_path, width, height,
                fetched_at, status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (media_id) DO UPDATE SET
                source_url = EXCLUDED.source_url,
                source_etag = EXCLUDED.source_etag,
                source_last_modified = EXCLUDED.source_last_modified,
                source_content_hash = EXCLUDED.source_content_hash,
                stored_format = EXCLUDED.stored_format,
                stored_rel_path = EXCLUDED.stored_rel_path,
                width = EXCLUDED.width,
                height = EXCLUDED.height,
                fetched_at = EXCLUDED.fetched_at,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message
            """,
            [
                media_id,
                target.sport.lower(),
                target.entity_type.lower(),
                str(target.entity_id),
                target.field_name.lower(),
                target.source_url,
                etag,
                last_modified,
                content_hash,
                "png",
                rel_path,
                width,
                height,
                now,
                status,
                error_message,
            ],
        )

    @staticmethod
    def _content_hash(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _hex_to_rgba(color: str) -> tuple[int, int, int, int]:
        value = color.strip().lstrip("#")
        if len(value) != 6:
            return (0, 0, 0, 255)
        return tuple(int(value[i:i + 2], 16) for i in (0, 2, 4)) + (255,)

    def _write_png(self, dest: Path, content: bytes) -> tuple[int | None, int | None]:
        from PIL import Image

        dest.parent.mkdir(parents=True, exist_ok=True)
        width: int | None = None
        height: int | None = None

        tmp_path = dest.with_suffix(dest.suffix + ".tmp")
        with Image.open(io.BytesIO(content)) as im:
            width, height = im.size
            normalized = im.convert("RGBA")
            normalized.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(dest)
        return width, height

    def _write_curated_badge(self, dest: Path, sport: str) -> tuple[int, int, str]:
        from PIL import Image, ImageDraw, ImageFont

        spec = _CURATED_LEAGUE_BADGES.get(sport.lower())
        if spec is None:
            raise ValueError(f"No curated badge spec for sport '{sport}'")

        size = 512
        bg = self._hex_to_rgba(spec["background"])
        fg = self._hex_to_rgba(spec["foreground"])

        image = Image.new("RGBA", (size, size), bg)
        draw = ImageDraw.Draw(image)
        draw.rounded_rectangle((20, 20, size - 20, size - 20), radius=84, outline=(255, 255, 255, 80), width=6)

        try:
            font = ImageFont.truetype("DejaVuSans-Bold.ttf", 168)
            sub_font = ImageFont.truetype("DejaVuSans.ttf", 32)
        except Exception:
            font = ImageFont.load_default()
            sub_font = ImageFont.load_default()

        label = spec["label"]
        bbox = draw.textbbox((0, 0), label, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        draw.text(((size - text_w) / 2, (size - text_h) / 2 - 20), label, fill=fg, font=font)

        sublabel = "esports"
        sub_bbox = draw.textbbox((0, 0), sublabel, font=sub_font)
        sub_w = sub_bbox[2] - sub_bbox[0]
        draw.text(((size - sub_w) / 2, size - 110), sublabel, fill=(255, 255, 255, 190), font=sub_font)

        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest.with_suffix(dest.suffix + ".tmp")
        image.save(tmp_path, format="PNG", optimize=True)
        tmp_path.replace(dest)
        return size, size, self._content_hash(dest.read_bytes())

    def resolve_local_url(self, target: MediaTarget) -> str | None:
        if not target.source_url:
            return None
        existing = self._get_existing(target)
        if not existing:
            return None
        rel_path = existing.get("stored_rel_path")
        if not rel_path:
            return None
        local_file = self._media_dir / rel_path
        if not local_file.exists():
            return None
        return self._local_url(rel_path)

    def sync_target(self, target: MediaTarget) -> str | None:
        if not target.source_url:
            return None

        rel_path = self._relative_path(target)
        local_file = self._media_dir / rel_path

        if target.source_url.startswith("curated://league/"):
            with self._lock:
                try:
                    width, height, content_hash = self._write_curated_badge(local_file, target.sport)
                    self._upsert_asset(
                        target,
                        rel_path=rel_path,
                        etag=None,
                        last_modified=None,
                        content_hash=content_hash,
                        width=width,
                        height=height,
                        status="synced",
                        error_message=None,
                    )
                    return self._local_url(rel_path)
                except Exception as exc:
                    logger.warning("Curated media sync failed for %s: %s", target.source_url, exc)
                    self._upsert_asset(
                        target,
                        rel_path=rel_path,
                        etag=None,
                        last_modified=None,
                        content_hash=None,
                        width=None,
                        height=None,
                        status="error",
                        error_message=str(exc),
                    )
                    return None

        with self._lock:
            existing = self._get_existing(target)
            headers: dict[str, str] = {}
            if existing and existing.get("source_etag"):
                headers["If-None-Match"] = str(existing["source_etag"])
            if existing and existing.get("source_last_modified"):
                headers["If-Modified-Since"] = str(existing["source_last_modified"])

            try:
                with httpx.Client(timeout=20.0, follow_redirects=True) as client:
                    resp = client.get(target.source_url, headers=headers)

                if resp.status_code == 304 and local_file.exists():
                    self._upsert_asset(
                        target,
                        rel_path=rel_path,
                        etag=headers.get("If-None-Match"),
                        last_modified=headers.get("If-Modified-Since"),
                        content_hash=None,
                        width=None,
                        height=None,
                        status="synced",
                    )
                    return self._local_url(rel_path)

                resp.raise_for_status()
                content = resp.content
                content_hash = self._content_hash(content)
                width, height = self._write_png(local_file, content)

                self._upsert_asset(
                    target,
                    rel_path=rel_path,
                    etag=resp.headers.get("etag"),
                    last_modified=resp.headers.get("last-modified"),
                    content_hash=content_hash,
                    width=width,
                    height=height,
                    status="synced",
                    error_message=None,
                )
                return self._local_url(rel_path)
            except Exception as exc:
                logger.warning("Media sync failed for %s: %s", target.source_url, exc)
                self._upsert_asset(
                    target,
                    rel_path=rel_path,
                    etag=None,
                    last_modified=None,
                    content_hash=None,
                    width=None,
                    height=None,
                    status="error",
                    error_message=str(exc),
                )
                if local_file.exists():
                    return self._local_url(rel_path)
                return None

    def team_logo_url(
        self,
        *,
        sport: str,
        team_id: str,
        source_url: str | None,
        auto_sync: bool,
    ) -> str | None:
        if not source_url:
            return source_url
        target = MediaTarget(
            sport=sport,
            entity_type="team",
            entity_id=str(team_id),
            field_name="logo_url",
            source_url=source_url,
        )
        local = self.resolve_local_url(target)
        if local:
            return local
        if auto_sync:
            return self.sync_target(target) or source_url
        return source_url

    def player_headshot_url(
        self,
        *,
        sport: str,
        player_id: str,
        source_url: str | None,
        auto_sync: bool,
    ) -> str | None:
        if not source_url:
            return source_url
        target = MediaTarget(
            sport=sport,
            entity_type="player",
            entity_id=str(player_id),
            field_name="headshot_url",
            source_url=source_url,
        )
        local = self.resolve_local_url(target)
        if local:
            return local
        if auto_sync:
            return self.sync_target(target) or source_url
        return source_url

    def league_image_url(
        self,
        *,
        sport: str,
        source_url: str | None,
        auto_sync: bool,
    ) -> str | None:
        if not source_url:
            return source_url
        target = MediaTarget(
            sport=sport,
            entity_type="league",
            entity_id=sport,
            field_name="image_url",
            source_url=source_url,
        )
        local = self.resolve_local_url(target)
        if local:
            return local
        if auto_sync:
            return self.sync_target(target) or source_url
        return source_url

    def stats(self) -> dict[str, Any]:
        if not self._ready or self._conn is None:
            return {
                "catalog_ready": False,
                "catalog_path": str(self._catalog_path),
                "media_dir": str(self._media_dir),
                "media_dir_exists": self._media_dir.exists(),
                "stale_thresholds": {
                    "warning_hours": self._stale_warning_hours,
                    "error_hours": self._stale_error_hours,
                },
                "total_assets": 0,
                "synced_assets": 0,
                "error_assets": 0,
                "stale_assets": 0,
                "latest_fetched_at": None,
                "by_status": {},
                "by_staleness": {
                    "fresh": 0,
                    "warning": 0,
                    "critical": 0,
                    "unknown": 0,
                },
                "by_entity_type": {},
                "by_sport": {},
                "stale_by_sport": {},
            }

        total_assets = int(self._conn.execute("SELECT COUNT(*) FROM media_assets").fetchone()[0] or 0)
        synced_assets = int(
            self._conn.execute("SELECT COUNT(*) FROM media_assets WHERE status = 'synced'").fetchone()[0] or 0
        )
        error_assets = int(
            self._conn.execute("SELECT COUNT(*) FROM media_assets WHERE status = 'error'").fetchone()[0] or 0
        )
        latest_fetched_at = self._conn.execute(
            "SELECT MAX(fetched_at) FROM media_assets"
        ).fetchone()[0]

        by_status_rows = self._conn.execute(
            "SELECT status, COUNT(*) AS c FROM media_assets GROUP BY status ORDER BY c DESC"
        ).fetchall()
        by_entity_rows = self._conn.execute(
            "SELECT entity_type, COUNT(*) AS c FROM media_assets GROUP BY entity_type ORDER BY c DESC"
        ).fetchall()
        by_sport_rows = self._conn.execute(
            "SELECT sport, COUNT(*) AS c FROM media_assets GROUP BY sport ORDER BY c DESC"
        ).fetchall()
        freshness_rows = self._conn.execute(
            "SELECT sport, status, fetched_at FROM media_assets"
        ).fetchall()

        now = datetime.now(timezone.utc)
        by_staleness = {"fresh": 0, "warning": 0, "critical": 0, "unknown": 0}
        stale_by_sport: dict[str, int] = {}

        for sport, status, fetched_at in freshness_rows:
            if status != "synced":
                by_staleness["unknown"] += 1
                continue
            if fetched_at is None:
                by_staleness["unknown"] += 1
                stale_by_sport[str(sport)] = stale_by_sport.get(str(sport), 0) + 1
                continue

            fetched_ts = fetched_at
            if getattr(fetched_ts, "tzinfo", None) is None:
                fetched_ts = fetched_ts.replace(tzinfo=timezone.utc)

            age_hours = (now - fetched_ts).total_seconds() / 3600.0
            if age_hours >= self._stale_error_hours:
                bucket = "critical"
            elif age_hours >= self._stale_warning_hours:
                bucket = "warning"
            else:
                bucket = "fresh"

            by_staleness[bucket] += 1
            if bucket != "fresh":
                stale_by_sport[str(sport)] = stale_by_sport.get(str(sport), 0) + 1

        stale_assets = by_staleness["warning"] + by_staleness["critical"] + by_staleness["unknown"]

        return {
            "catalog_ready": True,
            "catalog_path": str(self._catalog_path),
            "media_dir": str(self._media_dir),
            "media_dir_exists": self._media_dir.exists(),
            "stale_thresholds": {
                "warning_hours": self._stale_warning_hours,
                "error_hours": self._stale_error_hours,
            },
            "total_assets": total_assets,
            "synced_assets": synced_assets,
            "error_assets": error_assets,
            "stale_assets": stale_assets,
            "latest_fetched_at": latest_fetched_at.isoformat() if latest_fetched_at else None,
            "by_status": {str(r[0]): int(r[1]) for r in by_status_rows if r and r[0] is not None},
            "by_staleness": by_staleness,
            "by_entity_type": {str(r[0]): int(r[1]) for r in by_entity_rows if r and r[0] is not None},
            "by_sport": {str(r[0]): int(r[1]) for r in by_sport_rows if r and r[0] is not None},
            "stale_by_sport": dict(sorted(stale_by_sport.items(), key=lambda item: (-item[1], item[0]))),
        }


_service: MediaMirrorService | None = None


def get_media_mirror_service() -> MediaMirrorService:
    global _service
    if _service is None:
        _service = MediaMirrorService()
    return _service
