"""
CreatorFlow — services/drive_poller.py
Background Drive folder watcher.

Every 5 minutes (scheduled via APScheduler in main.py), poll_all_users():
  1. Iterates every Firestore user with googleConnected=True and a driveFolderId.
  2. Authenticates with that user's refreshed OAuth credentials.
  3. Lists .mp4 / .mov files in their Drive folder.
  4. Cross-references against the 'videos' collection to find NEW files.
  5. For each new file, calls GroqGenerator to create AI metadata and saves
     a new 'videos/{id}' document with status='scheduled'.

category + tags are forwarded from the manual sync endpoint into
generate_metadata() so the AI produces niche-targeted output.
The background scheduler always calls without tags — safe by default.
"""

import asyncio
import logging
import re
from typing import Final
from datetime import datetime, timedelta, timezone, date as date_type

from google.cloud import firestore as fs

from database import get_db
from services.google_auth import GoogleAuthManager
from services.groq_ai import GroqGenerator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SUPPORTED_MIME_TYPES: Final[list[str]] = [
    "video/mp4",
    "video/quicktime",
    "video/x-msvideo",
    "video/webm",
    "video/x-matroska",
]

MAX_DRIVE_FILES_PER_POLL: Final[int] = 200
SUPPORTED_EXTENSIONS: Final[set[str]] = {".mp4", ".mov", ".avi", ".webm", ".mkv"}
DRIVE_FIELDS: Final[str] = "files(id, name, mimeType, size, createdTime, parents)"
DRIVE_PAGE_SIZE: Final[int] = 100


# ---------------------------------------------------------------------------
# DrivePoller
# ---------------------------------------------------------------------------
class DrivePoller:
    """
    Scans every connected user's Google Drive folder for new video files
    and queues them for upload via Groq-generated metadata.
    """

    def __init__(self) -> None:
        self._db   = get_db()
        self._groq = GroqGenerator()

    # ------------------------------------------------------------------
    # Manual sync entry point (called from /api/drive/sync endpoint)
    # ------------------------------------------------------------------

    async def poll_single_user(
        self,
        user_id: str,
        category: str | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """
        Manually triggered sync for a single user.
        Accepts optional category + tags to guide Groq AI metadata generation.
        """
        tags = tags or []

        logger.info(
            "⚡ Manual Sync triggered for user '%s' | category=%s | tags=%s",
            user_id, category, tags
        )

        user_doc = self._db.collection("users").document(user_id).get()
        if not user_doc.exists:
            raise ValueError("User not found in database.")

        user_data = user_doc.to_dict() or {}
        folder_id = user_data.get("driveFolderId")

        if not folder_id:
            logger.warning(
                "[Poller] User '%s' requested sync but has no Drive folder connected.",
                user_id
            )
            raise ValueError("No Google Drive folder connected.")

        return await self._safe_process_user(
            user_id,
            folder_id,
            category=category,
            tags=tags,
        )

    # ------------------------------------------------------------------
    # Background scheduler entry point (no tags — filename only)
    # ------------------------------------------------------------------

    async def poll_all_users(self) -> None:
        """
        Called by APScheduler every 5 minutes.
        No category/tags — background job uses filename-only Groq generation.
        """
        logger.info("══════ Drive Watcher: Cycle START ══════")

        users_stream = (
            self._db.collection("users")
            .where("googleConnected", "==", True)
            .stream()
        )

        processed = skipped = errors = 0
        tasks = []

        for user_doc in users_stream:
            user_id: str        = user_doc.id
            user_data: dict     = user_doc.to_dict() or {}
            folder_id: str | None = user_data.get("driveFolderId")

            if not folder_id:
                logger.debug("[Poller] User '%s' has no driveFolderId — skipping.", user_id)
                skipped += 1
                continue

            tasks.append(self._safe_process_user(user_id, folder_id))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                errors += 1
            else:
                processed += 1

        logger.info(
            "══════ Drive Watcher: Cycle END — "
            "processed=%d  skipped=%d  errors=%d ══════",
            processed, skipped, errors,
        )

    # ------------------------------------------------------------------
    # Per-user processing
    # ------------------------------------------------------------------

    async def _safe_process_user(
        self,
        user_id: str,
        folder_id: str,
        category: str | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """Wrapper that catches and logs all exceptions for a single user."""
        try:
            return await self._process_user(
                user_id,
                folder_id,
                category=category,
                tags=tags or [],
            )
        except Exception as exc:
            logger.error(
                "[Poller] Unhandled error for user '%s': %s",
                user_id, exc, exc_info=True,
            )
            raise

    async def _process_user(
        self,
        user_id: str,
        folder_id: str,
        category: str | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """Core logic: find new Drive files → generate metadata → save to DB."""
        tags = tags or []

        logger.info("[Poller] Scanning folder '%s' for user '%s'…", folder_id, user_id)

        auth          = GoogleAuthManager(user_id)
        drive_service = await asyncio.get_running_loop().run_in_executor(
            None, auth.build_drive_service
        )
        drive_files   = await asyncio.get_running_loop().run_in_executor(
            None, self._list_drive_videos, drive_service, folder_id
        )

        if not drive_files:
            logger.info("[Poller] User '%s' — no video files in Drive folder.", user_id)
            return 0

        known_ids = self._get_known_drive_ids(user_id)
        new_files = [f for f in drive_files if f["id"] not in known_ids]

        if not new_files:
            logger.info("[Poller] User '%s' — no new videos detected.", user_id)
            return 0

        # ------------------------------------------------------------------
        # Intake Throttle — SECURITY FIX: handle both Timestamp and ISO string
        # ------------------------------------------------------------------
        user_doc  = self._db.collection("users").document(user_id).get()
        user_plan = (user_doc.to_dict() or {}).get("plan", "creator")
        daily_limit = 5 if user_plan == "studio" else 2

        all_vids       = (
            self._db.collection("videos")
            .where("userId", "==", user_id)
            .select(["createdAt"])
            .stream()
        )
        ingested_today = 0
        today_date     = datetime.now(timezone.utc).date()

        for doc in all_vids:
            dt_raw = (doc.to_dict() or {}).get("createdAt")
            if not dt_raw:
                continue
            try:
                # Firestore Timestamp object
                if hasattr(dt_raw, "date"):
                    doc_date = dt_raw.date()
                # ISO string fallback (e.g. from manual addDoc in frontend)
                elif isinstance(dt_raw, str):
                    doc_date = datetime.fromisoformat(
                        dt_raw.replace("Z", "+00:00")
                    ).date()
                else:
                    continue

                if doc_date == today_date:
                    ingested_today += 1
            except Exception:
                continue  # Malformed date — skip, don't crash the quota check

        remaining_quota = daily_limit - ingested_today

        if remaining_quota <= 0:
            logger.info(
                "[Poller] User '%s' hit daily quota (%d/%d). Ignoring %d file(s) until tomorrow.",
                user_id, ingested_today, daily_limit, len(new_files)
            )
            return 0

        files_to_process = new_files[:remaining_quota]

        logger.info(
            "[Poller] User '%s' — %d new video(s) found. Processing %d (quota: %d remaining).",
            user_id, len(new_files), len(files_to_process), remaining_quota,
        )

        await asyncio.gather(
            *[
                self._queue_new_video(user_id, f, category=category, tags=tags)
                for f in files_to_process
            ]
        )

        return len(files_to_process)

    # ------------------------------------------------------------------
    # Drive API helpers
    # ------------------------------------------------------------------

    def _list_drive_videos(self, drive_service, folder_id: str) -> list[dict]:
        """
        Query Drive for video files in `folder_id`.
        Handles pagination so large folders aren't truncated.
        """
        mime_query = " or ".join(
            f"mimeType='{mime}'" for mime in SUPPORTED_MIME_TYPES
        )
        query = (
            f"('{folder_id}' in parents)"
            f" and ({mime_query})"
            f" and trashed=false"
        )

        all_files: list[dict] = []
        page_token: str | None = None

        try:
            while True:
                request_kwargs: dict = dict(
                    q=query,
                    fields=DRIVE_FIELDS,
                    pageSize=DRIVE_PAGE_SIZE,
                    orderBy="createdTime desc",
                )
                if page_token:
                    request_kwargs["pageToken"] = page_token

                response = drive_service.files().list(**request_kwargs).execute()
                all_files.extend(response.get("files", []))

                if len(all_files) >= MAX_DRIVE_FILES_PER_POLL:
                    logger.warning(
                        "[Poller] Drive file cap (%d) reached for folder '%s'. Truncating.",
                        MAX_DRIVE_FILES_PER_POLL, folder_id,
                    )
                    all_files = all_files[:MAX_DRIVE_FILES_PER_POLL]
                    break

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

        except Exception as exc:
            logger.error("[Poller] Drive API list failed for folder '%s': %s", folder_id, exc)
            raise

        from pathlib import Path
        all_files = [
            f for f in all_files
            if Path(f.get("name", "")).suffix.lower() in SUPPORTED_EXTENSIONS
        ]

        logger.debug("[Poller] Drive returned %d file(s) for folder '%s'.", len(all_files), folder_id)
        return all_files

    # ------------------------------------------------------------------
    # Firestore helpers
    # ------------------------------------------------------------------

    def _get_known_drive_ids(self, user_id: str) -> set[str]:
        """Return driveFileIds already recorded in Firestore for this user."""
        docs = (
            self._db.collection("videos")
            .where("userId", "==", user_id)
            .select(["driveFileId"])
            .stream()
        )
        ids: set[str] = set()
        for doc in docs:
            fid = (doc.to_dict() or {}).get("driveFileId")
            if fid:
                ids.add(fid)
        return ids

    async def _queue_new_video(
        self,
        user_id: str,
        file_info: dict,
        category: str | None = None,
        tags: list[str] | None = None,
    ) -> None:
        """
        Enforce file size limits, generate AI metadata with aiConfig,
        apply waterfall calendar logic, and save scheduled video to Firestore.
        """
        tags      = tags or []
        file_id   = file_info["id"]
        raw_name  = file_info.get("name", "untitled")
        clean_name = self._sanitize_filename(raw_name)

        logger.info(
            "[Poller] Processing '%s' (driveFileId=%s, user=%s, category=%s, tags=%s)…",
            clean_name, file_id, user_id, category, tags
        )

        # ------------------------------------------------------------------
        # Step 1 — Plan & Settings
        # ------------------------------------------------------------------
        settings_doc = self._db.collection("settings").document(user_id).get()
        settings     = settings_doc.to_dict() or {}

        user_doc  = self._db.collection("users").document(user_id).get()
        user_plan = (user_doc.to_dict() or {}).get("plan", "creator")

        ai_enabled = settings.get("aiGen", True)

        upload_time_str = settings.get("uploadTime", "20:00")
        try:
            up_hour, up_min = map(int, upload_time_str.split(":"))
        except ValueError:
            up_hour, up_min = 20, 0

        # ------------------------------------------------------------------
        # Step 2 — Size Shield
        # ------------------------------------------------------------------
        file_size_bytes = int(file_info.get("size", 0))
        max_bytes = 1_000_000_000 if user_plan == "studio" else 500_000_000

        if file_size_bytes > max_bytes:
            max_mb    = max_bytes // 1_000_000
            actual_mb = file_size_bytes // 1_000_000
            error_msg = (
                f"File exceeds {max_mb}MB plan limit "
                f"(File is {actual_mb}MB). Upgrade to Studio to upload larger files."
            )
            logger.warning("[Poller] Size Shield blocked '%s': %s", clean_name, error_msg)
            self._db.collection("videos").document().set({
                "userId":        user_id,
                "title":         clean_name[:100],
                "desc":          "File size limit exceeded.",
                "driveFileId":   file_id,
                "driveFileName": raw_name,
                "status":        "failed",
                "errorMessage":  error_msg,
                "createdAt":     fs.SERVER_TIMESTAMP,
            })
            return

        # ------------------------------------------------------------------
        # Step 3 — AI Metadata Generation (with aiConfig + category/tags)
        # ------------------------------------------------------------------
        title       = clean_name[:100]
        description = (
            f"Automatically detected video: {raw_name}\n\n"
            "Edit this description before publishing."
        )

        if ai_enabled:
            logger.info(
                "[Poller] Generating metadata via Groq (category=%s, tags=%s)…",
                category, tags
            )
            try:
                # Load user's saved AI config from the aiConfig collection
                ai_config_doc  = self._db.collection("aiConfig").document(user_id).get()
                user_ai_config = ai_config_doc.to_dict() if ai_config_doc.exists else None

                title, description = await self._groq.generate_metadata(
                    clean_topic=clean_name,
                    category=category,
                    tags=tags,
                    config=user_ai_config,
                )
            except Exception as exc:
                logger.warning("[Poller] Groq failed: %s — using fallback title.", exc)

        # ------------------------------------------------------------------
        # Step 4 — Waterfall Calendar Logic
        # ------------------------------------------------------------------
        existing_vids = (
            self._db.collection("videos")
            .where("userId", "==", user_id)
            .where("status", "in", ["scheduled", "uploading", "success"])
            .select(["date"])
            .stream()
        )

        date_counts: dict[str, int] = {}
        for v_doc in existing_vids:
            v_date = (v_doc.to_dict() or {}).get("date")
            if v_date:
                date_counts[v_date] = date_counts.get(v_date, 0) + 1

        target_date        = datetime.now(timezone.utc) + timedelta(days=1)
        schedule_start     = target_date
        MAX_SCHEDULE_DAYS_AHEAD = 90

        while (target_date - schedule_start).days < MAX_SCHEDULE_DAYS_AHEAD:
            date_str = target_date.strftime("%Y-%m-%d")
            if date_counts.get(date_str, 0) < 1:
                break
            target_date += timedelta(days=1)
        else:
            logger.warning(
                "[Poller] User '%s' has no free slot in %d days. Skipping '%s'.",
                user_id, MAX_SCHEDULE_DAYS_AHEAD, clean_name,
            )
            return

        # Parse timezone offset
        tz_str            = settings.get("timezone", "UTC +00:00")
        tz_offset_minutes = 0
        try:
            import re as _re
            tz_match = _re.search(r'([+-])(\d{1,2}):(\d{2})', tz_str)
            if tz_match:
                sign = 1 if tz_match.group(1) == '+' else -1
                tz_offset_minutes = sign * (
                    int(tz_match.group(2)) * 60 + int(tz_match.group(3))
                )
        except Exception:
            tz_offset_minutes = 0

        local_naive    = target_date.replace(
            hour=up_hour, minute=up_min, second=0, microsecond=0, tzinfo=None
        )
        utc_publish    = (local_naive - timedelta(minutes=tz_offset_minutes)).replace(
            tzinfo=timezone.utc
        )
        iso_publish_at = utc_publish.strftime("%Y-%m-%dT%H:%M:%SZ")

        # ------------------------------------------------------------------
        # Save to Firestore
        # ------------------------------------------------------------------
        new_doc: dict = {
            "userId":        user_id,
            "title":         title,
            "desc":          description,
            "driveFileId":   file_id,
            "driveFileName": raw_name,
            "status":        "scheduled",
            "publishAt":     iso_publish_at,
            "date":          target_date.strftime("%Y-%m-%d"),
            "createdAt":     fs.SERVER_TIMESTAMP,
        }

        new_ref = self._db.collection("videos").document()
        new_ref.set(new_doc)

        logger.info(
            "[Poller] Queued — videoId='%s' scheduled for '%s'",
            new_ref.id, new_doc["date"]
        )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        name = re.sub(r"\.[^.]+$", "", filename)
        name = re.sub(r"[_\-\.]+", " ", name)
        name = re.sub(r"[^a-zA-Z0-9\s]", "", name)
        name = re.sub(r"\s+", " ", name)
        return name.strip()
