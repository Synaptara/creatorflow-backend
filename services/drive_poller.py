"""
Updrop — services/drive_poller.py
Background Drive folder watcher — fixed concurrent scheduling race condition.
"""

import asyncio
import logging
import os
import re
import time
from typing import Final
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.cloud import firestore as fs
from google.oauth2 import service_account
from googleapiclient.discovery import build

from database import get_db
from services.groq_ai import GroqGenerator

logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE: Final[str] = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "serviceAccountKey.json",
)
DRIVE_SCOPES: Final[list[str]] = ["https://www.googleapis.com/auth/drive.readonly"]

MAX_DRIVE_FILES_PER_POLL: Final[int] = 200
SUPPORTED_EXTENSIONS: Final[set[str]] = {".mp4", ".mov", ".avi", ".webm", ".mkv"}

# 🔥 FIX: Added nextPageToken so Google Drive pagination actually works
DRIVE_FIELDS: Final[str] = "nextPageToken, files(id, name, mimeType, size, createdTime, parents)"
DRIVE_PAGE_SIZE: Final[int] = 100


class DrivePoller:

    def __init__(self) -> None:
        self._db           = get_db()
        self._groq         = GroqGenerator()
        # FIX 1: Removed shared _drive_service instance to prevent SSL multi-threading crashes

    def _build_drive_service(self):
        if not os.path.isfile(SERVICE_ACCOUNT_FILE):
            raise FileNotFoundError(
                f"[DrivePoller] serviceAccountKey.json not found at: {SERVICE_ACCOUNT_FILE}\n"
                "Place the file in the project root and restart the server."
            )
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=DRIVE_SCOPES,
        )
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        logger.info("[DrivePoller] Service account Drive client initialised ✓")
        return service

    # ------------------------------------------------------------------
    # Manual sync
    # ------------------------------------------------------------------

    async def poll_single_user(
        self,
        user_id: str,
        category: str | None = None,
        tags: list[str] | None = None,
    ) -> int:
        tags = tags or []
        logger.info(
            "⚡ Manual Sync triggered for user '%s' | category=%s | tags=%s",
            user_id, category, tags,
        )
        user_doc  = self._db.collection("users").document(user_id).get()
        if not user_doc.exists:
            raise ValueError("User not found in database.")
        folder_id = (user_doc.to_dict() or {}).get("driveFolderId")
        if not folder_id:
            raise ValueError("No Google Drive folder connected.")
        return await self._safe_process_user(user_id, folder_id, category=category, tags=tags)

    # ------------------------------------------------------------------
    # Background scheduler
    # ------------------------------------------------------------------

    async def poll_all_users(self) -> None:
        logger.info("══════ Drive Watcher: Cycle START ══════")

        # 🔥 THE WALLET FIX: Ask Firestore to ONLY return users who have a folder.
        # This prevents 100 empty user profiles from costing you 100 DB reads every cycle.
        from google.cloud.firestore_v1.base_query import FieldFilter
        users_stream = (
            self._db.collection("users")
            .where(filter=FieldFilter("driveFolderId", ">", ""))
            .stream()
        )

        processed = skipped = errors = 0
        tasks = []

        for user_doc in users_stream:
            user_id:   str        = user_doc.id
            folder_id: str | None = (user_doc.to_dict() or {}).get("driveFolderId")

            if not folder_id:
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
            "══════ Drive Watcher: Cycle END — processed=%d  skipped=%d  errors=%d ══════",
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
        try:
            return await self._process_user(user_id, folder_id, category=category, tags=tags or [])
        except Exception as exc:
            logger.error("[Poller] Unhandled error for user '%s': %s", user_id, exc, exc_info=True)
            raise

    async def _process_user(
        self,
        user_id: str,
        folder_id: str,
        category: str | None = None,
        tags: list[str] | None = None,
    ) -> int:
        tags = tags or []
        logger.info("[Poller] Scanning folder '%s' for user '%s'…", folder_id, user_id)

        drive_files = await asyncio.get_running_loop().run_in_executor(
            None, self._list_drive_videos, folder_id
        )
        if not drive_files:
            return 0

        known_ids = self._get_known_drive_ids(user_id)
        new_files = [f for f in drive_files if f["id"] not in known_ids]
        if not new_files:
            return 0

        # FIX 3: Fetch user_doc once and pass the data forward
        user_doc  = self._db.collection("users").document(user_id).get()
        user_data = user_doc.to_dict() or {}
        user_plan = user_data.get("plan", "creator")
        daily_limit = 6 if user_plan == "studio" else 2

        all_vids       = self._db.collection("videos").where("userId", "==", user_id).select(["createdAt"]).stream()
        ingested_today = 0
        today_date     = datetime.now(timezone.utc).date()

        for doc in all_vids:
            dt_raw = (doc.to_dict() or {}).get("createdAt")
            if not dt_raw:
                continue
            try:
                doc_date = dt_raw.date() if hasattr(dt_raw, "date") else \
                    datetime.fromisoformat(dt_raw.replace("Z", "+00:00")).date()
                if doc_date == today_date:
                    ingested_today += 1
            except Exception:
                continue

        remaining_quota = daily_limit - ingested_today
        if remaining_quota <= 0:
            logger.info(
                "[Poller] User '%s' hit daily quota (%d/%d). Skipping %d file(s).",
                user_id, ingested_today, daily_limit, len(new_files),
            )
            return 0

        files_to_process = new_files[:remaining_quota]
        logger.info(
            "[Poller] User '%s' — %d new video(s). Processing %d (quota: %d remaining).",
            user_id, len(new_files), len(files_to_process), remaining_quota,
        )

        count = 0
        for f in files_to_process:
            # Pass user_data to prevent double fetching
            await self._queue_new_video(user_id, f, user_data, category=category, tags=tags)
            count += 1

        return count

    # ------------------------------------------------------------------
    # Drive API
    # ------------------------------------------------------------------

    def _list_drive_videos(self, folder_id: str) -> list[dict]:
        # FIX 1: Instantiate fresh Drive client per thread
        drive_service = self._build_drive_service()

        query = (
            f"('{folder_id}' in parents)"
            f" and trashed=false"
        )
        all_files: list[dict] = []
        page_token: str | None = None

        try:
            while True:
                kwargs: dict = dict(
                    q=query,
                    fields=DRIVE_FIELDS,
                    pageSize=DRIVE_PAGE_SIZE,
                    orderBy="createdTime desc",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                if page_token:
                    kwargs["pageToken"] = page_token

                # FIX 2: Retry logic with exponential backoff for SSL timeouts
                for attempt in range(3):
                    try:
                        response = drive_service.files().list(**kwargs).execute()
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(2 ** attempt)

                all_files.extend(response.get("files", []))

                if len(all_files) >= MAX_DRIVE_FILES_PER_POLL:
                    all_files = all_files[:MAX_DRIVE_FILES_PER_POLL]
                    break

                page_token = response.get("nextPageToken")
                if not page_token:
                    break
        except Exception as exc:
            logger.error("[Poller] Drive API list failed for folder '%s': %s", folder_id, exc)
            raise

        if not all_files:
            logger.info("[Poller] 👁️  Folder is empty or Bot has no permissions.")

        valid_files = []
        for f in all_files:
            name = f.get("name", "Untitled")
            mime = f.get("mimeType", "Unknown")
            ext = Path(name).suffix.lower()

            logger.info("[Poller] 👁️  Found file in Drive: '%s' (MIME: %s)", name, mime)

            if ext in SUPPORTED_EXTENSIONS:
                valid_files.append(f)
            else:
                logger.info("[Poller] 🚫 Ignoring '%s' because it is not an mp4/mov.", name)

        logger.info("[Poller] Drive returned %d VALID video file(s) for folder '%s'.", len(valid_files), folder_id)
        return valid_files

    # ------------------------------------------------------------------
    # Firestore helpers
    # ------------------------------------------------------------------

    def _get_known_drive_ids(self, user_id: str) -> set[str]:
        # FIX 4: Implemented query limitation to 1000 items
        docs = (
            self._db.collection("videos")
            .where("userId", "==", user_id)
            .select(["driveFileId"])
            .limit(1000)
            .stream()
        )
        return {(doc.to_dict() or {}).get("driveFileId") for doc in docs} - {None}

    async def _queue_new_video(
        self,
        user_id: str,
        file_info: dict,
        user_data: dict,
        category: str | None = None,
        tags: list[str] | None = None,
    ) -> None:
        tags       = tags or []
        file_id    = file_info["id"]
        raw_name   = file_info.get("name", "untitled")
        clean_name = self._sanitize_filename(raw_name)

        logger.info(
            "[Poller] Processing '%s' (driveFileId=%s, user=%s)…",
            clean_name, file_id, user_id,
        )

        settings_doc = self._db.collection("settings").document(user_id).get()
        settings     = settings_doc.to_dict() or {}

        # Pulled from user_data passed down
        user_plan = user_data.get("plan", "creator")

        ai_enabled = settings.get("aiGen", True)

        try:
            up_hour, up_min = map(int, settings.get("uploadTime", "20:00").split(":"))
        except ValueError:
            up_hour, up_min = 20, 0

        # Size shield
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
                "userId": user_id, "title": clean_name[:100],
                "desc": "File size limit exceeded.", "driveFileId": file_id,
                "driveFileName": raw_name, "status": "failed",
                "errorMessage": error_msg, "createdAt": fs.SERVER_TIMESTAMP,
            })
            return

        # AI metadata
        title       = clean_name[:100]
        description = f"Automatically detected video: {raw_name}\n\nEdit this description before publishing."

        if ai_enabled:
            try:
                ai_config_doc  = self._db.collection("aiConfig").document(user_id).get()
                user_ai_config = ai_config_doc.to_dict() if ai_config_doc.exists else None
                title, description = await self._groq.generate_metadata(
                    clean_topic=clean_name, category=category, tags=tags, config=user_ai_config,
                )
            except Exception as exc:
                logger.warning("[Poller] Groq failed: %s — using fallback title.", exc)

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

        target_date            = datetime.now(timezone.utc) + timedelta(days=1)
        schedule_start         = target_date
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

        # Timezone offset
        tz_str            = settings.get("timezone", "UTC +00:00")
        tz_offset_minutes = 0
        try:
            # FIX 5: Removed inline import re as _re, referencing top-level re
            tz_match = re.search(r'([+-])(\d{1,2}):(\d{2})', tz_str)
            if tz_match:
                sign = 1 if tz_match.group(1) == '+' else -1
                tz_offset_minutes = sign * (
                    int(tz_match.group(2)) * 60 + int(tz_match.group(3))
                )
        except Exception:
            tz_offset_minutes = 0

        local_naive    = target_date.replace(hour=up_hour, minute=up_min, second=0, microsecond=0, tzinfo=None)
        utc_publish    = (local_naive - timedelta(minutes=tz_offset_minutes)).replace(tzinfo=timezone.utc)
        iso_publish_at = utc_publish.strftime("%Y-%m-%dT%H:%M:%SZ")

        # FIX 6: Write Category and Tags directly to Firestore Document payload
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
            "category":      category or "",
            "tags":          tags or [],
        }
        new_ref = self._db.collection("videos").document()
        new_ref.set(new_doc)
        logger.info("[Poller] Queued — videoId='%s' scheduled for '%s'", new_ref.id, new_doc["date"])

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        name = re.sub(r"\.[^.]+$", "", filename)
        name = re.sub(r"[_\-\.]+", " ", name)
        name = re.sub(r"[^a-zA-Z0-9\s]", "", name)
        name = re.sub(r"\s+", " ", name)
        return name.strip()
