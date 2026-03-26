"""
Updrop — services/youtube_uploader.py
YouTube upload pipeline.

Given a Firestore videoId, this class:
  1. Fetches the video document (validates userId, driveFileId, title, desc).
  2. Marks the DB status as 'uploading'.
  3. Streams the file from Google Drive to a temp file under /tmp.
  4. Uploads the temp file to YouTube using the resumable upload API.
  5. On success: updates Firestore status='success', stores YouTube URL, writes audit log.
  6. On failure: updates Firestore status='failed', stores error, writes audit log.
  7. Always deletes the temp file.
"""

import asyncio
import logging
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.cloud import firestore as fs
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build
from services.email_service import EmailDispatcher

from database import get_db
from services.google_auth import GoogleAuthManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOWNLOAD_CHUNK_SIZE: int = 10 * 1024 * 1024   # 10 MB per Drive download chunk
UPLOAD_CHUNK_SIZE: int = 10 * 1024 * 1024     # 10 MB per YouTube resumable chunk
YOUTUBE_CATEGORY_ID: str = "22"               # 22 = People & Blogs (safe default)
YOUTUBE_PRIVACY_STATUS: str = "public"        # Change to "private" for staging
TMP_PREFIX: str = "cf_upload_"

# Minimum minutes a scheduledPublishTime must be in the future for YouTube to accept it.
# YouTube's documented minimum is 5 min; we use 30 for safety.
SCHEDULE_BUFFER_MINUTES: int = 30

# Drive Service Account setup
SERVICE_ACCOUNT_FILE: str = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "serviceAccountKey.json",
)
DRIVE_SCOPES: list[str] = ["https://www.googleapis.com/auth/drive.readonly"]


# ---------------------------------------------------------------------------
# YouTubeUploader
# ---------------------------------------------------------------------------
class YouTubeUploader:
    """
    Orchestrates the full Drive → YouTube upload pipeline for one video.

    Designed to be instantiated fresh per upload request; holds no
    cross-request state.
    """

    def __init__(self, video_id: str) -> None:
        if not video_id:
            raise ValueError("video_id must not be empty.")
        self.video_id = video_id
        self._db = get_db()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def execute_upload(self) -> None:
        """
        Run the complete upload pipeline with file size limits and pre-bake scheduling.
        """
        video_ref = self._db.collection("videos").document(self.video_id)
        video_data, user_id, drive_file_id, title, description = \
            self._validate_and_fetch(video_ref)

        # Grab the scheduled publish time set by the drive poller.
        # ── FIX: Validate publishAt before sending to YouTube.
        # If the scheduled time is in the past or within SCHEDULE_BUFFER_MINUTES,
        # clear it so the video uploads as a normal (immediate) upload instead of
        # a scheduled one. This was the root cause of drive-polled videos always
        # failing on first attempt — the poller sometimes set a time too close to
        # now, YouTube rejected the scheduledPublishTime, the except block caught it
        # and stamped status='failed' even though the upload itself would have worked.
        raw_publish_at: str | None = video_data.get("publishAt")
        publish_at: str | None = self._validate_publish_at(raw_publish_at)

        if raw_publish_at and not publish_at:
            logger.info(
                "[Uploader] publishAt '%s' is too soon or in the past — "
                "uploading immediately instead (videoId='%s')",
                raw_publish_at, self.video_id,
            )

        current_status: str = video_data.get("status", "")
        if current_status in ("uploading", "success"):
            logger.warning(
                "[Uploader] Video '%s' is already '%s' — skipping.",
                self.video_id, current_status,
            )
            return

        video_ref.update({
            "status": "uploading",
            "uploadStartedAt": fs.SERVER_TIMESTAMP,
            "errorMessage": fs.DELETE_FIELD,
        })
        logger.info(
            "[Uploader] Pipeline started — videoId='%s'  user='%s'",
            self.video_id, user_id,
        )

        tmp_path: Path | None = None
        settings_data: dict = {}  # initialised early so except block can always read it
        yt_url: str = ""
        yt_video_id: str = ""

        try:
            # 1. YOUTUBE SERVICE (uses user's OAuth tokens)
            auth = GoogleAuthManager(user_id)
            youtube_service = await asyncio.get_running_loop().run_in_executor(
                None, auth.build_youtube_service
            )

            # 2. DRIVE SERVICE (uses bot service account — no user OAuth needed)
            def _build_sa_drive():
                if not os.path.isfile(SERVICE_ACCOUNT_FILE):
                    raise FileNotFoundError(f"Missing {SERVICE_ACCOUNT_FILE}")
                creds = service_account.Credentials.from_service_account_file(
                    SERVICE_ACCOUNT_FILE, scopes=DRIVE_SCOPES
                )
                return build("drive", "v3", credentials=creds, cache_discovery=False)

            drive_service = await asyncio.get_running_loop().run_in_executor(
                None, _build_sa_drive
            )

            # 3. Fetch user settings + plan
            settings_doc = self._db.collection("settings").document(user_id).get()
            settings_data = settings_doc.to_dict() or {}

            user_privacy = settings_data.get("privacy", "private")
            yt_category  = settings_data.get("ytCategory", "22")

            user_doc  = self._db.collection("users").document(user_id).get()
            user_plan = (user_doc.to_dict() or {}).get("plan", "creator")

            # 4. Enforce daily upload quota
            daily_limit    = 6 if user_plan == "studio" else 2
            today_date     = datetime.now(timezone.utc).date()
            ingested_today = 0

            all_vids = (
                self._db.collection("videos")
                .where("userId", "==", user_id)
                .where("status", "==", "success")
                .select(["uploadCompletedAt"])
                .stream()
            )
            for doc in all_vids:
                dt_raw = (doc.to_dict() or {}).get("uploadCompletedAt")
                if not dt_raw:
                    continue
                try:
                    if hasattr(dt_raw, "date"):
                        doc_date = dt_raw.date()
                    elif isinstance(dt_raw, str):
                        doc_date = datetime.fromisoformat(
                            dt_raw.replace("Z", "+00:00")
                        ).date()
                    else:
                        continue
                    if doc_date == today_date:
                        ingested_today += 1
                except Exception:
                    continue

            if ingested_today >= daily_limit:
                raise RuntimeError(
                    f"Daily upload limit reached ({ingested_today}/{daily_limit}). "
                    "Please try again tomorrow or upgrade your plan."
                )

            # 5. Enforce file size limit (using bot's Drive access)
            file_meta = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: drive_service.files()
                .get(fileId=drive_file_id, fields="size")
                .execute(),
            )
            file_size_bytes = int(file_meta.get("size", 0))
            max_bytes = 1_000_000_000 if user_plan == "studio" else 500_000_000

            if file_size_bytes > max_bytes:
                max_mb    = max_bytes // 1_000_000
                actual_mb = file_size_bytes // 1_000_000
                raise RuntimeError(
                    f"File too large. Your {user_plan} plan allows up to {max_mb}MB "
                    f"(File is {actual_mb}MB). Please compress and try again."
                )

            # 6. Download from Drive (bot service account)
            tmp_path = await self._download_from_drive(drive_service, drive_file_id)

            # 7. Upload to YouTube (user's OAuth)
            yt_video_id = await self._upload_to_youtube(
                youtube_service, tmp_path, title, description,
                user_privacy, publish_at, yt_category,
            )

            # 8. Persist success to Firestore
            yt_url = f"https://youtu.be/{yt_video_id}"
            video_ref.update({
                "status":            "success",
                "youtubeVideoId":    yt_video_id,
                "youtubeUrl":        yt_url,
                "uploadCompletedAt": fs.SERVER_TIMESTAMP,
            })
            self._write_audit_log(
                user_id=user_id, status="success",
                msg=f"Upload complete. URL: {yt_url}",
            )
            logger.info(
                "[Uploader] ✓ SUCCESS — videoId='%s'  youtubeUrl='%s'",
                self.video_id, yt_url,
            )

            # ── FIX: Success email is now in its OWN try/except so that an SMTP
            # failure can NEVER overwrite status='success' with status='failed'.
            # Previously this email call was inside the main try block; if it threw,
            # the outer except would catch it and stamp the video as 'failed' even
            # though the upload had already succeeded on YouTube.
            try:
                if settings_data.get("notifSuccess", True):
                    EmailDispatcher().send_notification(
                        user_id=user_id,
                        subject="✅ Video Uploaded Successfully!",
                        body_text=(
                            f"Great news!\n\n"
                            f"Your video '{title}' has been successfully uploaded to YouTube"
                            f" and scheduled.\n\nWatch it here: {yt_url}\n\n"
                            f"- The Updrop Team"
                        ),
                    )
            except Exception as email_exc:
                # Log but DO NOT re-raise — the upload succeeded; email is best-effort.
                logger.warning(
                    "[Uploader] Success notification email failed "
                    "(upload still marked OK) — videoId='%s': %s",
                    self.video_id, email_exc,
                )

        except Exception as exc:
            raw_error = str(exc)
            logger.error(
                "[Uploader] ✗ FAILED — videoId='%s': %s",
                self.video_id, raw_error, exc_info=True,
            )

            video_ref.update({
                "status":          "failed",
                "errorMessage":    raw_error,
                "uploadFailedAt":  fs.SERVER_TIMESTAMP,
            })
            self._write_audit_log(user_id=user_id, status="failed", msg=raw_error)

            # ── Failure email is also isolated so an SMTP error never masks the
            # real upload error already stored above.
            try:
                if settings_data.get("notifFail", True):
                    human_error = "An unexpected server error occurred while processing your video."

                    if "uploadLimitExceeded" in raw_error:
                        human_error = (
                            "Your connected YouTube channel has reached its daily upload limit. "
                            "YouTube restricts how many videos can be uploaded per day. "
                            "Please try again tomorrow."
                        )
                    elif "File too large" in raw_error:
                        human_error = (
                            "The video file size exceeds the maximum limit allowed by your "
                            "current Updrop plan."
                        )
                    elif "invalid_grant" in raw_error or "Token has been expired" in raw_error:
                        human_error = (
                            "Your Google account connection has expired or been revoked. "
                            "Please visit your Updrop Settings to reconnect your YouTube channel."
                        )

                    email_body = (
                        f"Hello,\n\n"
                        f"We encountered an issue while attempting to publish your video, '{title}'.\n\n"
                        f"Reason for failure:\n{human_error}\n\n"
                        f"You can retry this upload at any time from your Updrop Dashboard.\n\n"
                        f"Best regards,\nThe Updrop Team"
                    )
                    EmailDispatcher().send_notification(
                        user_id=user_id,
                        subject="Action Required: Video Upload Failed",
                        body_text=email_body,
                    )
            except Exception as email_exc:
                logger.warning(
                    "[Uploader] Failure notification email could not be sent "
                    "— videoId='%s': %s",
                    self.video_id, email_exc,
                )

        finally:
            if tmp_path and tmp_path.exists():
                try:
                    import time
                    import gc
                    gc.collect()
                    time.sleep(0.5)
                    tmp_path.unlink(missing_ok=True)
                except OSError as e:
                    logger.warning("[Uploader] Could not delete temp file: %s", e)

    # ------------------------------------------------------------------
    # Instant Publish Override
    # ------------------------------------------------------------------

    async def make_video_public(self) -> None:
        """
        Instant Publish Override: modifies an existing YouTube video's privacy to Public.
        Does NOT re-upload the file.
        """
        video_ref = self._db.collection("videos").document(self.video_id)
        video_data, user_id, _, _, _ = self._validate_and_fetch(video_ref)

        yt_video_id: str = video_data.get("youtubeVideoId", "")
        if not yt_video_id:
            logger.error(
                "[Uploader] Cannot make public: No YouTube ID found for video '%s'",
                self.video_id,
            )
            raise ValueError("No YouTube Video ID found.")

        logger.info(
            "[Uploader] Executing Instant Publish for YouTube ID '%s'...",
            yt_video_id,
        )

        try:
            auth = GoogleAuthManager(user_id)
            youtube_service = await asyncio.get_running_loop().run_in_executor(
                None, auth.build_youtube_service
            )

            def _update_blocking() -> None:
                # 1. Fetch the CURRENT video details from YouTube to avoid missing fields
                video_response = youtube_service.videos().list(
                    part="snippet,status",
                    id=yt_video_id
                ).execute()

                if not video_response.get("items"):
                    raise ValueError(f"Video {yt_video_id} not found on YouTube.")

                yt_video = video_response["items"][0]

                # 2. Modify the privacy and forcefully REMOVE the scheduled publish timer
                yt_video["status"]["privacyStatus"] = "public"
                if "publishAt" in yt_video["status"]:
                    del yt_video["status"]["publishAt"]

                # 3. Send the modified video back to YouTube
                youtube_service.videos().update(
                    part="snippet,status",
                    body=yt_video
                ).execute()

            await asyncio.get_running_loop().run_in_executor(None, _update_blocking)

            # 4. Update our own database
            video_ref.update({
                "publishAt": fs.DELETE_FIELD,
                "date": "Live Now",
            })

            logger.info(
                "[Uploader] ✓ INSTANT PUBLISH SUCCESS — Video '%s' is now LIVE!",
                yt_video_id,
            )

        except Exception as exc:
            logger.error(
                "[Uploader] ✗ FAILED to update privacy for '%s': %s",
                self.video_id, exc, exc_info=True,
            )
            # 🔥 THE FIX: If YouTube fails, actually throw the error so the frontend knows!
            raise RuntimeError(f"YouTube API Error: {exc}") from exc

    # ------------------------------------------------------------------
    # publishAt validation helper
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_publish_at(publish_at: str | None) -> str | None:
        """
        Return `publish_at` unchanged if it is at least SCHEDULE_BUFFER_MINUTES
        in the future; otherwise return None so the upload proceeds immediately.

        YouTube rejects a scheduledPublishTime that is in the past or too close
        to the current time (minimum ~5 min; we use 30 min for safety).
        """
        if not publish_at:
            return None
        try:
            pub_dt      = datetime.fromisoformat(publish_at.replace("Z", "+00:00"))
            cutoff      = datetime.now(timezone.utc) + timedelta(minutes=SCHEDULE_BUFFER_MINUTES)
            return publish_at if pub_dt > cutoff else None
        except (ValueError, AttributeError):
            logger.warning(
                "[Uploader] Could not parse publishAt '%s' — treating as immediate.",
                publish_at,
            )
            return None

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_and_fetch(self, video_ref) -> tuple[dict, str, str, str, str]:
        """
        Fetch and validate the video document from Firestore.

        Returns:
            (video_data, user_id, drive_file_id, title, description)
        """
        video_doc = video_ref.get()
        if not video_doc.exists:
            raise ValueError(
                f"Video document '{self.video_id}' not found in Firestore."
            )

        data: dict        = video_doc.to_dict() or {}
        user_id:       str = data.get("userId", "").strip()
        drive_file_id: str = data.get("driveFileId", "").strip()
        title:         str = data.get("title", "").strip() or "Untitled Video"
        description:   str = data.get("desc", "").strip() or "Uploaded via Updrop."

        if not user_id:
            raise ValueError(f"Video '{self.video_id}' is missing 'userId'.")
        if not drive_file_id:
            raise ValueError(f"Video '{self.video_id}' is missing 'driveFileId'.")

        return data, user_id, drive_file_id, title, description

    # ------------------------------------------------------------------
    # Drive download
    # ------------------------------------------------------------------

    async def _download_from_drive(self, drive_service, file_id: str) -> Path:
        """Stream a file from Google Drive to a local /tmp file."""
        logger.info(
            "[Uploader] Downloading Drive file '%s' for video '%s'…",
            file_id, self.video_id,
        )

        file_meta: dict = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: drive_service.files()
            .get(fileId=file_id, fields="name,mimeType,size")
            .execute(),
        )
        original_name: str  = file_meta.get("name", "video.mp4")
        suffix: str         = Path(original_name).suffix or ".mp4"
        file_size_bytes: int = int(file_meta.get("size", 0))

        if file_size_bytes > 0:
            import shutil
            tmp_dir = tempfile.gettempdir()
            free_bytes     = shutil.disk_usage(tmp_dir).free
            required_bytes = int(file_size_bytes * 1.1)
            if free_bytes < required_bytes:
                raise RuntimeError(
                    f"Insufficient disk space in temp directory. "
                    f"Need {required_bytes / 1e6:.0f} MB, "
                    f"only {free_bytes / 1e6:.0f} MB free."
                )

        tmp_fd   = tempfile.NamedTemporaryFile(
            suffix=suffix, delete=False,
            prefix=f"{TMP_PREFIX}{self.video_id}_",
        )
        tmp_path = Path(tmp_fd.name)
        tmp_fd.close()

        def _download_blocking() -> None:
            request = drive_service.files().get_media(fileId=file_id)
            with open(tmp_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request, chunksize=DOWNLOAD_CHUNK_SIZE)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(
                            "[Uploader] Drive download %d%% — videoId='%s'",
                            int(status.progress() * 100), self.video_id,
                        )

        await asyncio.get_running_loop().run_in_executor(None, _download_blocking)

        downloaded_mb = tmp_path.stat().st_size / (1024 * 1024)
        expected_mb   = file_size_bytes / (1024 * 1024) if file_size_bytes else "?"
        logger.info(
            "[Uploader] Download complete — %.1f MB (expected ~%s MB) → %s",
            downloaded_mb, expected_mb, tmp_path,
        )
        return tmp_path

    # ------------------------------------------------------------------
    # YouTube upload
    # ------------------------------------------------------------------

    async def _upload_to_youtube(
        self,
        youtube_service,
        video_path: Path,
        title: str,
        description: str,
        privacy_status: str,
        publish_at: str | None,
        category_id: str,
    ) -> str:
        """Upload a local video file to YouTube using the resumable upload API."""
        logger.info(
            "[Uploader] Uploading to YouTube — title='%s'  privacy='%s'  publishAt='%s'",
            title, privacy_status, publish_at,
        )

        # YouTube requires privacyStatus='private' when scheduledPublishTime is set.
        yt_privacy = "private" if publish_at or privacy_status == "draft" else privacy_status

        status_body: dict = {
            "privacyStatus": yt_privacy,
            "selfDeclaredMadeForKids": False,
            "embeddable": True,
        }
        if publish_at:
            status_body["publishAt"] = publish_at

        body: dict = {
            "snippet": {
                "title":           title[:100],
                "description":     description,
                "tags":            self._extract_tags(title, description),
                "categoryId":      category_id,
                "defaultLanguage": "en",
            },
            "status": status_body,
        }

        def _upload_blocking() -> str:
            media = MediaFileUpload(
                str(video_path), mimetype="video/*",
                resumable=True, chunksize=UPLOAD_CHUNK_SIZE,
            )
            insert_req = youtube_service.videos().insert(
                part=",".join(body.keys()), body=body, media_body=media
            )
            response = None
            while response is None:
                status, response = insert_req.next_chunk()
                if status:
                    logger.debug(
                        "[Uploader] YouTube upload %d%%",
                        int(status.progress() * 100),
                    )
            return response.get("id", "")

        yt_video_id: str = await asyncio.get_running_loop().run_in_executor(
            None, _upload_blocking
        )
        if not yt_video_id:
            raise RuntimeError(
                "YouTube API did not return a video ID after upload completed."
            )
        return yt_video_id

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------

    def _write_audit_log(self, user_id: str, status: str, msg: str) -> None:
        """Write an entry to the Firestore 'logs' collection (fire-and-forget)."""
        try:
            self._db.collection("logs").add({
                "userId":    user_id,
                "videoId":   self.video_id,
                "status":    status,
                "msg":       msg,
                "timestamp": fs.SERVER_TIMESTAMP,
            })
        except Exception as exc:
            logger.warning(
                "[Uploader] Could not write audit log for video '%s': %s",
                self.video_id, exc,
            )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_tags(title: str, description: str) -> list[str]:
        """Derive simple keyword tags from the title and description."""
        combined = f"{title} {description}"
        words    = re.findall(r"\b[a-zA-Z]{4,}\b", combined)
        seen: set[str] = set()
        tags: list[str] = []
        for word in words:
            lower = word.lower()
            if lower not in seen:
                seen.add(lower)
                tags.append(lower)
            if len(tags) >= 15:
                break
        return tags
