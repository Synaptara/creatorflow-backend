"""
CreatorFlow — services/youtube_uploader.py
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
import re
import tempfile
from pathlib import Path

from google.cloud import firestore as fs
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
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

        # Grab the scheduled date if the user set one in the UI
        publish_at = video_data.get("publishAt")

        current_status: str = video_data.get("status", "")
        if current_status in ("uploading", "success"):
            logger.warning("[Uploader] Video '%s' is already '%s' — skipping.", self.video_id, current_status)
            return

        video_ref.update({
            "status": "uploading",
            "uploadStartedAt": fs.SERVER_TIMESTAMP,
            "errorMessage": fs.DELETE_FIELD,
        })
        logger.info("[Uploader] Pipeline started — videoId='%s'  user='%s'", self.video_id, user_id)

        tmp_path: Path | None = None
        settings_data: dict = {}  # initialised early so except block can always read it safely
        try:
            auth = GoogleAuthManager(user_id)
            drive_service = await asyncio.get_running_loop().run_in_executor(None, auth.build_drive_service)
            youtube_service = await asyncio.get_running_loop().run_in_executor(None, auth.build_youtube_service)

            # Fetch settings
            settings_doc = self._db.collection("settings").document(user_id).get()
            settings_data = settings_doc.to_dict() or {}

            user_privacy = settings_data.get("privacy", "private")
            yt_category = settings_data.get("ytCategory", "22") # 👈 NEW: Grab the YouTube category (Default: 22)

            # 👇 FIX: Fetch user plan from 'users' collection
            user_doc = self._db.collection("users").document(user_id).get()
            user_plan = (user_doc.to_dict() or {}).get("plan", "creator")

            # The QUOTA: Enforce Daily Upload limits
            daily_limit = 5 if user_plan == "studio" else 2
            from datetime import datetime, timezone
            today_date = datetime.now(timezone.utc).date()
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
                        doc_date = datetime.fromisoformat(dt_raw.replace("Z", "+00:00")).date()
                    else:
                        continue
                    if doc_date == today_date:
                        ingested_today += 1
                except Exception:
                    continue
                    
            if ingested_today >= daily_limit:
                raise RuntimeError(
                    f"Daily upload limit reached ({ingested_today}/{daily_limit}). "
                    f"Please try again tomorrow or upgrade your plan."
                )

            # THE SHIELD: Enforce File Size Limits
            file_meta = await asyncio.get_event_loop().run_in_executor(
                None, lambda: drive_service.files().get(fileId=drive_file_id, fields="size").execute()
            )
            file_size_bytes = int(file_meta.get("size", 0))
            max_bytes = 1_000_000_000 if user_plan == "studio" else 500_000_000

            if file_size_bytes > max_bytes:
                max_mb = max_bytes // 1_000_000
                actual_mb = file_size_bytes // 1_000_000
                raise RuntimeError(f"File too large. Your {user_plan} plan allows up to {max_mb}MB (File is {actual_mb}MB). Please compress and try again.")

            # Step 1 — Download from Drive
            tmp_path = await self._download_from_drive(drive_service, drive_file_id)

            # Step 2 — Upload to YouTube (Pass both privacy AND the schedule date!)
            yt_video_id = await self._upload_to_youtube(
                youtube_service, tmp_path, title, description, user_privacy, publish_at, yt_category
            )

            # Step 3 — Persist success
            yt_url = f"https://youtu.be/{yt_video_id}"
            video_ref.update({
                "status": "success",
                "youtubeVideoId": yt_video_id,
                "youtubeUrl": yt_url,
                "uploadCompletedAt": fs.SERVER_TIMESTAMP,
            })
            self._write_audit_log(user_id=user_id, status="success", msg=f"Upload complete. URL: {yt_url}")
            logger.info("[Uploader] ✓ SUCCESS — videoId='%s'  youtubeUrl='%s'", self.video_id, yt_url)

            # 👇 NEW: Send Success Email if user has it enabled!
            if settings_data.get("notifSuccess", True):
                EmailDispatcher().send_notification(
                    user_id=user_id,
                    subject="✅ Video Uploaded Successfully!",
                    body_text=f"Great news!\n\nYour video '{title}' has been successfully uploaded to YouTube and scheduled.\n\nWatch it here: {yt_url}\n\n- The CreatorFlow Team"
                )

        except Exception as exc:
            raw_error = str(exc)
            logger.error("[Uploader] ✗ FAILED — videoId='%s': %s", self.video_id, raw_error, exc_info=True)

            # 1. Save the raw error to the database for debugging
            video_ref.update({
                "status": "failed",
                "errorMessage": raw_error,
                "uploadFailedAt": fs.SERVER_TIMESTAMP,
            })
            self._write_audit_log(user_id=user_id, status="failed", msg=raw_error)

            # 2. Send the formal email
            if settings_data.get("notifFail", True):
                # Translate terminal errors into human-readable text
                human_error = "An unexpected server error occurred while processing your video."

                if "uploadLimitExceeded" in raw_error:
                    human_error = "Your connected YouTube channel has reached its daily upload limit. YouTube restricts how many videos can be uploaded per day. Please try again tomorrow."
                elif "File too large" in raw_error:
                    human_error = "The video file size exceeds the maximum limit allowed by your current CreatorFlow plan."
                elif "invalid_grant" in raw_error or "Token has been expired" in raw_error:
                    human_error = "Your Google Workspace connection has expired or been revoked. Please visit your CreatorFlow Settings to reconnect your account."

                email_body = (
                    f"Hello,\n\n"
                    f"We encountered an issue while attempting to publish your video, '{title}'.\n\n"
                    f"Reason for failure:\n{human_error}\n\n"
                    f"You can retry this upload at any time from your CreatorFlow Dashboard.\n\n"
                    f"Best regards,\n"
                    f"The CreatorFlow Team"
                )

                EmailDispatcher().send_notification(
                    user_id=user_id,
                    subject="Action Required: Video Upload Failed",
                    body_text=email_body
                )

        finally:
            if tmp_path and tmp_path.exists():
                try:
                    import time
                    import gc
                    gc.collect()      # 👈 Force Python to release the file from memory
                    time.sleep(0.5)   # 👈 Give Windows a half-second to unlock the file
                    tmp_path.unlink(missing_ok=True)
                except OSError as e:
                    logger.warning("[Uploader] Could not delete temp file: %s", e)

    # ------------------------------------------------------------------
    # Instant Publish Override
    # ------------------------------------------------------------------

    async def make_video_public(self) -> None:
        """
        Instant Publish Override: Modifies an existing YouTube video's privacy to Public.
        This does NOT upload the file again, saving massive server resources.
        """
        video_ref = self._db.collection("videos").document(self.video_id)
        video_data, user_id, _, _, _ = self._validate_and_fetch(video_ref)

        yt_video_id: str = video_data.get("youtubeVideoId", "")
        if not yt_video_id:
            logger.error("[Uploader] Cannot make public: No YouTube ID found for video '%s'", self.video_id)
            return

        logger.info("[Uploader] Executing Instant Publish for YouTube ID '%s'...", yt_video_id)

        try:
            auth = GoogleAuthManager(user_id)
            youtube_service = await asyncio.get_running_loop().run_in_executor(None, auth.build_youtube_service)

            # We only need to send the 'status' part to update privacy!
            body = {
                "id": yt_video_id,
                "status": {
                    "privacyStatus": "public",
                    "embeddable": True,
                    "selfDeclaredMadeForKids": False,
                }
            }

            def _update_blocking() -> None:
                youtube_service.videos().update(part="status", body=body).execute()

            await asyncio.get_event_loop().run_in_executor(None, _update_blocking)

            # Clear publishAt in Firestore so the dashboard shows it as live
            video_ref.update({
                "publishAt": fs.DELETE_FIELD,
                "date": "Live Now",
            })

            logger.info("[Uploader] ✓ INSTANT PUBLISH SUCCESS — Video '%s' is now LIVE!", yt_video_id)

        except Exception as exc:
            logger.error("[Uploader] ✗ FAILED to update privacy for '%s': %s", self.video_id, exc, exc_info=True)


    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_and_fetch(
        self, video_ref
    ) -> tuple[dict, str, str, str, str]:
        """
        Fetch and validate the video document from Firestore.

        Returns:
            (video_data, user_id, drive_file_id, title, description)

        Raises:
            ValueError: If the document is missing or has incomplete data.
        """
        video_doc = video_ref.get()
        if not video_doc.exists:
            raise ValueError(
                f"Video document '{self.video_id}' not found in Firestore."
            )

        data: dict = video_doc.to_dict() or {}

        user_id: str = data.get("userId", "").strip()
        drive_file_id: str = data.get("driveFileId", "").strip()
        title: str = data.get("title", "").strip()
        description: str = data.get("desc", "").strip()

        if not user_id:
            raise ValueError(f"Video '{self.video_id}' is missing 'userId'.")
        if not drive_file_id:
            raise ValueError(f"Video '{self.video_id}' is missing 'driveFileId'.")
        if not title:
            title = "Untitled Video"
        if not description:
            description = "Uploaded via CreatorFlow."

        return data, user_id, drive_file_id, title, description

    # ------------------------------------------------------------------
    # Drive download
    # ------------------------------------------------------------------

    async def _download_from_drive(
        self, drive_service, file_id: str
    ) -> Path:
        """
        Stream a file from Google Drive to a local /tmp file.

        Returns:
            Path to the downloaded temp file.
        """
        logger.info(
            "[Uploader] Downloading Drive file '%s' for video '%s'…",
            file_id, self.video_id,
        )

        # Resolve filename / extension from Drive metadata
        file_meta: dict = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: drive_service.files()
            .get(fileId=file_id, fields="name,mimeType,size")
            .execute(),
        )
        original_name: str = file_meta.get("name", "video.mp4")
        suffix: str = Path(original_name).suffix or ".mp4"
        file_size_bytes: int = int(file_meta.get("size", 0))

        # Disk space guard — ensure temp directory has enough room before writing
        if file_size_bytes > 0:
            import shutil
            import tempfile
            tmp_dir = tempfile.gettempdir()  # works on Windows, Linux, and Mac
            free_bytes = shutil.disk_usage(tmp_dir).free
            required_bytes = int(file_size_bytes * 1.1)  # 10% buffer
            if free_bytes < required_bytes:
                raise RuntimeError(
                    f"Insufficient disk space in temp directory. "
                    f"Need {required_bytes / 1e6:.0f} MB, "
                    f"only {free_bytes / 1e6:.0f} MB free."
                )

        # Create a named temp file so we can pass its path to the YouTube SDK
        tmp_fd = tempfile.NamedTemporaryFile(
            suffix=suffix,
            delete=False,
            prefix=f"{TMP_PREFIX}{self.video_id}_",
        )
        tmp_path = Path(tmp_fd.name)
        tmp_fd.close()  # close FD; we'll open it ourselves for writing

        def _download_blocking() -> None:
            request = drive_service.files().get_media(fileId=file_id)
            with open(tmp_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request, chunksize=DOWNLOAD_CHUNK_SIZE)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        pct = int(status.progress() * 100)
                        logger.debug(
                            "[Uploader] Drive download %d%% — videoId='%s'",
                            pct, self.video_id,
                        )

        await asyncio.get_running_loop().run_in_executor(None, _download_blocking)

        downloaded_mb = tmp_path.stat().st_size / (1024 * 1024)
        expected_mb = file_size_bytes / (1024 * 1024) if file_size_bytes else "?"
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
        category_id: str, # 👈 NEW: Accept the category
    ) -> str:
        """Upload a local video file to YouTube using the resumable upload API."""
        logger.info("[Uploader] Uploading to YouTube — title='%s'  privacy='%s'  publishAt='%s'", title, privacy_status, publish_at)

        # THE PRE-BAKE LOGIC: If a schedule date exists, YouTube forces it to be 'private' until that date.
        yt_privacy = "private" if publish_at or privacy_status == "draft" else privacy_status

        status_body = {
            "privacyStatus": yt_privacy,
            "selfDeclaredMadeForKids": False,
            "embeddable": True,
        }

        # Apply the schedule date if the user set one
        if publish_at:
            status_body["publishAt"] = publish_at

        body: dict = {
            "snippet": {
                "title": title[:100],
                "description": description,
                "tags": self._extract_tags(title, description),
                "categoryId": category_id, # 👈 NEW: Inject the user's custom category!
                "defaultLanguage": "en",
            },
            "status": status_body,
        }

        def _upload_blocking() -> str:
            media = MediaFileUpload(str(video_path), mimetype="video/*", resumable=True, chunksize=UPLOAD_CHUNK_SIZE)
            insert_req = youtube_service.videos().insert(part=",".join(body.keys()), body=body, media_body=media)
            response = None
            while response is None:
                status, response = insert_req.next_chunk()
                if status:
                    logger.debug("[Uploader] YouTube upload %d%%", int(status.progress() * 100))
            return response.get("id", "")

        yt_video_id: str = await asyncio.get_running_loop().run_in_executor(None, _upload_blocking)
        if not yt_video_id:
            raise RuntimeError("YouTube API did not return a video ID after upload completed.")

        return yt_video_id

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------

    def _write_audit_log(self, user_id: str, status: str, msg: str) -> None:
        """Write an entry to the Firestore 'logs' collection (fire-and-forget)."""
        try:
            self._db.collection("logs").add({
                "userId": user_id,
                "videoId": self.video_id,
                "status": status,
                "msg": msg,
                "timestamp": fs.SERVER_TIMESTAMP,
            })
        except Exception as exc:
            # Never let a logging failure mask the real error
            logger.warning(
                "[Uploader] Could not write audit log for video '%s': %s",
                self.video_id, exc,
            )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_tags(title: str, description: str) -> list[str]:
        """
        Derive simple keyword tags from the title and description.
        YouTube allows up to 500 characters of tags total; we keep it modest.
        """
        combined = f"{title} {description}"
        words = re.findall(r"\b[a-zA-Z]{4,}\b", combined)
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
