"""
CreatorFlow — routers/videos.py
FastAPI router for video management.

Endpoints:
  POST   /api/videos/upload/{video_id}      — Trigger a YouTube upload (or instant publish)
  GET    /api/videos/{video_id}/status      — Fetch current upload status
  GET    /api/videos/user/{user_id}         — List all videos for a user
  POST   /api/videos/{video_id}/retry       — Retry a failed upload
  DELETE /api/videos/{video_id}             — Remove a video record from Firestore
"""

import logging
from typing import Literal
import asyncio
from datetime import datetime, timedelta, timezone as tz

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from pydantic import BaseModel

from database import get_db
from dependencies import verify_firebase_token
from services.youtube_uploader import YouTubeUploader

logger = logging.getLogger(__name__)
router = APIRouter()

upload_lock = asyncio.Semaphore(1)

# ---------------------------------------------------------------------------
# Pydantic response / request models
# ---------------------------------------------------------------------------

VideoStatus = Literal["scheduled", "uploading", "success", "failed"]


class UploadAcceptedResponse(BaseModel):
    message: str
    videoId: str
    status: str


class VideoStatusResponse(BaseModel):
    videoId: str
    userId: str | None = None
    status: VideoStatus | str
    title: str | None = None
    desc: str | None = None
    driveFileId: str | None = None
    youtubeVideoId: str | None = None
    youtubeUrl: str | None = None
    errorMessage: str | None = None
    createdAt: str | None = None
    uploadStartedAt: str | None = None
    uploadCompletedAt: str | None = None


class VideoListResponse(BaseModel):
    userId: str
    count: int
    videos: list[dict]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize_doc(doc_id: str, data: dict) -> dict:
    """Convert a raw Firestore doc dict into a JSON-serialisable dict."""
    out = dict(data)
    out["videoId"] = doc_id
    # Convert Firestore DatetimeWithNanoseconds / datetime to ISO string
    timestamp_keys = (
        "createdAt", "uploadStartedAt", "uploadCompletedAt", "uploadFailedAt"
    )
    for key in timestamp_keys:
        val = out.get(key)
        if val is not None and hasattr(val, "isoformat"):
            out[key] = val.isoformat()
    return out


# ---------------------------------------------------------------------------
# POST /upload/{video_id}
# ---------------------------------------------------------------------------

@router.post(
    "/upload/{video_id}",
    response_model=UploadAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger a YouTube upload for a scheduled video",
    description=(
        "Queues the video for upload as a background task. "
        "Returns immediately with 202 Accepted. "
        "Poll GET /{video_id}/status to track progress."
    ),
)
async def trigger_upload(
    video_id: str,
    background_tasks: BackgroundTasks,
    uid: str = Depends(verify_firebase_token),
) -> UploadAcceptedResponse:
    db = get_db()
    video_doc = db.collection("videos").document(video_id).get()

    if not video_doc.exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Video '{video_id}' not found.",
        )

    data = video_doc.to_dict() or {}
    if data.get("userId") != uid:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to upload this video.",
        )

    current_status: str = data.get("status", "")

    if current_status == "uploading":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Upload already in progress for this video.",
        )
    uploader = YouTubeUploader(video_id)

    if current_status == "success":
        logger.info("[Router] Video already uploaded. Overriding privacy to PUBLIC — videoId='%s'", video_id)
        # Fire the instant publish API ping in the background!
        background_tasks.add_task(uploader.make_video_public)

        return UploadAcceptedResponse(
            message="Publish override accepted. Making video public.",
            videoId=video_id,
            status="success",
        )

    logger.info("[Router] Manual upload triggered — videoId='%s'", video_id)

    # 👇 Wrap it in the lock for full uploads!
    async def locked_upload():
        async with upload_lock:
            await uploader.execute_upload()

    background_tasks.add_task(locked_upload)

    return UploadAcceptedResponse(
        message="Upload job accepted and queued. Poll /status for updates.",
        videoId=video_id,
        status="uploading",
    )


# ---------------------------------------------------------------------------
# GET /{video_id}/status
# ---------------------------------------------------------------------------

@router.get(
    "/{video_id}/status",
    response_model=VideoStatusResponse,
    summary="Get the current status of a video",
)
async def get_video_status(
    video_id: str,
    uid: str = Depends(verify_firebase_token),
) -> VideoStatusResponse:
    db = get_db()
    doc = db.collection("videos").document(video_id).get()

    if not doc.exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Video '{video_id}' not found.",
        )

    data = _serialize_doc(video_id, doc.to_dict() or {})
    if data.get("userId") != uid:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this video.",
        )

    return VideoStatusResponse(**{k: data.get(k) for k in VideoStatusResponse.model_fields})


# ---------------------------------------------------------------------------
# GET /user/{user_id}
# ---------------------------------------------------------------------------

@router.get(
    "/user/{user_id}",
    response_model=VideoListResponse,
    summary="List all videos for a user",
)
async def list_user_videos(
    user_id: str,
    limit: int = Query(default=50, ge=1, le=200, description="Max videos to return"),
    filter_status: str | None = Query(
        default=None,
        alias="status",
        description="Filter by status: scheduled | uploading | success | failed",
    ),
    uid: str = Depends(verify_firebase_token),
) -> VideoListResponse:
    if uid != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only list your own videos.",
        )
    db = get_db()

    query = db.collection("videos").where("userId", "==", user_id)
    if filter_status:
        query = query.where("status", "==", filter_status)

    # Note: requires a composite Firestore index on (userId, createdAt)
    # Create it at: https://console.firebase.google.com/project/_/firestore/indexes
    docs = query.order_by("createdAt", direction="DESCENDING").limit(limit).stream()

    videos = [_serialize_doc(doc.id, doc.to_dict() or {}) for doc in docs]

    return VideoListResponse(userId=user_id, count=len(videos), videos=videos)


# ---------------------------------------------------------------------------
# POST /{video_id}/retry
# ---------------------------------------------------------------------------

@router.post(
    "/{video_id}/retry",
    response_model=UploadAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Retry a failed upload",
    description="Resets a failed video back to 'scheduled' and queues a new upload.",
)
async def retry_upload(
    video_id: str,
    background_tasks: BackgroundTasks,
    uid: str = Depends(verify_firebase_token),
) -> UploadAcceptedResponse:

    db = get_db()
    video_ref = db.collection("videos").document(video_id)
    doc       = video_ref.get()

    if not doc.exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Video '{video_id}' not found.",
        )

    data = doc.to_dict() or {}
    if data.get("userId") != uid:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to retry this video.",
        )

    current_status: str = data.get("status", "")
    if current_status == "success":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot retry a successfully uploaded video.",
        )
    if current_status == "uploading":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An upload is already in progress.",
        )

    # ── FIX: If the stored publishAt is in the past (or within 30 min), clear it.
    # The original poller scheduled the video for "tomorrow", but if the first
    # upload failed and the user retries the next day, publishAt is now stale.
    # YouTubeUploader._validate_publish_at() also guards against this, but
    # clearing it here keeps Firestore consistent with what YouTube will receive.
    update_fields: dict = {
        "status":          "scheduled",
        "errorMessage":    None,
        "uploadFailedAt":  None,
    }
    raw_publish_at: str | None = data.get("publishAt")
    if raw_publish_at:
        try:
            pub_dt  = datetime.fromisoformat(raw_publish_at.replace("Z", "+00:00"))
            cutoff  = datetime.now(tz.utc) + timedelta(minutes=30)
            if pub_dt <= cutoff:
                update_fields["publishAt"] = None   # treat as immediate upload
                logger.info(
                    "[Router] Cleared stale publishAt '%s' on retry for videoId='%s'",
                    raw_publish_at, video_id,
                )
        except (ValueError, AttributeError):
            update_fields["publishAt"] = None

    video_ref.update(update_fields)

    logger.info("[Router] Retry triggered — videoId='%s'", video_id)
    uploader = YouTubeUploader(video_id)

    async def locked_retry():
        async with upload_lock:
            await uploader.execute_upload()

    background_tasks.add_task(locked_retry)

    return UploadAcceptedResponse(
        message="Retry job accepted and queued.",
        videoId=video_id,
        status="uploading",
    )


# ---------------------------------------------------------------------------
# DELETE /{video_id}
# ---------------------------------------------------------------------------

@router.delete(
    "/{video_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a video record from Firestore",
    description=(
        "Removes the Firestore document. "
        "Does NOT delete the file from Drive or YouTube."
    ),
)
async def delete_video(
    video_id: str,
    uid: str = Depends(verify_firebase_token),
) -> None:
    db = get_db()
    video_ref = db.collection("videos").document(video_id)
    doc = video_ref.get()

    if not doc.exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Video '{video_id}' not found.",
        )

    if (doc.to_dict() or {}).get("userId") != uid:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to delete this video.",
        )

    video_ref.delete()
    logger.info("[Router] Video record deleted — videoId='%s'", video_id)


# ---------------------------------------------------------------------------
# POST /{video_id}/publish
# ---------------------------------------------------------------------------

class PublishRequest(BaseModel):
    userId: str

@router.post(
    "/{video_id}/publish",
    response_model=UploadAcceptedResponse,
    status_code=status.HTTP_200_OK,
    summary="Instantly publish a scheduled video",
)
async def publish_video_now(
    video_id: str,
    body: PublishRequest,
    uid: str = Depends(verify_firebase_token),
):
    db = get_db()
    doc = db.collection("videos").document(video_id).get()

    if not doc.exists:
        raise HTTPException(status_code=404, detail="Video not found.")

    if (doc.to_dict() or {}).get("userId") != uid:
        raise HTTPException(status_code=403, detail="Permission denied.")

    try:
        uploader = YouTubeUploader(video_id)
        await uploader.make_video_public()
        return UploadAcceptedResponse(
            message="Video is now live!",
            videoId=video_id,
            status="success"
        )
    except Exception as e:
        logger.error("[Router] Publish failed for '%s': %s", video_id, e)
        raise HTTPException(status_code=500, detail="Failed to make video public.")
