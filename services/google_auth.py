"""
CreatorFlow — services/google_auth.py
Google OAuth 2.0 credential manager.
"""

import logging
import os
from typing import Final

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.cloud import firestore as fs
from googleapiclient.discovery import Resource, build

from database import get_db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GOOGLE_TOKEN_URI: Final[str] = "https://oauth2.googleapis.com/token"

REQUIRED_SCOPES: Final[list[str]] = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

# ---------------------------------------------------------------------------
# GoogleAuthManager
# ---------------------------------------------------------------------------
class GoogleAuthManager:
    def __init__(self, user_id: str) -> None:
        if not user_id:
            raise ValueError("user_id must be a non-empty string.")
        self.user_id = user_id
        self._db = get_db()

    def get_credentials(self) -> Credentials:
        token_data = self._load_token_data()
        creds = self._build_credentials(token_data)

        if creds.expired:
            creds = self._refresh_and_persist(creds)

        return creds

    def build_drive_service(self) -> Resource:
        return build(
            "drive",
            "v3",
            credentials=self.get_credentials(),
            cache_discovery=False,
        )

    def build_youtube_service(self) -> Resource:
        return build(
            "youtube",
            "v3",
            credentials=self.get_credentials(),
            cache_discovery=False,
        )

    def _load_token_data(self) -> dict:
        user_ref = self._db.collection("users").document(self.user_id)
        user_doc = user_ref.get()

        if not user_doc.exists:
            raise ValueError(f"[Auth] User '{self.user_id}' not found in Firestore.")

        user_data: dict = user_doc.to_dict() or {}

        if not user_data.get("googleConnected"):
            raise ValueError(f"[Auth] User '{self.user_id}' has not connected their Google account.")

        token_data: dict | None = user_data.get("googleTokens")
        if not token_data:
            raise ValueError(f"[Auth] No 'googleTokens' found for user '{self.user_id}'.")

        if not token_data.get("refresh_token"):
            raise ValueError(f"[Auth] Missing refresh_token for user '{self.user_id}'. User must re-authenticate.")

        return token_data

    def _build_credentials(self, token_data: dict) -> Credentials:
        return Credentials(
            token=token_data.get("token"), # 👈 The fix is here!
            refresh_token=token_data.get("refresh_token"),
            token_uri=GOOGLE_TOKEN_URI,
            client_id=os.environ["GOOGLE_CLIENT_ID"],
            client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
            scopes=REQUIRED_SCOPES,
        )

    def _refresh_and_persist(self, creds: Credentials) -> Credentials:
        logger.info("[Auth] Access token expired for user '%s'. Refreshing…", self.user_id)
        try:
            creds.refresh(Request())
        except Exception as exc:
            logger.error("[Auth] Token refresh FAILED for user '%s': %s", self.user_id, exc, exc_info=True)
            raise RuntimeError(f"Token refresh failed for user '{self.user_id}': {exc}") from exc

        self._persist_refreshed_token(creds)
        logger.info("[Auth] Token refreshed and saved for user '%s'.", self.user_id)
        return creds

    def _persist_refreshed_token(self, creds: Credentials) -> None:
        update_payload: dict = {
            "googleTokens.token": creds.token, # 👈 And here!
            "googleTokens.token_refreshed_at": fs.SERVER_TIMESTAMP,
        }
        if creds.expiry:
            update_payload["googleTokens.token_expiry"] = creds.expiry.isoformat()

        try:
            self._db.collection("users").document(self.user_id).update(update_payload)
        except Exception as exc:
            logger.warning("[Auth] Could not persist refreshed token for user '%s': %s", self.user_id, exc)
