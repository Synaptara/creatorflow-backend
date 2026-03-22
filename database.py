import logging
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore import Client

logger = logging.getLogger(__name__)

# Module-level cache — set once, reused forever
_db: Client | None = None

def get_db() -> Client:
    """Return the Firestore client, initialising Firebase Admin SDK on first call."""
    global _db
    if _db is None:
        if not firebase_admin._apps:
            import os
            key_path = os.environ.get("FIREBASE_KEY_PATH", "serviceAccountKey.json")
            if not os.path.exists(key_path):
                raise FileNotFoundError(
                    f"Firebase service account key not found at: '{key_path}'. "
                    f"Set the FIREBASE_KEY_PATH environment variable or place "
                    f"serviceAccountKey.json in the project root."
                )
            cred = credentials.Certificate(key_path)
            firebase_admin.initialize_app(cred)
        _db = firestore.client()
        logger.info("Firebase Admin SDK initialised successfully.")
    return _db
