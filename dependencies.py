import logging
from fastapi import Header, HTTPException, status
from firebase_admin import auth
from firebase_admin.auth import (
    InvalidIdTokenError,
    ExpiredIdTokenError,
    RevokedIdTokenError,
    CertificateFetchError,
)

logger = logging.getLogger(__name__)

async def verify_firebase_token(authorization: str = Header(...)) -> str:
    """
    Verifies the Firebase Bearer token and returns the authenticated uid.
    - Differentiates token errors from infrastructure errors
    - Checks revoked tokens (covers logged-out / deleted accounts)
    - Logs unexpected errors for debugging without leaking details
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or malformed Authorization header. Expected: Bearer <token>",
        )

    token = authorization.split(" ", 1)[1]

    try:
        # check_revoked=True ensures banned/logged-out users can't reuse old tokens
        decoded = auth.verify_id_token(token, check_revoked=True)
        return decoded["uid"]

    except ExpiredIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired. Please sign in again.",
        )
    except RevokedIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been revoked. Please sign in again.",
        )
    except InvalidIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token. Please sign in again.",
        )
    except CertificateFetchError as e:
        # Firebase SDK couldn't reach Google's cert endpoint — infra issue, not user error
        logger.error("Firebase certificate fetch failed: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable. Try again shortly.",
        )
    except Exception as e:
        # Catch-all for unexpected SDK errors — log fully, return nothing useful to caller
        logger.error("Unexpected token verification error: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication error. Please try again.",
        )
