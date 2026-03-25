import os
import json
import time
import logging
import razorpay
from contextlib import asynccontextmanager
from datetime import datetime, timezone, date as date_type
from fastapi import FastAPI, HTTPException, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv
from google_auth_oauthlib.flow import Flow
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from database import get_db
from routers import videos
from services.drive_poller import DrivePoller
from dependencies import verify_firebase_token

# ── Only allow insecure transport in local dev ──
if os.getenv("ENVIRONMENT") == "development":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone="UTC")

from google.cloud.firestore_v1.base_query import FieldFilter

# ── RATE LIMITER ──
limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 CreatorFlow Backend Starting Up...")

    poller = DrivePoller()
    scheduler.add_job(
        poller.poll_all_users,
        trigger=IntervalTrigger(seconds=30),
        id="drive_poll_job",
        name="Drive Folder Watcher",
        replace_existing=True,
        max_instances=1,
    )

    async def process_priority_queue():
        db = get_db()
        scheduled_docs = list(
            db.collection("videos")
            .where(filter=FieldFilter("status", "==", "scheduled"))
            .order_by("createdAt")
            .stream()
        )

        if not scheduled_docs:
            return

        user_plans_cache = {}
        videos_to_process = []

        for doc in scheduled_docs:
            data    = doc.to_dict() or {}
            user_id = data.get("userId")

            if user_id not in user_plans_cache:
                settings_doc = db.collection("settings").document(user_id).get()
                user_plans_cache[user_id] = (settings_doc.to_dict() or {}).get("plan", "creator")

            plan           = user_plans_cache[user_id]
            priority_score = 1 if plan == "studio" else 2

            videos_to_process.append({
                "doc_id":   doc.id,
                "priority": priority_score,
                "plan":     plan
            })

        videos_to_process.sort(key=lambda x: x["priority"])

        from services.youtube_uploader import YouTubeUploader
        for v in videos_to_process:
            logger.info("🚦 Priority Queue: Uploading video '%s' (Tier: %s)", v["doc_id"], v["plan"].upper())
            uploader = YouTubeUploader(v["doc_id"])
            await uploader.execute_upload()

    scheduler.add_job(
        process_priority_queue,
        trigger=IntervalTrigger(minutes=2),
        id="priority_queue_job",
        name="Smart Upload Queue",
        replace_existing=True,
        max_instances=1,
    )

    scheduler.start()
    logger.info("✅ Scheduler started → Drive Watcher & Smart Queue running.")

    yield

    scheduler.shutdown(wait=False)
    logger.info("🛑 CreatorFlow Backend shut down cleanly.")


app = FastAPI(title="CreatorFlow API", lifespan=lifespan)

# ── RATE LIMITER SETUP ──
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS: Read from env, refuse to start if not set ──
_raw_origins  = os.getenv("ALLOWED_ORIGINS", "")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]
if not ALLOWED_ORIGINS:
    raise RuntimeError(
        "ALLOWED_ORIGINS environment variable is not set. "
        "Set it to your frontend URL(s) before starting. "
        "Example: ALLOWED_ORIGINS=https://yourdomain.com"
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(videos.router, prefix="/api/videos", tags=["Videos"])

# ── RAZORPAY SETUP ──
KEY_ID         = os.getenv("RAZORPAY_KEY_ID")
KEY_SECRET     = os.getenv("RAZORPAY_KEY_SECRET")
WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET")
rzp_client     = razorpay.Client(auth=(KEY_ID, KEY_SECRET))

# ── GOOGLE OAUTH SETUP ──
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/youtube.upload',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email'
]

REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8000/api/auth/google/callback")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

PLANS = {
    "creator": 88500,
    "studio":  295000
}

# ── DATA MODELS ──

class OrderRequest(BaseModel):
    plan_id: str = Field(..., max_length=50)
    user_id: str = Field(..., max_length=128)

class VerifyPaymentRequest(BaseModel):
    razorpay_payment_id: str = Field(..., max_length=100)
    razorpay_order_id:   str = Field(..., max_length=100)
    razorpay_signature:  str = Field(..., max_length=200)
    plan_id:             str = Field(..., max_length=50)
    user_id:             str = Field(..., max_length=128)

class FolderRequest(BaseModel):
    folder_id: str = Field(..., min_length=1, max_length=200)

class RevokeRequest(BaseModel):
    user_id: str = Field(..., max_length=128)

class SyncRequest(BaseModel):
    category: str | None = Field(None, max_length=50)
    tags:     list[str]  = Field(default_factory=list)

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v):
        sanitized = [str(tag)[:60] for tag in v]
        return sanitized[:10]

    @field_validator("category")
    @classmethod
    def validate_category(cls, v):
        if v is None:
            return v
        allowed = {
            "gaming", "tutorial", "vlog", "shorts",
            "podcast", "review", "education", "finance",
            "fitness", "tech", "cooking", "other"
        }
        if v not in allowed:
            raise ValueError(f"category must be one of {allowed}")
        return v

class AIConfigPayload(BaseModel):
    aiEnabled:         bool = True
    channelNiche:      str  = Field("", max_length=50)
    tone:              str  = Field("casual", max_length=50)
    titleStyle:        str  = Field("curiosity", max_length=50)
    targetAudience:    str  = Field("", max_length=200)
    alwaysInclude:     str  = Field("", max_length=300)
    alwaysAvoid:       str  = Field("", max_length=300)
    customInstruction: str  = Field("", max_length=500)
    descriptionCTA:    str  = Field("", max_length=200)
    includeHashtags:   bool = True
    includeEmojis:     bool = False

    @field_validator("tone")
    @classmethod
    def validate_tone(cls, v):
        allowed = {"professional", "casual", "hype", "educational", "funny", "minimal"}
        if v not in allowed:
            raise ValueError(f"tone must be one of {allowed}")
        return v

    @field_validator("titleStyle")
    @classmethod
    def validate_title_style(cls, v):
        allowed = {"curiosity", "listicle", "howto", "story", "direct"}
        if v not in allowed:
            raise ValueError(f"titleStyle must be one of {allowed}")
        return v

class PreviewRequest(BaseModel):
    topic:  str            = Field(..., min_length=1, max_length=200)
    config: AIConfigPayload


# ══════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════

@app.get("/")
def read_root():
    return {"status": "online", "message": "CreatorFlow API is running."}


# 1. RAZORPAY: Create Order
@app.post("/api/payment/create-order")
@limiter.limit("10/minute")
async def create_order(request: Request, body: OrderRequest):
    if body.plan_id not in PLANS:
        raise HTTPException(status_code=400, detail="Invalid plan selected")

    amount     = PLANS[body.plan_id]
    order_data = {
        "amount":   amount,
        "currency": "INR",
        "receipt":  f"receipt_{body.plan_id}",
        "notes": {
            "plan":    body.plan_id,
            "user_id": body.user_id
        }
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            return rzp_client.order.create(data=order_data)
        except Exception as e:
            logger.warning("⚠️ Razorpay attempt %d failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                raise HTTPException(status_code=500, detail="Failed to create payment order")


# 1.5 RAZORPAY: Verify Payment (Client-side fallback)
@app.post("/api/payment/verify")
@limiter.limit("10/minute")
async def verify_payment(request: Request, body: VerifyPaymentRequest):
    if body.plan_id not in PLANS:
        raise HTTPException(status_code=400, detail="Invalid plan selected")

    try:
        rzp_client.utility.verify_payment_signature({
            'razorpay_order_id':   body.razorpay_order_id,
            'razorpay_payment_id': body.razorpay_payment_id,
            'razorpay_signature':  body.razorpay_signature
        })
    except razorpay.errors.SignatureVerificationError:
        logger.warning("🚨 Invalid Razorpay signature: user=%s order=%s", body.user_id, body.razorpay_order_id)
        raise HTTPException(status_code=400, detail="Payment verification failed.")

    try:
        db = get_db()
        db.collection("users").document(body.user_id).set({
            "plan":      body.plan_id,
            "paymentId": body.razorpay_payment_id,
            "orderId":   body.razorpay_order_id
        }, merge=True)
        logger.info("✅ Plan upgraded via Frontend Verify: user=%s plan=%s", body.user_id, body.plan_id)
        return {"status": "success"}
    except Exception as e:
        logger.error("🚨 Verify DB error for user=%s: %s", body.user_id, e)
        raise HTTPException(status_code=500, detail="Failed to save payment status")


# 2. RAZORPAY: Webhook Listener
@app.post("/api/webhooks/razorpay")
async def razorpay_webhook(
    request: Request,
    x_razorpay_signature: str = Header(None)
):
    if not x_razorpay_signature:
        raise HTTPException(status_code=400, detail="Missing Razorpay signature header")

    try:
        raw_body = await request.body()

        rzp_client.utility.verify_webhook_signature(
            raw_body.decode('utf-8'),
            x_razorpay_signature,
            WEBHOOK_SECRET
        )

        payload = json.loads(raw_body)
        event   = payload.get("event")

        if event in ("payment.captured", "order.paid"):
            payment_entity = payload['payload']['payment']['entity']
            notes   = payment_entity.get("notes", {})
            user_id = notes.get("user_id")
            plan    = notes.get("plan")

            if plan not in PLANS:
                logger.warning(
                    "Webhook received unknown plan '%s' for user '%s' — ignoring.",
                    plan, user_id
                )
                return {"status": "ok"}

            if user_id and plan:
                db = get_db()
                db.collection("users").document(user_id).set({
                    "plan":      plan,
                    "paymentId": payment_entity.get("id"),
                    "orderId":   payment_entity.get("order_id")
                }, merge=True)
                logger.info("✅ Plan upgraded: user=%s plan=%s", user_id, plan)

        return {"status": "ok"}

    except Exception as e:
        logger.error("🚨 Webhook processing error: %s", e, exc_info=True)
        return {"status": "ok"}


# 3. GOOGLE AUTH: Get Login URL
@app.get("/api/auth/google/login")
@limiter.limit("10/minute")
async def google_login(request: Request, userId: str):
    client_id     = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Google credentials not configured")

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id":     client_id,
                "client_secret": client_secret,
                "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
                "token_uri":     "https://oauth2.googleapis.com/token",
            }
        },
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )

    auth_url, _ = flow.authorization_url(
        prompt='consent',
        access_type='offline',
        state=userId
    )

    # ── FIX: Store BOTH createdAt AND code_verifier in Firestore ──
    db = get_db()
    db.collection("oauthStates").document(userId).set({
        "createdAt":     datetime.now(timezone.utc).isoformat(),
        "code_verifier": flow.code_verifier or "",
    })

    logger.info("[OAuth] Login URL generated for userId=%s", userId)
    return {"url": auth_url}


# 4. GOOGLE AUTH: Handle Callback & Save Tokens
@app.get("/api/auth/google/callback")
async def google_callback(code: str, state: str):
    userId    = state
    db        = get_db()
    state_doc = db.collection("oauthStates").document(userId).get()

    # Validate state exists
    if not state_doc.exists:
        logger.warning("[OAuth] Invalid or missing state for userId=%s", userId)
        return RedirectResponse(url=f"{FRONTEND_URL}/onboarding?error=invalid_state")

    state_data    = state_doc.to_dict() or {}
    created_str   = state_data.get("createdAt", "")
    code_verifier = state_data.get("code_verifier", "")  # ── FIX: retrieve it

    # Validate state is fresh (10-minute expiry)
    try:
        created_at  = datetime.fromisoformat(created_str)
        age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
        if age_seconds > 600:
            db.collection("oauthStates").document(userId).delete()
            logger.warning("[OAuth] Expired state for userId=%s (age=%ds)", userId, age_seconds)
            return RedirectResponse(url=f"{FRONTEND_URL}/onboarding?error=state_expired")
    except Exception:
        pass

    # Consume state immediately — one-time use
    db.collection("oauthStates").document(userId).delete()

    client_id     = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id":     client_id,
                "client_secret": client_secret,
                "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
                "token_uri":     "https://oauth2.googleapis.com/token",
            }
        },
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )

    # ── FIX: Restore code_verifier BEFORE fetching token ──
    if code_verifier:
        flow.code_verifier = code_verifier

    try:
        flow.fetch_token(code=code)
        creds    = flow.credentials
        user_ref = db.collection("users").document(userId)

        user_ref.set({
            "googleConnected": True,
            "googleTokens": {
                "token":         creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri":     creds.token_uri,
                "client_id":     creds.client_id,
                "client_secret": creds.client_secret,
                "scopes":        list(creds.scopes) if creds.scopes else [],
            }
        }, merge=True)

        logger.info("✅ Google tokens saved for user=%s", userId)
        return RedirectResponse(url=f"{FRONTEND_URL}/onboarding?google=success")

    except Exception as e:
        logger.error("OAuth callback error for userId=%s: %s", userId, e, exc_info=True)
        return RedirectResponse(url=f"{FRONTEND_URL}/onboarding?error=google_failed")


# 5. INTEGRATIONS: Save Drive Folder ID
@app.post("/api/settings/folder")
@limiter.limit("10/minute")
async def set_drive_folder(
    request: Request,
    body: FolderRequest,
    uid: str = Depends(verify_firebase_token)
):
    try:
        db       = get_db()
        user_ref = db.collection("users").document(uid)
        user_ref.set({"driveFolderId": body.folder_id}, merge=True)
        logger.info("✅ Drive folder saved | user=%s folder=%s", uid, body.folder_id)
        return {"status": "success"}
    except Exception as e:
        logger.error("🚨 Error saving folder for user=%s: %s", uid, e)
        raise HTTPException(status_code=500, detail="Failed to save folder ID")


# 6. GOOGLE AUTH: Revoke Access
@app.post("/api/auth/google/revoke")
@limiter.limit("5/minute")
async def revoke_google(
    request: Request,
    uid: str = Depends(verify_firebase_token)
):
    try:
        from google.cloud import firestore
        db = get_db()
        db.collection("users").document(uid).update({
            "googleConnected": False,
            "googleTokens":    firestore.DELETE_FIELD,
            "driveFolderId":   firestore.DELETE_FIELD,
        })
        logger.info("✅ Google access revoked for user=%s", uid)
        return {"status": "revoked"}
    except Exception as e:
        logger.error("🚨 Revoke error for user=%s: %s", uid, e)
        raise HTTPException(status_code=500, detail="Failed to revoke access")


# 7. DRIVE SYNC: Manual Trigger with AI Context Tags
@app.post("/api/drive/sync")
@limiter.limit("5/minute")
async def manual_drive_sync(
    request: Request,
    body: SyncRequest,
    uid: str = Depends(verify_firebase_token)
):
    try:
        logger.info(
            "🔄 Manual Sync | user=%s | category=%s | tags=%s",
            uid, body.category, body.tags
        )
        poller       = DrivePoller()
        synced_count = await poller.poll_single_user(
            user_id=uid,
            category=body.category,
            tags=body.tags,
        )
        if not synced_count:
            return {"status": "success", "message": "No new videos detected in Drive."}
        return {"status": "success", "message": f"Successfully queued {synced_count} new video(s)!"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("🚨 Manual Sync Error for user=%s: %s", uid, e)
        raise HTTPException(status_code=500, detail="Failed to sync Drive. Check server logs.")


# 8. AI PREVIEW: Test metadata generation from the AI Generation UI
@app.post("/api/ai/preview")
@limiter.limit("10/minute")
async def preview_metadata(
    request: Request,
    body: PreviewRequest,
    uid: str = Depends(verify_firebase_token)
):
    try:
        db = get_db()

        # 1. Securely check user plan from DB — never trust client
        user_doc = db.collection("users").document(uid).get()
        plan     = (user_doc.to_dict() or {}).get("plan", "free")
        limit    = 5 if plan == "studio" else 3 if plan == "creator" else 1

        # 2. Check and reset daily counter
        config_ref  = db.collection("aiConfig").document(uid)
        config_doc  = config_ref.get()
        config_data = config_doc.to_dict() or {}

        today_str      = str(date_type.today())
        last_reset_str = config_data.get("testCountResetDate", "")
        current_count  = config_data.get("testCount", 0) if last_reset_str == today_str else 0

        # 3. Block if over daily limit
        if current_count >= limit:
            raise HTTPException(
                status_code=429,
                detail=f"Daily preview limit reached ({limit}/day). Resets tomorrow or upgrade your plan."
            )

        # 4. Build enriched tags from validated config model
        cfg  = body.config
        tags = []
        if cfg.tone:              tags.append(f"Tone: {cfg.tone}")
        if cfg.titleStyle:        tags.append(f"Title style: {cfg.titleStyle}")
        if cfg.targetAudience:    tags.append(f"Target audience: {cfg.targetAudience}")
        if cfg.alwaysInclude:     tags.append(f"Always include keywords: {cfg.alwaysInclude}")
        if cfg.alwaysAvoid:       tags.append(f"Never use words: {cfg.alwaysAvoid}")
        if cfg.descriptionCTA:    tags.append(f"End description with: {cfg.descriptionCTA}")
        if cfg.includeHashtags:   tags.append("Include relevant hashtags at end of description")
        if cfg.includeEmojis:     tags.append("Add relevant emojis to the title")
        if cfg.customInstruction: tags.append(f"Extra instruction: {cfg.customInstruction}")

        # 5. Generate
        from services.groq_ai import GroqGenerator
        groq_gen    = GroqGenerator()
        title, desc = await groq_gen.generate_metadata(
            clean_topic=body.topic,
            category=cfg.channelNiche or None,
            tags=tags,
        )

        # 6. Increment daily usage counter
        config_ref.set({
            "testCount":          current_count + 1,
            "testCountResetDate": today_str,
        }, merge=True)

        return {"title": title, "description": desc}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("🚨 AI Preview Error for user=%s: %s", uid, e, exc_info=True)
        raise HTTPException(status_code=500, detail="AI generation failed. Try again.")
