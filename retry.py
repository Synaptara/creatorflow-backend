import os
import sys
import asyncio

backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["FIREBASE_KEY_PATH"] = os.path.join(backend_dir, "serviceAccountKey.json")
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from dotenv import load_dotenv
load_dotenv(os.path.join(backend_dir, ".env"))

from database import get_db
from services.youtube_uploader import YouTubeUploader

async def main():
    db = get_db()
    video_id = "NHQma5POz7BdG7mH7nfZ"
    
    print(f"Resetting video {video_id} to 'scheduled'...")
    db.collection("videos").document(video_id).update({
        "status": "scheduled",
        "errorMessage": db.field_deletions() if hasattr(db, 'field_deletions') else None
    })
    
    print(f"Running uploader for {video_id}...")
    uploader = YouTubeUploader(video_id)
    await uploader.execute_upload()
    print("Uploader finished!")
    
    v = db.collection("videos").document(video_id).get().to_dict()
    print(f"Final Status: {v.get('status')}")
    print(f"Error Message: {v.get('errorMessage')}")

if __name__ == "__main__":
    asyncio.run(main())
