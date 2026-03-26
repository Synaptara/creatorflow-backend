import os
import sys

# Add backend directory to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["FIREBASE_KEY_PATH"] = os.path.join(backend_dir, "serviceAccountKey.json")
# also OAUTHLIB_INSECURE_TRANSPORT if needed
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from database import get_db

db = get_db()
videos = db.collection("videos").where("status", "==", "failed").get()

with open(os.path.join(backend_dir, "failed_videos.txt"), "w") as f:
    if not videos:
        f.write("No failed videos found.\n")
    else:
        f.write(f"Found {len(videos)} failed videos.\n")
        open_videos = [v for v in videos]
        # output the last 5
        for v in open_videos[-5:]:
            data = v.to_dict()
            f.write(f"Video ID: {v.id}\n")
            f.write(f"Title: {data.get('title')}\n")
            f.write(f"Error Message: {data.get('errorMessage')}\n")
            f.write(f"Upload Failed At: {data.get('uploadFailedAt')}\n")
            f.write("-" * 40 + "\n")
