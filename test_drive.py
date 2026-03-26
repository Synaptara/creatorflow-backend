import os
import sys

backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["FIREBASE_KEY_PATH"] = os.path.join(backend_dir, "serviceAccountKey.json")

from google.oauth2 import service_account
from googleapiclient.discovery import build

SERVICE_ACCOUNT_FILE = os.path.join(backend_dir, "serviceAccountKey.json")
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

try:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=DRIVE_SCOPES
    )
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    
    file_id = "1AHKn8wNMfYal7QbNxxycNpSyequrj2l4"
    print(f"Trying to get file meta for {file_id}...")
    
    # Try getting metadata
    file_meta = drive_service.files().get(fileId=file_id, fields="size", supportsAllDrives=True).execute()
    print("Meta (supportsAllDrives=True):", file_meta)
    
except Exception as e:
    print(f"Error occurred: {e}")

try:
    file_meta2 = drive_service.files().get(fileId=file_id, fields="size").execute()
    print("Meta (supportsAllDrives=False):", file_meta2)
except Exception as e:
    print(f"Error occurred without supportsAllDrives: {e}")
