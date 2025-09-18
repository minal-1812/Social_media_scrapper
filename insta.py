import os
import time
import csv
import requests
from instagrapi import Client
from apscheduler.schedulers.blocking import BlockingScheduler

# === Config ===
USERNAME = "mystical_smile_._"
PASSWORD = "#minu@18"
USERNAME_FILE = "instausername.txt"
DOWNLOAD_FOLDER = os.path.abspath("post")
DELAY_BETWEEN_DOWNLOADS = 5  # seconds
POSTS_PER_USER = 20

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)


def load_existing_ids(username):
    metadata_csv = os.path.join(DOWNLOAD_FOLDER, username, "media_metadata.csv")
    if not os.path.exists(metadata_csv):
        return set()
    with open(metadata_csv, "r", newline="", encoding="utf-8") as f:
        return set(row["id"] for row in csv.DictReader(f))


def download_user_posts(cl: Client, username: str, limit=POSTS_PER_USER):
    try:
        user_id = cl.user_id_from_username(username)
        medias = cl.user_medias(user_id, amount=limit)

        if not medias:
            print(f"⚠️ No posts found for @{username}")
            return

        user_folder = os.path.join(DOWNLOAD_FOLDER, username)
        os.makedirs(user_folder, exist_ok=True)
        metadata_csv = os.path.join(user_folder, "media_metadata.csv")

        existing_ids = load_existing_ids(username)
        new_metadata = []

        for media in medias:
            if str(media.pk) in existing_ids:
                print(f"⏭️ Skipping existing media {media.pk}")
                continue

            media_type = media.media_type
            caption = media.caption_text or ""
            taken_at = str(media.taken_at)
            like_count = media.like_count
            comment_count = media.comment_count
            username_owner = media.user.username

            downloaded = False
            file_paths = []

            # === Handle Reels/Videos ===
            if media_type == 2 and media.video_url:
                filename = f"reel_{media.pk}.mp4"
                path = os.path.join(user_folder, filename)
                if download_file(media.video_url, path):
                    file_paths.append(path)
                    downloaded = True

            # === Handle Single Image Post ===
            elif media_type == 1 and media.thumbnail_url:
                filename = f"image_{media.pk}.jpg"
                path = os.path.join(user_folder, filename)
                if download_file(media.thumbnail_url, path):
                    file_paths.append(path)
                    downloaded = True

            # === Handle Carousel ===
            elif media_type == 8 and media.resources:
                for idx, item in enumerate(media.resources):
                    url = item.thumbnail_url or item.video_url
                    if url:
                        ext = ".mp4" if item.video_url else ".jpg"
                        filename = f"carousel_{media.pk}_{idx}{ext}"
                        path = os.path.join(user_folder, filename)
                        if download_file(url, path):
                            file_paths.append(path)
                            downloaded = True

            if downloaded:
                for path in file_paths:
                    metadata = {
                        "id": str(media.pk),
                        "taken_at": taken_at,
                        "caption": caption,
                        "likes": like_count,
                        "comments": comment_count,
                        "media_url": path,
                        "media_type": media_type,
                        "username": username_owner
                    }
                    new_metadata.append(metadata)
                time.sleep(DELAY_BETWEEN_DOWNLOADS)

        # === Save Metadata ===
        if new_metadata:
            new_metadata = sorted(new_metadata, key=lambda x: x["taken_at"], reverse=True)

            old_rows = []
            if os.path.exists(metadata_csv):
                with open(metadata_csv, "r", newline="", encoding="utf-8") as f:
                    old_rows = list(csv.DictReader(f))

            with open(metadata_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=new_metadata[0].keys())
                writer.writeheader()
                writer.writerows(new_metadata + old_rows)

    except Exception as e:
        print(f"❌ Error processing @{username}: {e}")


def download_file(url, path):
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
        print(f"✅ Downloaded: {path}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {url}: {e}")
        return False


def main():
    if not os.path.exists(USERNAME_FILE):
        print("❌ Missing 'instausername.txt' file.")
        return

    cl = Client()
    cl.login(USERNAME, PASSWORD)
    print(f"✅ Logged in as @{USERNAME}")

    with open(USERNAME_FILE, "r") as file:
        accounts = [line.strip().lstrip('@') for line in file if line.strip()]

    for username in accounts:
        print(f"\n🔍 Fetching all content for @{username}")
        download_user_posts(cl, username)

    print(f"\n📁 All downloads saved to: {DOWNLOAD_FOLDER}")


def run_every_4_hours():
    print(f"\n⏰ Running scheduled fetch at {time.ctime()}")
    main()


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run_every_4_hours, 'interval', hours=4)
    print("🔁 Script scheduled to run every 4 hours. Press Ctrl+C to stop.")
    run_every_4_hours()
    scheduler.start()
