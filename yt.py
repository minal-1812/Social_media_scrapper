import os
import time
import subprocess
import yt_dlp
import csv
from apscheduler.schedulers.blocking import BlockingScheduler

# === Config ===
DOWNLOAD_FOLDER = os.path.abspath("post/youtube_shorts")
YT_ACCOUNTS_FILE = "ytaccounts.txt"
DELAY_BETWEEN_DOWNLOADS = 5  # seconds

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# === Check FFmpeg ===
def check_ffmpeg():
    try:
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except Exception:
        print("❌ FFmpeg is NOT installed. Please install FFmpeg to merge audio+video.")
        return False

# === Load existing metadata ===
def load_existing_ids(metadata_csv):
    if not os.path.exists(metadata_csv):
        return set(), []
    with open(metadata_csv, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        return set(row["id"] for row in reader), reader

# === Save metadata ===
def save_metadata(new_entries, old_entries, metadata_csv):
    if not new_entries:
        print("ℹ️ No new metadata to save.")
        return

    keys = new_entries[0].keys()
    combined = new_entries + [row for row in old_entries if row["id"] not in {e["id"] for e in new_entries}]
    combined.sort(key=lambda x: x.get("upload_date", ""), reverse=True)  # Sort newest first

    with open(metadata_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(combined)

    print(f"✅ {len(new_entries)} new metadata entries saved to {metadata_csv}.")

# === Download YouTube Shorts ===
def download_youtube_shorts(channel_name):
    if not check_ffmpeg():
        return []

    subfolder = os.path.join(DOWNLOAD_FOLDER, channel_name)
    os.makedirs(subfolder, exist_ok=True)
    metadata_csv = os.path.join(subfolder, f"{channel_name}_metadata.csv")

    existing_ids, old_entries = load_existing_ids(metadata_csv)

    ydl_opts = {
        'outtmpl': os.path.join(subfolder, '%(title).100s.%(ext)s'),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4',
        'merge_output_format': 'mp4',
        'quiet': False
    }

    new_metadata = []
    print(f"\n🔍 Searching Shorts from YouTube channel: {channel_name}")
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(f"ytsearch10:{channel_name}", download=False)
            found = False
            for entry in result.get('entries', []):
                url = entry.get("webpage_url", "")
                duration = entry.get("duration", 0)
                video_id = entry.get("id")
                if not video_id:
                    continue

                if ("shorts" in url or duration <= 60) and video_id not in existing_ids:
                    print(f"▶️ Downloading: {entry.get('title', '')}")
                    ydl.download([url])
                    metadata = {
                        "id": video_id,
                        "title": entry.get("title", ""),
                        "uploader": entry.get("uploader", ""),
                        "upload_date": entry.get("upload_date", ""),
                        "view_count": entry.get("view_count", 0),
                        "like_count": entry.get("like_count", 0),
                        "url": url,
                        "channel": channel_name
                    }
                    new_metadata.append(metadata)
                    existing_ids.add(video_id)
                    time.sleep(DELAY_BETWEEN_DOWNLOADS)
                    found = True
            if not found:
                print(f"❌ No new Shorts found for {channel_name}")
            else:
                print("✅ Shorts download complete for", channel_name)
    except Exception as e:
        print(f"⚠️ Error fetching shorts from {channel_name}: {e}")

    save_metadata(new_metadata, old_entries, metadata_csv)
    return new_metadata

# === Main Download Job ===
def main():
    if not os.path.exists(YT_ACCOUNTS_FILE):
        print(f"❌ Missing '{YT_ACCOUNTS_FILE}'.")
        return

    with open(YT_ACCOUNTS_FILE, "r") as file:
        accounts = [line.strip() for line in file if line.strip() and not line.startswith("@")]

    for account in accounts:
        download_youtube_shorts(account)

    print(f"\n📁 All YouTube Shorts saved in: {DOWNLOAD_FOLDER}")

# === Scheduler ===
if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', hours=4)
    print("🔁 Script scheduled to run every 4 hours. Press Ctrl+C to stop.")
    main()  # Initial run
    scheduler.start()
