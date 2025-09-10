import requests
import csv
import string
import time
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
BASE_DIR_URL = "https://www.k-online.com/vis-api/vis/v1/en/directory/{}"
PROFILE_URL = "https://www.k-online.com/vis-api/vis/v1/en/exhibitors/{}/slices/profile"
OUTPUT_FILE = "exhibitors_full.csv"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
    "X-Vis-Domain": "www.k-online.com",
}

# ---------- Session with retries ----------
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)


# ---------- Helper functions ----------
def get_json(url):
    try:
        resp = session.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"[ERROR] Could not fetch {url}: {e}")
        return None


def get_exhibitors_by_letter(letter):
    url = BASE_DIR_URL.format(letter)
    data = get_json(url)
    return data if data else []


def get_profiles(exhibitor_id):
    url = PROFILE_URL.format(exhibitor_id)
    data = get_json(url)
    return data.get("slices", []) if data else []


# ---------- Main scraper ----------
def main():
    all_data = []

    for letter in string.ascii_lowercase:
        print(f"\nScraping letter: {letter.upper()} ...")
        exhibitors = get_exhibitors_by_letter(letter)
        print(f"  Found {len(exhibitors)} exhibitors")

        for ex in exhibitors:
            exhibitor_id = ex.get("exh")
            if not exhibitor_id:
                continue

            profiles = get_profiles(exhibitor_id) or [{}]

            for profile in profiles:
                record = {
                    "exhibitor_id": exhibitor_id,
                    "name": ex.get("name", ""),
                    "country": ex.get("country", ""),
                    "city": ex.get("city", ""),
                    "location": ex.get("location", ""),
                    "profile_text": profile.get("text", "").replace("\n", " ").strip(),
                    # store raw profile JSON as a string
                    "raw_profile": json.dumps(profile, ensure_ascii=False)
                }
                all_data.append(record)

            print(f"  → {ex.get('name')} ({len(profiles)} profiles)")
            time.sleep(0.5)

    # Save CSV
    fieldnames = ["exhibitor_id", "name", "country", "city", "location", "profile_text", "raw_profile"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_data)

    print(f"\n✅ Scraping complete! Saved {len(all_data)} records to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
