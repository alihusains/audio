#!/usr/bin/env python3
"""
download_and_push.py

Usage:
  - Place this script in the root of your local git repository.
  - Ensure git is configured with push permissions (e.g., origin set, proper credentials).
  - Run: python3 download_and_push.py

What it does:
  1. Crawls the remote directory listing starting at REMOTE_BASE_URL and downloads
     files under the specified extensions into local DEST_DIR (apps_audio by default).
  2. Skips downloading files that already exist locally with identical size (uses HEAD/Content-Length).
  3. Generates/updates audio_links.csv with mapping info.
  4. Stages changed/untracked files and commits them in batches of BATCH_SIZE (default 20).
  5. Pushes after each batch.

Notes:
  - This script expects the remote site to either allow directory listing or provide links that wget/requests can find.
  - If the remote server does not provide a directory index, you may need an explicit list of files.
  - For large files or many files consider using Git LFS or external storage.
"""

import os
import sys
import csv
import time
import math
import shutil
import hashlib
import subprocess
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

# ---------- Configuration ----------
REMOTE_BASE_URL = "https://ya-mahdi.net/apps_audio/"  # must end with '/'
DEST_DIR = "apps_audio"
CSV_FILE = "audio_links.csv"
EXTENSIONS = ('.mp3', '.m4a', '.png', '.jpg', '.jpeg')
BATCH_SIZE = 20
USER_AGENT = "download_and_push/1.0 (+https://github.com/{})".format("alihusains")
GIT_REMOTE = "origin"
GIT_BRANCH = None  # None -> current branch
SKIP_IF_SAME_SIZE = True
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
# -----------------------------------

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def ensure_dir(p):
    if not os.path.isdir(p):
        os.makedirs(p, exist_ok=True)


def get_remote_index(url):
    """
    Fetch and parse an HTML directory listing at url.
    Returns list of hrefs (possibly relative) found on the page.
    """
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"Failed to GET {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    hrefs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Skip parent dir anchors
        if href in ("../", "/"):
            continue
        hrefs.append(href)
    return hrefs


def is_directory_listing(url):
    """
    Try to decide if the URL returns an HTML directory listing by checking its content-type
    and presence of <a href> tags.
    """
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "html" in ct.lower():
            if "<a " in r.text.lower():
                return True
    except Exception:
        pass
    return False


def walk_remote(base_url):
    """
    Walk the remote directory tree starting at base_url and yield remote file URLs
    whose path ends with one of the desired extensions.
    This assumes the remote exposes HTML directory listings with links.
    """
    to_visit = [base_url]
    seen_dirs = set()
    files = []

    while to_visit:
        url = to_visit.pop(0)
        if url in seen_dirs:
            continue
        seen_dirs.add(url)
        print(f"Listing: {url}")
        hrefs = get_remote_index(url)
        for href in hrefs:
            full = urljoin(url, href)
            parsed = urlparse(full)
            path = parsed.path
            # If href ends with '/', treat as directory
            if href.endswith("/"):
                to_visit.append(full)
            else:
                # if extension matches, add
                if os.path.splitext(path)[1].lower() in EXTENSIONS:
                    files.append(full)
    return files


def get_remote_size(url):
    try:
        r = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            cl = r.headers.get("Content-Length")
            if cl:
                return int(cl)
    except Exception:
        pass
    # fallback: try GET with stream and read headers
    try:
        r = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        if cl:
            return int(cl)
    except Exception:
        pass
    return None


def download_file(url, dest_path):
    ensure_dir(os.path.dirname(dest_path))
    # Check remote size and local size
    remote_size = get_remote_size(url)
    if SKIP_IF_SAME_SIZE and os.path.exists(dest_path) and remote_size is not None:
        local_size = os.path.getsize(dest_path)
        if local_size == remote_size:
            print(f"Skipping (same size): {dest_path}")
            return False  # not downloaded
    # Download with retries
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
                r.raise_for_status()
                tmp_path = dest_path + ".part"
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                os.replace(tmp_path, dest_path)
            print(f"Downloaded: {dest_path}")
            return True
        except Exception as e:
            print(f"Download failed for {url} (attempt {attempt}): {e}")
            time.sleep(2 ** attempt)
    print(f"Failed to download after retries: {url}")
    return False


def relpath_in_dest(url):
    """Compute relative path under DEST_DIR from remote URL."""
    parsed = urlparse(url)
    path = parsed.path
    # remote base path may include leading parts; we want path after /apps_audio/
    idx = path.find("/apps_audio/")
    if idx != -1:
        rel = path[idx + len("/apps_audio/"):]
    else:
        # fallback: take basename
        rel = os.path.basename(path)
    return rel.lstrip("/")


def generate_csv(dest_dir, csv_file, base_url):
    rows = []
    github_repo = f"https://github.com/{get_git_repo_fullname()}/blob/main/{DEST_DIR}/"
    raw_repo = f"https://raw.githubusercontent.com/{get_git_repo_fullname()}/main/{DEST_DIR}/"
    cdnjs_prefix = "https://cdnjs.cloudflare.com/ajax/libs/"

    for root, _, files in os.walk(dest_dir):
        for f in files:
            if f.lower().endswith(EXTENSIONS):
                full_path = os.path.join(root, f)
                rel_path = os.path.relpath(full_path, dest_dir).replace(os.sep, "/")
                size_mb = os.path.getsize(full_path) / (1024 * 1024)
                original_url = base_url + rel_path
                if size_mb < 20:
                    github_url = github_repo + rel_path
                    cdn_url = cdnjs_prefix + rel_path
                else:
                    github_url = raw_repo + rel_path
                    cdn_url = github_url
                rows.append([original_url, github_url, cdn_url, f"{size_mb:.2f} MB"])
    # write CSV
    rows.sort()
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Original URL", "GitHub URL", "CDNJS/Raw URL", "File Size"])
        writer.writerows(rows)
    print(f"Wrote CSV: {csv_file}")


def run_git(args, check=True, capture_output=False):
    cmd = ["git"] + args
    if capture_output:
        res = subprocess.run(cmd, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return res.stdout.strip()
    else:
        return subprocess.run(cmd, check=check)


def get_git_repo_fullname():
    # e.g. git remote get-url origin -> git@github.com:user/repo.git or https://github.com/user/repo.git
    try:
        url = run_git(["remote", "get-url", GIT_REMOTE], capture_output=True)
    except Exception:
        return "unknown/unknown"
    # normalize
    if url.startswith("git@github.com:"):
        path = url.split(":", 1)[1]
    elif url.startswith("https://") or url.startswith("http://"):
        path = url.split("github.com/", 1)[1]
    else:
        path = url
    if path.endswith(".git"):
        path = path[:-4]
    return path


def get_changed_files(paths):
    """
    Return list of changed/untracked files among the provided paths relative to repo root,
    using git status --porcelain.
    """
    # Use porcelain status
    out = run_git(["status", "--porcelain", "--untracked-files=all", "--"] + list(paths), capture_output=True)
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    files = []
    for line in lines:
        # format: XY <path>  or "?? <path>"
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            files.append(parts[1])
    return files


def commit_and_push_in_batches(file_list, batch_size=BATCH_SIZE):
    if not file_list:
        print("No files to commit.")
        return
    total = len(file_list)
    print(f"Committing {total} files in batches of {batch_size}...")
    for i in range(0, total, batch_size):
        batch = file_list[i:i + batch_size]
        print(f"Batch {i // batch_size + 1}: adding {len(batch)} files")
        # git add
        run_git(["add"] + batch)
        # commit (ignore if nothing to commit)
        try:
            run_git(["commit", "-m", f"Update audio files (batch {i // batch_size + 1})"])
        except subprocess.CalledProcessError:
            print("No changes to commit in this batch.")
            # unstage to keep state clean
            run_git(["reset", "--"] + batch)
            continue
        # push
        push_cmd = ["push", GIT_REMOTE]
        if GIT_BRANCH:
            push_cmd.append(GIT_BRANCH)
        else:
            # push current HEAD
            push_cmd.append("HEAD")
        try:
            run_git(push_cmd)
        except subprocess.CalledProcessError as e:
            print(f"Push failed for batch {i // batch_size + 1}: {e}")
            # decide to continue or abort; we'll continue
            continue
    print("All batches pushed.")


def main():
    ensure_dir(DEST_DIR)

    # Discover remote files
    print("Discovering remote files...")
    remote_files = walk_remote(REMOTE_BASE_URL)
    print(f"Found {len(remote_files)} remote files (matching extensions).")

    # Download files, track which local files changed
    changed_local_paths = []
    for url in remote_files:
        rel = relpath_in_dest(url)
        dest_path = os.path.join(DEST_DIR, rel)
        # normalize dest_path
        dest_path = os.path.normpath(dest_path)
        downloaded = download_file(url, dest_path)
        if downloaded:
            changed_local_paths.append(dest_path)

    # After downloads, generate CSV (this may change CSV even if no files changed)
    generate_csv(DEST_DIR, CSV_FILE, REMOTE_BASE_URL)
    # If CSV changed, include it
    # Determine changed/untracked files relative to git
    paths_to_check = [DEST_DIR, CSV_FILE]
    changed = get_changed_files(paths_to_check)

    # Ensure we have paths as relative to repo root in git format
    if changed:
        print("Git reports changed/untracked files:")
        for c in changed:
            print("  ", c)
    else:
        print("No git changes detected.")

    # Commit and push in batches of BATCH_SIZE
    # We prioritize files under DEST_DIR, then CSV
    # Filter to only real paths
    changed = [p for p in changed if p]  # remove empties
    commit_and_push_in_batches(changed, batch_size=BATCH_SIZE)

    print("Done.")


if __name__ == "__main__":
    main()