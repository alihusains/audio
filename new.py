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
  3. Skips files larger than MAX_FILE_SIZE_MB (50 MB by default).
  4. Generates/updates audio_links.csv with mapping info.
  5. Stages changed/untracked files and commits them in batches of BATCH_SIZE (default 20).
  6. Pushes after each batch.

Notes:
  - This script must be run from the repository root to ensure git paths are correct.
  - For large files or many files consider using Git LFS or external storage.
"""

import os
import sys
import csv
import time
import subprocess
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

# ---------- Configuration ----------
REMOTE_BASE_URL = "https://ya-mahdi.net/apps_audio/"  # must end with '/'
DEST_DIR = "apps_audio"
CSV_FILE = "audio_links.csv"
EXTENSIONS = ('.mp3', '.m4a', '.png', '.jpg', '.jpeg')
BATCH_SIZE = 20  # number of files after which to git add/commit/push
MAX_FILE_SIZE_MB = 50  # skip files larger than this (MB)
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
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
        if href in ("../", "/"):
            continue
        hrefs.append(href)
    return hrefs


def walk_remote(base_url):
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
            if href.endswith("/"):
                to_visit.append(full)
            else:
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
    remote_size = get_remote_size(url)
    if remote_size is not None and remote_size > MAX_FILE_SIZE_BYTES:
        print(f"Skipping (too large > {MAX_FILE_SIZE_MB} MB): {url}")
        return False

    if SKIP_IF_SAME_SIZE and os.path.exists(dest_path) and remote_size is not None:
        local_size = os.path.getsize(dest_path)
        if local_size == remote_size:
            print(f"Skipping (same size): {dest_path}")
            return False

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
    parsed = urlparse(url)
    path = parsed.path
    idx = path.find(f"/{DEST_DIR}/")
    if idx != -1:
        rel = path[idx + len(f"/{DEST_DIR}/"):]
    else:
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
    try:
        url = run_git(["remote", "get-url", GIT_REMOTE], capture_output=True)
    except Exception:
        return "unknown/unknown"
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
    out = run_git(["status", "--porcelain", "--untracked-files=all", "--"] + list(paths), capture_output=True)
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    files = []
    for line in lines:
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            files.append(parts[1])
    return files


def normalize_repo_relative_path(path):
    """
    Ensure path is relative to repo root and uses forward slashes for git.
    Called with an absolute or relative path; returns a relative path.
    """
    abs_path = os.path.abspath(path)
    repo_root = os.path.abspath(os.getcwd())
    try:
        rel = os.path.relpath(abs_path, repo_root)
    except Exception:
        rel = path
    # Convert backslashes on Windows to forward slashes for git
    return rel.replace(os.sep, "/")


def commit_and_push_paths(paths, batch_index=None):
    """
    Stage, commit, and push the provided list of file paths while preserving folder structure.
    Paths should be repo-relative paths (or will be normalized).
    """
    if not paths:
        return False
    # Normalize and deduplicate while preserving order
    normed = []
    seen = set()
    for p in paths:
        np = normalize_repo_relative_path(p)
        if np not in seen:
            seen.add(np)
            normed.append(np)

    print(f"Committing batch of {len(normed)} files (preserving folder structure).")
    try:
        run_git(["add"] + normed)
    except subprocess.CalledProcessError as e:
        print(f"git add failed: {e}")
        return False

    msg = "Update audio files"
    if batch_index is not None:
        msg = f"{msg} (batch {batch_index})"

    try:
        run_git(["commit", "-m", msg])
    except subprocess.CalledProcessError:
        print("No changes to commit in this batch.")
        try:
            run_git(["reset", "--"] + normed)
        except Exception:
            pass
        return False

    push_cmd = ["push", GIT_REMOTE]
    if GIT_BRANCH:
        push_cmd.append(GIT_BRANCH)
    else:
        push_cmd.append("HEAD")
    try:
        run_git(push_cmd)
        print("Pushed batch successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Push failed for batch: {e}")
        return False


def main():
    # Important: run from repo root to preserve relative paths for git
    repo_root = os.path.abspath(os.getcwd())
    print(f"Repository root: {repo_root}")
    ensure_dir(DEST_DIR)

    print("Discovering remote files...")
    remote_files = walk_remote(REMOTE_BASE_URL)
    print(f"Found {len(remote_files)} remote files (matching extensions).")

    downloaded_for_batch = []
    total_downloaded = 0
    batch_count = 0

    for url in remote_files:
        rel = relpath_in_dest(url)
        dest_path = os.path.join(DEST_DIR, rel)
        dest_path = os.path.normpath(dest_path)

        remote_size = get_remote_size(url)
        if remote_size is not None and remote_size > MAX_FILE_SIZE_BYTES:
            print(f"Skipping remote file (size {remote_size} bytes > {MAX_FILE_SIZE_MB} MB): {url}")
            continue

        downloaded = download_file(url, dest_path)
        if downloaded:
            downloaded_for_batch.append(dest_path)
            total_downloaded += 1

        if len(downloaded_for_batch) >= BATCH_SIZE:
            batch_count += 1
            print(f"Batch {batch_count}: preparing to commit {len(downloaded_for_batch)} files.")
            commit_and_push_paths(downloaded_for_batch, batch_index=batch_count)
            downloaded_for_batch = []

    # Final partial batch
    if downloaded_for_batch:
        batch_count += 1
        print(f"Final batch {batch_count}: preparing to commit {len(downloaded_for_batch)} files.")
        commit_and_push_paths(downloaded_for_batch, batch_index=batch_count)
        downloaded_for_batch = []

    print(f"Total downloaded files: {total_downloaded}")

    # Generate CSV and commit/push if changed
    generate_csv(DEST_DIR, CSV_FILE, REMOTE_BASE_URL)
    changed_csv = get_changed_files([CSV_FILE])
    if changed_csv:
        print("CSV changed; committing and pushing CSV.")
        commit_and_push_paths(changed_csv, batch_index="csv")
    else:
        print("No CSV changes detected.")

    print("Done.")


if __name__ == "__main__":
    main()