#!/usr/bin/env python3
"""
primesrc_pipeline.py  –  Unified PrimeSrc pipeline (Browserbase edition)
=========================================================================
Stage 1  – fetch /api/v1/s for every tmdb embed URL → api_url_list.txt
Stage 2  – open every /api/v1/l?key=… via Browserbase cloud browser
           (falls back to local Chrome if no BB credentials)
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse
from urllib.request import Request, urlopen

warnings.filterwarnings("ignore", category=ResourceWarning)

# ═══════════════════════════════════════════════════════════════
# PATHS & TUNABLES
# ═══════════════════════════════════════════════════════════════

HERE                 = Path(__file__).parent
DEFAULT_INPUT_FILE   = HERE / "multiple_primesrc.txt"
DEFAULT_API_LIST     = HERE / "api_url_list.txt"
DEFAULT_STREAM_OUT   = HERE / "final_stream_urls.txt"
DEFAULT_JSON_SUMMARY = HERE / "pipeline_summary.json"
DEFAULT_HTML_OUT     = HERE / "pipeline_report.html"

# Platform-specific Chrome paths (used only when Browserbase is not available)
if sys.platform == "win32":
    CHROME_EXE       = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    CHROME_EXE_ALT   = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
    CHROME_USER_DATA = r"C:\Users\AC\AppData\Local\Google\Chrome\User Data"
    CHROME_PROFILE   = "Profile 2"
elif sys.platform == "darwin":
    CHROME_EXE       = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    CHROME_EXE_ALT   = "/Applications/Chromium.app/Contents/MacOS/Chromium"
    CHROME_USER_DATA = os.path.expanduser("~/Library/Application Support/Google/Chrome")
    CHROME_PROFILE   = "Default"
else:
    CHROME_EXE       = "/usr/bin/google-chrome"
    CHROME_EXE_ALT   = "/usr/bin/chromium-browser"
    CHROME_USER_DATA = os.path.expanduser("~/.config/google-chrome")
    CHROME_PROFILE   = "Default"

CHROME_DEBUG_PORT    = 9222
CHROME_PROFILE_CACHE = os.path.join(tempfile.gettempdir(), "primesrc_profile_cache")

STAGE1_REQUEST_TIMEOUT = 20
STAGE2_PAGE_TIMEOUT    = 60
STAGE2_BLANK_TIMEOUT   = 1
STAGE2_BATCH_SIZE      = 5
STAGE2_RELOADS         = 2
STAGE2_FINAL_RETRIES   = 1

CACHE_NAMES = {
    "AutofillAiModelCache", "Cache", "CacheStorage", "Code Cache",
    "DawnGraphiteCache", "DawnWebGPUCache", "GPUCache", "GrShaderCache",
    "LOCK", "LOG", "LOG.old", "optimization_guide_hint_cache_store",
    "ShaderCache", "SingletonCookie", "SingletonLock", "SingletonSocket",
}

TMDB_ID_RE = re.compile(r"^\d+$")

# Browserbase API endpoint
BROWSERBASE_API = "https://www.browserbase.com/v1"

# ═══════════════════════════════════════════════════════════════
# CONSOLE HELPERS
# ═══════════════════════════════════════════════════════════════

_RESET  = "\033[0m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"

def _c(text: str, colour: str) -> str:
    try:
        return colour + text + _RESET if sys.stdout.isatty() else text
    except Exception:
        return text

def log_info(msg: str) -> None: print(_c(f"[INFO]  {msg}", _CYAN))
def log_ok(msg: str)   -> None: print(_c(f"[OK]    {msg}", _GREEN))
def log_warn(msg: str) -> None: print(_c(f"[WARN]  {msg}", _YELLOW))
def log_err(msg: str)  -> None: print(_c(f"[ERR]   {msg}", _RED))
def log_head(msg: str) -> None: print(_c(f"\n{'='*60}\n{msg}\n{'='*60}", _BOLD))

# ═══════════════════════════════════════════════════════════════
# STAGE 1  –  embed URLs → /api/v1/s → api_url_list.txt
# ═══════════════════════════════════════════════════════════════

@dataclass
class ServerOption:
    server_name: str
    key: str
    api_url: str
    main_url: str
    title: str = ""
    quality: str = ""
    audio_language: str = ""


def _build_server_api_url(main_url: str) -> str:
    parsed = urlparse(main_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if parsed.path.startswith("/embed/movie"):
        params.setdefault("type", "movie")
    elif parsed.path.startswith("/embed/tv"):
        params.setdefault("type", "tv")
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'primesrc.me'}"
    return f"{base}/api/v1/s?{urlencode(params)}"


def _fetch_json_http(url: str, referer: str) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, */*",
            "Referer": referer,
        },
    )
    with urlopen(req, timeout=STAGE1_REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return json.loads(resp.read().decode(charset, errors="replace"))


def _normalise_embed_url(raw: str, media_type: str = "movie") -> str:
    raw = raw.strip()
    if TMDB_ID_RE.fullmatch(raw):
        return f"https://primesrc.me/embed/{media_type}?tmdb={raw}"
    if raw.startswith("primesrc.me/"):
        return "https://" + raw
    if raw.startswith("/embed/"):
        return "https://primesrc.me" + raw
    return raw


def _find_server_lists(obj: Any) -> list[dict[str, Any]]:
    lists: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        servers = obj.get("servers")
        if isinstance(servers, list) and servers:
            if any(
                "key" in item or "file_name" in item
                for item in servers
                if isinstance(item, dict)
            ):
                info = obj.get("info") if isinstance(obj.get("info"), dict) else {}
                lists.append({"servers": servers, "info": info})
        for v in obj.values():
            lists.extend(_find_server_lists(v))
    elif isinstance(obj, list):
        for item in obj:
            lists.extend(_find_server_lists(item))
    return lists


def _options_from_server_list(servers: list[dict], main_url: str) -> list[ServerOption]:
    options: list[ServerOption] = []
    for item in servers:
        key  = str(item.get("key")  or "").strip()
        name = str(item.get("name") or "").strip()
        if not key:
            continue
        options.append(ServerOption(
            server_name    = name,
            key            = key,
            api_url        = f"https://primesrc.me/api/v1/l?key={quote(key, safe='')}",
            main_url       = main_url,
            title          = str(item.get("file_name")      or "").strip(),
            quality        = str(item.get("quality")        or "").strip(),
            audio_language = str(item.get("audio_language") or "").strip(),
        ))
    return options


def stage1_fetch_api_keys(
    input_file: Path,
    api_list_file: Path,
    media_type: str = "movie",
) -> list[ServerOption]:
    log_head("STAGE 1  –  Fetch server keys from PrimeSrc /api/v1/s")

    raw_lines = [
        l.strip()
        for l in input_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
    log_info(f"Input embed URLs : {len(raw_lines)}  ({input_file})")

    seen_urls: set[str] = set()
    embed_urls: list[str] = []
    for raw in raw_lines:
        url = _normalise_embed_url(raw, media_type)
        if url not in seen_urls:
            seen_urls.add(url)
            embed_urls.append(url)

    all_options: list[ServerOption] = []
    errors: list[tuple[str, str]] = []

    for idx, embed_url in enumerate(embed_urls, 1):
        label = f"  [{idx:>4}/{len(embed_urls)}]"
        api_url = _build_server_api_url(embed_url)
        try:
            obj = _fetch_json_http(api_url, embed_url)
            server_lists = _find_server_lists(obj)
            if not server_lists:
                log_warn(f"{label} no server list  {embed_url}")
                continue
            for sl in server_lists:
                opts = _options_from_server_list(sl.get("servers", []), embed_url)
                all_options.extend(opts)
            count = sum(
                len(_options_from_server_list(sl.get("servers", []), embed_url))
                for sl in server_lists
            )
            log_ok(f"{label} {count} keys  {embed_url}")
        except Exception as exc:
            errors.append((embed_url, str(exc)))
            log_err(f"{label} {exc}  {embed_url}")

    seen_api: set[str] = set()
    unique_options: list[ServerOption] = []
    for opt in all_options:
        if opt.api_url not in seen_api:
            seen_api.add(opt.api_url)
            unique_options.append(opt)

    api_list_file.write_text(
        "\n".join(opt.api_url for opt in unique_options) + "\n",
        encoding="utf-8",
    )
    log_info(f"Total keys : {len(all_options)}  (unique: {len(unique_options)})")
    log_info(f"Errors     : {len(errors)}")
    log_ok(f"Written → {api_list_file}")

    if errors:
        log_warn("Failed embed URLs (stage 1):")
        for url, err in errors:
            log_warn(f"  {url}  → {err}")

    return unique_options


# ═══════════════════════════════════════════════════════════════
# BROWSERBASE HELPERS
# ═══════════════════════════════════════════════════════════════

def _bb_request(method: str, path: str, api_key: str, body: dict | None = None) -> dict:
    """Make a request to the Browserbase API."""
    url  = f"{BROWSERBASE_API}{path}"
    data = json.dumps(body).encode() if body else None
    req  = Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "x-bb-api-key": api_key,
        },
        method=method,
    )
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _bb_create_session(api_key: str, project_id: str) -> dict:
    """Create a new Browserbase session with stealth settings."""
    log_info("Creating Browserbase session...")
    session = _bb_request(
        "POST",
        "/sessions",
        api_key,
        {
            "projectId": project_id,
            "browserSettings": {
                "fingerprint": {
                    "browsers":         ["chrome"],
                    "devices":          ["desktop"],
                    "operatingSystems": ["windows"],
                    "locales":          ["en-US"],
                },
                "viewport": {"width": 1280, "height": 800},
                "stealth": True,
            },
        },
    )
    log_ok(f"Browserbase session created: {session['id']}")
    return session


def _bb_stop_session(api_key: str, session_id: str) -> None:
    """Stop a Browserbase session."""
    try:
        _bb_request("POST", f"/sessions/{session_id}", api_key, {"status": "REQUEST_RELEASE"})
        log_info(f"Browserbase session released: {session_id}")
    except Exception as e:
        log_warn(f"Could not release Browserbase session: {e}")


# ═══════════════════════════════════════════════════════════════
# LOCAL CHROME HELPERS  (fallback when no Browserbase creds)
# ═══════════════════════════════════════════════════════════════

def _kill_chrome() -> None:
    subprocess.run(
        ["taskkill", "/F", "/IM", "chrome.exe"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    log_info("Killed existing Chrome processes")


def _is_chrome_running() -> bool:
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq chrome.exe"],
            capture_output=True, text=True, check=False,
        )
        return "chrome.exe" in result.stdout.lower()
    except Exception:
        return False


def _remove_profile_lock(user_data_dir: str) -> None:
    for rel in (
        "SingletonLock", "SingletonCookie", "SingletonSocket",
        os.path.join(CHROME_PROFILE, "SingletonLock"),
        os.path.join(CHROME_PROFILE, "SingletonCookie"),
        os.path.join(CHROME_PROFILE, "SingletonSocket"),
        os.path.join(CHROME_PROFILE, "LOCK"),
    ):
        p = os.path.join(user_data_dir, rel)
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass


def _get_chrome_exe() -> str:
    env_chrome = os.environ.get("CHROME_EXE")
    if env_chrome and os.path.exists(env_chrome):
        return env_chrome
    for exe in (CHROME_EXE, CHROME_EXE_ALT):
        if os.path.exists(exe):
            return exe
    common_paths = [
        "/usr/bin/chromium",
        "/snap/bin/chromium",
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chrome"),
    ]
    for path in common_paths:
        if path and os.path.exists(path):
            return path
    raise FileNotFoundError("Chrome not found at default locations.")


def _copy_profile_for_automation(refresh: bool = False) -> str:
    src         = os.path.join(CHROME_USER_DATA, CHROME_PROFILE)
    dst_root    = CHROME_PROFILE_CACHE
    dst_profile = os.path.join(dst_root, CHROME_PROFILE)

    if refresh and os.path.isdir(dst_root):
        log_info(f"Refreshing automation profile: {dst_root}")
        shutil.rmtree(dst_root, ignore_errors=True)

    if os.path.isdir(dst_profile):
        log_info(f"Reusing automation profile: {dst_root}")
        _remove_profile_lock(dst_root)
        return dst_root

    os.makedirs(dst_root, exist_ok=True)

    local_state = os.path.join(CHROME_USER_DATA, "Local State")
    if os.path.exists(local_state):
        shutil.copy2(local_state, os.path.join(dst_root, "Local State"))

    if not os.path.isdir(src):
        log_warn(f"Chrome profile not found: {src}")
        log_info("Creating minimal Chrome profile for automation")
        os.makedirs(dst_profile, exist_ok=True)
        prefs_path = os.path.join(dst_profile, "Preferences")
        with open(prefs_path, "w", encoding="utf-8") as f:
            json.dump({"profile": {"exit_type": "Normal", "exited_cleanly": True}}, f)
        log_ok(f"Created minimal profile at: {dst_root}")
        return dst_root

    log_info(f"Copying {CHROME_PROFILE} → {dst_root}")
    shutil.copytree(
        src, dst_profile,
        ignore=lambda _d, ns: [n for n in ns if n in CACHE_NAMES],
    )
    return dst_root


def _launch_chrome(chrome_exe: str, user_data_dir: str, port: int) -> "subprocess.Popen[bytes]":
    is_ci = os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"
    args = [
        chrome_exe,
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={CHROME_PROFILE}",
        "--remote-debugging-host=127.0.0.1",
        f"--remote-debugging-port={port}",
        "--remote-allow-origins=*",
        "--window-size=1280,800",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-popup-blocking",
        "--disable-infobars",
        "--disable-notifications",
        "--disable-dev-shm-usage",
        "--no-sandbox",
    ]
    if is_ci:
        args.extend([
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--metrics-recording-only",
            "--mute-audio",
            "--disable-setuid-sandbox",
        ])
    args.append("about:blank")
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


async def _wait_for_debug_endpoint(port: int, timeout: int = 45) -> dict:
    url  = f"http://127.0.0.1:{port}/json/version"
    loop = asyncio.get_running_loop()
    log_info(f"Waiting for Chrome debug endpoint on port {port}...")
    start_time = time.time()
    for attempt in range(timeout * 4):
        try:
            result = await loop.run_in_executor(
                None,
                lambda: json.loads(urlopen(url, timeout=2).read()),
            )
            elapsed = time.time() - start_time
            log_ok(f"Chrome debug endpoint ready after {elapsed:.1f}s")
            return result
        except Exception:
            if attempt % 20 == 0 and attempt > 0:
                elapsed = time.time() - start_time
                log_info(f"Still waiting for Chrome... ({elapsed:.1f}s)")
            await asyncio.sleep(0.25)
    elapsed = time.time() - start_time
    raise TimeoutError(f"Chrome debug endpoint never opened on port {port} after {elapsed:.1f}s")


async def _debug_endpoint_is_open(port: int) -> bool:
    try:
        await _wait_for_debug_endpoint(port, timeout=1)
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
# BROWSER STARTUP  –  Browserbase first, local Chrome fallback
# ═══════════════════════════════════════════════════════════════

async def _start_controlled_browser(
    args: argparse.Namespace,
    chrome_exe: str,
) -> tuple[Any, Any, str | None]:
    """
    Returns (browser, local_process, bb_session_id).
    bb_session_id is set only when using Browserbase.
    """
    import nodriver as uc

    bb_api_key    = os.environ.get("BROWSERBASE_API_KEY", "").strip()
    bb_project_id = os.environ.get("BROWSERBASE_PROJECT_ID", "").strip()

    # ── Browserbase path ──────────────────────────────────────────
    if bb_api_key and bb_project_id:
        log_info("Browserbase credentials detected — using cloud browser")
        session    = _bb_create_session(bb_api_key, bb_project_id)
        session_id = session["id"]
        ws_url     = session.get("connectUrl") or session.get("wsUrl", "")

        if not ws_url:
            raise RuntimeError(
                f"Browserbase session created but no connectUrl returned: {session}"
            )

        log_info(f"Connecting nodriver to Browserbase WebSocket...")
        log_info(f"  WS URL: {ws_url[:80]}...")

        # nodriver connects via the remote WebSocket endpoint
        browser = await uc.start(browser_wsEndpoint=ws_url)
        log_ok("Connected to Browserbase cloud browser")
        return browser, None, session_id

    # ── Local Chrome fallback ─────────────────────────────────────
    log_info("No Browserbase credentials — using local Chrome")
    port = args.port

    if await _debug_endpoint_is_open(port):
        log_info(f"Reusing open automation Chrome on port {port}")
        return await uc.start(host="127.0.0.1", port=port), None, None

    if args.live_profile:
        user_data_dir = CHROME_USER_DATA
        if args.kill_chrome:
            _kill_chrome()
            log_info("Waiting 4 s for Chrome to fully exit…")
            await asyncio.sleep(4)
            _remove_profile_lock(user_data_dir)
    else:
        user_data_dir = _copy_profile_for_automation(refresh=args.refresh_profile)

    log_info(f"Launching Chrome on debug port {port}")
    process = _launch_chrome(chrome_exe, user_data_dir, port)
    try:
        await _wait_for_debug_endpoint(port)
    except Exception:
        if process.poll() is None:
            process.terminate()
        raise

    return await uc.start(host="127.0.0.1", port=port), process, None


# ═══════════════════════════════════════════════════════════════
# JSON / URL HELPERS
# ═══════════════════════════════════════════════════════════════

def extract_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty page content")
    if text[0] in "{[":
        return json.loads(text)
    s = text.find("{")
    e = text.rfind("}") + 1
    if s == -1 or e <= s:
        raise ValueError("No JSON object found in page")
    return json.loads(text[s:e])


def get_play_url(data: Any) -> str | None:
    if isinstance(data, dict):
        for key in ("link", "url", "file", "src", "stream"):
            v = data.get(key)
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                return v
        for key in ("sources", "tracks", "streams"):
            items = data.get(key)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and item.startswith(("http://", "https://")):
                        return item
                    if isinstance(item, dict):
                        nested = get_play_url(item)
                        if nested:
                            return nested
    elif isinstance(data, list):
        for item in data:
            nested = get_play_url(item)
            if nested:
                return nested
    return None


async def wait_for_json_fast(page: Any, timeout: int = 60, blank_timeout: int = 1) -> str:
    """Poll every 100 ms; bail out as soon as body starts with { or [."""
    deadline = time.monotonic() + timeout
    started  = time.monotonic()
    last_text = ""
    tick = 0
    while time.monotonic() < deadline:
        await asyncio.sleep(0.1)
        tick += 1
        try:
            text = await page.evaluate("document.body.innerText")
            last_text = (text or "").strip()
            if last_text and last_text[0] in "{[":
                return last_text

            if tick % 10 == 0:
                title = await page.evaluate("document.title")
                if title == "" and time.monotonic() - started >= blank_timeout:
                    raise ValueError("Blank page stalled before JSON")
        except ValueError:
            raise
        except Exception:
            pass

        if tick % 50 == 0:
            elapsed = int(time.monotonic() - (deadline - timeout))
            try:
                title = await page.evaluate("document.title")
                print(f"      [{elapsed:02d}s] title='{title}'")
            except Exception:
                print(f"      [{elapsed:02d}s] waiting…")

    return last_text


# ═══════════════════════════════════════════════════════════════
# PER-TAB WORKER
# ═══════════════════════════════════════════════════════════════

_print_lock: asyncio.Lock | None = None


async def safe_print(*a: Any, **kw: Any) -> None:
    async with _print_lock:  # type: ignore[union-attr]
        print(*a, **kw)


async def extract_one(
    browser: Any,
    api_url: str,
    timeout: int,
    blank_timeout: int,
    reloads: int,
    sem: asyncio.Semaphore,
    index: int,
    total: int,
) -> dict[str, Any]:
    async with sem:
        label = f"[{index:>3}/{total}]"
        await safe_print(f"{label} → {api_url}")

        try:
            page = await browser.get(api_url, new_tab=True)
        except Exception as e:
            await safe_print(f"{label} ✗ open tab failed: {e}")
            return {"index": index, "api_url": api_url, "error": str(e), "extracted_url": None}

        last_error = None
        try:
            for attempt in range(reloads + 1):
                if attempt:
                    await safe_print(f"{label} ↻ reload {attempt}/{reloads}")
                    await page.reload(ignore_cache=True)
                    await asyncio.sleep(0.2)

                try:
                    text = await wait_for_json_fast(
                        page, timeout=timeout, blank_timeout=blank_timeout,
                    )

                    if not text or text[0] not in "{[":
                        text = await page.evaluate("document.body.innerHTML")

                    data     = extract_json(text)
                    play_url = get_play_url(data)

                    if play_url:
                        await safe_print(f"{label} ✓ {play_url}")
                        return {
                            "index": index,
                            "api_url": api_url,
                            "data": data,
                            "extracted_url": play_url,
                        }

                    last_error = "no URL in response"
                    await safe_print(f"{label} ✗ {last_error}")

                except Exception as e:
                    last_error = str(e)
                    await safe_print(f"{label} ✗ {last_error}")

            return {
                "index": index,
                "api_url": api_url,
                "error": last_error or "failed",
                "extracted_url": None,
            }

        finally:
            try:
                await page.close()
            except Exception:
                pass


async def process_batch(
    browser: Any,
    indexed_urls: list[tuple[int, str]],
    total: int,
    timeout: int,
    blank_timeout: int,
    reloads: int,
    title: str,
) -> list[dict[str, Any]]:
    print(f"\n{title}: opening {len(indexed_urls)} URL(s)")
    sem   = asyncio.Semaphore(max(1, len(indexed_urls)))
    tasks = [
        asyncio.create_task(
            extract_one(browser, url, timeout, blank_timeout, reloads, sem, index, total)
        )
        for index, url in indexed_urls
    ]
    return await asyncio.gather(*tasks)


async def _close_browser(browser: Any, proc: Any, bb_api_key: str, bb_session_id: str | None) -> None:
    try:
        log_info("Closing browser…")
        browser.stop()
        await asyncio.sleep(0.8)
    except Exception:
        pass
    if proc and proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
    if bb_session_id and bb_api_key:
        _bb_stop_session(bb_api_key, bb_session_id)


# ═══════════════════════════════════════════════════════════════
# STAGE 2 MAIN RUNNER
# ═══════════════════════════════════════════════════════════════

async def stage2_extract_stream_urls(
    api_list_file: Path,
    stream_out_file: Path,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    log_head("STAGE 2  –  Resolve keys → stream/embed URLs")

    global _print_lock
    _print_lock = asyncio.Lock()

    api_urls = [
        l.strip()
        for l in api_list_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
    if not api_urls:
        log_warn("api_url_list.txt is empty – nothing to resolve in Stage 2.")
        return []

    bb_api_key = os.environ.get("BROWSERBASE_API_KEY", "").strip()
    using_bb   = bool(bb_api_key and os.environ.get("BROWSERBASE_PROJECT_ID", "").strip())

    log_info(f"Browser backend     : {'Browserbase (cloud)' if using_bb else 'Local Chrome'}")
    log_info(f"API keys to resolve : {len(api_urls)}")
    log_info(f"Batch size          : {args.batch_size}")
    log_info(f"Reloads per tab     : {args.reloads}")
    log_info(f"Final retry passes  : {args.final_retries}")
    log_info(f"Tab timeout         : {args.timeout}s")

    chrome_exe = "N/A (Browserbase)" if using_bb else _get_chrome_exe()
    log_info(f"Chrome              : {chrome_exe}")

    log_info("Starting browser…")
    browser, chrome_process, bb_session_id = await _start_controlled_browser(args, chrome_exe)

    t_start = time.monotonic()
    results: list[dict[str, Any]] = []

    try:
        # Warm-up: first URL alone
        results.extend(await process_batch(
            browser, [(1, api_urls[0])], len(api_urls),
            args.timeout, args.blank_timeout, args.reloads,
            "Warm-up 1/1",
        ))

        # Remaining URLs in batches
        remaining   = list(enumerate(api_urls[1:], 2))
        batch_total = (len(remaining) + args.batch_size - 1) // args.batch_size
        for batch_num, start in enumerate(range(0, len(remaining), args.batch_size), 1):
            batch = remaining[start : start + args.batch_size]
            results.extend(await process_batch(
                browser, batch, len(api_urls),
                args.timeout, args.blank_timeout, args.reloads,
                f"Batch {batch_num}/{batch_total}",
            ))

        # Final retry passes
        for attempt in range(1, args.final_retries + 1):
            failed = [
                (item["index"], item["api_url"])
                for item in results
                if not item.get("extracted_url")
            ]
            if not failed:
                break
            retry_results  = await process_batch(
                browser, failed, len(api_urls),
                args.timeout, args.blank_timeout, 0,
                f"Final retry {attempt}/{args.final_retries}",
            )
            retry_by_index = {r["index"]: r for r in retry_results}
            results = [
                retry_by_index.get(item["index"], item)
                if not item.get("extracted_url")
                else item
                for item in results
            ]

    finally:
        if not args.keep_open:
            await _close_browser(browser, chrome_process, bb_api_key, bb_session_id)

    results.sort(key=lambda r: r.get("index", 0))

    elapsed = time.monotonic() - t_start
    ok      = [r for r in results if r.get("extracted_url")]
    fails   = [r for r in results if not r.get("extracted_url")]

    log_head(f"STAGE 2 RESULTS  ({elapsed:.1f}s total)")
    for item in results:
        if item.get("extracted_url"):
            log_ok(item["extracted_url"])
        else:
            log_err(f"FAILED : {item['api_url']}  ({item.get('error', 'no URL')})")

    log_info(f"Success : {len(ok)} / {len(results)}    Failed : {len(fails)}")

    # Write plain text output
    stream_out_file.write_text(
        "\n".join(r["extracted_url"] for r in ok) + "\n",
        encoding="utf-8",
    )

    return results


# ═══════════════════════════════════════════════════════════════
# TMDB TITLE LOOKUP
# ═══════════════════════════════════════════════════════════════

TMDB_API_KEY = "6fad3f86b8452ee232deb7977d7dcf58"


def _tmdb_request(path: str) -> dict:
    base = "https://api.themoviedb.org/3"
    sep  = "&" if "?" in path else "?"
    url  = f"{base}{path}{sep}language=en-US"
    if TMDB_API_KEY:
        url += f"&api_key={TMDB_API_KEY}"
    req = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    })
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _fetch_tmdb_info(tmdb_id: str) -> tuple[str, str]:
    title   = ""
    imdb_id = None
    try:
        data    = _tmdb_request(f"/movie/{tmdb_id}")
        title   = data.get("title") or data.get("original_title") or ""
        imdb_id = data.get("imdb_id") or None
        if not imdb_id:
            ext     = _tmdb_request(f"/movie/{tmdb_id}/external_ids")
            imdb_id = ext.get("imdb_id") or None
    except Exception as exc:
        log_warn(f"TMDB info fetch failed for tmdb={tmdb_id}: {exc}")
    return title, imdb_id


# ═══════════════════════════════════════════════════════════════
# GZIP / BASE64 COMPRESSOR
# ═══════════════════════════════════════════════════════════════

def _to_gz_b64_json(pretty_path: Path, gz_path: Path) -> None:
    raw     = pretty_path.read_bytes()
    gz      = gzip.compress(raw, compresslevel=9)
    b64     = base64.b64encode(gz).decode("ascii")
    wrapper = {"encoding": "gzip+base64", "source_file": pretty_path.name, "compressed": b64}
    gz_path.write_text(json.dumps(wrapper, ensure_ascii=False), encoding="utf-8")
    log_ok(f"Compressed JSON → {gz_path}  ({len(raw):,} B → {len(gz):,} B gz → {len(b64):,} B b64)")


# ═══════════════════════════════════════════════════════════════
# SUMMARY WRITER
# ═══════════════════════════════════════════════════════════════

def _format_summary_json(records: list[dict[str, Any]]) -> str:
    import re as _re

    def _jv(v: Any) -> str:
        return json.dumps(v, ensure_ascii=False)

    lines: list[str] = ["["]
    for rec_idx, rec in enumerate(records):
        lines.append("  {")
        header_keys = ["serial", "title", "tmdb_id", "imdb_id", "extracted_at"]
        n_sources   = sum(1 for k in rec if _re.fullmatch(r"host-\d+", k))

        all_field_lines: list[str] = []
        for hk in header_keys:
            if hk in rec:
                all_field_lines.append(f'    {_jv(hk)}: {_jv(rec[hk])}')

        for n in range(1, n_sources + 1):
            hkey      = f"host-{n}"
            ukey      = f"url-{n}"
            host_part = f'{_jv(hkey)}: {_jv(rec.get(hkey, ""))}'
            url_part  = f'{_jv(ukey)}: {_jv(rec.get(ukey, ""))}'
            all_field_lines.append(f"    {host_part}, {url_part}")

        is_last_rec = rec_idx == len(records) - 1
        for fi, fl in enumerate(all_field_lines):
            is_last_field = fi == len(all_field_lines) - 1
            lines.append(fl if is_last_field else fl + ",")

        lines.append("  }" if is_last_rec else "  },")

    lines.append("]")
    return "\n".join(lines) + "\n"


def _write_summary(
    stage1_options: list[ServerOption],
    stage2_results: list[dict[str, Any]],
    json_path: Path,
    html_path: Path,
) -> None:
    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}

    new_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups[tmdb].append({"host": urlparse(stream_url).netloc, "url": stream_url})

    existing: list[dict[str, Any]] = []
    if json_path.exists():
        try:
            existing = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(existing, list):
                existing = []
            log_info(f"Loaded {len(existing)} existing entries from {json_path}")
        except Exception as exc:
            log_warn(f"Could not load existing JSON ({exc}) — starting fresh")

    index: dict[int, dict[str, Any]] = {}
    for e in existing:
        tmdb_int = e["tmdb_id"]
        sources: list[dict[str, str]] = []
        n = 1
        while f"host-{n}" in e:
            sources.append({"host": e[f"host-{n}"], "url": e[f"url-{n}"]})
            n += 1
        index[tmdb_int] = {
            "tmdb_id":      tmdb_int,
            "imdb_id":      e.get("imdb_id"),
            "title":        e.get("title", ""),
            "extracted_at": e["extracted_at"],
            "_sources":     sources,
        }

    extracted_at    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry        = index[tmdb_int]
            existing_urls = {s["url"] for s in entry["_sources"]}
            added        = [s for s in new_sources if s["url"] not in existing_urls]
            entry["_sources"].extend(added)
            entry["extracted_at"] = extracted_at
            log_info(f"  tmdb={tmdb_int} — merged {len(added)} new source(s)")
        else:
            if tmdb_int not in tmdb_meta_cache:
                log_info(f"  tmdb={tmdb_int} — fetching title + imdb_id…")
                title, imdb_id = _fetch_tmdb_info(tmdb_str)
                tmdb_meta_cache[tmdb_int] = (title, imdb_id)
                log_ok(f"  tmdb={tmdb_int} — '{title}'  imdb={imdb_id}")
            else:
                title, imdb_id = tmdb_meta_cache[tmdb_int]
            index[tmdb_int] = {
                "tmdb_id":      tmdb_int,
                "imdb_id":      imdb_id,
                "title":        title,
                "extracted_at": extracted_at,
                "_sources":     list(new_sources),
            }
            log_ok(f"  tmdb={tmdb_int} — '{title}'  sources: {len(new_sources)}")

    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb_id":      e["tmdb_id"],
            "imdb_id":      e.get("imdb_id"),
            "extracted_at": e["extracted_at"],
        }
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["host"]
            row[f"url-{n}"]  = src["url"]
        output.append(row)

    json_path.write_text(_format_summary_json(output), encoding="utf-8")
    log_ok(f"Pretty JSON → {json_path}")
    total_sources = sum(sum(1 for k in row if k.startswith("url-")) for row in output)
    log_info(f"Movies : {len(output)}   Sources : {total_sources}")

    gz_path = json_path.with_suffix("").with_suffix(".gz.json")
    _to_gz_b64_json(json_path, gz_path)


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PrimeSrc unified pipeline (Browserbase edition)"
    )
    p.add_argument("--input",           type=Path, default=DEFAULT_INPUT_FILE)
    p.add_argument("--api-list",        type=Path, default=DEFAULT_API_LIST)
    p.add_argument("--output",          type=Path, default=DEFAULT_STREAM_OUT)
    p.add_argument("--json-out",        type=Path, default=DEFAULT_JSON_SUMMARY)
    p.add_argument("--html-out",        type=Path, default=DEFAULT_HTML_OUT)
    p.add_argument("--skip-stage1",     action="store_true")
    p.add_argument("--skip-stage2",     action="store_true")
    p.add_argument("--type",            choices=("movie", "tv"), default="movie")
    p.add_argument("--port",            type=int, default=CHROME_DEBUG_PORT)
    p.add_argument("--timeout",         type=int, default=STAGE2_PAGE_TIMEOUT)
    p.add_argument("--blank-timeout",   type=int, default=STAGE2_BLANK_TIMEOUT)
    p.add_argument("--batch-size",      type=int, default=STAGE2_BATCH_SIZE, dest="batch_size")
    p.add_argument("--reloads",         type=int, default=STAGE2_RELOADS)
    p.add_argument("--final-retries",   type=int, default=STAGE2_FINAL_RETRIES, dest="final_retries")
    p.add_argument("--live-profile",    action="store_true", dest="live_profile")
    p.add_argument("--kill-chrome",     action="store_true", dest="kill_chrome")
    p.add_argument("--refresh-profile", action="store_true", dest="refresh_profile")
    p.add_argument("--keep-open",       action="store_true", dest="keep_open")
    return p.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    log_head("PrimeSRC UNIFIED PIPELINE  (Browserbase edition)")
    log_info(f"Input   : {args.input}")
    log_info(f"API list: {args.api_list}")
    log_info(f"Output  : {args.output}")

    bb_key = os.environ.get("BROWSERBASE_API_KEY", "")
    bb_pid = os.environ.get("BROWSERBASE_PROJECT_ID", "")
    if bb_key and bb_pid:
        log_ok("Browserbase credentials found — cloud browser will be used")
    else:
        log_warn("No Browserbase credentials — falling back to local Chrome")

    stage1_options: list[ServerOption] = []
    stage2_results: list[dict[str, Any]] = []

    if args.skip_stage1:
        log_info("Stage 1 skipped — using existing api_url_list.txt")
    else:
        if not args.input.exists():
            log_err(f"Input file not found: {args.input}")
            return 1
        stage1_options = stage1_fetch_api_keys(args.input, args.api_list, args.type)

    if args.skip_stage2:
        log_info("Stage 2 skipped.")
    else:
        if not args.api_list.exists():
            log_err(f"API list not found: {args.api_list}")
            return 1
        try:
            stage2_results = await stage2_extract_stream_urls(
                args.api_list, args.output, args
            )
        except ImportError:
            log_err("nodriver not installed.  Run:  pip install nodriver")
            return 2

    if stage1_options or stage2_results:
        if not stage1_options and args.api_list.exists():
            for line in args.api_list.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key = line.split("key=")[-1] if "key=" in line else ""
                stage1_options.append(ServerOption("", key, line, ""))
        _write_summary(stage1_options, stage2_results, args.json_out, args.html_out)

    log_head("DONE")
    if not args.skip_stage2 and stage2_results:
        ok = sum(1 for r in stage2_results if r.get("extracted_url"))
        log_ok(f"Stream URLs extracted : {ok} / {len(stage2_results)}")
        log_ok(f"Results written to    : {args.output}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
