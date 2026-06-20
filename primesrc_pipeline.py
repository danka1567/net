"""
primesrc_pipeline.py  –  Unified PrimeSrc pipeline (Browserbase + Playwright edition)
======================================================================================
Stage 1  – fetch /api/v1/s for every tmdb embed URL → api_url_list.txt
Stage 2  – open every /api/v1/l?key=… via Browserbase (playwright) or local Chrome (nodriver)
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

TMDB_ID_RE   = re.compile(r"^\d+$")
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

def _c(text, colour):
    try:
        return colour + text + _RESET if sys.stdout.isatty() else text
    except Exception:
        return text

def log_info(msg): print(_c(f"[INFO]  {msg}", _CYAN))
def log_ok(msg):   print(_c(f"[OK]    {msg}", _GREEN))
def log_warn(msg): print(_c(f"[WARN]  {msg}", _YELLOW))
def log_err(msg):  print(_c(f"[ERR]   {msg}", _RED))
def log_head(msg): print(_c(f"\n{'='*60}\n{msg}\n{'='*60}", _BOLD))

# ═══════════════════════════════════════════════════════════════
# STAGE 1
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


def _build_server_api_url(main_url):
    parsed = urlparse(main_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if parsed.path.startswith("/embed/movie"):
        params.setdefault("type", "movie")
    elif parsed.path.startswith("/embed/tv"):
        params.setdefault("type", "tv")
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'primesrc.me'}"
    return f"{base}/api/v1/s?{urlencode(params)}"


def _fetch_json_http(url, referer):
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, */*",
        "Referer": referer,
    })
    with urlopen(req, timeout=STAGE1_REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return json.loads(resp.read().decode(charset, errors="replace"))


def _normalise_embed_url(raw, media_type="movie"):
    raw = raw.strip()
    if TMDB_ID_RE.fullmatch(raw):
        return f"https://primesrc.me/embed/{media_type}?tmdb={raw}"
    if raw.startswith("primesrc.me/"):
        return "https://" + raw
    if raw.startswith("/embed/"):
        return "https://primesrc.me" + raw
    return raw


def _find_server_lists(obj):
    lists = []
    if isinstance(obj, dict):
        servers = obj.get("servers")
        if isinstance(servers, list) and servers:
            if any("key" in item or "file_name" in item for item in servers if isinstance(item, dict)):
                lists.append({"servers": servers, "info": obj.get("info") or {}})
        for v in obj.values():
            lists.extend(_find_server_lists(v))
    elif isinstance(obj, list):
        for item in obj:
            lists.extend(_find_server_lists(item))
    return lists


def _options_from_server_list(servers, main_url):
    options = []
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


def stage1_fetch_api_keys(input_file, api_list_file, media_type="movie"):
    log_head("STAGE 1  –  Fetch server keys from PrimeSrc /api/v1/s")
    raw_lines = [l.strip() for l in input_file.read_text(encoding="utf-8").splitlines()
                 if l.strip() and not l.startswith("#")]
    log_info(f"Input embed URLs : {len(raw_lines)}  ({input_file})")

    seen_urls, embed_urls = set(), []
    for raw in raw_lines:
        url = _normalise_embed_url(raw, media_type)
        if url not in seen_urls:
            seen_urls.add(url)
            embed_urls.append(url)

    all_options, errors = [], []
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
                all_options.extend(_options_from_server_list(sl.get("servers", []), embed_url))
            count = sum(len(_options_from_server_list(sl.get("servers", []), embed_url)) for sl in server_lists)
            log_ok(f"{label} {count} keys  {embed_url}")
        except Exception as exc:
            errors.append((embed_url, str(exc)))
            log_err(f"{label} {exc}  {embed_url}")

    seen_api, unique_options = set(), []
    for opt in all_options:
        if opt.api_url not in seen_api:
            seen_api.add(opt.api_url)
            unique_options.append(opt)

    api_list_file.write_text("\n".join(opt.api_url for opt in unique_options) + "\n", encoding="utf-8")
    log_info(f"Total keys : {len(all_options)}  (unique: {len(unique_options)})")
    log_info(f"Errors     : {len(errors)}")
    log_ok(f"Written → {api_list_file}")
    return unique_options


# ═══════════════════════════════════════════════════════════════
# BROWSERBASE HELPERS
# ═══════════════════════════════════════════════════════════════

def _bb_request(method, path, api_key, body=None):
    url  = f"{BROWSERBASE_API}{path}"
    data = json.dumps(body).encode() if body else None
    req  = Request(url, data=data, headers={
        "Content-Type": "application/json",
        "x-bb-api-key": api_key,
    }, method=method)
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _bb_create_session(api_key, project_id):
    log_info("Creating Browserbase session...")
    session = _bb_request("POST", "/sessions", api_key, {
        "projectId": project_id,
        "browserSettings": {
            "fingerprint": {
                "browsers":         ["chrome"],
                "devices":          ["desktop"],
                "operatingSystems": ["windows"],
                "locales":          ["en-US"],
            },
            "viewport": {"width": 1280, "height": 800},
        },
    })
    log_ok(f"Browserbase session created: {session['id']}")
    return session


def _bb_stop_session(api_key, session_id):
    try:
        _bb_request("POST", f"/sessions/{session_id}", api_key, {"status": "REQUEST_RELEASE"})
        log_info(f"Browserbase session released: {session_id}")
    except Exception as e:
        log_warn(f"Could not release Browserbase session: {e}")


# ═══════════════════════════════════════════════════════════════
# STAGE 2  –  BROWSERBASE PATH  (uses Playwright)
# ═══════════════════════════════════════════════════════════════

async def _extract_urls_browserbase(api_urls, args):
    """Use Playwright to connect to Browserbase and extract stream URLs."""
    from playwright.async_api import async_playwright  # type: ignore

    bb_api_key    = os.environ["BROWSERBASE_API_KEY"].strip()
    bb_project_id = os.environ["BROWSERBASE_PROJECT_ID"].strip()

    session = _bb_create_session(bb_api_key, bb_project_id)
    session_id = session["id"]
    ws_url     = session.get("connectUrl") or session.get("wsUrl", "")

    if not ws_url:
        raise RuntimeError(f"Browserbase gave no connectUrl: {session}")

    log_info(f"Connecting Playwright to Browserbase...")
    log_info(f"  Session: {session_id}")

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(ws_url)
        log_ok("Connected to Browserbase cloud browser via Playwright")

        context = browser.contexts[0] if browser.contexts else await browser.new_context()

        for idx, api_url in enumerate(api_urls, 1):
            label = f"[{idx:>3}/{len(api_urls)}]"
            print(f"{label} → {api_url}")

            extracted_url = None
            last_error    = None

            for attempt in range(args.reloads + 1):
                if attempt:
                    print(f"{label} ↻ reload {attempt}/{args.reloads}")

                page = await context.new_page()
                try:
                    # Wait for the page to load and get JSON
                    response = await page.goto(api_url, timeout=args.timeout * 1000, wait_until="domcontentloaded")

                    # Poll for JSON content
                    deadline = time.monotonic() + args.timeout
                    text     = ""
                    while time.monotonic() < deadline:
                        await asyncio.sleep(0.5)
                        try:
                            text = await page.evaluate("document.body.innerText")
                            text = (text or "").strip()
                            if text and text[0] in "{[":
                                break
                            title = await page.title()
                            if "Just a moment" not in title and text:
                                break
                        except Exception:
                            pass

                    if not text or text[0] not in "{[":
                        # Try innerHTML fallback
                        try:
                            text = await page.evaluate("document.body.innerHTML")
                        except Exception:
                            pass

                    if text:
                        data = extract_json(text)
                        play_url = get_play_url(data)
                        if play_url:
                            print(f"{label} ✓ {play_url}")
                            extracted_url = play_url
                            break
                        else:
                            last_error = "no URL in response"
                            print(f"{label} ✗ {last_error}")
                    else:
                        last_error = "empty response"
                        print(f"{label} ✗ {last_error}")

                except Exception as e:
                    last_error = str(e)
                    print(f"{label} ✗ {last_error}")
                finally:
                    try:
                        await page.close()
                    except Exception:
                        pass

            results.append({
                "index":         idx,
                "api_url":       api_url,
                "extracted_url": extracted_url,
                "error":         last_error if not extracted_url else None,
            })

        await browser.close()

    _bb_stop_session(bb_api_key, session_id)
    return results


# ═══════════════════════════════════════════════════════════════
# STAGE 2  –  LOCAL CHROME PATH  (uses nodriver, fallback)
# ═══════════════════════════════════════════════════════════════

def _get_chrome_exe():
    env_chrome = os.environ.get("CHROME_EXE")
    if env_chrome and os.path.exists(env_chrome):
        return env_chrome
    for exe in (CHROME_EXE, CHROME_EXE_ALT):
        if os.path.exists(exe):
            return exe
    for path in ["/usr/bin/chromium", "/snap/bin/chromium",
                 shutil.which("google-chrome"), shutil.which("chromium"), shutil.which("chrome")]:
        if path and os.path.exists(path):
            return path
    raise FileNotFoundError("Chrome not found.")


def _remove_profile_lock(user_data_dir):
    for rel in ("SingletonLock", "SingletonCookie", "SingletonSocket",
                os.path.join(CHROME_PROFILE, "SingletonLock"),
                os.path.join(CHROME_PROFILE, "LOCK")):
        p = os.path.join(user_data_dir, rel)
        if os.path.exists(p):
            try: os.remove(p)
            except Exception: pass


def _copy_profile_for_automation(refresh=False):
    src         = os.path.join(CHROME_USER_DATA, CHROME_PROFILE)
    dst_root    = CHROME_PROFILE_CACHE
    dst_profile = os.path.join(dst_root, CHROME_PROFILE)

    if refresh and os.path.isdir(dst_root):
        shutil.rmtree(dst_root, ignore_errors=True)

    if os.path.isdir(dst_profile):
        _remove_profile_lock(dst_root)
        return dst_root

    os.makedirs(dst_root, exist_ok=True)
    local_state = os.path.join(CHROME_USER_DATA, "Local State")
    if os.path.exists(local_state):
        shutil.copy2(local_state, os.path.join(dst_root, "Local State"))

    if not os.path.isdir(src):
        os.makedirs(dst_profile, exist_ok=True)
        with open(os.path.join(dst_profile, "Preferences"), "w") as f:
            json.dump({"profile": {"exit_type": "Normal", "exited_cleanly": True}}, f)
        return dst_root

    shutil.copytree(src, dst_profile, ignore=lambda _d, ns: [n for n in ns if n in CACHE_NAMES])
    return dst_root


def _launch_chrome(chrome_exe, user_data_dir, port):
    is_ci = os.environ.get("CI") == "true"
    args  = [
        chrome_exe,
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={CHROME_PROFILE}",
        "--remote-debugging-host=127.0.0.1",
        f"--remote-debugging-port={port}",
        "--remote-allow-origins=*",
        "--window-size=1280,800",
        "--no-first-run", "--no-default-browser-check",
        "--disable-popup-blocking", "--disable-infobars",
        "--disable-notifications", "--disable-dev-shm-usage", "--no-sandbox",
    ]
    if is_ci:
        args.extend(["--disable-gpu", "--disable-software-rasterizer",
                     "--disable-extensions", "--mute-audio", "--disable-setuid-sandbox"])
    args.append("about:blank")
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


async def _wait_for_debug_endpoint(port, timeout=45):
    url  = f"http://127.0.0.1:{port}/json/version"
    loop = asyncio.get_running_loop()
    log_info(f"Waiting for Chrome debug endpoint on port {port}...")
    for attempt in range(timeout * 4):
        try:
            result = await loop.run_in_executor(None, lambda: json.loads(urlopen(url, timeout=2).read()))
            log_ok(f"Chrome ready")
            return result
        except Exception:
            await asyncio.sleep(0.25)
    raise TimeoutError(f"Chrome debug endpoint never opened on port {port}")


def extract_json(text):
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


def get_play_url(data):
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


async def wait_for_json_fast(page, timeout=60, blank_timeout=1):
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
                    raise ValueError("Blank page stalled")
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


_print_lock = None


async def safe_print(*a, **kw):
    async with _print_lock:
        print(*a, **kw)


async def extract_one_nodriver(browser, api_url, timeout, blank_timeout, reloads, sem, index, total):
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
                    text     = await wait_for_json_fast(page, timeout=timeout, blank_timeout=blank_timeout)
                    if not text or text[0] not in "{[":
                        text = await page.evaluate("document.body.innerHTML")
                    data     = extract_json(text)
                    play_url = get_play_url(data)
                    if play_url:
                        await safe_print(f"{label} ✓ {play_url}")
                        return {"index": index, "api_url": api_url, "data": data, "extracted_url": play_url}
                    last_error = "no URL in response"
                    await safe_print(f"{label} ✗ {last_error}")
                except Exception as e:
                    last_error = str(e)
                    await safe_print(f"{label} ✗ {last_error}")
            return {"index": index, "api_url": api_url, "error": last_error or "failed", "extracted_url": None}
        finally:
            try:
                await page.close()
            except Exception:
                pass


async def _extract_urls_local_chrome(api_urls, args):
    """Fallback: use nodriver with local Chrome."""
    import nodriver as uc

    chrome_exe    = _get_chrome_exe()
    user_data_dir = _copy_profile_for_automation(refresh=args.refresh_profile)
    log_info(f"Launching local Chrome: {chrome_exe}")
    process = _launch_chrome(chrome_exe, user_data_dir, args.port)
    await _wait_for_debug_endpoint(args.port)
    browser = await uc.start(host="127.0.0.1", port=args.port)

    results = []
    try:
        sem   = asyncio.Semaphore(args.batch_size)
        tasks = [
            asyncio.create_task(
                extract_one_nodriver(browser, url, args.timeout, args.blank_timeout,
                                     args.reloads, sem, idx, len(api_urls))
            )
            for idx, url in enumerate(api_urls, 1)
        ]
        results = await asyncio.gather(*tasks)
    finally:
        try:
            browser.stop()
        except Exception:
            pass
        if process.poll() is None:
            process.terminate()

    return list(results)


# ═══════════════════════════════════════════════════════════════
# STAGE 2 MAIN RUNNER
# ═══════════════════════════════════════════════════════════════

async def stage2_extract_stream_urls(api_list_file, stream_out_file, args):
    log_head("STAGE 2  –  Resolve keys → stream/embed URLs")

    global _print_lock
    _print_lock = asyncio.Lock()

    api_urls = [
        l.strip()
        for l in api_list_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
    if not api_urls:
        log_warn("api_url_list.txt is empty.")
        return []

    bb_api_key    = os.environ.get("BROWSERBASE_API_KEY", "").strip()
    bb_project_id = os.environ.get("BROWSERBASE_PROJECT_ID", "").strip()
    using_bb      = bool(bb_api_key and bb_project_id)

    log_info(f"Browser backend     : {'Browserbase (Playwright)' if using_bb else 'Local Chrome (nodriver)'}")
    log_info(f"API keys to resolve : {len(api_urls)}")
    log_info(f"Tab timeout         : {args.timeout}s")
    log_info(f"Reloads per tab     : {args.reloads}")

    t_start = time.monotonic()

    if using_bb:
        results = await _extract_urls_browserbase(api_urls, args)
    else:
        results = await _extract_urls_local_chrome(api_urls, args)

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

    stream_out_file.write_text(
        "\n".join(r["extracted_url"] for r in ok) + "\n", encoding="utf-8"
    )
    return results


# ═══════════════════════════════════════════════════════════════
# TMDB LOOKUP
# ═══════════════════════════════════════════════════════════════

TMDB_API_KEY = "6fad3f86b8452ee232deb7977d7dcf58"


def _tmdb_request(path):
    base = "https://api.themoviedb.org/3"
    sep  = "&" if "?" in path else "?"
    url  = f"{base}{path}{sep}language=en-US"
    if TMDB_API_KEY:
        url += f"&api_key={TMDB_API_KEY}"
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 Chrome/124.0.0.0",
        "Accept": "application/json",
    })
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _fetch_tmdb_info(tmdb_id):
    title = ""
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
# GZIP / BASE64
# ═══════════════════════════════════════════════════════════════

def _to_gz_b64_json(pretty_path, gz_path):
    raw     = pretty_path.read_bytes()
    gz      = gzip.compress(raw, compresslevel=9)
    b64     = base64.b64encode(gz).decode("ascii")
    wrapper = {"encoding": "gzip+base64", "source_file": pretty_path.name, "compressed": b64}
    gz_path.write_text(json.dumps(wrapper, ensure_ascii=False), encoding="utf-8")
    log_ok(f"Compressed JSON → {gz_path}  ({len(raw):,} B → {len(gz):,} B gz)")


# ═══════════════════════════════════════════════════════════════
# SUMMARY WRITER
# ═══════════════════════════════════════════════════════════════

def _format_summary_json(records):
    def _jv(v):
        return json.dumps(v, ensure_ascii=False)
    lines = ["["]
    for rec_idx, rec in enumerate(records):
        lines.append("  {")
        header_keys = ["serial", "title", "tmdb_id", "imdb_id", "extracted_at"]
        n_sources   = sum(1 for k in rec if re.fullmatch(r"host-\d+", k))
        all_field_lines = []
        for hk in header_keys:
            if hk in rec:
                all_field_lines.append(f'    {_jv(hk)}: {_jv(rec[hk])}')
        for n in range(1, n_sources + 1):
            host_part = f'{_jv(f"host-{n}")}: {_jv(rec.get(f"host-{n}", ""))}'
            url_part  = f'{_jv(f"url-{n}")}: {_jv(rec.get(f"url-{n}", ""))}'
            all_field_lines.append(f"    {host_part}, {url_part}")
        is_last_rec = rec_idx == len(records) - 1
        for fi, fl in enumerate(all_field_lines):
            lines.append(fl if fi == len(all_field_lines) - 1 else fl + ",")
        lines.append("  }" if is_last_rec else "  },")
    lines.append("]")
    return "\n".join(lines) + "\n"


def _write_summary(stage1_options, stage2_results, json_path, html_path):
    link_map   = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}
    new_groups = defaultdict(list)

    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups[tmdb].append({"host": urlparse(stream_url).netloc, "url": stream_url})

    existing = []
    if json_path.exists():
        try:
            existing = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(existing, list):
                existing = []
            log_info(f"Loaded {len(existing)} existing entries from {json_path}")
        except Exception as exc:
            log_warn(f"Could not load existing JSON ({exc}) — starting fresh")

    index = {}
    for e in existing:
        tmdb_int = e["tmdb_id"]
        sources, n = [], 1
        while f"host-{n}" in e:
            sources.append({"host": e[f"host-{n}"], "url": e[f"url-{n}"]})
            n += 1
        index[tmdb_int] = {
            "tmdb_id": tmdb_int, "imdb_id": e.get("imdb_id"),
            "title": e.get("title", ""), "extracted_at": e["extracted_at"],
            "_sources": sources,
        }

    extracted_at    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache = {}

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry         = index[tmdb_int]
            existing_urls = {s["url"] for s in entry["_sources"]}
            added         = [s for s in new_sources if s["url"] not in existing_urls]
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
                "tmdb_id": tmdb_int, "imdb_id": imdb_id, "title": title,
                "extracted_at": extracted_at, "_sources": list(new_sources),
            }

    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    output = []
    for e in sorted_entries:
        row = {"serial": e["serial"], "title": e.get("title", ""),
               "tmdb_id": e["tmdb_id"], "imdb_id": e.get("imdb_id"),
               "extracted_at": e["extracted_at"]}
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["host"]
            row[f"url-{n}"]  = src["url"]
        output.append(row)

    json_path.write_text(_format_summary_json(output), encoding="utf-8")
    log_ok(f"Pretty JSON → {json_path}")
    total_sources = sum(sum(1 for k in row if k.startswith("url-")) for row in output)
    log_info(f"Movies : {len(output)}   Sources : {total_sources}")
    _to_gz_b64_json(json_path, json_path.with_suffix("").with_suffix(".gz.json"))


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="PrimeSrc unified pipeline (Browserbase + Playwright)")
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
    return p.parse_args(argv if argv is not None else sys.argv[1:])


async def _run(args):
    log_head("PrimeSRC UNIFIED PIPELINE  (Browserbase + Playwright edition)")
    log_info(f"Input   : {args.input}")
    log_info(f"API list: {args.api_list}")
    log_info(f"Output  : {args.output}")

    bb_key = os.environ.get("BROWSERBASE_API_KEY", "")
    bb_pid = os.environ.get("BROWSERBASE_PROJECT_ID", "")
    if bb_key and bb_pid:
        log_ok("Browserbase credentials found — Playwright cloud browser will be used")
    else:
        log_warn("No Browserbase credentials — falling back to local Chrome (nodriver)")

    stage1_options, stage2_results = [], []

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
        stage2_results = await stage2_extract_stream_urls(args.api_list, args.output, args)

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


def main(argv=None):
    args = _parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
