# yt_fresh_simple_bot.py (v2.4, RSS + Pages + автообновление + age)
# Команды:
#   /fresh [часы]     — разовый сбор (по умолчанию 12 часов)
#   /auto M [H]       — автообновление каждые M минут (по умолчанию H=12 часов)
#   /stopauto         — остановить автообновление
#
# Что делает:
# - Для каждого канала определяем UCID со страницы канала (ytInitialData.metadata.channelMetadataRenderer.externalId),
#   фоллбэк на canonical/og:url.
# - Читаем публичный RSS https://www.youtube.com/feeds/videos.xml?channel_id=UCID (есть и видео, и Shorts).
# - Фильтруем по времени публикации (UTC), отправляем нумерованный список, показываем «возраст».
# - Пишем latest_results.json локально и (если есть github_token.txt/github_repo.txt) публикуем его на GitHub Pages.
# - В авто-режиме запоминаем уже увиденные видео (seen_ids.json) и присылаем уведомление, если есть новые.
#
# Требования:
#   pip install requests python-telegram-bot==21.6
#
# Файлы рядом со скриптом:
#   bot_token.txt        — токен бота Telegram (одна строка)
#   channels.txt         — список каналов (@handle, ссылка, или UC... по одному в строке)
#   results.html         — (опционально) страница для GitHub Pages
#   github_token.txt     — (опционально) GitHub Personal Access Token (repo scope)
#   github_repo.txt      — (опционально) owner/repo
#   github_branch.txt    — (опционально) имя ветки (по умолчанию main)
#
import logging
import re
import time
import xml.etree.ElementTree as ET
import json as _json
import base64 as _b64
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

BASE = Path(__file__).resolve().parent
TOKEN_FILE = BASE / "bot_token.txt"
CHANNELS_FILE = BASE / "channels.txt"
GITHUB_TOKEN_FILE = BASE / "github_token.txt"
GITHUB_REPO_FILE = BASE / "github_repo.txt"
GITHUB_BRANCH_FILE = BASE / "github_branch.txt"
SEEN_FILE = BASE / "seen_ids.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("yt-fresh-rss-bot")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cookie": "CONSENT=YES+1; SOCS=CAI"  # EU consent bypass
}
RSS_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "application/atom+xml,text/xml;q=0.9,*/*;q=0.8",
}

TIMEOUT = 10
RETRIES = 2
SLEEP = 0.4  # пауза между каналами

# ---------- HTTP ----------
def http_get(url: str, headers: Dict[str, str]) -> requests.Response:
    last = None
    for i in range(RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
            if resp.status_code == 200 and resp.text:
                return resp
            time.sleep(0.3 + i * 0.4)
        except Exception as e:
            last = e
            time.sleep(0.4 + i * 0.5)
    if last:
        raise last
    raise RuntimeError(f"HTTP error for {url}")

# ---------- JSON extractor from HTML ----------
def extract_json_object(html: str, key: str):
    idx = html.find(key)
    if idx == -1:
        return None
    start = html.find("{", idx)
    if start == -1:
        return None
    depth = 0
    i = start
    while i < len(html):
        ch = html[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return _json.loads(html[start:i + 1])
                except Exception:
                    try:
                        cleaned = html[start:i + 1].encode("utf-8", "ignore").decode("utf-8", "ignore")
                        return _json.loads(cleaned)
                    except Exception:
                        return None
        i += 1
    return None

# ---------- UCID resolution (строго со страницы канала) ----------
def normalize_to_channel_base(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    if re.fullmatch(r"UC[0-9A-Za-z_-]{22}", s):
        return f"https://www.youtube.com/channel/{s}"
    if s.startswith("@"):
        return f"https://www.youtube.com/{s}"
    if s.startswith("youtube.com"):
        s = "https://" + s
    if s.startswith("http"):
        s = s.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        KNOWN = {"videos", "shorts", "streams", "live", "playlists", "community", "about", "featured", "search", "popular"}
        parts = s.split("/")
        if parts and parts[-1].lower() in KNOWN:
            s = "/".join(parts[:-1])
        return s
    return None

def resolve_ucid(channel_ref: str) -> Optional[str]:
    """Возвращает UCID для @handle/любой канальной ссылки/UC... с приоритетом ДАННОЙ страницы канала."""
    if re.fullmatch(r"UC[0-9A-Za-z_-]{22}", channel_ref or ""):
        return channel_ref

    base = normalize_to_channel_base(channel_ref or "")
    if not base:
        return None

    def try_page(url: str) -> Optional[str]:
        try:
            resp = http_get(f"{url}?hl=ru&persist_hl=1", HEADERS)
            html = resp.text
        except Exception:
            return None
        # 1) ytInitialData.metadata.channelMetadataRenderer.externalId
        data = extract_json_object(html, "ytInitialData")
        if isinstance(data, dict):
            try:
                ucid = data.get("metadata", {}).get("channelMetadataRenderer", {}).get("externalId")
                if isinstance(ucid, str) and re.fullmatch(r"UC[0-9A-Za-z_-]{22}", ucid):
                    return ucid
            except Exception:
                pass
        # 2) rel="canonical" ... /channel/UC...
        m = re.search(r'rel=["\']canonical["\'][^>]*href=["\'][^"\']*/channel/(UC[0-9A-Za-z_-]{22})["\']', html, re.I)
        if m:
            return m.group(1)
        # 3) property="og:url" content=".../channel/UC..."
        m = re.search(r'property=["\']og:url["\'][^>]*content=["\'][^"\']*/channel/(UC[0-9A-Za-z_-]{22})["\']', html, re.I)
        if m:
            return m.group(1)
        return None

    for suff in ("", "/about", "/videos"):
        ucid = try_page(base + suff)
        if ucid:
            return ucid
    return None

# ---------- RSS parsing ----------
ATOM = "{http://www.w3.org/2005/Atom}"
YT = "{http://www.youtube.com/xml/schemas/2015}"
YT_OLD = "{http://gdata.youtube.com/schemas/2007}"

def parse_rfc3339(dt: str) -> Optional[datetime]:
    if not dt:
        return None
    s = dt.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def fetch_fresh_from_rss(ucid: str, hours: int) -> Tuple[str, List[Tuple[str, str, datetime]]]:
    """Возвращает (channel_title, [ (title, link, when_utc) ... ]) для роликов новее чем N часов (включая Shorts)."""
    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={ucid}"
    resp = http_get(url, RSS_HEADERS)
    xml = resp.text

    try:
        root = ET.fromstring(xml)
    except Exception as e:
        log.warning("Ошибка парсинга RSS для %s: %s", ucid, e)
        return ucid, []

    title_el = root.find(ATOM + "title")
    ch_title = title_el.text.strip() if title_el is not None and title_el.text else ucid

    fresh: List[Tuple[str, str, datetime]] = []
    entries = root.findall(ATOM + "entry")
    now = datetime.now(timezone.utc)

    for e in entries:
        vid_el = e.find(YT + "videoId") or e.find(YT_OLD + "videoid")
        vid = vid_el.text.strip() if vid_el is not None and vid_el.text else None
        link_el = e.find(ATOM + "link")
        link = link_el.get("href") if link_el is not None and link_el.get("href") else (f"https://youtu.be/{vid}" if vid else None)
        t_el = e.find(ATOM + "title")
        title = (t_el.text or "").strip() if t_el is not None else "(без названия)"
        p_el = e.find(ATOM + "published")
        when = parse_rfc3339(p_el.text if p_el is not None else "")
        if not when or not link:
            continue
        age = now - when
        if age <= timedelta(hours=hours):
            fresh.append((title, link, when))

    log.info("RSS канал: %s — свежих: %d (всего в RSS: %d)", ch_title, len(fresh), len(entries))
    return ch_title, fresh

# ---------- Formatting, JSON & GitHub Pages ----------
def format_age(delta: timedelta) -> str:
    secs = int(delta.total_seconds())
    if secs < 60:
        return f"{secs} сек назад"
    mins = secs // 60
    if mins < 60:
        return f"{mins} мин назад"
    hours = mins // 60
    mins = mins % 60
    if hours < 24:
        return f"{hours} ч {mins} мин назад" if mins else f"{hours} ч назад"
    days = hours // 24
    hours = hours % 24
    if days < 7:
        return f"{days} д {hours} ч назад" if hours else f"{days} д назад"
    weeks = days // 7
    days = days % 7
    return f"{weeks} нед {days} д назад" if days else f"{weeks} нед назад"

def safe_json_dump(path: Path, data: Dict[str, Any]) -> None:
    try:
        tmp = Path(str(path) + ".tmp")
        tmp.write_text(_json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        log.warning("Не смог записать %s: %s", path, e)

def gh_read_cfg():
    try:
        token = GITHUB_TOKEN_FILE.read_text(encoding="utf-8").strip()
        repo = GITHUB_REPO_FILE.read_text(encoding="utf-8").strip()
        branch = GITHUB_BRANCH_FILE.read_text(encoding="utf-8").strip() if GITHUB_BRANCH_FILE.exists() else "main"
        if token and repo:
            return token, repo, branch
    except Exception:
        pass
    return None, None, None

def gh_headers(token: str):
    # Можно заменить на Bearer при необходимости
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github+json", "User-Agent": "yt-fresh-bot"}

def gh_get_sha(token: str, repo: str, branch: str, path: str) -> Optional[str]:
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=gh_headers(token), timeout=10)
    if r.status_code == 200:
        try:
            return r.json().get("sha")
        except Exception:
            return None
    return None

def gh_publish_file(token: str, repo: str, branch: str, path: str, content_bytes: bytes, message: str) -> None:
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    sha = gh_get_sha(token, repo, branch, path)
    data = {
        "message": message,
        "content": _b64.b64encode(content_bytes).decode("ascii"),
        "branch": branch
    }
    if sha:
        data["sha"] = sha
    r = requests.put(url, headers=gh_headers(token), json=data, timeout=15)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"GitHub PUT {path} failed: {r.status_code} {r.text[:300]}")

def gh_publish_latest(out_json_text: str):
    token, repo, branch = gh_read_cfg()
    if not token or not repo:
        return False, "no_cfg"
    try:
        # latest_results.json
        gh_publish_file(token, repo, branch, "latest_results.json", out_json_text.encode("utf-8"), "update latest_results.json")
        # results.html (если есть локально)
        html_path = BASE / "results.html"
        if html_path.exists():
            gh_publish_file(token, repo, branch, "results.html", html_path.read_bytes(), "update results.html")
        # .nojekyll
        try:
            gh_publish_file(token, repo, branch, ".nojekyll", b"", "ensure .nojekyll")
        except Exception:
            pass
        return True, "ok"
    except Exception as e:
        log.warning("GitHub publish error: %s", e)
        return False, str(e)

def chunk_entries(entries: List[str], max_len: int = 3800) -> List[str]:
    chunks, cur, cur_len = [], [], 0
    for e in entries:
        add = len(e) + 2
        if cur_len + add > max_len:
            chunks.append("\n\n".join(cur))
            cur, cur_len = [e], add
        else:
            cur.append(e)
            cur_len += add
    if cur:
        chunks.append("\n\n".join(cur))
    return chunks

def extract_video_id_from_url(url: str) -> str:
    try:
        u = url.split("?",1)[0]
        if "youtu.be/" in u:
            return u.rsplit("/",1)[-1]
        if "/watch" in url:
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(url).query)
            return (qs.get("v") or [""])[0]
    except Exception:
        pass
    return ""

def load_seen_ids() -> set:
    try:
        data = _json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return set(str(x) for x in data)
    except Exception:
        pass
    return set()

def save_seen_ids(seen: set) -> None:
    try:
        tmp = Path(str(SEEN_FILE) + ".tmp")
        tmp.write_text(_json.dumps(sorted(seen), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(SEEN_FILE)
    except Exception as e:
        log.warning("Не смог записать seen_ids.json: %s", e)

# ---------- Core collector ----------
async def collect_and_publish(hours: int) -> tuple[list[tuple[str,str,str,datetime]], list[dict]]:
    # читаем список каналов
    try:
        channels: List[str] = []
        for line in CHANNELS_FILE.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#"):
                channels.append(s)
        seen_local = set()
        uniq = []
        for s in channels:
            if s not in seen_local:
                uniq.append(s); seen_local.add(s)
        channels = uniq
    except Exception as e:
        log.warning("Ошибка чтения channels.txt: %s", e)
        return [], []

    results: List[Tuple[str, str, str, datetime]] = []
    results_json: List[Dict[str, Any]] = []

    for raw in channels:
        ucid = resolve_ucid(raw)
        if not ucid:
            log.warning("Пропускаю: не удалось определить UCID для %s", raw)
            continue
        try:
            ch_title, fresh = fetch_fresh_from_rss(ucid, hours)
        except Exception as e:
            log.warning("Ошибка RSS для %s: %s", raw, e)
            continue
        now = datetime.now(timezone.utc)
        for title, link, when in fresh:
            results.append((ch_title, title, link, when))
            results_json.append({
                "channel": ch_title,
                "title": title,
                "url": link,
                "published_at_utc": when.astimezone(timezone.utc).isoformat(),
                "age_seconds": int((now - when).total_seconds())
            })
        time.sleep(SLEEP)

    if results_json:
        try:
            out = {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "hours_window": hours,
                "items": results_json,
            }
            safe_json_dump(BASE / "latest_results.json", out)
            text = _json.dumps(out, ensure_ascii=False, indent=2)
            ok, msg = gh_publish_latest(text)
            if ok:
                log.info("Опубликовано на GitHub Pages.")
            else:
                log.info("GitHub Pages пропущен (%s).", msg)
        except Exception as e:
            log.warning("Не смог обновить latest_results.json/опубликовать: %s", e)

    return results, results_json

# ---------- Telegram Bot ----------
async def cmd_fresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    try:
        hours = int(args[0]) if args else 12
        if hours <= 0 or hours > 168:
            hours = 12
    except Exception:
        hours = 12

    await update.message.reply_text(f"Проверяю RSS каналов за {hours} ч...", disable_web_page_preview=True)

    results, _ = await collect_and_publish(hours)
    if not results:
        await update.message.reply_text("Свежих видео нет.", disable_web_page_preview=True)
        return

    entries: List[str] = []
    now = datetime.now(timezone.utc)
    for i, (ch, title, link, when) in enumerate(results):
        age = format_age(now - when)
        entries.append(f"{i + 1}) [{ch}] {title} ({age})\n{link}")

    for chunk in chunk_entries(entries):
        await update.message.reply_text(chunk, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
        await asyncio_sleep(0.3)

async def _auto_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    minutes = data.get("minutes", 10)
    hours = data.get("hours", 12)
    chat_id = data.get("chat_id")
    seen = load_seen_ids()
    results, results_json = await collect_and_publish(hours)
    # выявим новые ролики
    new_items = []
    for item in results_json:
        vid = extract_video_id_from_url(item.get("url",""))
        if vid and vid not in seen:
            new_items.append(item)
            seen.add(vid)
    if new_items and chat_id:
        # короткий отчёт в чат
        lines = [f"Новые видео: {len(new_items)} за последние {hours} ч."]
        for it in new_items[:5]:
            lines.append(f"- [{it.get('channel')}] {it.get('title')}")
        try:
            await context.bot.send_message(chat_id=chat_id, text="\n".join(lines), disable_web_page_preview=True)
        except Exception as e:
            log.warning("Не удалось отправить авто-уведомление: %s", e)
    save_seen_ids(seen)

async def cmd_auto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    try:
        minutes = int(args[0]) if args else 10
        if minutes < 2 or minutes > 120:
            minutes = 10
    except Exception:
        minutes = 10
    try:
        hours = int(args[1]) if len(args) > 1 else 12
        if hours < 1 or hours > 168:
            hours = 12
    except Exception:
        hours = 12

    # отменим старые
    if context.job_queue:
        for job in context.job_queue.jobs():
            if job.data and job.data.get("owner") == "auto":
                job.schedule_removal()

    context.job_queue.run_repeating(_auto_job, interval=minutes*60, first=0, data={"minutes": minutes, "hours": hours, "chat_id": update.effective_chat.id, "owner": "auto"})
    await update.message.reply_text(f"Автообновление включено: каждые {minutes} мин, окно {hours} ч.", disable_web_page_preview=True)

async def cmd_stopauto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    stopped = 0
    if context.job_queue:
        for job in list(context.job_queue.jobs()):
            if job.data and job.data.get("owner") == "auto":
                job.schedule_removal(); stopped += 1
    await update.message.reply_text("Автообновление остановлено." if stopped else "Автообновление не было запущено.", disable_web_page_preview=True)

async def asyncio_sleep(sec: float) -> None:
    import asyncio
    await asyncio.sleep(sec)

def main():
    token = TOKEN_FILE.read_text(encoding="utf-8").strip()
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("fresh", cmd_fresh))
    app.add_handler(CommandHandler("auto", cmd_auto))
    app.add_handler(CommandHandler("stopauto", cmd_stopauto))
    log.info("Бот запущен. Команды: /fresh [часы], /auto <мин> [часы], /stopauto")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
