import os
import re
import json
import asyncio
import logging
import sqlite3
import html
import imaplib
import email
from email.header import decode_header
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

import aiohttp
from aiohttp import web
import feedparser
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import Command

# ============================================================
#                   КОНФИГ ИЗ .env
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = int(os.getenv("CHAT_ID", "0"))

AI_PROVIDER = os.getenv("AI_PROVIDER", "gemini").strip().lower()   # gemini / openai / anthropic / groq
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# Несколько ключей Gemini через запятую: GEMINI_API_KEY=key1,key2,key3
_gemini_keys = [k.strip() for k in os.getenv("GEMINI_API_KEY", "").split(",") if k.strip()]
GEMINI_API_KEY = _gemini_keys[0] if _gemini_keys else ""

USE_AI_FILTER = os.getenv("USE_AI_FILTER", "true").strip().lower() == "true"

# Дефолты (можно менять командами из чата — они переопределяют эти значения)
MAX_DIFFICULTY = os.getenv("MAX_DIFFICULTY", "easy").strip().lower()   # easy/medium/hard
MIN_BUDGET = int(os.getenv("MIN_BUDGET", "1000"))                       # минимум в ₽, 0 = без фильтра
STAR_THRESHOLD = int(os.getenv("STAR_THRESHOLD", "8"))                  # с какого скора ставить ⭐

# Английские биржи (заказы в валюте). true — включить
ENABLE_ENGLISH = os.getenv("ENABLE_ENGLISH", "false").strip().lower() == "true"

# Тихие часы (по локальному времени): ночью копим, утром шлём пачкой. start==end = выкл
QUIET_START = int(os.getenv("QUIET_START", "23"))
QUIET_END = int(os.getenv("QUIET_END", "8"))
TZ_OFFSET = int(os.getenv("TZ_OFFSET", "2"))     # смещение от UTC (Варшава летом = +2)
DIGEST_HOUR = int(os.getenv("DIGEST_HOUR", "9")) # час утренней сводки (локальный)

# Ключевые теги — заказы с ними помечаются 🔔 (через запятую)
WATCH_KEYWORDS = [w.strip().lower() for w in os.getenv("WATCH_KEYWORDS", "").split(",") if w.strip()]

MAX_JOB_AGE_HOURS = int(os.getenv("MAX_JOB_AGE_HOURS", "48"))  # заказы старше — пропускаем
MAX_PER_AUTHOR = int(os.getenv("MAX_PER_AUTHOR", "3"))         # макс. заказов от 1 автора за 12ч
SCAM_THRESHOLD = int(os.getenv("SCAM_THRESHOLD", "7"))        # с какого риска резать скам

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY", "").strip()
AI_DELAY = int(os.getenv("AI_DELAY", "13"))   # пауза между запросами к ИИ

# --- Биржи через почту (Gmail IMAP) ---
# Подпишись на email-уведомления о новых заказах на нужных биржах (Kwork, FL.ru,
# Freelance.ru, YouDo и т.д.), указав этот Gmail. Бот читает письма и достаёт
# заказы — так обходятся площадки с капчей/антибот-защитой без нарушений.
# Нужен app password Gmail (не основной пароль).
# EMAIL_ENABLED — новое имя; KWORK_EMAIL_ENABLED оставлен для совместимости.
KWORK_EMAIL_ENABLED = (
    os.getenv("EMAIL_ENABLED", os.getenv("KWORK_EMAIL_ENABLED", "false"))
    .strip().lower() == "true"
)
IMAP_HOST = os.getenv("IMAP_HOST", "imap.gmail.com").strip()
IMAP_USER = os.getenv("IMAP_USER", "").strip()
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD", "").strip()   # app password

# ============================================================
#                   ИСТОЧНИКИ (биржи)
# ============================================================

SOURCES = [
    # --- русские биржи с откликами ---
    {"name": "Habr Freelance", "enabled": True,
     "url": "https://freelance.habr.com/tasks.rss"},
    # Weblancer / FL.ru отключены — отклики платные (нужен PRO)
    {"name": "Weblancer", "enabled": False,
     "url": "https://www.weblancer.net/rss/projects/"},
    {"name": "FL.ru", "enabled": False,
     "url": "https://www.fl.ru/rss/all.xml"},
    # Freelance.ru оставлен — фильтр заказов можно настроить через почтовые подписки
    {"name": "Freelance.ru", "enabled": True,
     "url": "https://freelance.ru/rss/projects"},
    {"name": "Workspace", "enabled": True,
     "url": "https://workspace.ru/tenders/rss/"},
    {"name": "Habr Карьера", "enabled": True,
     "url": "https://career.habr.com/vacancies/rss"},
    # --- английские биржи (включены всегда, язык не важен) ---
    # Freelancer.com отключён — требует верификацию телефона
    {"name": "Freelancer.com", "enabled": False,
     "url": "https://www.freelancer.com/rss.xml"},
    {"name": "RemoteOK", "enabled": True,
     "url": "https://remoteok.com/remote-dev-jobs.rss"},
    {"name": "WeWorkRemotely", "enabled": True,
     "url": "https://weworkremotely.com/categories/remote-programming-jobs.rss"},
    {"name": "Jobicy", "enabled": True,
     "url": "https://jobicy.com/?feed=job_feed&job_categories=dev"},
    # Upwork: вставь свой RSS из сохранённого поиска и поставь enabled True
    {"name": "Upwork", "enabled": False,
     "url": "https://www.upwork.com/ab/feed/jobs/rss?q=..."},
    # Kwork: открытого RSS нет, нужен HTML-парсинг с риском капчи — выключено
]

# ============================================================
#                   ФИЛЬТРЫ (грубый предотбор)
# ============================================================

WHITELIST = [
    # боты и автоматизация — самое вайбкодинговое
    "бот", "bot", "telegram", "чат-бот", "chatbot",
    "автоматизац", "автоматизировать", "automation",
    # парсинг
    "парсер", "парсинг", "scrap",
    # ИИ-интеграции
    "ai", "gpt", "нейросет", "openai", "gemini", "llm",
    # no-code / интеграции
    "no-code", "no code", "ноукод",
    "api", "интеграц", "webhook",
    # скрипты и утилиты
    "скрипт", "script",
    # конкретные инструменты (не просто "сайт")
    "google sheets", "airtable", "notion", "zapier", "make.com",
    "дашборд", "dashboard",
    # веб-приложения (уточнённые — не просто "сайт")
    "веб-приложен", "web app", "лендинг", "landing",
]
BLACKLIST = [
    "дизайн", "логотип", "logo", "smm", "видеомонтаж", "видео", "монтаж",
    "копирайт", "рерайт", "перевод текст", "озвучк", "иллюстрац", "анимац",
    "3d", "моделирование", "верстальщик", "наполнение", "seo-текст",
]

DB_PATH = "seen.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("freelance-bot")


# ============================================================
#                          МОДЕЛЬ
# ============================================================

@dataclass
class Job:
    source: str
    title: str
    link: str
    description: str
    budget: str = ""
    difficulty: str = ""
    score: int = 0
    watched: bool = False
    published_at: str = ""   # ISO строка UTC, пустая если RSS не отдал время
    ru_summary: str = ""     # краткий пересказ/перевод на русский от ИИ
    author: str = ""         # автор заказа (для антидубля)
    lang: str = ""           # язык описания: ru / en / other
    scam_risk: int = 0       # риск скама 0-10 (ставит ИИ)

    @property
    def uid(self) -> str:
        return f"{self.source}::{self.link}"

    @property
    def age_hours(self) -> float | None:
        if not self.published_at:
            return None
        try:
            pub = datetime.fromisoformat(self.published_at)
            return (datetime.now(timezone.utc) - pub).total_seconds() / 3600
        except Exception:
            return None

    @property
    def age_label(self) -> str:
        h = self.age_hours
        if h is None:
            return ""
        if h < 1:
            return f"⏱ {int(h * 60)} мин назад"
        if h < 24:
            return f"⏱ {int(h)} ч назад"
        return f"⏱ {int(h / 24)} д назад"

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    @staticmethod
    def from_dict(d: dict) -> "Job":
        return Job(**{k: d[k] for k in (
            "source", "title", "link", "description", "budget",
            "difficulty", "score", "watched", "published_at",
            "ru_summary", "author", "lang", "scam_risk") if k in d})


# ============================================================
#                          БАЗА
# ============================================================

def db_init():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS seen (uid TEXT PRIMARY KEY, title_key TEXT, ts TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS prefs (word TEXT PRIMARY KEY, w INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS favorites (uid TEXT PRIMARY KEY, data TEXT, ts TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS jobs_log (uid TEXT PRIMARY KEY, title TEXT, link TEXT, score INTEGER, source TEXT, ts TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS pending (uid TEXT PRIMARY KEY, data TEXT)")
    c.execute("""CREATE TABLE IF NOT EXISTS scan_stats (
        id INTEGER PRIMARY KEY,
        ts TEXT,
        scanned INTEGER,
        passed_filter INTEGER,
        sent INTEGER
    )""")
    c.execute("CREATE TABLE IF NOT EXISTS authors_seen (author TEXT, ts TEXT)")
    conn.commit()
    conn.close()


def _conn():
    return sqlite3.connect(DB_PATH)


def title_key(title: str) -> str:
    """Нормализованный ключ заголовка для отлова дублей с разных бирж."""
    return re.sub(r"[^a-zа-яё0-9]", "", title.lower())[:80]


def detect_lang(text: str) -> str:
    """Определяет язык по соотношению кириллицы/латиницы. Без токенов."""
    cyr = len(re.findall(r"[а-яё]", text.lower()))
    lat = len(re.findall(r"[a-z]", text.lower()))
    if cyr == 0 and lat == 0:
        return "other"
    if cyr >= lat:
        return "ru"
    return "en"


LANG_FLAG = {"ru": "🇷🇺", "en": "🇬🇧", "other": "🌐"}


def is_seen(uid: str, t_key: str) -> bool:
    conn = _conn()
    row = conn.execute(
        "SELECT 1 FROM seen WHERE uid=? OR (title_key=? AND title_key!='')",
        (uid, t_key),
    ).fetchone()
    conn.close()
    return row is not None


def mark_seen(uid: str, t_key: str):
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO seen (uid, title_key, ts) VALUES (?,?,?)",
                 (uid, t_key, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def get_setting(key: str, default):
    conn = _conn()
    row = conn.execute("SELECT v FROM settings WHERE k=?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key: str, value):
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO settings (k, v) VALUES (?,?)", (key, str(value)))
    conn.commit()
    conn.close()


# эффективные настройки (команды из чата переопределяют .env)
def eff_min_budget() -> int:
    return int(get_setting("min_budget", MIN_BUDGET))


def eff_max_difficulty() -> str:
    return str(get_setting("max_difficulty", MAX_DIFFICULTY))


def is_paused() -> bool:
    return get_setting("paused", "0") == "1"


def add_favorite(job: Job):
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO favorites (uid, data, ts) VALUES (?,?,?)",
                 (job.uid, json.dumps(job.to_dict(), ensure_ascii=False),
                  datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def list_favorites() -> list[Job]:
    conn = _conn()
    rows = conn.execute("SELECT data FROM favorites ORDER BY ts DESC LIMIT 20").fetchall()
    conn.close()
    return [Job.from_dict(json.loads(r[0])) for r in rows]


def log_job(job: Job):
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO jobs_log (uid, title, link, score, source, ts) "
                 "VALUES (?,?,?,?,?,?)",
                 (job.uid, job.title, job.link, job.score, job.source,
                  datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def jobs_last_24h() -> list[tuple]:
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    conn = _conn()
    rows = conn.execute(
        "SELECT title, link, score, source FROM jobs_log WHERE ts > ? ORDER BY score DESC",
        (since,)).fetchall()
    conn.close()
    return rows


def author_recent_count(author: str, hours: int = 12) -> int:
    """Сколько заказов от этого автора за последние N часов (антидубль/антиспам)."""
    if not author:
        return 0
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    conn = _conn()
    n = conn.execute("SELECT COUNT(*) FROM authors_seen WHERE author=? AND ts>?",
                     (author, since)).fetchone()[0]
    conn.close()
    return n


def mark_author(author: str):
    if not author:
        return
    conn = _conn()
    conn.execute("INSERT INTO authors_seen (author, ts) VALUES (?,?)",
                 (author, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def activity_by_hour() -> list[int]:
    """Возвращает 24 числа — сколько заказов было в каждый локальный час за 7 дней."""
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    conn = _conn()
    rows = conn.execute("SELECT ts FROM jobs_log WHERE ts > ?", (since,)).fetchall()
    conn.close()
    hours = [0] * 24
    for (ts,) in rows:
        try:
            dt = datetime.fromisoformat(ts) + timedelta(hours=TZ_OFFSET)
            hours[dt.hour] += 1
        except Exception:
            pass
    return hours


def queue_pending(job: Job):
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO pending (uid, data) VALUES (?,?)",
                 (job.uid, json.dumps(job.to_dict(), ensure_ascii=False)))
    conn.commit()
    conn.close()


def pop_pending() -> list[Job]:
    conn = _conn()
    rows = conn.execute("SELECT data FROM pending").fetchall()
    conn.execute("DELETE FROM pending")
    conn.commit()
    conn.close()
    return [Job.from_dict(json.loads(r[0])) for r in rows]


def log_scan(scanned: int, passed: int, sent: int):
    conn = _conn()
    conn.execute(
        "INSERT INTO scan_stats (ts, scanned, passed_filter, sent) VALUES (?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(), scanned, passed, sent),
    )
    conn.commit()
    conn.close()


def get_stats() -> dict:
    conn = _conn()
    total_seen = conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
    total_sent = conn.execute("SELECT COUNT(*) FROM jobs_log").fetchone()[0]
    total_favs = conn.execute("SELECT COUNT(*) FROM favorites").fetchone()[0]
    since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    sent_24h = conn.execute(
        "SELECT COUNT(*) FROM jobs_log WHERE ts > ?", (since_24h,)
    ).fetchone()[0]
    # топ бирж по количеству отправленных
    top_sources = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM jobs_log GROUP BY source ORDER BY cnt DESC LIMIT 5"
    ).fetchall()
    # среднее время между запросами (последние 10 сканов)
    scans = conn.execute(
        "SELECT scanned, passed_filter, sent FROM scan_stats ORDER BY id DESC LIMIT 10"
    ).fetchall()
    conn.close()

    total_scanned = sum(r[0] for r in scans)
    total_passed = sum(r[1] for r in scans)
    return {
        "total_seen": total_seen,
        "total_sent": total_sent,
        "total_favs": total_favs,
        "sent_24h": sent_24h,
        "top_sources": top_sources,
        "scans_count": len(scans),
        "scanned_last10": total_scanned,
        "passed_last10": total_passed,
    }


# ============================================================
#                   ВРЕМЯ / ТИХИЕ ЧАСЫ
# ============================================================

def now_local() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=TZ_OFFSET)


def in_quiet_hours() -> bool:
    if QUIET_START == QUIET_END:
        return False
    h = now_local().hour
    if QUIET_START < QUIET_END:
        return QUIET_START <= h < QUIET_END
    return h >= QUIET_START or h < QUIET_END   # период через полночь


# ============================================================
#                   ПАРСИНГ ИСТОЧНИКОВ
# ============================================================

def _strip_tags(s: str) -> str:
    out, skip = [], False
    for ch in s:
        if ch == "<":
            skip = True
        elif ch == ">":
            skip = False
        elif not skip:
            out.append(ch)
    return "".join(out).strip()


_BUDGET_RE = re.compile(
    r"(?:от\s*|до\s*|~\s*)?\d[\d\s.,]*\s*"
    r"(?:руб|рублей|₽|р\.|rub|usd|\$|€|eur|грн)"
    r"(?:\s*/?\s*(?:час|hour|шт))?",
    re.IGNORECASE,
)


def extract_budget(text: str) -> str:
    m = _BUDGET_RE.search(text)
    return re.sub(r"\s+", " ", m.group(0)).strip() if m else ""


def budget_to_number(budget: str) -> int:
    """Грубо вытаскиваем число. $/€ приблизительно переводим в ₽ (×90)."""
    if not budget:
        return 0
    chunk = budget.split("/")[0]
    digits = re.sub(r"[^\d]", "", chunk)
    if not digits:
        return 0
    val = int(digits)
    if "$" in budget or "usd" in budget.lower() or "€" in budget or "eur" in budget.lower():
        val *= 90
    return val


async def fetch_source(session: aiohttp.ClientSession, src: dict) -> list[Job]:
    jobs: list[Job] = []
    try:
        async with session.get(src["url"], timeout=aiohttp.ClientTimeout(total=30)) as resp:
            raw = await resp.text()
    except Exception as e:
        log.warning("Не удалось загрузить %s: %s", src["name"], e)
        return jobs

    feed = feedparser.parse(raw)
    for entry in feed.entries:
        title = html.unescape(getattr(entry, "title", "")).strip()
        link = getattr(entry, "link", "").strip()
        desc = _strip_tags(html.unescape(getattr(entry, "summary", "")))
        budget = extract_budget(f"{title} {desc}")

        published_at = ""
        for time_field in ("published_parsed", "updated_parsed"):
            t = getattr(entry, time_field, None)
            if t:
                try:
                    published_at = datetime(*t[:6], tzinfo=timezone.utc).isoformat()
                except Exception:
                    pass
                break

        author = (getattr(entry, "author", "") or "").strip()
        lang = detect_lang(f"{title} {desc}")

        if link:
            jobs.append(Job(source=src["name"], title=title, link=link,
                            description=desc, budget=budget,
                            published_at=published_at,
                            author=author, lang=lang))
    return jobs


# ============================================================
#                   KWORK ЧЕРЕЗ ПОЧТУ (IMAP)
# ============================================================

def _decode_mime(s: str) -> str:
    """Декодирует MIME-заголовок (=?utf-8?...?=) в нормальную строку."""
    if not s:
        return ""
    parts = []
    for chunk, enc in decode_header(s):
        if isinstance(chunk, bytes):
            try:
                parts.append(chunk.decode(enc or "utf-8", errors="replace"))
            except Exception:
                parts.append(chunk.decode("utf-8", errors="replace"))
        else:
            parts.append(chunk)
    return "".join(parts).strip()


def _email_body(msg: email.message.Message) -> str:
    """Достаёт текст письма: предпочитаем html (там ссылки на проекты)."""
    html_body, text_body = "", ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if part.get("Content-Disposition"):
                continue
            try:
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                charset = part.get_content_charset() or "utf-8"
                decoded = payload.decode(charset, errors="replace")
            except Exception:
                continue
            if ctype == "text/html":
                html_body += decoded
            elif ctype == "text/plain":
                text_body += decoded
    else:
        try:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            body = payload.decode(charset, errors="replace") if payload else ""
        except Exception:
            body = ""
        if msg.get_content_type() == "text/html":
            html_body = body
        else:
            text_body = body
    return html_body or text_body


# Площадки, заказы которых ловим через почтовые уведомления.
# Для каждой: name — как подписать источник, domain — часть адреса отправителя,
# link_re — как выглядят ссылки на заказ в письме.
# Добавляешь новую биржу → дописываешь сюда строку и подписываешься на её рассылку.
EMAIL_SOURCES = [
    {"name": "Kwork", "domain": "kwork.ru",
     "link_re": re.compile(r"https?://kwork\.ru/projects/\d+[^\s\"'<>]*", re.I)},
    {"name": "FL.ru", "domain": "fl.ru",
     "link_re": re.compile(r"https?://(?:www\.)?fl\.ru/projects/\d+[^\s\"'<>]*", re.I)},
    {"name": "Freelance.ru", "domain": "freelance.ru",
     "link_re": re.compile(r"https?://(?:www\.)?freelance\.ru/project/\d+[^\s\"'<>]*", re.I)},
    {"name": "Weblancer", "domain": "weblancer.net",
     "link_re": re.compile(r"https?://(?:www\.)?weblancer\.net/projects/[^\s\"'<>]+", re.I)},
    {"name": "Habr Freelance", "domain": "freelance.habr.com",
     "link_re": re.compile(r"https?://freelance\.habr\.com/tasks/\d+[^\s\"'<>]*", re.I)},
    {"name": "YouDo", "domain": "youdo.com",
     "link_re": re.compile(r"https?://youdo\.com/t\d+[^\s\"'<>]*", re.I)},
]


def _match_email_source(sender: str) -> dict | None:
    """По адресу отправителя находим биржу из EMAIL_SOURCES."""
    s = sender.lower()
    for src in EMAIL_SOURCES:
        if src["domain"] in s:
            return src
    return None


def _parse_email(src: dict, subject: str, body: str) -> list[Job]:
    """Из одного письма достаём заказы: ссылки на проекты + заголовок."""
    jobs: list[Job] = []
    links = list(dict.fromkeys(src["link_re"].findall(body)))  # уникальные, порядок
    text = _strip_tags(body)
    text = re.sub(r"\s+", " ", text).strip()
    title = _decode_mime(subject) or f"Заказ с {src['name']}"
    budget = extract_budget(f"{title} {text}")
    lang = detect_lang(f"{title} {text}")
    for link in links:
        jobs.append(Job(source=src["name"], title=title, link=link,
                        description=text[:800], budget=budget, lang=lang,
                        published_at=datetime.now(timezone.utc).isoformat()))
    return jobs


def _fetch_email_blocking() -> list[Job]:
    """Синхронное чтение непрочитанных писем по IMAP со всех бирж из EMAIL_SOURCES."""
    jobs: list[Job] = []
    try:
        imap = imaplib.IMAP4_SSL(IMAP_HOST)
        imap.login(IMAP_USER, IMAP_PASSWORD)
        imap.select("INBOX")
        status, data = imap.search(None, "UNSEEN")
        if status != "OK":
            imap.logout()
            return jobs
        ids = data[0].split()
        for mid in ids:
            status, msg_data = imap.fetch(mid, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            sender = str(msg.get("From", ""))
            src = _match_email_source(sender)
            if not src:                       # письмо не от биржи — пропускаем
                continue
            subject = msg.get("Subject", "")
            body = _email_body(msg)
            jobs.extend(_parse_email(src, subject, body))
        imap.logout()
    except Exception as e:
        log.warning("Почта недоступна: %s", e)
    return jobs


async def fetch_email_jobs() -> list[Job]:
    """Асинхронная обёртка — IMAP блокирующий, выносим в поток."""
    if not (KWORK_EMAIL_ENABLED and IMAP_USER and IMAP_PASSWORD):
        return []
    return await asyncio.to_thread(_fetch_email_blocking)


# ============================================================
#                   ИИ: анализ + генерация
# ============================================================

ANALYZE_SYSTEM = (
    "Ты анализируешь заказ для фрилансера, который хочет ТОЛЬКО вайбкодить — "
    "быстро собирать решения с помощью ИИ-инструментов (сайты, Telegram-боты, "
    "парсеры, автоматизации, no-code, ИИ-интеграции), без тяжёлой ручной "
    "разработки. Верни СТРОГО JSON без пояснений и без markdown:\n"
    '{"fit": "easy|medium|hard|no", "score": число от 1 до 10, "ru": "суть на русском, 1-2 предложения", "scam": число от 0 до 10}\n'
    "fit: easy — собирается за вечер чистым вайбкодингом; medium — вайбкодинг + "
    "ручная доработка; hard — нужна серьёзная ручная разработка; no — заказ "
    "вообще не про код (дизайн, видео, тексты).\n"
    "score: насколько заказ хорош ИМЕННО для чистого вайбкодинга и выгоден "
    "(10 — идеально простой и денежный, 1 — почти не подходит).\n"
    "ru: коротко перескажи суть заказа на русском (если оригинал на английском — переведи). Максимум 2 предложения.\n"
    "scam: риск развода/скама от 0 до 10. Высокий риск: просят предоплату/депозит, "
    "уводят в личку до обсуждения, бесплатное 'тестовое', нереально низкая цена за "
    "сложную работу, обещают золотые горы, мутное описание без конкретики."
)

DIFFICULTY_LABELS = {
    "easy": "🟢 Лёгкая — хватит вайбкодинга",
    "medium": "🟡 Средняя — вайбкодинг + доработка",
    "hard": "🔴 Сложная — нужна ручная разработка",
}
RANK = {"easy": 1, "medium": 2, "hard": 3}


_ai_lock = asyncio.Lock()   # чтобы запросы к ИИ шли по одному

# Ротация Gemini-ключей при исчерпании лимита
_gemini_key_index = 0

def _current_gemini_key() -> str:
    return _gemini_keys[_gemini_key_index] if _gemini_keys else ""

def _rotate_gemini_key() -> bool:
    """Переключается на следующий ключ. Возвращает False если ключи кончились."""
    global _gemini_key_index
    if _gemini_key_index + 1 < len(_gemini_keys):
        _gemini_key_index += 1
        log.warning("Gemini: переключаюсь на ключ #%d", _gemini_key_index + 1)
        return True
    return False

async def ai_analyze(session: aiohttp.ClientSession, job: Job) -> tuple[str, int]:
    msg = (f"Заголовок: {job.title}\nОписание: {job.description[:800]}\n"
           f"Бюджет: {job.budget or 'не указан'}")
    try:
        async with _ai_lock:
            await asyncio.sleep(AI_DELAY)
            raw = await call_ai(session, ANALYZE_SYSTEM, msg, max_tokens=160)
    except Exception as e:
        msg_e = str(e)
        if "RESOURCE_EXHAUSTED" in msg_e or "429" in msg_e:
            if AI_PROVIDER == "gemini" and _rotate_gemini_key():
                log.warning("Лимит ключа Gemini — переключился, повторяю запрос…")
                try:
                    async with _ai_lock:
                        raw = await call_ai(session, ANALYZE_SYSTEM, msg, max_tokens=160)
                except Exception as e2:
                    log.warning("Новый ключ тоже не помог: %s", e2)
                    return "no", 0
            else:
                if GROQ_API_KEY:
                    log.warning("Все ключи Gemini исчерпаны — переключаюсь на Groq…")
                    try:
                        async with _ai_lock:
                            raw = await _call_groq(session, ANALYZE_SYSTEM, msg, max_tokens=160)
                    except Exception as e3:
                        log.warning("Groq тоже не помог: %s", e3)
                        return "no", 0
                else:
                    log.warning("Все ключи Gemini исчерпаны, жду 60с…")
                    await asyncio.sleep(60)
                    return "no", 0
        else:
            log.warning("ИИ-анализ недоступен, заказ пропускаю: %s", e)
            return "no", 0
    
    txt = raw.strip().strip("`")
    txt = re.sub(r"^json", "", txt, flags=re.IGNORECASE).strip()
    try:
        data = json.loads(txt[txt.find("{"): txt.rfind("}") + 1])
        fit = str(data.get("fit", "medium")).lower()
        score = int(data.get("score", 5))
        job.ru_summary = str(data.get("ru", "")).strip()[:400]
        job.scam_risk = max(0, min(10, int(data.get("scam", 0))))
    except Exception:
        fit, score = "medium", 5
    if fit not in ("easy", "medium", "hard", "no"):
        fit = "medium"
    return fit, max(1, min(10, score))


REPLY_SYSTEM = (
    "Ты пишешь отклики на фриланс-заказы от лица живого разработчика. "
    "Пиши как настоящий человек, а не как ИИ. ЗАПРЕЩЕНО: канцелярит, "
    "шаблонные фразы ('Я внимательно ознакомился', 'Готов взяться за реализацию', "
    "'В кратчайшие сроки'), восклицания через слово, обороты вроде 'не просто X, "
    "а Y', тире-перечисления, эмодзи. Пиши живым разговорным языком, как будто "
    "быстро печатаешь заказчику в личку: простыми короткими предложениями, "
    "по-человечески, с конкретикой по задаче.\n"
    "Напиши ТРИ варианта на русском, разделённые строкой '---'.\n"
    "Вариант 1 — короткий, по делу (2-3 предложения).\n"
    "Вариант 2 — чуть подробнее: как именно сделаешь, какой стек/инструменты, срок.\n"
    "Вариант 3 — расслабленный, неформальный, будто пишешь знакомому.\n"
    "В каждом покажи, что понял задачу, и позови обсудить детали. "
    "Не используй заголовки и нумерацию внутри вариантов."
)

PRICE_SYSTEM = (
    "Ты оцениваешь заказ для вайбкодера. Кратко (3-5 строк) дай: "
    "справедливую вилку цены в рублях, примерные часы работы и 1-2 риска/нюанса. "
    "Без воды, по делу."
)

EARNINGS_SYSTEM = (
    "Ты помогаешь вайбкодеру понять, выгоден ли заказ. "
    "Вайбкодер — это фрилансер, который собирает решения с помощью ИИ быстро и дёшево по себестоимости. "
    "Дай конкретный ответ в 4-5 строках:\n"
    "1. Сколько часов реально займёт (с учётом вайбкодинга — обычно быстрее обычного разработчика).\n"
    "2. Какой чистый заработок в рублях (бюджет минус ~500₽/час твоего времени).\n"
    "3. Эффективная ставка в час (бюджет ÷ часы).\n"
    "4. Вывод одной фразой: стоит браться или нет и почему.\n"
    "Если бюджет не указан — оцени сам по рынку. Без воды, только цифры и вывод."
)


async def generate_reply(session, job: Job) -> str:
    msg = (f"Заказ с биржи {job.source}.\nЗаголовок: {job.title}\n"
           f"Описание: {job.description[:1500]}\n\nНапиши три варианта отклика.")
    try:
        return await call_ai(session, REPLY_SYSTEM, msg, max_tokens=900)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось сгенерировать отклик. Проверь ключ/лимиты."


async def estimate_price(session, job: Job) -> str:
    msg = (f"Заголовок: {job.title}\nОписание: {job.description[:1200]}\n"
           f"Указанный бюджет: {job.budget or 'не указан'}")
    try:
        return await call_ai(session, PRICE_SYSTEM, msg, max_tokens=300)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось оценить заказ."


async def estimate_earnings(session, job: Job) -> str:
    msg = (f"Заголовок: {job.title}\nОписание: {job.description[:1200]}\n"
           f"Бюджет заказчика: {job.budget or 'не указан'}\n"
           f"Сложность по оценке ИИ: {job.difficulty or 'не оценена'}")
    try:
        return await call_ai(session, EARNINGS_SYSTEM, msg, max_tokens=300)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось рассчитать заработок."


async def _call_anthropic(session, system, user_msg, max_tokens):
    headers = {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
               "content-type": "application/json"}
    payload = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens, "system": system,
               "messages": [{"role": "user", "content": user_msg}]}
    async with session.post("https://api.anthropic.com/v1/messages", headers=headers,
                            json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
        data = await resp.json()
    return "".join(b.get("text", "") for b in data.get("content", [])).strip()


async def _call_openai(session, system, user_msg, max_tokens):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": OPENAI_MODEL, "max_tokens": max_tokens,
               "messages": [{"role": "system", "content": system},
                            {"role": "user", "content": user_msg}]}
    async with session.post("https://api.openai.com/v1/chat/completions", headers=headers,
                            json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
        data = await resp.json()
    if "choices" not in data:
        # показываем реальную причину (нет ключа / нет квоты / не та модель)
        raise RuntimeError(f"OpenAI вернул ошибку: {data.get('error', data)}")
    return data["choices"][0]["message"]["content"].strip()


async def _call_gemini(session, system, user_msg, max_tokens):
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    headers = {"x-goog-api-key": _current_gemini_key(), "Content-Type": "application/json"}
    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"parts": [{"text": user_msg}]}],
        # thinkingBudget=0 выключает «размышления» 2.5-моделей, иначе ответ
        # может прийти пустым при маленьком лимите токенов
        "generationConfig": {"maxOutputTokens": max_tokens,
                             "thinkingConfig": {"thinkingBudget": 0}},
    }
    async with session.post(url, headers=headers, json=payload,
                            timeout=aiohttp.ClientTimeout(total=60)) as resp:
        data = await resp.json()
    if "candidates" not in data:
        raise RuntimeError(f"Gemini вернул ошибку: {data.get('error', data)}")
    parts = data["candidates"][0].get("content", {}).get("parts", [])
    return "".join(p.get("text", "") for p in parts).strip()


async def _call_groq(session, system, user_msg, max_tokens):
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": GROQ_MODEL, "max_tokens": max_tokens,
               "messages": [{"role": "system", "content": system},
                             {"role": "user", "content": user_msg}]}
    async with session.post("https://api.groq.com/openai/v1/chat/completions",
                            headers=headers, json=payload,
                            timeout=aiohttp.ClientTimeout(total=60)) as resp:
        data = await resp.json()
    if "choices" not in data:
        raise RuntimeError(f"Groq вернул ошибку: {data.get('error', data)}")
    return data["choices"][0]["message"]["content"].strip()


async def call_ai(session, system, user_msg, max_tokens):
    """Единая точка вызова ИИ — выбирает провайдера по AI_PROVIDER."""
    if AI_PROVIDER == "anthropic":
        return await _call_anthropic(session, system, user_msg, max_tokens)
    if AI_PROVIDER == "gemini":
        return await _call_gemini(session, system, user_msg, max_tokens)
    if AI_PROVIDER == "groq":
        return await _call_groq(session, system, user_msg, max_tokens)
    return await _call_openai(session, system, user_msg, max_tokens)


# ============================================================
#                   ФИЛЬТРАЦИЯ ЗАКАЗА
# ============================================================

async def evaluate(session, job: Job) -> bool:
    """Решает, слать ли заказ. Заполняет job.difficulty, job.score, job.watched."""
    text = f"{job.title} {job.description}".lower()

    if any(bad in text for bad in BLACKLIST):
        return False
    keyword_ok = any(good in text for good in WHITELIST)

    if USE_AI_FILTER:
        if not keyword_ok:                # грубый предотбор экономит токены
            return False
        fit, score = await ai_analyze(session, job)
    else:
        fit, score = ("easy" if keyword_ok else "no"), 5

    if fit == "no":
        return False
    if RANK.get(fit, 3) > RANK.get(eff_max_difficulty(), 1):
        return False

    # фильтр по бюджету (если он указан и ниже порога — мимо; неизвестный не режем)
    bv = budget_to_number(job.budget)
    min_b = eff_min_budget()
    if bv and min_b and bv < min_b:
        return False

    score = max(1, min(10, score))
    job.difficulty = DIFFICULTY_LABELS.get(fit, "")
    job.score = score
    job.watched = any(w in text for w in WATCH_KEYWORDS)
    return True


# ============================================================
#                          TELEGRAM
# ============================================================

_session = AiohttpSession(proxy=TELEGRAM_PROXY) if TELEGRAM_PROXY else AiohttpSession()
_session.timeout = 60
bot = Bot(token=BOT_TOKEN, session=_session)
dp = Dispatcher()

job_cache: dict[str, Job] = {}


def _key(job: Job) -> str:
    k = str(abs(hash(job.uid)) % (10**12))
    job_cache[k] = job
    return k


def stars(score: int) -> str:
    return "⭐" if score >= STAR_THRESHOLD else ""


def build_card(job: Job) -> tuple[str, InlineKeyboardMarkup]:
    bell = "🔔 " if job.watched else ""
    age = f"  {job.age_label}" if job.age_label else ""
    flag = LANG_FLAG.get(job.lang, "")
    text = f"{bell}🆕 <b>{html.escape(job.title)}</b> {stars(job.score)}\n"
    text += f"📍 {job.source} {flag}  📈 Скор: {job.score}/10{age}\n"
    # предупреждение о возможном скаме (риск ниже порога отсева, но заметный)
    if job.scam_risk >= 4:
        text += f"⚠️ Возможный скам (риск {job.scam_risk}/10) — будь осторожен\n"
    if job.difficulty:
        text += f"⚙️ {job.difficulty}\n"
    if job.budget:
        text += f"💰 {html.escape(job.budget)}\n"
    # перевод/суть на русском от ИИ (если есть)
    if job.ru_summary:
        text += f"\n📝 {html.escape(job.ru_summary)}\n"
    if job.description:
        text += f"\n<i>{html.escape(job.description[:200])}…</i>\n"
    text += f"\n🔗 {job.link}"

    k = _key(job)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Открыть", url=job.link),
         InlineKeyboardButton(text="✍️ Промпт", callback_data=f"reply:{k}")],
        [InlineKeyboardButton(text="📊 Оценить", callback_data=f"price:{k}"),
         InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav:{k}")],
        [InlineKeyboardButton(text="💸 Заработок", callback_data=f"earn:{k}")],
    ])
    return text, kb


async def send_card(job: Job):
    text, kb = build_card(job)
    try:
        await bot.send_message(CHAT_ID, text, reply_markup=kb,
                               parse_mode="HTML", disable_web_page_preview=False)
        await asyncio.sleep(0.4)
    except Exception as e:
        log.error("Ошибка отправки: %s", e)


def _get_job(cb: CallbackQuery) -> Job | None:
    return job_cache.get(cb.data.split(":", 1)[1])


@dp.callback_query(F.data.startswith("reply:"))
async def cb_reply(cb: CallbackQuery):
    job = _get_job(cb)
    if not job:
        await cb.answer("Заказ устарел", show_alert=True); return
    await cb.answer("Генерирую 3 варианта…")
    async with aiohttp.ClientSession() as s:
        reply = await generate_reply(s, job)
    await cb.message.answer(f"✍️ <b>Отклики для «{html.escape(job.title)}»</b>\n\n"
                            f"{html.escape(reply)}", parse_mode="HTML")


@dp.callback_query(F.data.startswith("price:"))
async def cb_price(cb: CallbackQuery):
    job = _get_job(cb)
    if not job:
        await cb.answer("Заказ устарел", show_alert=True); return
    await cb.answer("Оцениваю…")
    async with aiohttp.ClientSession() as s:
        est = await estimate_price(s, job)
    await cb.message.answer(f"📊 <b>Оценка заказа</b>\n\n{html.escape(est)}",
                            parse_mode="HTML")


@dp.callback_query(F.data.startswith("earn:"))
async def cb_earn(cb: CallbackQuery):
    job = _get_job(cb)
    if not job:
        await cb.answer("Заказ устарел", show_alert=True); return
    await cb.answer("Считаю заработок…")
    async with aiohttp.ClientSession() as s:
        result = await estimate_earnings(s, job)
    await cb.message.answer(
        f"💸 <b>Потенциальный заработок</b>\n"
        f"<i>{html.escape(job.title)}</i>\n\n"
        f"{html.escape(result)}",
        parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("fav:"))
async def cb_fav(cb: CallbackQuery):
    job = _get_job(cb)
    if not job:
        await cb.answer("Заказ устарел", show_alert=True); return
    add_favorite(job)
    await cb.answer("Добавлено в избранное ⭐")


# -------------------- команды --------------------

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    await msg.answer(
        "Привет! Мониторю фриланс-биржи и шлю заказы под чистый вайбкодинг.\n\n"
        "Команды:\n"
        "/check — проверить сейчас\n"
        "/filter — показать настройки\n"
        "/budget 5000 — мин. бюджет в ₽ (0 = без фильтра)\n"
        "/difficulty easy|medium|hard — макс. сложность\n"
        "/favorites — избранное\n"
        "/digest — сводка за 24 часа\n"
        "/stats — статистика бота\n"
        "/activity — график активности по часам\n"
        "/settings — настройки кнопками\n"
        "/pause /resume — пауза/возобновить"
    )


@dp.message(Command("check"))
async def cmd_check(msg: Message):
    await msg.answer("Проверяю биржи…")
    n = await run_scan()
    await msg.answer(f"Готово. Новых подходящих: {n}")


@dp.message(Command("filter"))
async def cmd_filter(msg: Message):
    await msg.answer(
        "⚙️ <b>Текущие настройки</b>\n"
        f"Макс. сложность: {eff_max_difficulty()}\n"
        f"Мин. бюджет: {eff_min_budget()} ₽\n"
        f"ИИ-фильтр: {'вкл' if USE_AI_FILTER else 'выкл'}\n"
        f"Английские биржи: {'вкл' if ENABLE_ENGLISH else 'выкл'}\n"
        f"Тихие часы: {QUIET_START}:00–{QUIET_END}:00\n"
        f"Пауза: {'да' if is_paused() else 'нет'}",
        parse_mode="HTML")


@dp.message(Command("budget"))
async def cmd_budget(msg: Message):
    parts = msg.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await msg.answer("Использование: /budget 5000"); return
    set_setting("min_budget", int(parts[1]))
    await msg.answer(f"Мин. бюджет установлен: {parts[1]} ₽")


@dp.message(Command("difficulty"))
async def cmd_difficulty(msg: Message):
    parts = msg.text.split()
    if len(parts) < 2 or parts[1].lower() not in ("easy", "medium", "hard"):
        await msg.answer("Использование: /difficulty easy|medium|hard"); return
    set_setting("max_difficulty", parts[1].lower())
    await msg.answer(f"Макс. сложность: {parts[1].lower()}")


@dp.message(Command("pause"))
async def cmd_pause(msg: Message):
    set_setting("paused", "1")
    await msg.answer("⏸ Мониторинг на паузе. /resume чтобы продолжить.")


@dp.message(Command("resume"))
async def cmd_resume(msg: Message):
    set_setting("paused", "0")
    await msg.answer("▶️ Мониторинг возобновлён.")


@dp.message(Command("favorites"))
async def cmd_favorites(msg: Message):
    favs = list_favorites()
    if not favs:
        await msg.answer("В избранном пока пусто."); return
    text = "⭐ <b>Избранное</b>\n\n" + "\n\n".join(
        f"• <b>{html.escape(j.title)}</b>\n{j.link}" for j in favs)
    await msg.answer(text[:4000], parse_mode="HTML", disable_web_page_preview=True)


@dp.message(Command("digest"))
async def cmd_digest(msg: Message):
    await send_digest(force=True)


@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    s = get_stats()
    sources_text = "\n".join(
        f"  {src}: {cnt}" for src, cnt in s["top_sources"]
    ) or "  нет данных"
    filter_rate = (
        f"{s['passed_last10'] / s['scanned_last10'] * 100:.0f}%"
        if s["scanned_last10"] else "—"
    )
    await msg.answer(
        "📊 <b>Статистика бота</b>\n\n"
        f"👁 Просмотрено всего: {s['total_seen']}\n"
        f"✅ Отправлено всего: {s['total_sent']}\n"
        f"📅 Отправлено за 24ч: {s['sent_24h']}\n"
        f"⭐ В избранном: {s['total_favs']}\n\n"
        f"🔍 Прошло фильтр (последние {s['scans_count']} сканов): "
        f"{s['passed_last10']} из {s['scanned_last10']} ({filter_rate})\n\n"
        f"🏆 Топ бирж по отправленным:\n{sources_text}",
        parse_mode="HTML",
    )


@dp.message(Command("activity"))
async def cmd_activity(msg: Message):
    hours = activity_by_hour()
    total = sum(hours)
    if total == 0:
        await msg.answer("Пока нет данных для графика. Подожди, пока накопятся заказы.")
        return
    peak = max(hours)
    lines = []
    for h in range(24):
        bars = round(hours[h] / peak * 10) if peak else 0
        bar = "█" * bars
        lines.append(f"{h:02d}:00 {bar} {hours[h]}")
    # топ-3 часа
    top_hours = sorted(range(24), key=lambda h: hours[h], reverse=True)[:3]
    top_txt = ", ".join(f"{h:02d}:00" for h in top_hours if hours[h] > 0)
    await msg.answer(
        "📈 <b>Активность по часам</b> (за 7 дней, локальное время)\n\n"
        f"<code>{chr(10).join(lines)}</code>\n\n"
        f"🔥 Пик заказов: {top_txt}\nВсего за неделю: {total}",
        parse_mode="HTML",
    )


def _settings_keyboard() -> InlineKeyboardMarkup:
    diff = eff_max_difficulty()
    paused = is_paused()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚙️ Сложность: {diff}", callback_data="set:diff")],
        [InlineKeyboardButton(text="💰 Бюджет: 0₽", callback_data="set:b0"),
         InlineKeyboardButton(text="1000₽", callback_data="set:b1000"),
         InlineKeyboardButton(text="5000₽", callback_data="set:b5000")],
        [InlineKeyboardButton(
            text="▶️ Возобновить" if paused else "⏸ Пауза",
            callback_data="set:toggle")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="set:refresh")],
    ])


def _settings_text() -> str:
    return (
        "⚙️ <b>Настройки</b>\n\n"
        f"Макс. сложность: <b>{eff_max_difficulty()}</b>\n"
        f"Мин. бюджет: <b>{eff_min_budget()} ₽</b>\n"
        f"Статус: <b>{'на паузе ⏸' if is_paused() else 'активен ▶️'}</b>\n\n"
        "Меняй кнопками ниже:"
    )


@dp.message(Command("settings"))
async def cmd_settings(msg: Message):
    await msg.answer(_settings_text(), reply_markup=_settings_keyboard(),
                     parse_mode="HTML")


@dp.callback_query(F.data.startswith("set:"))
async def cb_settings(cb: CallbackQuery):
    action = cb.data.split(":", 1)[1]
    if action == "diff":
        # циклически переключаем easy -> medium -> hard -> easy
        order = ["easy", "medium", "hard"]
        cur = eff_max_difficulty()
        nxt = order[(order.index(cur) + 1) % 3] if cur in order else "easy"
        set_setting("max_difficulty", nxt)
        await cb.answer(f"Сложность: {nxt}")
    elif action.startswith("b"):
        val = action[1:]
        set_setting("min_budget", int(val))
        await cb.answer(f"Бюджет: {val} ₽")
    elif action == "toggle":
        set_setting("paused", "0" if is_paused() else "1")
        await cb.answer("Готово")
    else:
        await cb.answer("Обновлено")
    try:
        await cb.message.edit_text(_settings_text(),
                                   reply_markup=_settings_keyboard(),
                                   parse_mode="HTML")
    except Exception:
        pass


async def send_digest(force: bool = False):
    rows = jobs_last_24h()
    if not rows:
        if force:
            await bot.send_message(CHAT_ID, "За последние 24 часа подходящих заказов не было.")
        return
    top = rows[:5]
    text = f"📋 <b>Сводка за 24 часа</b>\nВсего подходящих: {len(rows)}\n\nТоп:\n"
    text += "\n".join(f"{stars(s)} {sc}/10 — <a href='{lnk}'>{html.escape(t)}</a> ({src})"
                      for t, lnk, sc, src in top)
    await bot.send_message(CHAT_ID, text, parse_mode="HTML", disable_web_page_preview=True)


# ============================================================
#                       ЦИКЛЫ
# ============================================================

async def run_scan() -> int:
    if is_paused():
        return 0
    scanned = 0
    passed = 0
    sent = 0
    new_jobs: list[Job] = []

    async with aiohttp.ClientSession() as session:
        # собираем кандидатов: RSS-биржи + Kwork из почты
        candidates: list[Job] = []
        for src in SOURCES:
            if not src.get("enabled"):
                continue
            candidates.extend(await fetch_source(session, src))
        candidates.extend(await fetch_email_jobs())

        for job in candidates:
            tk = title_key(job.title)
            if is_seen(job.uid, tk):
                continue
            # пропускаем заказы старше MAX_JOB_AGE_HOURS (если время известно)
            age = job.age_hours
            if age is not None and age > MAX_JOB_AGE_HOURS:
                continue
            # антидубль/антиспам по автору: если он уже накидал MAX_PER_AUTHOR
            # заказов за 12ч — пропускаем (один заказчик не должен заваливать ленту)
            if job.author and author_recent_count(job.author) >= MAX_PER_AUTHOR:
                mark_seen(job.uid, tk)
                continue
            mark_seen(job.uid, tk)
            scanned += 1
            if not await evaluate(session, job):
                continue
            # скам-фильтр: высокий риск — режем
            if job.scam_risk >= SCAM_THRESHOLD:
                log.info("Скам-риск %d, пропускаю: %s", job.scam_risk, job.title[:50])
                continue
            passed += 1
            mark_author(job.author)
            log_job(job)
            new_jobs.append(job)

    # сортируем: сначала самые свежие (у кого нет времени — в конец)
    new_jobs.sort(key=lambda j: j.published_at or "", reverse=True)

    for job in new_jobs:
        if in_quiet_hours():
            queue_pending(job)
        else:
            await send_card(job)
            sent += 1

    log_scan(scanned, passed, sent)
    return passed


async def poller():
    while True:
        try:
            n = await run_scan()
            if n:
                log.info("Новых заказов: %s", n)
        except Exception as e:
            log.error("Ошибка сканирования: %s", e)
        await asyncio.sleep(POLL_INTERVAL)


async def quiet_flush_loop():
    """Когда тихие часы закончились — отправляем накопленное пачкой."""
    while True:
        await asyncio.sleep(60)
        if not in_quiet_hours():
            pend = pop_pending()
            if pend:
                await bot.send_message(CHAT_ID, f"☀️ За ночь накопилось заказов: {len(pend)}")
                for job in sorted(pend, key=lambda j: j.score, reverse=True):
                    await send_card(job)


async def digest_loop():
    """Раз в день в DIGEST_HOUR шлём утреннюю сводку."""
    while True:
        await asyncio.sleep(60)
        if now_local().hour == DIGEST_HOUR:
            today = now_local().date().isoformat()
            if get_setting("last_digest", "") != today:
                set_setting("last_digest", today)
                await send_digest()


async def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="bot alive"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("Health-сервер слушает порт %s", port)


async def ensure_connection():
    attempt = 0
    while True:
        attempt += 1
        try:
            me = await bot.get_me()
            log.info("Подключился к Telegram как @%s", me.username)
            return
        except Exception as e:
            wait = min(60, 5 * attempt)
            log.warning("Нет связи с Telegram (попытка %s): %s. Жду %sс…",
                        attempt, type(e).__name__, wait)
            await asyncio.sleep(wait)


def check_config():
    problems = []
    if not BOT_TOKEN:
        problems.append("BOT_TOKEN пуст")
    if not CHAT_ID:
        problems.append("CHAT_ID пуст или 0")
    if AI_PROVIDER == "anthropic" and not ANTHROPIC_API_KEY:
        problems.append("ANTHROPIC_API_KEY пуст")
    if AI_PROVIDER == "openai" and not OPENAI_API_KEY:
        problems.append("OPENAI_API_KEY пуст")
    if AI_PROVIDER == "groq" and not GROQ_API_KEY:
        problems.append("GROQ_API_KEY пуст")
    if AI_PROVIDER == "gemini" and not GEMINI_API_KEY:
        problems.append("GEMINI_API_KEY пуст")
    if problems:
        log.error("Проверь .env: %s", "; ".join(problems))
        raise SystemExit(1)


async def main():
    check_config()
    db_init()
    await start_health_server()
    await ensure_connection()

    # На бесплатном Render деплой не zero-downtime: старый экземпляр ещё
    # держит getUpdates, пока стартует новый. Сбрасываем вебхук и копим
    # обновления заново, а небольшая пауза даёт старому процессу умереть —
    # так конфликтов при старте меньше.
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log.warning("Не удалось сбросить вебхук: %s", e)
    log.info("Жду 15с, чтобы старый экземпляр освободил соединение…")
    await asyncio.sleep(15)

    asyncio.create_task(poller())
    asyncio.create_task(quiet_flush_loop())
    asyncio.create_task(digest_loop())
    while True:
        try:
            await dp.start_polling(bot, handle_signals=False,
                                   drop_pending_updates=True)
        except Exception as e:
            log.error("Polling упал (%s). Перезапуск через 10с…", type(e).__name__)
            await asyncio.sleep(10)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Остановлено.")