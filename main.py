import os
import re
import json
import asyncio
import logging
import sqlite3
import html
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from collections import OrderedDict

import aiohttp
from aiohttp import web
import feedparser
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, ForceReply,
)
from aiogram.filters import Command

# Парсер Telegram-каналов через веб-превью t.me/s/ (без api_id/userbot).
# Если TG-парсер выключен или TG_CHANNELS пуст — бот работает как раньше, по RSS.
import tg_parser

# ============================================================
#                   КОНФИГ ИЗ .env
# ============================================================

load_dotenv()


def env_int(name: str, default: int) -> int:
    """Числовая переменная окружения, устойчивая к пустым значениям и inline-комментам.

    dotenv не всегда обрезает 'KEY=123  # коммент' и оставляет пустую 'KEY='
    как '' — на таких int() падал. Здесь: режем '#...', пробелы, пусто → default.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.split("#", 1)[0].strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = env_int("CHAT_ID", 0)

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
MIN_BUDGET = env_int("MIN_BUDGET", 1000)                       # минимум в ₽, 0 = без фильтра
STAR_THRESHOLD = env_int("STAR_THRESHOLD", 8)                  # с какого скора ставить ⭐

# Тихие часы (по локальному времени): ночью копим, утром шлём пачкой. start==end = выкл
QUIET_START = env_int("QUIET_START", 23)
QUIET_END = env_int("QUIET_END", 8)
TZ_OFFSET = env_int("TZ_OFFSET", 2)     # смещение от UTC (Варшава летом = +2)
DIGEST_HOUR = env_int("DIGEST_HOUR", 9)  # час утренней сводки (локальный)

# Ключевые теги — заказы с ними помечаются 🔔 (через запятую)
WATCH_KEYWORDS = [w.strip().lower() for w in os.getenv("WATCH_KEYWORDS", "").split(",") if w.strip()]

MAX_JOB_AGE_HOURS = env_int("MAX_JOB_AGE_HOURS", 48)  # заказы старше — пропускаем
MAX_PER_AUTHOR = env_int("MAX_PER_AUTHOR", 3)         # макс. заказов от 1 автора за 12ч
SCAM_THRESHOLD = env_int("SCAM_THRESHOLD", 7)        # с какого риска резать скам

POLL_INTERVAL = env_int("POLL_INTERVAL", 300)
# Раз в столько дней бот сам ищет новые TG-каналы и присылает их на одобрение.
# 0 = выключить авто-поиск (команда /discover всё равно работает вручную).
TG_DISCOVER_DAYS = env_int("TG_DISCOVER_DAYS", 7)
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY", "").split("#", 1)[0].strip()
# Прокси для запросов к ИИ. По умолчанию = TELEGRAM_PROXY (Groq/Gemini в РФ
# блокируются по гео и отдают 403 — локально нужен иностранный прокси).
# На Render обе пустые → запросы идут напрямую (там блокировки нет).
AI_PROXY = (os.getenv("AI_PROXY", "").split("#", 1)[0].strip() or TELEGRAM_PROXY) or None
AI_DELAY = env_int("AI_DELAY", 13)   # пауза между запросами к ИИ

# --- Бэкап базы в Telegram ---
# Render стирает локальный диск при каждом деплое — seen.db (история, избранное,
# настройки) теряется. Поэтому раз в BACKUP_INTERVAL_HOURS бот выгружает seen.db
# файлом в чат. После деплоя пересылаешь боту последний файл → команда /restore
# (или просто отправка .db-файла) восстанавливает базу. Bot API не даёт боту
# читать свою историю, поэтому восстановление ручное — зато надёжное и без рисков.
BACKUP_INTERVAL_HOURS = env_int("BACKUP_INTERVAL_HOURS", 6)
# Куда складывать бэкапы. Пусто = тот же чат, что и заказы (CHAT_ID).
BACKUP_CHAT_ID = env_int("BACKUP_CHAT_ID", 0) or CHAT_ID

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
    # --- зарубежные биржи отключены ---
    # Остановились на русских площадках с бесплатным откликом. Зарубежные
    # (англоязычные, валютные, часто с верификацией) убраны из ленты.
    # Freelancer.com — требует верификацию телефона.
    {"name": "Freelancer.com", "enabled": False,
     "url": "https://www.freelancer.com/rss.xml"},
    {"name": "RemoteOK", "enabled": False,
     "url": "https://remoteok.com/remote-dev-jobs.rss"},
    {"name": "WeWorkRemotely", "enabled": False,
     "url": "https://weworkremotely.com/categories/remote-programming-jobs.rss"},
    {"name": "Jobicy", "enabled": False,
     "url": "https://jobicy.com/?feed=job_feed&job_categories=dev"},
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
            return f"{int(h * 60)} мин назад"
        if h < 24:
            return f"{int(h)} ч назад"
        return f"{int(h / 24)} д назад"

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

def _ensure_column(c, table: str, column: str, coldef: str):
    """Добавляет колонку, если её нет (миграция старых баз / восстановленных бэкапов)."""
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}")
        log.info("Миграция БД: добавлена колонка %s.%s", table, column)


def db_init():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # WAL ускоряет запись и снимает блокировки чтения/записи
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("CREATE TABLE IF NOT EXISTS seen (uid TEXT PRIMARY KEY, title_key TEXT, ts TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT)")
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
    # каналы, добавленные на лету через /discover (хранятся в БД, не в .env)
    c.execute("CREATE TABLE IF NOT EXISTS tg_channels (uname TEXT PRIMARY KEY, ts TEXT)")
    # миграция старых баз: добавляем колонки, появившиеся в новых версиях,
    # чтобы индексы ниже и восстановленные старые бэкапы не падали
    _ensure_column(c, "seen", "title_key", "TEXT")
    _ensure_column(c, "seen", "ts", "TEXT")
    # индексы — быстрый поиск по часто запрашиваемым колонкам
    c.execute("CREATE INDEX IF NOT EXISTS idx_seen_titlekey ON seen(title_key)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_jobs_ts ON jobs_log(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_authors ON authors_seen(author, ts)")
    conn.commit()
    conn.close()


def _conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA busy_timeout=5000")   # ждём до 5с при блокировке вместо ошибки
    return conn


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


# тихие часы: время и вкл/выкл переопределяются командами из чата
def eff_quiet_start() -> int:
    return int(get_setting("quiet_start", QUIET_START))


def eff_quiet_end() -> int:
    return int(get_setting("quiet_end", QUIET_END))


def eff_quiet_enabled() -> bool:
    return get_setting("quiet_enabled", "1") == "1"


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


def tg_add_channel(uname: str):
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO tg_channels (uname, ts) VALUES (?,?)",
                 (uname.lower().lstrip("@"), datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def tg_del_channel(uname: str):
    conn = _conn()
    conn.execute("DELETE FROM tg_channels WHERE uname=?", (uname.lower().lstrip("@"),))
    conn.commit()
    conn.close()


def tg_get_channels() -> list[str]:
    conn = _conn()
    rows = conn.execute("SELECT uname FROM tg_channels ORDER BY ts").fetchall()
    conn.close()
    return [r[0] for r in rows]


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
    if rows:                       # пишем (DELETE) только когда есть что забирать
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
    if not eff_quiet_enabled():
        return False
    start, end = eff_quiet_start(), eff_quiet_end()
    if start == end:
        return False
    h = now_local().hour
    if start < end:
        return start <= h < end
    return h >= start or h < end   # период через полночь


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
    "easy": "лёгкая — хватит вайбкодинга",
    "medium": "средняя — вайбкодинг + доработка",
    "hard": "сложная — нужна ручная разработка",
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


def _is_rate_limit(e: Exception) -> bool:
    """Похоже ли это на исчерпание лимита провайдера (429 / RESOURCE_EXHAUSTED)."""
    s = str(e)
    return "RESOURCE_EXHAUSTED" in s or "429" in s


def _provider_chain() -> list[str]:
    """Основной провайдер первым, затем резерв (groq↔gemini), если ключ есть."""
    chain = [AI_PROVIDER]
    if AI_PROVIDER == "groq" and _gemini_keys:
        chain.append("gemini")
    elif AI_PROVIDER == "gemini" and GROQ_API_KEY:
        chain.append("groq")
    return chain


async def _ai_with_fallback(session, system, user_msg, max_tokens) -> str:
    """Запрос к ИИ с симметричным фолбэком: основной провайдер → резерв при лимите.

    Для gemini внутри перебираются все доступные ключи. Не-лимитные ошибки
    (нет ключа, кривая модель и т.п.) пробрасываются сразу — фолбэк только на 429.
    """
    last_err: Exception | None = None
    for provider in _provider_chain():
        try:
            return await call_ai_provider(session, provider, system, user_msg, max_tokens)
        except Exception as e:
            last_err = e
            if not _is_rate_limit(e):
                raise
            # gemini: до переключения на другой провайдер перебираем оставшиеся ключи
            if provider == "gemini":
                while _rotate_gemini_key():
                    try:
                        return await call_ai_provider(session, "gemini", system, user_msg, max_tokens)
                    except Exception as e2:
                        last_err = e2
                        if not _is_rate_limit(e2):
                            raise
            log.warning("Провайдер %s исчерпан по лимиту, перехожу на резерв…", provider)
    raise last_err if last_err else RuntimeError("ИИ недоступен")


async def ai_analyze(session: aiohttp.ClientSession, job: Job) -> tuple[str, int]:
    msg = (f"Заголовок: {job.title}\nОписание: {job.description[:800]}\n"
           f"Бюджет: {job.budget or 'не указан'}")
    try:
        async with _ai_lock:
            await asyncio.sleep(AI_DELAY)
            raw = await _ai_with_fallback(session, ANALYZE_SYSTEM, msg, max_tokens=160)
    except Exception as e:
        if _is_rate_limit(e):
            log.warning("Все провайдеры ИИ исчерпаны по лимиту — заказ пропускаю")
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

EARNINGS_SYSTEM = (
    "Ты помогаешь вайбкодеру понять, выгоден ли заказ. "
    "Вайбкодер — это фрилансер, который собирает решения с помощью ИИ быстро и дёшево по себестоимости. "
    "Дай конкретный разбор строго по этим пунктам, каждый с новой строки, БЕЗ эмодзи:\n"
    "Сложность: насколько легко сделать это чистым вайбкодингом — оценка X/10 и одна фраза почему "
    "(типовая ли задача, есть ли подводные камни, где вайбкодинг может дать слабый результат).\n"
    "Время: сколько часов реально займёт вайбкодингом (обычно быстрее обычного разработчика). Дай вилку, напр. «3–5 ч».\n"
    "Заработок: чистыми в рублях (бюджет минус ~500₽/час твоего времени).\n"
    "Ставка: эффективная ставка в час (бюджет ÷ часы).\n"
    "Вывод: одной фразой — стоит браться или нет и почему.\n"
    "Если бюджет не указан — оцени сам по рынку. Без воды, только цифры и короткие фразы."
)


async def generate_reply(session, job: Job) -> str:
    msg = (f"Заказ с биржи {job.source}.\nЗаголовок: {job.title}\n"
           f"Описание: {job.description[:1500]}\n\nНапиши три варианта отклика.")
    try:
        return await _ai_with_fallback(session, REPLY_SYSTEM, msg, max_tokens=900)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось сгенерировать отклик. Проверь ключ/лимиты."


async def estimate_earnings(session, job: Job) -> str:
    msg = (f"Заголовок: {job.title}\nОписание: {job.description[:1200]}\n"
           f"Бюджет заказчика: {job.budget or 'не указан'}\n"
           f"Сложность по оценке ИИ: {job.difficulty or 'не оценена'}")
    try:
        return await _ai_with_fallback(session, EARNINGS_SYSTEM, msg, max_tokens=450)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось рассчитать заработок."


async def _call_anthropic(session, system, user_msg, max_tokens):
    headers = {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
               "content-type": "application/json"}
    payload = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens, "system": system,
               "messages": [{"role": "user", "content": user_msg}]}
    async with session.post("https://api.anthropic.com/v1/messages", headers=headers,
                            json=payload, proxy=AI_PROXY,
                            timeout=aiohttp.ClientTimeout(total=60)) as resp:
        data = await resp.json()
    return "".join(b.get("text", "") for b in data.get("content", [])).strip()


async def _call_openai(session, system, user_msg, max_tokens):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": OPENAI_MODEL, "max_tokens": max_tokens,
               "messages": [{"role": "system", "content": system},
                            {"role": "user", "content": user_msg}]}
    async with session.post("https://api.openai.com/v1/chat/completions", headers=headers,
                            json=payload, proxy=AI_PROXY,
                            timeout=aiohttp.ClientTimeout(total=60)) as resp:
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
    async with session.post(url, headers=headers, json=payload, proxy=AI_PROXY,
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
                            headers=headers, json=payload, proxy=AI_PROXY,
                            timeout=aiohttp.ClientTimeout(total=60)) as resp:
        data = await resp.json()
    if "choices" not in data:
        raise RuntimeError(f"Groq вернул ошибку: {data.get('error', data)}")
    return data["choices"][0]["message"]["content"].strip()


async def call_ai_provider(session, provider, system, user_msg, max_tokens):
    """Вызывает конкретный провайдер по имени."""
    if provider == "anthropic":
        return await _call_anthropic(session, system, user_msg, max_tokens)
    if provider == "gemini":
        return await _call_gemini(session, system, user_msg, max_tokens)
    if provider == "groq":
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

    # дешёвый отсев по бюджету ДО вызова ИИ: если бюджет указан и ниже порога —
    # мимо (неизвестный бюджет не режем). Экономит запросы к ИИ на слабых заказах.
    bv = budget_to_number(job.budget)
    min_b = eff_min_budget()
    if bv and min_b and bv < min_b:
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

# Доступ только владельцу: бот игнорирует всех, кроме CHAT_ID. Без этого любой,
# кто найдёт бота по username, мог бы менять настройки и даже перезатереть базу,
# прислав .db-файл. Фильтр на диспетчере применяется ко ВСЕМ хендлерам разом.
dp.message.filter(F.chat.id == CHAT_ID)
dp.callback_query.filter(F.message.chat.id == CHAT_ID)

job_cache: "OrderedDict[str, Job]" = OrderedDict()
JOB_CACHE_LIMIT = 500   # держим последние N заказов для кнопок, старые вытесняем


def _key(job: Job) -> str:
    k = str(abs(hash(job.uid)) % (10**12))
    job_cache[k] = job
    job_cache.move_to_end(k)
    # вытесняем самые старые, чтобы память не росла бесконечно (бесплатный Render 512MB)
    while len(job_cache) > JOB_CACHE_LIMIT:
        job_cache.popitem(last=False)
    return k


def stars(score: int) -> str:
    return "⭐" if score >= STAR_THRESHOLD else ""


def build_card(job: Job) -> tuple[str, InlineKeyboardMarkup]:
    bell = "🔔 " if job.watched else ""
    star = " ⭐" if job.score >= STAR_THRESHOLD else ""
    # строка-мета: биржа · скор · возраст · язык — обычным текстом, без иконок
    meta = [job.source, f"скор {job.score}/10"]
    if job.age_label:
        meta.append(job.age_label)
    lang = {"ru": "RU", "en": "EN"}.get(job.lang, "")
    if lang:
        meta.append(lang)

    text = f"{bell}<b>{html.escape(job.title)}</b>{star}\n"
    text += " · ".join(meta) + "\n"
    # предупреждение о возможном скаме (риск ниже порога отсева, но заметный)
    if job.scam_risk >= 4:
        text += f"⚠️ Возможный скам (риск {job.scam_risk}/10)\n"
    if job.difficulty:
        text += f"Сложность: {job.difficulty}\n"
    if job.budget:
        text += f"Бюджет: {html.escape(job.budget)}\n"
    # перевод/суть на русском от ИИ (если есть)
    if job.ru_summary:
        text += f"\nСуть: {html.escape(job.ru_summary)}\n"
    if job.description:
        text += f"\n<i>{html.escape(job.description[:200])}…</i>\n"
    text += f"\nСсылка: {job.link}"

    k = _key(job)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть", url=job.link),
         InlineKeyboardButton(text="Промпт", callback_data=f"reply:{k}")],
        [InlineKeyboardButton(text="Разбор", callback_data=f"earn:{k}"),
         InlineKeyboardButton(text="В избранное", callback_data=f"fav:{k}")],
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
    await cb.message.answer(f"<b>Отклики для «{html.escape(job.title)}»</b>\n\n"
                            f"{html.escape(reply)}", parse_mode="HTML",
                            reply_markup=home_kb())


@dp.callback_query(F.data.startswith("earn:"))
async def cb_earn(cb: CallbackQuery):
    job = _get_job(cb)
    if not job:
        await cb.answer("Заказ устарел", show_alert=True); return
    await cb.answer("Разбираю заказ…")
    async with aiohttp.ClientSession() as s:
        result = await estimate_earnings(s, job)
    await cb.message.answer(
        f"<b>Разбор заказа</b> (сложность · время · деньги)\n"
        f"<i>{html.escape(job.title)}</i>\n\n"
        f"{html.escape(result)}",
        parse_mode="HTML",
        reply_markup=home_kb(),
    )


@dp.callback_query(F.data.startswith("fav:"))
async def cb_fav(cb: CallbackQuery):
    job = _get_job(cb)
    if not job:
        await cb.answer("Заказ устарел", show_alert=True); return
    add_favorite(job)
    await cb.answer("Добавлено в избранное ⭐")


# -------------------- команды --------------------

def commands_text() -> str:
    return (
        "Мониторю фриланс-биржи и шлю заказы под вайбкодинг.\n"
        "Управляй кнопками ниже или командами:\n\n"
        "/check — проверить сейчас\n"
        "/status — что бот делает прямо сейчас\n"
        "/settings — настройки (бюджет, сложность, тихие часы, пауза)\n"
        "/favorites — избранное\n"
        "/digest — сводка за 24 часа\n"
        "/stats — статистика\n"
        "/activity — график активности по часам\n"
        "/tg — статус парсера Telegram-каналов\n"
        "/discover — найти новые каналы\n"
        "/tgchannels — список каналов (добавить/удалить)\n"
        "/quiet — тихие часы\n"
        "/backup /restore — бэкап и восстановление базы\n"
        "/pause /resume — пауза/возобновить"
    )


def main_menu_kb() -> InlineKeyboardMarkup:
    """Главное меню кнопками — то же, что команды, но без набора."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Проверить сейчас", callback_data="menu:check")],
        [InlineKeyboardButton(text="🟢 Статус", callback_data="menu:status"),
         InlineKeyboardButton(text="Избранное", callback_data="menu:fav")],
        [InlineKeyboardButton(text="Сводка 24ч", callback_data="menu:digest"),
         InlineKeyboardButton(text="Статистика", callback_data="menu:stats")],
        [InlineKeyboardButton(text="Активность", callback_data="menu:activity"),
         InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings")],
    ])


def home_button() -> InlineKeyboardButton:
    """Кнопка возврата в главное меню из любого ответа."""
    return InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home")


def home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[home_button()]])


@dp.message(Command("start"))
async def cmd_start(msg: Message):
    await msg.answer(commands_text(), reply_markup=main_menu_kb())


@dp.callback_query(F.data.startswith("menu:"))
async def cb_menu(cb: CallbackQuery):
    """Главное меню: кнопки дублируют команды, чтобы не набирать вручную."""
    action = cb.data.split(":", 1)[1]
    if action == "home":
        await cb.answer()
        try:
            await cb.message.edit_text(commands_text(), reply_markup=main_menu_kb())
        except Exception:
            await cb.message.answer(commands_text(), reply_markup=main_menu_kb())
    elif action == "check":
        await cb.answer("Проверяю биржи…")
        n = await run_scan()
        await cb.message.answer(f"Готово. Новых подходящих: {n}", reply_markup=home_kb())
    elif action == "status":
        await cb.answer()
        await cb.message.answer(bot_status_text(), parse_mode="HTML", reply_markup=home_kb())
    elif action == "fav":
        await cb.answer()
        await show_favorites(cb.message)
    elif action == "digest":
        await cb.answer("Собираю сводку…")
        await send_digest(force=True)
    elif action == "stats":
        await cb.answer()
        await show_stats(cb.message)
    elif action == "activity":
        await cb.answer()
        await show_activity(cb.message)
    elif action == "settings":
        await cb.answer()
        await cb.message.answer(_settings_text(), reply_markup=_settings_keyboard(),
                                parse_mode="HTML")
    else:
        await cb.answer()


@dp.message(Command("check"))
async def cmd_check(msg: Message):
    await msg.answer("Проверяю биржи…")
    n = await run_scan()
    await msg.answer(f"Готово. Новых подходящих: {n}", reply_markup=home_kb())


@dp.message(Command("filter"))
async def cmd_filter(msg: Message):
    # /filter — алиас настроек (раньше дублировал /settings отдельным текстом)
    await msg.answer(_settings_text(), reply_markup=_settings_keyboard(),
                     parse_mode="HTML")


@dp.message(Command("budget"))
async def cmd_budget(msg: Message):
    parts = msg.text.split()
    # с числом — ставим сразу; без аргумента — открываем настройки с кнопками бюджета
    if len(parts) >= 2 and parts[1].isdigit():
        set_setting("min_budget", int(parts[1]))
        await msg.answer(f"Мин. бюджет: {parts[1]} ₽", reply_markup=home_kb())
        return
    await msg.answer(_settings_text(), reply_markup=_settings_keyboard(),
                     parse_mode="HTML")


@dp.message(Command("difficulty"))
async def cmd_difficulty(msg: Message):
    parts = msg.text.split()
    if len(parts) >= 2 and parts[1].lower() in ("easy", "medium", "hard"):
        set_setting("max_difficulty", parts[1].lower())
        await msg.answer(f"Макс. сложность: {parts[1].lower()}", reply_markup=home_kb())
        return
    await msg.answer(_settings_text(), reply_markup=_settings_keyboard(),
                     parse_mode="HTML")


@dp.message(Command("pause"))
async def cmd_pause(msg: Message):
    set_setting("paused", "1")
    await msg.answer("Мониторинг на паузе. /resume чтобы продолжить.", reply_markup=home_kb())


@dp.message(Command("resume"))
async def cmd_resume(msg: Message):
    set_setting("paused", "0")
    await msg.answer("Мониторинг возобновлён.", reply_markup=home_kb())


def _quiet_status_text() -> str:
    state = "включены" if eff_quiet_enabled() else "выключены"
    return (
        "🌙 <b>Тихие часы</b>\n"
        f"Сейчас: {state}\n"
        f"Время: {eff_quiet_start():02d}:00–{eff_quiet_end():02d}:00\n"
        "(ночью заказы копятся, утром приходят пачкой)"
    )


def _quiet_keyboard() -> InlineKeyboardMarkup:
    on = eff_quiet_enabled()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("Выключить" if on else "Включить"),
                              callback_data="set:quiet")],
        [InlineKeyboardButton(text="Задать время…", callback_data="set:qtime")],
        [home_button()],
    ])


@dp.message(Command("quiet"))
async def cmd_quiet(msg: Message):
    parts = msg.text.split()
    # без аргументов — показываем статус с кнопками
    if len(parts) < 2:
        await msg.answer(_quiet_status_text(), parse_mode="HTML",
                         reply_markup=_quiet_keyboard())
        return
    # текстовые аргументы оставлены для совместимости (on/off/23 8)
    arg = parts[1].lower()
    if arg in ("on", "вкл"):
        set_setting("quiet_enabled", "1")
    elif arg in ("off", "выкл"):
        set_setting("quiet_enabled", "0")
    elif arg.isdigit() and len(parts) >= 3 and parts[2].isdigit():
        start, end = int(arg), int(parts[2])
        if not (0 <= start <= 23 and 0 <= end <= 23):
            await msg.answer("Часы должны быть от 0 до 23. Пример: /quiet 23 8",
                             reply_markup=_quiet_keyboard())
            return
        set_setting("quiet_start", start)
        set_setting("quiet_end", end)
        set_setting("quiet_enabled", "1")   # задал время — значит хочешь, чтобы работало
    else:
        await msg.answer("Использование: /quiet on | off | /quiet 23 8",
                         reply_markup=_quiet_keyboard())
        return
    await msg.answer(_quiet_status_text(), parse_mode="HTML",
                     reply_markup=_quiet_keyboard())


async def show_favorites(target):
    favs = list_favorites()
    if not favs:
        await target.answer("В избранном пока пусто.", reply_markup=home_kb()); return
    text = "⭐ <b>Избранное</b>\n\n" + "\n\n".join(
        f"• <b>{html.escape(j.title)}</b>\n{j.link}" for j in favs)
    await target.answer(text[:4000], parse_mode="HTML", disable_web_page_preview=True,
                        reply_markup=home_kb())


@dp.message(Command("favorites"))
async def cmd_favorites(msg: Message):
    await show_favorites(msg)


@dp.message(Command("digest"))
async def cmd_digest(msg: Message):
    await send_digest(force=True)


async def show_stats(target):
    s = get_stats()
    sources_text = "\n".join(
        f"  {src}: {cnt}" for src, cnt in s["top_sources"]
    ) or "  нет данных"
    filter_rate = (
        f"{s['passed_last10'] / s['scanned_last10'] * 100:.0f}%"
        if s["scanned_last10"] else "—"
    )
    await target.answer(
        "📊 <b>Статистика бота</b>\n\n"
        f"Просмотрено всего: {s['total_seen']}\n"
        f"Отправлено всего: {s['total_sent']}\n"
        f"Отправлено за 24ч: {s['sent_24h']}\n"
        f"В избранном: {s['total_favs']}\n\n"
        f"Прошло фильтр (последние {s['scans_count']} сканов): "
        f"{s['passed_last10']} из {s['scanned_last10']} ({filter_rate})\n\n"
        f"Топ бирж по отправленным:\n{sources_text}",
        parse_mode="HTML",
        reply_markup=home_kb(),
    )


@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    await show_stats(msg)


async def show_activity(target):
    hours = activity_by_hour()
    total = sum(hours)
    if total == 0:
        await target.answer("Пока нет данных для графика. Подожди, пока накопятся заказы.",
                            reply_markup=home_kb())
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
    await target.answer(
        "📈 <b>Активность по часам</b> (за 7 дней, локальное время)\n\n"
        f"<code>{chr(10).join(lines)}</code>\n\n"
        f"Пик заказов: {top_txt}\nВсего за неделю: {total}",
        parse_mode="HTML",
        reply_markup=home_kb(),
    )


@dp.message(Command("activity"))
async def cmd_activity(msg: Message):
    await show_activity(msg)


@dp.message(Command("tg"))
async def cmd_tg(msg: Message):
    await msg.answer(await tg_parser.tg_status(), parse_mode="HTML",
                     reply_markup=home_kb(), disable_web_page_preview=True)


def _ago_min(ts: str) -> str:
    """«N мин назад» из ISO-времени UTC. Пусто/битое → '—'."""
    try:
        m = (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).total_seconds() / 60
        return f"{int(m)} мин назад"
    except Exception:
        return "—"


def bot_status_text() -> str:
    """Живой статус: анализирует ли бот сейчас, когда был последний анализ и т.д."""
    st = _scan_status
    if is_paused():
        head = "⏸ <b>Бот на паузе</b>\n(/resume или кнопка в настройках — продолжить)"
    elif st["running"]:
        head = "🟢 <b>Бот сейчас анализирует вакансии…</b>"
    else:
        head = "🟢 <b>Бот работает</b> — ждёт следующего скана"

    lines = [head, ""]
    if st["last_ts"]:
        lines.append(
            f"Последний анализ: {_ago_min(st['last_ts'])}\n"
            f"  оценено ИИ: {st['last_scanned']} · прошло фильтр: {st['last_passed']}"
        )
    else:
        lines.append("Анализа ещё не было — подожди первый цикл.")
    lines.append(f"\nRSS-сканы: каждые {max(1, POLL_INTERVAL // 60)} мин")
    lines.append(f"ИИ-провайдер: <b>{AI_PROVIDER}</b>")

    if tg_parser.tg_available():
        ls = tg_parser._last_scan
        tg_line = f"\nTG-каналы: {len(tg_parser.TG_CHANNELS)} шт."
        if ls:
            tg_line += (f"\n  последний скан каналов: {_ago_min(ls.get('ts', ''))} "
                        f"(постов {ls.get('candidates', 0)}, прошло {ls.get('passed', 0)})")
        lines.append(tg_line)

    return "\n".join(lines)


@dp.message(Command("status"))
async def cmd_status(msg: Message):
    await msg.answer(bot_status_text(), parse_mode="HTML", reply_markup=home_kb())


# -------------------- поиск и список TG-каналов --------------------

def _discover_kb(found: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"➕ @{u}", callback_data=f"tgadd:{u}")]
            for u, _ in found]
    rows.append([home_button()])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def run_discover(target):
    """Ищет каналы-кандидаты и присылает их с кнопками «Добавить»."""
    if not tg_parser.tg_available():
        await target.answer("TG-парсер выключен (TG_ENABLED). Включи его, потом ищи каналы.",
                            reply_markup=home_kb())
        return
    await target.answer("🔎 Ищу новые каналы — это займёт до минуты…")
    async with aiohttp.ClientSession() as s:
        found = await tg_parser.discover(s, AI_PROXY)
    if not found:
        await target.answer(
            "Новых подходящих каналов не нашёл. Добавь пару рабочих каналов "
            "(в TG_CHANNELS или из найденных) — от них поиск находит похожие.",
            reply_markup=home_kb())
        return
    text = ("🔎 <b>Каналы-кандидаты</b>\nНажми «➕», чтобы подключить:\n\n"
            + "\n".join(f"• @{u} — релевантность {sc}\n  https://t.me/{u}"
                        for u, sc in found))
    await target.answer(text, parse_mode="HTML", disable_web_page_preview=True,
                        reply_markup=_discover_kb(found))


@dp.message(Command("discover"))
async def cmd_discover(msg: Message):
    await run_discover(msg)


@dp.callback_query(F.data.startswith("tgadd:"))
async def cb_tgadd(cb: CallbackQuery):
    u = cb.data.split(":", 1)[1]
    tg_add_channel(u)
    await cb.answer(f"Добавлен @{u} ✅")


def _tgchannels_text() -> str:
    env = [tg_parser._channel_username(c).lower() for c in tg_parser.TG_CHANNELS]
    added = tg_get_channels()
    lines = ["📡 <b>Каналы парсера</b>"]
    if env:
        lines.append("\nИз .env (меняются на Render):")
        lines += [f"  • @{c}" for c in env]
    if added:
        lines.append("\nДобавленные (можно удалить кнопкой ниже):")
        lines += [f"  • @{c}" for c in added]
    if not env and not added:
        lines.append("\nПока пусто. Найди каналы командой /discover.")
    return "\n".join(lines)


def _tgchannels_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"🗑 @{c}", callback_data=f"tgdel:{c}")]
            for c in tg_get_channels()]
    rows.append([InlineKeyboardButton(text="🔎 Найти новые", callback_data="tgfind"),
                 home_button()])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("tgchannels"))
async def cmd_tgchannels(msg: Message):
    await msg.answer(_tgchannels_text(), parse_mode="HTML",
                     reply_markup=_tgchannels_kb(), disable_web_page_preview=True)


@dp.callback_query(F.data == "tgfind")
async def cb_tgfind(cb: CallbackQuery):
    await cb.answer("Ищу каналы…")
    await run_discover(cb.message)


@dp.callback_query(F.data.startswith("tgdel:"))
async def cb_tgdel(cb: CallbackQuery):
    u = cb.data.split(":", 1)[1]
    tg_del_channel(u)
    await cb.answer(f"Удалён @{u}")
    try:
        await cb.message.edit_text(_tgchannels_text(), parse_mode="HTML",
                                   reply_markup=_tgchannels_kb(),
                                   disable_web_page_preview=True)
    except Exception:
        pass


# Подсказки для ForceReply — текст сравнивается точь-в-точь в on_force_reply,
# поэтому шлём их без parse_mode (иначе text в reply_to_message может отличаться)
ASK_BUDGET = "Впиши минимальный бюджет в рублях (число). 0 — без фильтра."
ASK_QUIET = "Впиши тихие часы: начало и конец через пробел (0–23). Пример: 23 8"

BUDGET_PRESETS = [0, 1000, 3000, 5000]
DIFF_BUTTONS = [("easy", "Лёгкая"), ("medium", "Средняя"), ("hard", "Сложная")]


def _settings_keyboard() -> InlineKeyboardMarkup:
    cur_b = eff_min_budget()
    cur_d = eff_max_difficulty()
    paused = is_paused()
    quiet = eff_quiet_enabled()

    def mark(active: bool, label: str) -> str:
        return f"• {label}" if active else label

    diff_row = [InlineKeyboardButton(text=mark(code == cur_d, label),
                                     callback_data=f"set:d{code}")
                for code, label in DIFF_BUTTONS]
    budget_row = [InlineKeyboardButton(text=mark(v == cur_b, f"{v}₽"),
                                       callback_data=f"set:b{v}")
                  for v in BUDGET_PRESETS]

    return InlineKeyboardMarkup(inline_keyboard=[
        diff_row,
        budget_row,
        [InlineKeyboardButton(text="✏️ Свой бюджет…", callback_data="set:bcustom")],
        [InlineKeyboardButton(text=("Тихие часы: вкл" if quiet else "Тихие часы: выкл"),
                              callback_data="set:quiet"),
         InlineKeyboardButton(text="Задать время…", callback_data="set:qtime")],
        [InlineKeyboardButton(text=("Возобновить" if paused else "Пауза"),
                              callback_data="set:toggle")],
        [InlineKeyboardButton(text="Обновить", callback_data="set:refresh"),
         home_button()],
    ])


def _settings_text() -> str:
    return (
        "⚙️ <b>Настройки</b>\n\n"
        f"Сложность: <b>{eff_max_difficulty()}</b>\n"
        f"Мин. бюджет: <b>{eff_min_budget()} ₽</b>\n"
        f"Тихие часы: <b>{'вкл' if eff_quiet_enabled() else 'выкл'} "
        f"({eff_quiet_start():02d}:00–{eff_quiet_end():02d}:00)</b>\n"
        f"Статус: <b>{'на паузе' if is_paused() else 'активен'}</b>\n\n"
        "Меняй кнопками ниже. «•» — текущий выбор."
    )


@dp.message(Command("settings"))
async def cmd_settings(msg: Message):
    await msg.answer(_settings_text(), reply_markup=_settings_keyboard(),
                     parse_mode="HTML")


@dp.callback_query(F.data.startswith("set:"))
async def cb_settings(cb: CallbackQuery):
    action = cb.data.split(":", 1)[1]
    # запросы ввода числа — шлём ForceReply и выходим (настройки не перерисовываем)
    if action == "bcustom":
        await cb.answer()
        await cb.message.answer(
            ASK_BUDGET, reply_markup=ForceReply(input_field_placeholder="например 5000"))
        return
    if action == "qtime":
        await cb.answer()
        await cb.message.answer(
            ASK_QUIET, reply_markup=ForceReply(input_field_placeholder="23 8"))
        return

    if action.startswith("d") and action[1:] in ("easy", "medium", "hard"):
        set_setting("max_difficulty", action[1:])
        await cb.answer(f"Сложность: {action[1:]}")
    elif action.startswith("b") and action[1:].isdigit():
        set_setting("min_budget", int(action[1:]))
        await cb.answer(f"Бюджет: {action[1:]} ₽")
    elif action == "toggle":
        set_setting("paused", "0" if is_paused() else "1")
        await cb.answer("Готово")
    elif action == "quiet":
        set_setting("quiet_enabled", "0" if eff_quiet_enabled() else "1")
        await cb.answer("Тихие часы: " + ("вкл" if eff_quiet_enabled() else "выкл"))
    else:
        await cb.answer("Обновлено")
    try:
        await cb.message.edit_text(_settings_text(),
                                   reply_markup=_settings_keyboard(),
                                   parse_mode="HTML")
    except Exception:
        pass


@dp.message(F.reply_to_message, F.text)
async def on_force_reply(msg: Message):
    """Ловит ответ на ForceReply-подсказку (ввод бюджета / часов вручную)."""
    src = (msg.reply_to_message.text or "").strip()
    val = (msg.text or "").strip()
    if src == ASK_BUDGET:
        if not val.isdigit():
            await msg.answer("Нужно число. Пример: 5000", reply_markup=home_kb())
            return
        set_setting("min_budget", int(val))
        await msg.answer(f"Мин. бюджет: {val} ₽",
                         reply_markup=_settings_keyboard())
    elif src == ASK_QUIET:
        parts = val.split()
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            await msg.answer("Нужно два числа через пробел. Пример: 23 8",
                             reply_markup=home_kb())
            return
        start, end = int(parts[0]), int(parts[1])
        if not (0 <= start <= 23 and 0 <= end <= 23):
            await msg.answer("Часы должны быть от 0 до 23. Пример: 23 8",
                             reply_markup=home_kb())
            return
        set_setting("quiet_start", start)
        set_setting("quiet_end", end)
        set_setting("quiet_enabled", "1")
        await msg.answer(_quiet_status_text(), parse_mode="HTML",
                         reply_markup=_quiet_keyboard())
    # ответ не на нашу подсказку — игнорируем


async def send_digest(force: bool = False):
    rows = jobs_last_24h()
    if not rows:
        if force:
            await bot.send_message(CHAT_ID, "За последние 24 часа подходящих заказов не было.")
        return
    top = rows[:5]
    text = f"📋 <b>Сводка за 24 часа</b>\nВсего подходящих: {len(rows)}\n\nТоп:\n"
    text += "\n".join(
        f"{stars(sc)} {sc}/10 — "
        f"<a href='{html.escape(lnk, quote=True)}'>{html.escape(t)}</a> ({src})"
        for t, lnk, sc, src in top)
    await bot.send_message(CHAT_ID, text, parse_mode="HTML", disable_web_page_preview=True)


# ============================================================
#                   БЭКАП БАЗЫ В TELEGRAM
# ============================================================

def _checkpoint_db():
    """Сливает WAL в основной файл, чтобы seen.db был полным перед выгрузкой."""
    try:
        conn = _conn()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("Не удалось сделать checkpoint БД: %s", e)


async def backup_db(note: str = "") -> bool:
    """Выгружает seen.db файлом в BACKUP_CHAT_ID."""
    if not os.path.exists(DB_PATH):
        return False
    _checkpoint_db()
    stamp = now_local().strftime("%Y-%m-%d %H:%M")
    caption = f"💾 Бэкап базы · {stamp}"
    if note:
        caption += f"\n{note}"
    try:
        fname = f"seen-{now_local().strftime('%Y%m%d-%H%M')}.db"
        await bot.send_document(
            BACKUP_CHAT_ID,
            FSInputFile(DB_PATH, filename=fname),
            caption=caption,
        )
        return True
    except Exception as e:
        log.error("Ошибка бэкапа БД: %s", e)
        return False


async def restore_db(file_id: str) -> bool:
    """Скачивает присланный .db-файл и заменяет им текущую базу."""
    tmp = DB_PATH + ".incoming"
    try:
        f = await bot.get_file(file_id)
        await bot.download_file(f.file_path, tmp)
        # проверяем, что это валидная SQLite-база, прежде чем подменять
        test = sqlite3.connect(tmp)
        test.execute("SELECT count(*) FROM sqlite_master")
        test.close()
        # старые WAL/SHM убираем, иначе SQLite может смешать старые данные с новыми
        for suffix in ("-wal", "-shm"):
            p = DB_PATH + suffix
            if os.path.exists(p):
                os.remove(p)
        os.replace(tmp, DB_PATH)
        db_init()   # доставит недостающие таблицы/индексы, если бэкап старый
        return True
    except Exception as e:
        log.error("Ошибка восстановления БД: %s", e)
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass
        return False


@dp.message(Command("backup"))
async def cmd_backup(msg: Message):
    await msg.answer("Делаю бэкап базы…")
    ok = await backup_db(note="Ручной бэкап. Сохрани этот файл — пришлёшь обратно после деплоя.")
    if not ok:
        await msg.answer("⚠️ Не удалось сделать бэкап (базы ещё нет или ошибка).",
                         reply_markup=home_kb())
    else:
        await msg.answer("Готово, файл отправлен выше.", reply_markup=home_kb())


@dp.message(Command("restore"))
async def cmd_restore(msg: Message):
    await msg.answer(
        "♻️ <b>Восстановление базы</b>\n\n"
        "Просто отправь (или перешли) мне сюда файл бэкапа — "
        "<code>seen.db</code> или <code>seen-….db</code>. "
        "Я заменю им текущую базу: вернутся история, избранное и настройки.\n\n"
        f"Свежий бэкап я присылаю сам раз в {BACKUP_INTERVAL_HOURS} ч и по команде /backup.",
        parse_mode="HTML",
        reply_markup=home_kb(),
    )


@dp.message(F.document)
async def on_document(msg: Message):
    doc = msg.document
    name = (doc.file_name or "").lower()
    if not name.endswith(".db"):
        await msg.answer("Это не файл базы. Чтобы восстановить базу, пришли файл seen.db (.db).",
                         reply_markup=home_kb())
        return
    await msg.answer("Восстанавливаю базу из файла…")
    if await restore_db(doc.file_id):
        s = get_stats()
        await msg.answer(
            "✅ База восстановлена.\n"
            f"👁 Просмотрено: {s['total_seen']} · ⭐ Избранное: {s['total_favs']}",
            reply_markup=home_kb(),
        )
    else:
        await msg.answer("⚠️ Не удалось восстановить — файл повреждён или это не SQLite-база.",
                         reply_markup=home_kb())


# ============================================================
#                       ЦИКЛЫ
# ============================================================

_scan_lock = asyncio.Lock()   # один скан за раз: /check и poller не параллелятся

# Живое состояние для кнопки/команды статуса: анализирует ли бот прямо сейчас,
# когда был последний анализ и с каким результатом. Обновляется в process_candidates.
_scan_status = {
    "running": False,
    "last_ts": "",       # ISO-время конца последнего анализа
    "last_scanned": 0,   # сколько заказов оценено ИИ в последнем проходе
    "last_passed": 0,    # сколько прошло фильтр
}


async def run_scan() -> int:
    if is_paused():
        return 0
    # без лока ручной /check мог бы пойти параллельно с poller и отправить
    # один и тот же заказ дважды (оба прошли is_seen до mark_seen)
    async with _scan_lock:
        return await _do_scan()


async def process_candidates(session, candidates: list[Job]) -> tuple[int, int, list[Job]]:
    """Общий конвейер для кандидатов из любого источника (RSS-биржи и TG-каналы).

    Дедуп → фильтр по возрасту → антидубль по автору → ИИ-оценка → скам-фильтр.
    Возвращает (сколько оценено, сколько прошло фильтр, годные заказы).
    """
    scanned = 0
    passed = 0
    new_jobs: list[Job] = []
    _scan_status["running"] = True
    try:
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
    finally:
        # снимаем флаг и фиксируем итог даже если что-то упало в середине
        _scan_status["running"] = False
        _scan_status["last_ts"] = datetime.now(timezone.utc).isoformat()
        _scan_status["last_scanned"] = scanned
        _scan_status["last_passed"] = passed
    return scanned, passed, new_jobs


async def dispatch_jobs(new_jobs: list[Job]) -> int:
    """Сортирует по свежести и отправляет (или копит в тихие часы). Возвращает число отправленных."""
    # сначала самые свежие (у кого нет времени — в конец)
    new_jobs.sort(key=lambda j: j.published_at or "", reverse=True)
    sent = 0
    for job in new_jobs:
        if in_quiet_hours():
            queue_pending(job)
        else:
            await send_card(job)
            sent += 1
    return sent


async def _do_scan() -> int:
    async with aiohttp.ClientSession() as session:
        # собираем кандидатов с RSS-бирж (параллельно)
        candidates: list[Job] = []
        enabled = [s for s in SOURCES if s.get("enabled")]
        tasks = [fetch_source(session, src) for src in enabled]
        # параллельная загрузка: все биржи грузятся разом, а не по очереди
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                log.warning("Источник упал при загрузке: %s", res)
                continue
            candidates.extend(res)
        scanned, passed, new_jobs = await process_candidates(session, candidates)
    sent = await dispatch_jobs(new_jobs)
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


async def discover_loop():
    """Раз в TG_DISCOVER_DAYS бот сам ищет новые каналы и шлёт их на одобрение."""
    if TG_DISCOVER_DAYS <= 0 or not tg_parser.tg_available():
        return
    while True:
        await asyncio.sleep(TG_DISCOVER_DAYS * 86400)
        try:
            async with aiohttp.ClientSession() as s:
                found = await tg_parser.discover(s, AI_PROXY)
            if found:
                text = ("🔎 <b>Авто-поиск нашёл новые каналы</b>\nДобавить?\n\n"
                        + "\n".join(f"• @{u} — релевантность {sc}" for u, sc in found))
                await bot.send_message(CHAT_ID, text, parse_mode="HTML",
                                       disable_web_page_preview=True,
                                       reply_markup=_discover_kb(found))
        except Exception as e:
            log.error("Авто-поиск каналов упал: %s", e)


async def backup_loop():
    """Раз в BACKUP_INTERVAL_HOURS выгружает базу в Telegram (защита от обнуления Render)."""
    if BACKUP_INTERVAL_HOURS <= 0:
        return
    while True:
        await asyncio.sleep(BACKUP_INTERVAL_HOURS * 3600)
        await backup_db(note="Авто-бэкап. Сохрани последний файл — пригодится после деплоя.")


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
    port = env_int("PORT", 10000)
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
    # обновления заново. Раньше тут была фиксированная пауза 15с — теперь
    # стартуем сразу, а паузу/ретраи берёт на себя цикл polling ниже
    # (он сам переживает TelegramConflictError). Так бот стартует быстрее.
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log.warning("Не удалось сбросить вебхук: %s", e)

    asyncio.create_task(poller())
    asyncio.create_task(quiet_flush_loop())
    asyncio.create_task(digest_loop())
    asyncio.create_task(backup_loop())
    # парсер Telegram-каналов — отдельной задачей в том же event loop
    if tg_parser.tg_available():
        asyncio.create_task(tg_parser.tg_poll_loop())
        asyncio.create_task(discover_loop())
        log.info("TG-парсер каналов включён (каналов: %d)",
                 len(tg_parser.effective_channels()))
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