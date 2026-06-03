"""
Telegram-бот для мониторинга фриланс-бирж под ЧИСТЫЙ вайбкодинг.
Парсит RSS бирж, через ИИ фильтрует/оценивает заказы, шлёт карточки с
кнопками: промпт-отклик (3 варианта), оценка цены/времени, избранное, 👍/👎.

Фичи: скоринг 1-10, фильтр по сложности и бюджету, обучение на 👍/👎,
команды управления из чата, сводка за день, тихие часы, дедуп дублей,
русские и английские биржи, мониторинг ключевых тегов.

Секреты — в .env. Установка: pip install -r requirements.txt
Запуск: python main.py  (или кнопка ▷ в VS Code)

ВАЖНО про Render free: диск временный, при передеплое seen.db обнуляется
(избранное/обучение/настройки сбросятся). Для постоянного хранения — Postgres.
"""

import os
import re
import json
import asyncio
import logging
import sqlite3
import html
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

AI_PROVIDER = os.getenv("AI_PROVIDER", "openai").strip().lower()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

USE_AI_FILTER = os.getenv("USE_AI_FILTER", "true").strip().lower() == "true"

# Дефолты (можно менять командами из чата — они переопределяют эти значения)
MAX_DIFFICULTY = os.getenv("MAX_DIFFICULTY", "easy").strip().lower()   # easy/medium/hard
MIN_BUDGET = int(os.getenv("MIN_BUDGET", "0"))                          # минимум в ₽, 0 = без фильтра
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

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY", "").strip()

# ============================================================
#                   ИСТОЧНИКИ (биржи)
# ============================================================

SOURCES = [
    # --- русские биржи с откликами ---
    {"name": "Habr Freelance", "enabled": True,
     "url": "https://freelance.habr.com/tasks.rss"},
    {"name": "Weblancer", "enabled": True,
     "url": "https://www.weblancer.net/rss/projects/"},
    {"name": "FL.ru", "enabled": True,
     "url": "https://www.fl.ru/rss/all.xml"},
    {"name": "Freelance.ru", "enabled": True,
     "url": "https://freelance.ru/rss/projects"},
    # --- английские биржи (включаются флагом ENABLE_ENGLISH) ---
    {"name": "Freelancer.com", "enabled": ENABLE_ENGLISH,
     "url": "https://www.freelancer.com/rss.xml"},
    {"name": "RemoteOK", "enabled": ENABLE_ENGLISH,
     "url": "https://remoteok.com/remote-dev-jobs.rss"},
    # Upwork: вставь свой RSS из сохранённого поиска и поставь enabled True
    {"name": "Upwork", "enabled": False,
     "url": "https://www.upwork.com/ab/feed/jobs/rss?q=..."},
    # Kwork: открытого RSS нет, нужен HTML-парсинг с риском капчи — выключено
]

# ============================================================
#                   ФИЛЬТРЫ (грубый предотбор)
# ============================================================

WHITELIST = [
    "лендинг", "landing", "сайт", "site", "бот", "bot", "telegram", "автоматизац",
    "парсер", "парсинг", "scrap", "no-code", "no code", "ноукод", "интеграц",
    "api", "скрипт", "script", "чат-бот", "chatbot", "ai", "gpt", "нейросет",
    "автоматизировать", "веб-приложен", "web app", "виджет", "форма", "form",
    "google sheets", "таблиц", "дашборд", "dashboard", "automation", "website",
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

    @property
    def uid(self) -> str:
        return f"{self.source}::{self.link}"

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    @staticmethod
    def from_dict(d: dict) -> "Job":
        return Job(**{k: d[k] for k in (
            "source", "title", "link", "description", "budget",
            "difficulty", "score", "watched") if k in d})


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
    conn.commit()
    conn.close()


def _conn():
    return sqlite3.connect(DB_PATH)


def title_key(title: str) -> str:
    """Нормализованный ключ заголовка для отлова дублей с разных бирж."""
    return re.sub(r"[^a-zа-яё0-9]", "", title.lower())[:80]


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
        if link:
            jobs.append(Job(source=src["name"], title=title, link=link,
                            description=desc, budget=budget))
    return jobs


# ============================================================
#                   ИИ: анализ + генерация
# ============================================================

ANALYZE_SYSTEM = (
    "Ты анализируешь заказ для фрилансера, который хочет ТОЛЬКО вайбкодить — "
    "быстро собирать решения с помощью ИИ-инструментов (сайты, Telegram-боты, "
    "парсеры, автоматизации, no-code, ИИ-интеграции), без тяжёлой ручной "
    "разработки. Верни СТРОГО JSON без пояснений и без markdown:\n"
    '{"fit": "easy|medium|hard|no", "score": число от 1 до 10}\n'
    "fit: easy — собирается за вечер чистым вайбкодингом; medium — вайбкодинг + "
    "ручная доработка; hard — нужна серьёзная ручная разработка; no — заказ "
    "вообще не про код (дизайн, видео, тексты).\n"
    "score: насколько заказ хорош ИМЕННО для чистого вайбкодинга и выгоден "
    "(10 — идеально простой и денежный, 1 — почти не подходит)."
)

DIFFICULTY_LABELS = {
    "easy": "🟢 Лёгкая — хватит вайбкодинга",
    "medium": "🟡 Средняя — вайбкодинг + доработка",
    "hard": "🔴 Сложная — нужна ручная разработка",
}
RANK = {"easy": 1, "medium": 2, "hard": 3}


async def ai_analyze(session: aiohttp.ClientSession, job: Job) -> tuple[str, int]:
    msg = (f"Заголовок: {job.title}\nОписание: {job.description[:800]}\n"
           f"Бюджет: {job.budget or 'не указан'}")
    try:
        if AI_PROVIDER == "anthropic":
            raw = await _call_anthropic(session, ANALYZE_SYSTEM, msg, max_tokens=40)
        else:
            raw = await _call_openai(session, ANALYZE_SYSTEM, msg, max_tokens=40)
    except Exception as e:
        log.warning("ИИ-анализ недоступен, заказ пропускаю (не оценён): %s", e)
        return "no", 0      # не смогли оценить → не шлём, чтобы не было мусора
    txt = raw.strip().strip("`")
    txt = re.sub(r"^json", "", txt, flags=re.IGNORECASE).strip()
    try:
        data = json.loads(txt[txt.find("{"): txt.rfind("}") + 1])
        fit = str(data.get("fit", "medium")).lower()
        score = int(data.get("score", 5))
    except Exception:
        fit, score = "medium", 5
    if fit not in ("easy", "medium", "hard", "no"):
        fit = "medium"
    return fit, max(1, min(10, score))


REPLY_SYSTEM = (
    "Ты помогаешь фрилансеру-вайбкодеру писать отклики на заказы. "
    "Напиши ТРИ варианта на русском, разделённые строкой '---'. "
    "Вариант 1 — короткий и деловой (3-4 предложения). "
    "Вариант 2 — подробный, с конкретным подходом и стеком. "
    "Вариант 3 — лёгкий, неформальный. В каждом: понимание задачи, подход, "
    "сроки, призыв обсудить. Без воды и канцелярита. Не нумеруй заголовки."
)

PRICE_SYSTEM = (
    "Ты оцениваешь заказ для вайбкодера. Кратко (3-5 строк) дай: "
    "справедливую вилку цены в рублях, примерные часы работы и 1-2 риска/нюанса. "
    "Без воды, по делу."
)


async def generate_reply(session, job: Job) -> str:
    msg = (f"Заказ с биржи {job.source}.\nЗаголовок: {job.title}\n"
           f"Описание: {job.description[:1500]}\n\nНапиши три варианта отклика.")
    try:
        if AI_PROVIDER == "anthropic":
            return await _call_anthropic(session, REPLY_SYSTEM, msg, max_tokens=900)
        return await _call_openai(session, REPLY_SYSTEM, msg, max_tokens=900)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось сгенерировать отклик. Проверь ключ/лимиты."


async def estimate_price(session, job: Job) -> str:
    msg = (f"Заголовок: {job.title}\nОписание: {job.description[:1200]}\n"
           f"Указанный бюджет: {job.budget or 'не указан'}")
    try:
        if AI_PROVIDER == "anthropic":
            return await _call_anthropic(session, PRICE_SYSTEM, msg, max_tokens=300)
        return await _call_openai(session, PRICE_SYSTEM, msg, max_tokens=300)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось оценить заказ."


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
    text = f"{bell}🆕 <b>{html.escape(job.title)}</b> {stars(job.score)}\n"
    text += f"📍 {job.source}   📈 Скор: {job.score}/10\n"
    if job.difficulty:
        text += f"⚙️ {job.difficulty}\n"
    if job.budget:
        text += f"💰 {html.escape(job.budget)}\n"
    if job.description:
        text += f"\n{html.escape(job.description[:280])}…\n"
    text += f"\n🔗 {job.link}"

    k = _key(job)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Открыть", url=job.link),
         InlineKeyboardButton(text="✍️ Промпт", callback_data=f"reply:{k}")],
        [InlineKeyboardButton(text="📊 Оценить", callback_data=f"price:{k}"),
         InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav:{k}")],
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
    found = 0
    async with aiohttp.ClientSession() as session:
        for src in SOURCES:
            if not src.get("enabled"):
                continue
            for job in await fetch_source(session, src):
                tk = title_key(job.title)
                if is_seen(job.uid, tk):
                    continue
                mark_seen(job.uid, tk)
                if not await evaluate(session, job):
                    continue
                log_job(job)
                if in_quiet_hours():
                    queue_pending(job)       # ночью копим
                else:
                    await send_card(job)
                found += 1
    return found


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
    if problems:
        log.error("Проверь .env: %s", "; ".join(problems))
        raise SystemExit(1)


async def main():
    check_config()
    db_init()
    await start_health_server()
    await ensure_connection()
    asyncio.create_task(poller())
    asyncio.create_task(quiet_flush_loop())
    asyncio.create_task(digest_loop())
    while True:
        try:
            await dp.start_polling(bot, handle_signals=False)
        except Exception as e:
            log.error("Polling упал (%s). Перезапуск через 10с…", type(e).__name__)
            await asyncio.sleep(10)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Остановлено.")