
import os
import re
import asyncio
import logging
import sqlite3
import html
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
import feedparser
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import Command

# ============================================================
#               КОНФИГ ИЗ .env (секреты не в коде)
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = int(os.getenv("CHAT_ID", "0"))

AI_PROVIDER = os.getenv("AI_PROVIDER", "anthropic").strip().lower()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ИИ-фильтрация: true — ИИ финально решает, подходит ли заказ
USE_AI_FILTER = os.getenv("USE_AI_FILTER", "true").strip().lower() == "true"

# Прокси для Telegram (если VPN не помогает). Пусто = без прокси.
# Примеры: http://127.0.0.1:8080  |  socks5://127.0.0.1:1080 (нужен aiohttp_socks)
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY", "").strip()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))   # секунды

# ============================================================
#                   ИСТОЧНИКИ (биржи с откликами)
# ============================================================

SOURCES = [
    {"name": "Habr Freelance", "enabled": True,
     "url": "https://freelance.habr.com/tasks.rss"},
    {"name": "Weblancer",      "enabled": True,
     "url": "https://www.weblancer.net/rss/projects/"},
    {"name": "FL.ru",          "enabled": True,
     "url": "https://www.fl.ru/rss/all.xml"},
    {"name": "Freelance.ru",   "enabled": True,
     "url": "https://freelance.ru/rss/projects"},
    # Upwork: вставь свой RSS из сохранённого поиска и поставь enabled True
    {"name": "Upwork",         "enabled": False,
     "url": "https://www.upwork.com/ab/feed/jobs/rss?q=..."},
]

# ============================================================
#                       ФИЛЬТРЫ
# ============================================================

# Грубый предфильтр (чтобы зря не дёргать ИИ).
WHITELIST = [
    "лендинг", "landing", "сайт", "бот", "bot", "telegram", "автоматизац",
    "парсер", "парсинг", "scrap", "no-code", "no code", "ноукод",
    "интеграц", "api", "скрипт", "script", "чат-бот", "chatbot",
    "ai", "gpt", "нейросет", "автоматизировать", "веб-приложен",
    "web app", "виджет", "форма", "google sheets", "таблиц", "дашборд",
]
# Жёсткий чёрный список — сразу мимо.
BLACKLIST = [
    "дизайн", "логотип", "smm", "видеомонтаж", "видео", "монтаж",
    "копирайт", "рерайт", "перевод текст", "озвучк", "иллюстрац",
    "анимац", "3d", "моделирование", "верстальщик", "наполнение",
]

DB_PATH = "seen.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("freelance-bot")


# ============================================================
#                          МОДЕЛИ
# ============================================================

@dataclass
class Job:
    source: str
    title: str
    link: str
    description: str
    budget: str = ""

    @property
    def uid(self) -> str:
        return f"{self.source}::{self.link}"


# ============================================================
#                     БАЗА (дедупликация)
# ============================================================

def db_init():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS seen (uid TEXT PRIMARY KEY, ts TEXT)")
    conn.commit()
    conn.close()


def is_seen(uid: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT 1 FROM seen WHERE uid=?", (uid,)).fetchone()
    conn.close()
    return row is not None


def mark_seen(uid: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR IGNORE INTO seen (uid, ts) VALUES (?, ?)",
        (uid, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


# ============================================================
#                     ПАРСИНГ ИСТОЧНИКОВ
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


# Ищем сумму в тексте: "5000 руб", "от 10 000 ₽", "$500", "300 руб/час" и т.п.
_BUDGET_RE = re.compile(
    r"(?:от\s*|до\s*|~\s*)?\d[\d\s.,]*\s*"
    r"(?:руб|рублей|₽|р\.|rub|usd|\$|€|eur|грн)"
    r"(?:\s*/?\s*(?:час|hour|шт))?",
    re.IGNORECASE,
)


def extract_budget(text: str) -> str:
    m = _BUDGET_RE.search(text)
    return re.sub(r"\s+", " ", m.group(0)).strip() if m else ""


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
        budget = extract_budget(f"{title} {desc}")   # вытаскиваем сумму, если есть
        if link:
            jobs.append(Job(source=src["name"], title=title, link=link,
                            description=desc, budget=budget))
    return jobs


# ============================================================
#                     ФИЛЬТРАЦИЯ (слова + ИИ)
# ============================================================

async def is_suitable(session: aiohttp.ClientSession, job: Job) -> bool:
    text = f"{job.title} {job.description}".lower()

    # 1) жёсткий чёрный список — мимо
    if any(bad in text for bad in BLACKLIST):
        return False

    # 2) грубый предфильтр по ключевым словам (экономим токены ИИ)
    keyword_ok = any(good in text for good in WHITELIST)

    if not USE_AI_FILTER:
        return keyword_ok

    # 3) если совсем мимо по словам — ИИ не дёргаем
    if not keyword_ok:
        return False

    # 4) финальное решение принимает ИИ
    return await ai_is_suitable(session, job)


FILTER_SYSTEM = (
    "Ты фильтр заказов для фрилансера-вайбкодера (быстро собирает сайты, "
    "Telegram-ботов, парсеры, автоматизации, ИИ-интеграции, no-code решения "
    "с помощью ИИ). Оцени, подходит ли заказ под такой профиль. "
    "Ответь СТРОГО одним словом: ДА или НЕТ."
)


async def ai_is_suitable(session: aiohttp.ClientSession, job: Job) -> bool:
    msg = f"Заголовок: {job.title}\nОписание: {job.description[:800]}"
    try:
        if AI_PROVIDER == "anthropic":
            ans = await _call_anthropic(session, FILTER_SYSTEM, msg, max_tokens=5)
        else:
            ans = await _call_openai(session, FILTER_SYSTEM, msg, max_tokens=5)
    except Exception as e:
        log.warning("ИИ-фильтр недоступен, пропускаю по ключевым словам: %s", e)
        return True   # не теряем заказ из-за сбоя ИИ
    return "да" in ans.strip().lower()


# ============================================================
#                   ГЕНЕРАЦИЯ ОТКЛИКА (ИИ)
# ============================================================

REPLY_SYSTEM = (
    "Ты помогаешь фрилансеру-вайбкодеру писать короткие цепляющие отклики на "
    "заказы. Пиши на русском, по делу, 4-6 предложений: поздоровайся, покажи "
    "что понял задачу, предложи конкретный подход/стек, упомяни сроки и "
    "предложи обсудить детали. Без воды и канцелярита."
)


async def generate_reply(session: aiohttp.ClientSession, job: Job) -> str:
    msg = (
        f"Заказ с биржи {job.source}.\n"
        f"Заголовок: {job.title}\n"
        f"Описание: {job.description[:1500]}\n\n"
        f"Напиши отклик от моего лица."
    )
    try:
        if AI_PROVIDER == "anthropic":
            return await _call_anthropic(session, REPLY_SYSTEM, msg, max_tokens=600)
        return await _call_openai(session, REPLY_SYSTEM, msg, max_tokens=600)
    except Exception as e:
        log.error("Ошибка ИИ: %s", e)
        return "⚠️ Не удалось сгенерировать отклик. Проверь API-ключ/лимиты."


async def _call_anthropic(session, system: str, user_msg: str, max_tokens: int) -> str:
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user_msg}],
    }
    async with session.post(
        "https://api.anthropic.com/v1/messages", headers=headers, json=payload,
        timeout=aiohttp.ClientTimeout(total=60),
    ) as resp:
        data = await resp.json()
    return "".join(b.get("text", "") for b in data.get("content", [])).strip()


async def _call_openai(session, system: str, user_msg: str, max_tokens: int) -> str:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
    }
    async with session.post(
        "https://api.openai.com/v1/chat/completions", headers=headers, json=payload,
        timeout=aiohttp.ClientTimeout(total=60),
    ) as resp:
        data = await resp.json()
    return data["choices"][0]["message"]["content"].strip()


# ============================================================
#                         TELEGRAM
# ============================================================

# Сессия с прокси (если задан) и увеличенным таймаутом — против WinError 121
_session = AiohttpSession(proxy=TELEGRAM_PROXY) if TELEGRAM_PROXY else AiohttpSession()
_session.timeout = 60

bot = Bot(token=BOT_TOKEN, session=_session)
dp = Dispatcher()

job_cache: dict[str, Job] = {}   # ключ -> Job для callback-кнопок


def build_card(job: Job) -> tuple[str, InlineKeyboardMarkup]:
    text = f"🆕 <b>{html.escape(job.title)}</b>\n📍 {job.source}\n"
    if job.budget:
        text += f"💰 {html.escape(job.budget)}\n"
    if job.description:
        text += f"\n{html.escape(job.description[:300])}…\n"
    text += f"\n🔗 {job.link}"

    key = str(abs(hash(job.uid)) % (10**12))   # короткий ключ (лимит 64 байта)
    job_cache[key] = job

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔗 Открыть", url=job.link),
        InlineKeyboardButton(text="✍️ Промпт", callback_data=f"reply:{key}"),
    ]])
    return text, kb


@dp.callback_query(F.data.startswith("reply:"))
async def on_reply(cb: CallbackQuery):
    job = job_cache.get(cb.data.split(":", 1)[1])
    if not job:
        await cb.answer("Заказ устарел, перезапусти поиск", show_alert=True)
        return
    await cb.answer("Генерирую отклик…")
    async with aiohttp.ClientSession() as session:
        reply = await generate_reply(session, job)
    await cb.message.answer(
        f"✍️ <b>Отклик для «{html.escape(job.title)}»</b>\n\n{html.escape(reply)}",
        parse_mode="HTML",
    )


@dp.message(Command("start"))
async def on_start(msg: Message):
    await msg.answer(
        "Привет! Мониторю фриланс-биржи и шлю подходящие под вайбкодинг заказы. "
        "По каждому — кнопка «✍️ Промпт».\n\n/check — проверить сейчас"
    )


@dp.message(Command("check"))
async def on_check(msg: Message):
    await msg.answer("Проверяю биржи…")
    n = await run_scan()
    await msg.answer(f"Готово. Новых подходящих: {n}")


# ============================================================
#                        ОСНОВНОЙ ЦИКЛ
# ============================================================

async def run_scan() -> int:
    found = 0
    async with aiohttp.ClientSession() as session:
        for src in SOURCES:
            if not src.get("enabled"):
                continue
            for job in await fetch_source(session, src):
                if is_seen(job.uid):
                    continue
                mark_seen(job.uid)
                if not await is_suitable(session, job):
                    continue
                text, kb = build_card(job)
                try:
                    await bot.send_message(
                        CHAT_ID, text, reply_markup=kb,
                        parse_mode="HTML", disable_web_page_preview=False,
                    )
                    found += 1
                    await asyncio.sleep(0.5)   # анти-флуд
                except Exception as e:
                    log.error("Ошибка отправки: %s", e)
    return found


async def poller():
    while True:
        try:
            n = await run_scan()
            if n:
                log.info("Отправлено новых заказов: %s", n)
        except Exception as e:
            log.error("Ошибка сканирования: %s", e)
        await asyncio.sleep(POLL_INTERVAL)


async def ensure_connection():
    """Ждём связь с Telegram, не падаем на WinError 121 / таймауте."""
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


async def start_health_server():
    """Мини HTTP-сервер: Render видит открытый порт, пинговалка его дёргает,
    чтобы бесплатный Web Service не засыпал. Локально просто висит на 10000."""
    port = int(os.getenv("PORT", "10000"))
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="bot alive"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("Health-сервер слушает порт %s", port)


async def main():
    check_config()
    db_init()
    await start_health_server()              # для Render Web Service + пинговалки
    await ensure_connection()
    asyncio.create_task(poller())            # фоновый мониторинг
    while True:                              # авто-перезапуск polling при сбоях сети
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