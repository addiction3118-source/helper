"""Парсер заказов из Telegram-каналов через userbot (Telethon).

Работает в том же процессе и event loop, что и основной бот (main.py). Раз в
TG_POLL_INTERVAL читает историю указанных каналов и отдаёт найденные посты в тот
же конвейер, что и RSS-биржи: ИИ-фильтр, дедуп по seen, скам-детектор, отправка
карточкой. Никакого дублирования логики — только добыча текста из каналов.

Сессия хранится строкой в TG_SESSION (генерируется один раз локально через
gen_session.py). Так на Render не нужен интерактивный вход по коду из СМС, и
сессия переживает стирание диска при деплое.

telethon импортируется лениво (внутри функций): если TG-парсер выключен или
библиотека не установлена, импорт этого модуля ничего не ломает — бот работает
по RSS как раньше.
"""
import os
import re
import asyncio
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger("freelance-bot")


def _int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").split("#", 1)[0].strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


TG_ENABLED = os.getenv("TG_ENABLED", "false").strip().lower() == "true"
TG_API_ID = _int("TG_API_ID", 0)
TG_API_HASH = os.getenv("TG_API_HASH", "").strip()
TG_SESSION = os.getenv("TG_SESSION", "").strip()
# Каналы через запятую: @distantsiya, @forkwork, https://t.me/... — всё ок
TG_CHANNELS = [c.strip() for c in os.getenv("TG_CHANNELS", "").split(",") if c.strip()]
TG_POLL_INTERVAL = _int("TG_POLL_INTERVAL", 300)   # как часто читать каналы, сек
TG_FETCH_LIMIT = _int("TG_FETCH_LIMIT", 30)        # сколько последних постов на канал
TG_MIN_LEN = _int("TG_MIN_LEN", 40)                # короче — это не заказ (отсев флуда)

# Авто-вступление в каналы из TG_CHANNELS, которых аккаунт ещё не состоит.
# По умолчанию выключено — для свежего аккаунта первые каналы безопаснее добавить
# руками. Вступаем по одному с паузой TG_JOIN_DELAY (анти-флуд).
TG_AUTOJOIN = os.getenv("TG_AUTOJOIN", "false").strip().lower() == "true"
TG_JOIN_DELAY = _int("TG_JOIN_DELAY", 45)          # пауза между вступлениями, сек

_client = None   # один клиент на весь процесс


def tg_available() -> bool:
    """Всё ли настроено, чтобы запускать парсер каналов."""
    if not TG_ENABLED:
        return False
    if not (TG_API_ID and TG_API_HASH and TG_SESSION and TG_CHANNELS):
        log.warning(
            "TG-парсер: TG_ENABLED=true, но не хватает "
            "TG_API_ID/TG_API_HASH/TG_SESSION/TG_CHANNELS — пропускаю"
        )
        return False
    return True


async def _get_client():
    """Создаёт (один раз) и возвращает подключённый Telethon-клиент."""
    global _client
    if _client is not None:
        return _client
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("TG-сессия невалидна/протухла — перегенерируй gen_session.py")
    me = await client.get_me()
    log.info("TG-парсер: вошёл как @%s, каналов: %d",
             me.username or me.id, len(TG_CHANNELS))
    _client = client
    return _client


def _invite_hash(channel: str):
    """Вытаскивает хеш приватной инвайт-ссылки (t.me/+hash, joinchat/hash), иначе None."""
    m = re.search(r"(?:joinchat/|t\.me/\+|^\+)([\w-]+)/?$", channel)
    return m.group(1) if m else None


async def _join_channels(client):
    """Вступает в каналы из TG_CHANNELS, которых аккаунт ещё не состоит.

    Безопасно: по одному, с паузой TG_JOIN_DELAY, с обработкой FloodWaitError
    (если Telegram просит подождать — ждём, а не долбим). Публичные каналы по
    @username можно читать и без вступления, так что неудачный join не критичен.
    """
    from telethon.errors import FloodWaitError, UserAlreadyParticipantError
    from telethon.tl.functions.channels import JoinChannelRequest
    from telethon.tl.functions.messages import ImportChatInviteRequest

    # что уже есть — чтобы не дёргать Telegram лишний раз (и не плодить флуд)
    joined_ids, joined_unames = set(), set()
    try:
        async for d in client.iter_dialogs():
            ent = d.entity
            joined_ids.add(getattr(ent, "id", None))
            u = getattr(ent, "username", None)
            if u:
                joined_unames.add(u.lower())
    except Exception as e:
        log.warning("TG-парсер: не смог получить список диалогов: %s", e)

    for ch in TG_CHANNELS:
        try:
            invite = _invite_hash(ch)
            if invite:                       # приватный канал по инвайт-ссылке
                try:
                    await client(ImportChatInviteRequest(invite))
                    log.info("TG-парсер: вступил по инвайту %s", ch)
                    await asyncio.sleep(TG_JOIN_DELAY)
                except UserAlreadyParticipantError:
                    pass
                continue

            uname = ch.lstrip("@").split("/")[-1].lower()
            if uname in joined_unames:       # уже состоим — пропускаем
                continue
            entity = await client.get_entity(ch)
            if getattr(entity, "id", None) in joined_ids:
                continue
            await client(JoinChannelRequest(entity))
            log.info("TG-парсер: вступил в %s", ch)
            await asyncio.sleep(TG_JOIN_DELAY)
        except FloodWaitError as e:
            log.warning("TG-парсер: Telegram просит подождать %dс перед вступлением — жду",
                        e.seconds)
            await asyncio.sleep(e.seconds + 5)
        except Exception as e:
            log.warning("TG-парсер: не смог вступить в %s: %s", ch, e)


def _make_title(text: str) -> str:
    """Заголовок из первой непустой строки поста."""
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line[:120]
    return "Заказ из Telegram"


async def _fetch_channel(client, channel: str, max_age_hours: int) -> list:
    """Читает последние посты канала и лепит из них Job-кандидаты."""
    from main import Job, extract_budget, detect_lang

    jobs = []
    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        log.warning("TG-парсер: канал %s недоступен: %s", channel, e)
        return jobs

    uname = getattr(entity, "username", None)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    try:
        async for m in client.iter_messages(entity, limit=TG_FETCH_LIMIT):
            text = (m.message or "").strip()
            if len(text) < TG_MIN_LEN:
                continue
            mdate = m.date.astimezone(timezone.utc) if m.date else None
            # сообщения идут от новых к старым — встретили старое, дальше не нужно
            if mdate and mdate < cutoff:
                break
            link = f"https://t.me/{uname}/{m.id}" if uname else f"tg-{channel}-{m.id}"
            jobs.append(Job(
                source=f"TG: {channel}",
                title=_make_title(text),
                link=link,
                description=text[:2000],
                budget=extract_budget(text),
                published_at=mdate.isoformat() if mdate else "",
                author="",   # в канале «автор» = сам канал; антидубль по автору не нужен
                lang=detect_lang(text),
            ))
    except Exception as e:
        log.warning("TG-парсер: ошибка чтения %s: %s", channel, e)
    return jobs


async def _scan_once(client, process_candidates, dispatch_jobs, log_scan,
                     scan_lock, max_age_hours):
    import aiohttp

    candidates = []
    for ch in TG_CHANNELS:          # последовательно — чтобы не словить флуд-лимит
        candidates.extend(await _fetch_channel(client, ch, max_age_hours))
    if not candidates:
        return

    # общий лок с RSS-сканом: не обрабатываем одни и те же источники параллельно
    async with scan_lock:
        async with aiohttp.ClientSession() as session:
            scanned, passed, new_jobs = await process_candidates(session, candidates)
        sent = await dispatch_jobs(new_jobs)
    log_scan(scanned, passed, sent)
    if passed:
        log.info("TG-парсер: новых заказов из каналов: %d", passed)


async def tg_poll_loop():
    """Главный цикл: раз в TG_POLL_INTERVAL читаем каналы и шлём найденное."""
    if not tg_available():
        return
    from main import (process_candidates, dispatch_jobs, log_scan,
                      _scan_lock, MAX_JOB_AGE_HOURS, is_paused)

    try:
        client = await _get_client()
    except Exception as e:
        log.error("TG-парсер: не удалось запустить клиент: %s", e)
        return

    if TG_AUTOJOIN:
        log.info("TG-парсер: проверяю подписки и вступаю в недостающие каналы…")
        try:
            await _join_channels(client)
        except Exception as e:
            log.warning("TG-парсер: авто-вступление прервано: %s", e)

    await asyncio.sleep(10)   # не стартуем впритык к первому RSS-скану
    while True:
        try:
            if not is_paused():
                await _scan_once(client, process_candidates, dispatch_jobs,
                                 log_scan, _scan_lock, MAX_JOB_AGE_HOURS)
        except Exception as e:
            log.error("TG-парсер: ошибка скана: %s", e)
        await asyncio.sleep(TG_POLL_INTERVAL)
