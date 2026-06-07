"""Парсер заказов из Telegram-каналов через веб-превью t.me/s/<канал>.

Публичные каналы Telegram отдают последние посты обычной HTML-страницей по
адресу https://t.me/s/<username> — без логина, без api_id, без userbot. Мы
просто скачиваем её (как RSS-биржу) и парсим посты.

Плюсы такого подхода: не нужен my.telegram.org / Telethon / сессия, нулевой
риск бана аккаунта, работает на Render напрямую. Минус: только ПУБЛИЧНЫЕ каналы
(по @username); приватные/по-инвайту так не прочитать.

Найденные посты уходят в тот же конвейер, что и RSS-биржи (main.py):
ИИ-фильтр, скам-детектор, дедуп по seen, перевод, отправка карточкой.
"""
import os
import re
import html
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import aiohttp

log = logging.getLogger("freelance-bot")


def _int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").split("#", 1)[0].strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


TG_ENABLED = os.getenv("TG_ENABLED", "false").strip().lower() == "true"
# Каналы через запятую: @distantsiya, https://t.me/forkwork, norm_rabota — всё ок
TG_CHANNELS = [c.strip() for c in os.getenv("TG_CHANNELS", "").split(",") if c.strip()]
TG_POLL_INTERVAL = _int("TG_POLL_INTERVAL", 300)   # как часто читать каналы, сек
TG_FETCH_LIMIT = _int("TG_FETCH_LIMIT", 30)        # сколько последних постов брать на канал
TG_MIN_LEN = _int("TG_MIN_LEN", 40)                # короче — это не заказ (отсев флуда)

_last_scan: dict = {}   # инфо о последнем скане каналов (для команды /tg)

_MSG_SPLIT = '<div class="tgme_widget_message_wrap'


def tg_available() -> bool:
    """Всё ли настроено, чтобы запускать парсер каналов."""
    if not TG_ENABLED:
        return False
    if not TG_CHANNELS:
        log.warning("TG-парсер: TG_ENABLED=true, но TG_CHANNELS пуст — пропускаю")
        return False
    return True


def _channel_username(channel: str) -> str:
    """Достаёт чистый username из @name / ссылки / голого имени."""
    ch = channel.strip()
    ch = re.sub(r"^https?://t\.me/", "", ch, flags=re.IGNORECASE)
    ch = ch.lstrip("@").strip("/")
    return ch


def _clean(s: str) -> str:
    """HTML поста → плоский текст: <br> в перевод строки, теги долой, entities назад."""
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(p|div)>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _make_title(text: str) -> str:
    """Заголовок из первой непустой строки поста."""
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line[:120]
    return "Заказ из Telegram"


def _parse_page(page: str, uname: str, max_age_hours: int) -> list:
    """Разбирает HTML страницы t.me/s/<uname> на Job-кандидаты."""
    from main import Job, extract_budget, detect_lang

    jobs = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    # посты в HTML идут от старых к новым (новые внизу)
    for chunk in page.split(_MSG_SPLIT)[1:]:
        post = re.search(r'data-post="([^"]+)"', chunk)
        text_m = re.search(
            r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', chunk, re.S)
        if not post or not text_m:
            continue
        text = _clean(text_m.group(1))
        if len(text) < TG_MIN_LEN:
            continue

        published_at = ""
        tm = re.search(r'<time[^>]*datetime="([^"]+)"', chunk)
        if tm:
            try:
                dt = datetime.fromisoformat(tm.group(1)).astimezone(timezone.utc)
                if dt < cutoff:
                    continue   # слишком старый пост — пропускаем
                published_at = dt.isoformat()
            except Exception:
                pass

        jobs.append(Job(
            source=f"TG: @{uname}",
            title=_make_title(text),
            link=f"https://t.me/{post.group(1)}",
            description=text[:2000],
            budget=extract_budget(text),
            published_at=published_at,
            author="",   # в канале «автор» = сам канал; антидубль по автору не нужен
            lang=detect_lang(text),
        ))
    # оставляем только самые свежие TG_FETCH_LIMIT (новые — в конце списка)
    return jobs[-TG_FETCH_LIMIT:]


async def _fetch_channel(session, channel: str, max_age_hours: int, proxy) -> list:
    """Качает веб-превью канала и парсит посты в Job-кандидаты."""
    uname = _channel_username(channel)
    if not uname or "joinchat" in channel or uname.startswith("+"):
        log.warning("TG-парсер: %s — приватный канал, веб-превью недоступно, пропускаю", channel)
        return []
    url = f"https://t.me/s/{uname}"
    try:
        async with session.get(url, proxy=proxy,
                               headers={"User-Agent": "Mozilla/5.0"},
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            page = await resp.text()
    except Exception as e:
        log.warning("TG-парсер: не удалось загрузить %s: %s", uname, e)
        return []
    return _parse_page(page, uname, max_age_hours)


async def _scan_once(process_candidates, dispatch_jobs, log_scan, scan_lock, max_age_hours):
    from main import AI_PROXY   # t.me блокируется по гео так же, как Telegram — тот же прокси

    candidates = []
    async with aiohttp.ClientSession() as session:
        for ch in TG_CHANNELS:      # последовательно — без бурста по t.me
            candidates.extend(await _fetch_channel(session, ch, max_age_hours, AI_PROXY))

        scanned = passed = sent = 0
        if candidates:
            # общий лок с RSS-сканом: не обрабатываем источники параллельно
            async with scan_lock:
                scanned, passed, new_jobs = await process_candidates(session, candidates)
                sent = await dispatch_jobs(new_jobs)
            log_scan(scanned, passed, sent)
            if passed:
                log.info("TG-парсер: новых заказов из каналов: %d", passed)

    _last_scan.update(ts=datetime.now(timezone.utc).isoformat(),
                      candidates=len(candidates), passed=passed, sent=sent)


async def tg_poll_loop():
    """Главный цикл: раз в TG_POLL_INTERVAL читаем каналы и шлём найденное."""
    if not tg_available():
        return
    from main import (process_candidates, dispatch_jobs, log_scan,
                      _scan_lock, MAX_JOB_AGE_HOURS, is_paused)

    log.info("TG-парсер каналов запущен (t.me/s/), каналов: %d", len(TG_CHANNELS))
    await asyncio.sleep(10)   # не стартуем впритык к первому RSS-скану
    while True:
        try:
            if not is_paused():
                await _scan_once(process_candidates, dispatch_jobs,
                                 log_scan, _scan_lock, MAX_JOB_AGE_HOURS)
        except Exception as e:
            log.error("TG-парсер: ошибка скана: %s", e)
        await asyncio.sleep(TG_POLL_INTERVAL)


async def tg_status() -> str:
    """Текст для команды /tg — текущее состояние парсера каналов (HTML)."""
    if not TG_ENABLED:
        return "🔌 <b>TG-парсер каналов</b>\nВыключен (TG_ENABLED не задан)."
    if not TG_CHANNELS:
        return "📡 <b>TG-парсер каналов</b>\n⚠️ Включён, но TG_CHANNELS пуст."

    lines = [
        "📡 <b>TG-парсер каналов</b> (веб-превью t.me/s/)",
        f"Интервал: каждые {max(1, TG_POLL_INTERVAL // 60)} мин",
        f"\nКаналов: <b>{len(TG_CHANNELS)}</b>",
        "\n".join(f"  • {html.escape(c)}" for c in TG_CHANNELS),
    ]
    if _last_scan:
        try:
            ago = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(_last_scan["ts"])).total_seconds() / 60
            ago_txt = f"{int(ago)} мин назад"
        except Exception:
            ago_txt = "недавно"
        lines.append(
            f"\nПоследний скан: {ago_txt}\n"
            f"  постов: {_last_scan.get('candidates', 0)} · "
            f"прошло фильтр: {_last_scan.get('passed', 0)} · "
            f"отправлено: {_last_scan.get('sent', 0)}"
        )
    else:
        lines.append("\nЕщё не сканировал — подожди первый цикл.")
    return "\n".join(lines)
