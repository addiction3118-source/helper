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
TG_FETCH_LIMIT = _int("TG_FETCH_LIMIT", 30)        # сколько последних постов брать на канал (на одну страницу)
TG_MIN_LEN = _int("TG_MIN_LEN", 40)                # короче — это не заказ (отсев флуда)
# Глубина РАЗОВОГО первого скана канала (бэкафилл архива), страниц по ~20 постов.
# Читается один раз при первом появлении канала в этом запуске бота, дальше — нет
# смысла (дедуп). 0 = листать до самого начала канала (полный архив, тоже разово).
TG_BACKFILL_PAGES = _int("TG_BACKFILL_PAGES", 50)
# Глубина КАЖДОГО последующего скана (ловим новые посты сверху). 1 = только свежие.
TG_HISTORY_PAGES = _int("TG_HISTORY_PAGES", 1)

# каналы, чей архив уже разово прочитан в текущем запуске (бэкафилл сделан)
_backfilled: set = set()

TG_DISCOVER_MIN_SCORE = _int("TG_DISCOVER_MIN_SCORE", 3)  # порог релевантности кандидата

_last_scan: dict = {}   # инфо о последнем скане каналов (для команды /tg)

_MSG_SPLIT = '<div class="tgme_widget_message_wrap'

# Стартовые «затравочные» каналы для поиска, если своих ещё нет.
# Только публичные broadcast-каналы с веб-превью (проверены живыми).
# Невалидные/мёртвые в любом случае отсеются при проверке через t.me/s/.
CURATED_SEEDS = [
    # проверенные каналы с ЗАКАЗАМИ (прямой контакт, бесплатный отклик).
    # Невалидные/мёртвые отсеются при проверке через t.me/s/.
    # freelance_zakazy убран: чистые репосты Kwork (отклик там платный).
    "forkwork", "remoteit", "it_freelancing",
    "it_zakazy", "zakazi_freelance",
    "rabota_freelancer", "nocodejobs",
    # no-code биржи заказов (проверены: t.me/s/, поток заказов, не вакансии)
    "zerocode_jobs", "nocode_jobs", "itjobs_nocode",
]

# имя канала из @mention или t.me/-ссылки внутри поста
_UNAME_RE = re.compile(r"(?:@|t\.me/)([A-Za-z][A-Za-z0-9_]{3,31})")
# служебные пути t.me, которые не являются каналами
_UNAME_STOP = {
    "s", "joinchat", "share", "addstickers", "addemoji", "proxy", "socks",
    "iv", "bg", "login", "setlanguage", "telegram", "durov", "c",
}


def tg_available() -> bool:
    """Включён ли парсер каналов (каналы могут добавляться на лету через /discover)."""
    return TG_ENABLED


def effective_channels() -> list[str]:
    """Итоговый список каналов: из .env (TG_CHANNELS) + добавленные в БД, без дублей."""
    out, seen = [], set()
    extra = []
    try:
        from main import tg_get_channels
        extra = tg_get_channels()
    except Exception:
        pass
    for ch in list(TG_CHANNELS) + list(extra):
        u = _channel_username(ch).lower()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


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
    # max_age_hours <= 0 — фильтр возраста выключен (берём посты любой давности)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
              if max_age_hours > 0 else None)
    # посты в HTML идут от старых к новым (новые внизу)
    for chunk in page.split(_MSG_SPLIT)[1:]:
        # строго username/id: значение идёт в ссылку t.me/<...> и в HTML карточки,
        # произвольные символы из атрибута туда попадать не должны
        post = re.search(r'data-post="([A-Za-z0-9_]+/\d+)"', chunk)
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
                if cutoff is not None and dt < cutoff:
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


async def _fetch_page(session, uname: str, proxy, timeout: int = 30, before=None):
    """Скачивает t.me/s/<uname> (опц. ?before=<id> — страница истории до этого поста).
    Возвращает HTML, либо None если это не публичный канал."""
    url = f"https://t.me/s/{uname}"
    if before:
        url += f"?before={before}"
    try:
        async with session.get(url, proxy=proxy,
                               headers={"User-Agent": "Mozilla/5.0"},
                               timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            # страница t.me/s/ — сотни КБ; читаем не больше лимита, чтобы
            # аномально огромный ответ не уронил бота по памяти (Render 512 МБ)
            from main import read_capped
            page = await read_capped(resp)
    except Exception as e:
        log.warning("TG-парсер: не удалось загрузить %s: %s", uname, e)
        return None
    return page if _MSG_SPLIT in page else None   # нет постов → канал приватный/пустой/нет такого


def _min_post_id(page: str) -> int | None:
    """Наименьший id поста на странице — точка отсчёта для ?before= (страница глубже)."""
    ids = [int(m) for m in re.findall(r'data-post="[^"/]+/(\d+)"', page)]
    return min(ids) if ids else None


def _page_text(page: str) -> str:
    """Весь текст постов страницы одной строкой — для оценки релевантности канала."""
    parts = re.findall(
        r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', page, re.S)
    return _clean(" ".join(parts))


async def _fetch_channel(session, channel: str, max_age_hours: int, proxy) -> list:
    """Качает веб-превью канала и парсит посты в Job-кандидаты."""
    uname = _channel_username(channel)
    if not uname or "joinchat" in channel or uname.startswith("+"):
        log.warning("TG-парсер: %s — приватный канал, веб-превью недоступно, пропускаю", channel)
        return []
    # Первый скан канала — глубокий бэкафилл архива (TG_BACKFILL_PAGES, 0=до начала).
    # Последующие — мелкие (TG_HISTORY_PAGES): новые посты приходят сверху, перечитывать
    # весь архив каждый цикл бессмысленно (дедуп) и грузит t.me/Render впустую.
    if uname not in _backfilled:
        pages = TG_BACKFILL_PAGES if TG_BACKFILL_PAGES > 0 else 10_000
    else:
        pages = max(1, TG_HISTORY_PAGES)
    jobs, before = [], None
    for _ in range(pages):
        page = await _fetch_page(session, uname, proxy, before=before)
        if not page:
            break
        jobs.extend(_parse_page(page, uname, max_age_hours))
        mid = _min_post_id(page)
        if mid is None or mid <= 1:
            break          # дошли до начала канала
        before = mid       # следующая итерация — посты старше этого id
    _backfilled.add(uname)   # архив прочитан — дальше только свежие посты
    return jobs


async def _scan_once(process_candidates, dispatch_jobs, log_scan, scan_lock, max_age_hours):
    from main import AI_PROXY   # t.me блокируется по гео так же, как Telegram — тот же прокси

    candidates = []
    channels = effective_channels()   # .env + добавленные через /discover
    async with aiohttp.ClientSession() as session:
        for ch in channels:         # последовательно — без бурста по t.me
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


async def discover(session, proxy, max_suggest: int = 8) -> list:
    """Ищет новые каналы: собирает упоминания из постов текущих каналов (и curated),
    проверяет каждого через t.me/s/ и оценивает по фриланс-ключевикам (RU+EN).
    Возвращает [(uname, score), ...] — кандидатов на одобрение."""
    from main import WHITELIST

    have = set(effective_channels())
    base = effective_channels() or list(CURATED_SEEDS)

    # 1. собираем кандидатов из постов базовых каналов
    found, seen = [], set(have)
    for ch in base[:30]:
        page = await _fetch_page(session, ch, proxy, timeout=20)
        if not page:
            continue
        for m in _UNAME_RE.finditer(page):
            u = m.group(1).lower()
            if u in seen or u in _UNAME_STOP:
                continue
            seen.add(u)
            found.append(u)
    for u in CURATED_SEEDS:           # curated тоже в кандидаты, если ещё не наш
        if u.lower() not in seen:
            seen.add(u.lower())
            found.append(u.lower())

    # 2. валидируем и оцениваем (это HTTP-запросы — ограничиваем число)
    scored = []
    for u in found[:60]:
        page = await _fetch_page(session, u, proxy, timeout=20)
        if not page:
            continue
        low = _page_text(page).lower()
        if not low:
            continue
        score = sum(low.count(k) for k in WHITELIST)
        if score >= TG_DISCOVER_MIN_SCORE:
            scored.append((u, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:max_suggest]


async def tg_status() -> str:
    """Текст для команды /tg — текущее состояние парсера каналов (HTML)."""
    if not TG_ENABLED:
        return "🔌 <b>TG-парсер каналов</b>\nВыключен (TG_ENABLED не задан)."

    chans = effective_channels()
    lines = [
        "📡 <b>TG-парсер каналов</b> (веб-превью t.me/s/)",
        f"Интервал: каждые {max(1, TG_POLL_INTERVAL // 60)} мин",
        f"\nКаналов: <b>{len(chans)}</b>",
    ]
    if chans:
        lines.append("\n".join(f"  • @{html.escape(c)}" for c in chans))
    else:
        lines.append("  пусто — найди каналы командой /discover")
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
