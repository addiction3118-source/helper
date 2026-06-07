"""Одноразовый ЛОКАЛЬНЫЙ генератор строки сессии Telethon для userbot-парсера.

Запусти у себя на ПК (НЕ на Render):

    pip install telethon
    python gen_session.py

Введёшь номер телефона (тот самый ОТДЕЛЬНЫЙ аккаунт под бота), код из Telegram
и, если включён, пароль 2FA. Скрипт напечатает длинную строку — это TG_SESSION.
Скопируй её в .env локально и в Environment на Render.

TG_API_ID и TG_API_HASH возьми на https://my.telegram.org → API development tools
(под тем же отдельным номером). Можно заранее положить их в .env — тогда скрипт
не будет спрашивать.

⚠️  Строка сессии = полный доступ к аккаунту. Не коммить её и никому не показывай.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    from telethon.sync import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    raise SystemExit("Сначала установи Telethon:  pip install telethon")


def main():
    api_id = os.getenv("TG_API_ID") or input("TG_API_ID: ").strip()
    api_hash = os.getenv("TG_API_HASH") or input("TG_API_HASH: ").strip()
    try:
        api_id = int(api_id)
    except (TypeError, ValueError):
        raise SystemExit("TG_API_ID должен быть числом")
    if not api_hash:
        raise SystemExit("TG_API_HASH пуст")

    print("\nПодключаюсь к Telegram. Введи номер телефона, код и (если есть) пароль 2FA…\n")
    with TelegramClient(StringSession(), api_id, api_hash) as client:
        session_str = client.session.save()
        me = client.get_me()
        line = "=" * 64
        print("\n" + line)
        print(f"Готово! Вошёл как: @{me.username or me.id}")
        print("Скопируй строку ниже в переменную TG_SESSION (.env и Render):")
        print(line)
        print(session_str)
        print(line)
        print("⚠️  Это секрет — не коммить и никому не показывай.")


if __name__ == "__main__":
    main()
