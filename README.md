# Stelio Leads Bot

Telegram бот для анализа заявок из META Ads.

## Файлы

- `bot.py` — код бота
- `requirements.txt` — зависимости

## Деплой на Railway

1. Залейте репо на GitHub
2. Зайдите на railway.app → New Project → GitHub repo
3. В разделе Variables добавьте:
   BOT_TOKEN = ваш_токен_от_BotFather
4. Railway сам запустит бота

## Команды

/report              — за сегодня
/report 22.02.2026   — за конкретный день
/report 22.02        — за день текущего года
/report 01.02-22.02  — за период
/report месяц        — за текущий месяц

Автоматический отчёт каждый день в 20:00 по Киеву.
