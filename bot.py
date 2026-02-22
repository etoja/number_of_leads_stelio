"""
Telegram бот для анализа заявок от ApiX-Drive
- Читает заявки из группы
- Отчёт в 20:00 автоматически (18:00 UTC)
- Команда /report — за сегодня
- Команда /report 22.02.2026 — за день
- Команда /report 01.02-22.02 — за период
- Команда /report месяц — за текущий месяц
"""

import os
import logging
import re
from datetime import datetime, time, timedelta
from collections import defaultdict

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ── Настройки ────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
CHAT_ID     = None

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# leads = { "2026-02-22": [ {...}, ... ] }
leads: dict[str, list[dict]] = defaultdict(list)


def parse_lead(text: str) -> dict | None:
    if "Новый лид из META Ads" not in text:
        return None

    def extract(pattern, default="—"):
        m = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        return m.group(1).strip() if m else default

    return {
        "name":     extract(r"Имя[:\s]+(.+)"),
        "phone":    extract(r"Номер телефона[:\s]+(.+)"),
        "area":     extract(r"Площадь помещения[:\s]+(.+)"),
        "location": extract(r"Локация[:\s]+(.+)"),
        "mount":    extract(r"Как будут крепиться шторы[?\s]*\n?(.+)"),
        "timing":   extract(r"Когда планируете установку[?\s]*\n?(.+)"),
        "platform": extract(r"Платформа[:\s]+(.+)"),
        "date":     datetime.now(),
    }


def build_report(leads_list: list[dict], label: str) -> str:
    if not leads_list:
        return f"📭 За {label} заявок не поступало."

    # Дедупликация по номеру телефона
    seen, unique, duplicates = set(), [], 0
    for l in leads_list:
        phone = l["phone"].strip()
        if phone not in seen:
            seen.add(phone)
            unique.append(l)
        else:
            duplicates += 1
    leads_list = unique

    total = len(leads_list)

    cities: dict[str, int] = defaultdict(int)
    for l in leads_list:
        city = re.sub(r"[_\-]", " ", l["location"].lower().strip())
        cities[city] += 1

    cities_str = "\n".join(
        f"  • {c.title()} — {n} ({n/total*100:.0f}%)"
        for c, n in sorted(cities.items(), key=lambda x: -x[1])
    )

    areas: dict[str, int] = defaultdict(int)
    for l in leads_list:
        areas[l["area"]] += 1

    areas_str = "\n".join(
        f"  • {a} — {n} ({n/total*100:.0f}%)"
        for a, n in sorted(areas.items(), key=lambda x: -x[1])
    )

    platforms: dict[str, int] = defaultdict(int)
    for l in leads_list:
        platforms[l["platform"].lower()] += 1

    platforms_str = " | ".join(
        f"{p.upper()}: {c}"
        for p, c in sorted(platforms.items(), key=lambda x: -x[1])
    )

    return (
        f"📊 *Отчёт за {label}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Всего заявок: *{total}*\n\n"
        f"🏙 *Города:*\n{cities_str}\n\n"
        f"📐 *Площадь:*\n{areas_str}\n\n"
        f"📱 *Платформы:* {platforms_str}"
    )


def get_leads_for_range(date_from: datetime, date_to: datetime) -> list[dict]:
    result = []
    current = date_from
    while current <= date_to:
        key = current.strftime("%Y-%m-%d")
        result.extend(leads.get(key, []))
        current += timedelta(days=1)
    return result


def parse_report_args(args: list[str]):
    today = datetime.now()
    text = " ".join(args).strip().lower()

    if not text or text == "сегодня":
        key = today.strftime("%Y-%m-%d")
        return leads.get(key, []), f"сегодня ({today.strftime('%d.%m.%Y')})"

    if "месяц" in text:
        return get_leads_for_range(today.replace(day=1), today), today.strftime("%B %Y")

    period = re.search(r"(\d{2}\.\d{2}(?:\.\d{4})?)\s*[-–]\s*(\d{2}\.\d{2}(?:\.\d{4})?)", text)
    if period:
        def pd(s):
            return datetime.strptime(s, "%d.%m.%Y") if len(s) > 5 else datetime.strptime(f"{s}.{today.year}", "%d.%m.%Y")
        d1, d2 = pd(period.group(1)), pd(period.group(2))
        return get_leads_for_range(d1, d2), f"{d1.strftime('%d.%m')}–{d2.strftime('%d.%m.%Y')}"

    date_m = re.search(r"(\d{2}\.\d{2}(?:\.\d{4})?)", text)
    if date_m:
        ds = date_m.group(1)
        d = datetime.strptime(ds, "%d.%m.%Y") if len(ds) > 5 else datetime.strptime(f"{ds}.{today.year}", "%d.%m.%Y")
        return leads.get(d.strftime("%Y-%m-%d"), []), d.strftime("%d.%m.%Y")

    key = today.strftime("%Y-%m-%d")
    return leads.get(key, []), f"сегодня ({today.strftime('%d.%m.%Y')})"


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CHAT_ID
    if update.message is None:
        return
    if CHAT_ID is None:
        CHAT_ID = update.message.chat_id
        logger.info(f"Chat ID: {CHAT_ID}")
    text = update.message.text or ""
    logger.info(f"MSG [{update.message.chat_id}]: {text[:100]}")
    lead = parse_lead(text)
    if lead:
        key = datetime.now().strftime("%Y-%m-%d")
        leads[key].append(lead)
        logger.info(f"Заявка: {lead['name']} / {lead['location']}")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    leads_list, label = parse_report_args(context.args or [])
    report = build_report(leads_list, label)
    await update.message.reply_text(report, parse_mode="Markdown")



async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📋 *Доступные команды:*\n\n"
        "/report — отчёт за сегодня\n"
        "/report 22.02.2026 — за конкретный день\n"
        "/report 22.02 — за день текущего года\n"
        "/report 01.02-22.02 — за период\n"
        "/report месяц — за текущий месяц\n\n"
        "Автоматический отчёт каждый день в *20:00* по Киеву."
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def send_daily_report(context: ContextTypes.DEFAULT_TYPE):
    if CHAT_ID is None:
        return
    today = datetime.now()
    key   = today.strftime("%Y-%m-%d")
    label = f"сегодня ({today.strftime('%d.%m.%Y')})"
    report = build_report(leads.get(key, []), label)
    await context.bot.send_message(chat_id=CHAT_ID, text=report, parse_mode="Markdown")
    leads.pop(key, None)
    logger.info("Ежедневный отчёт отправлен")


def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не задан!")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # 18:00 UTC = 20:00 Киев
    app.job_queue.run_daily(send_daily_report, time=time(18, 0), name="daily_report")

    logger.info("Бот запущен")
    app.run_polling()


if __name__ == "__main__":
    main()
