import os
import json
import logging
import re
import asyncio
from datetime import datetime, time, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ── Настройки ─────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
REPORT_HOUR = int(os.environ.get("REPORT_HOUR_UTC", "21"))
CHAT_ID     = None
LEADS_FILE  = "/app/data/leads.json"
KYIV_TZ     = ZoneInfo("Europe/Kiev")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

leads: dict[str, list[dict]] = defaultdict(list)

def save_leads():
    data = {}
    for k, v in leads.items():
        data[k] = [{**lead, "date": lead["date"].isoformat()} for lead in v]
    with open(LEADS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def load_leads():
    if not os.path.exists(LEADS_FILE):
        return
    with open(LEADS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    for k, v in data.items():
        leads[k] = [{**lead, "date": datetime.fromisoformat(lead["date"])} for lead in v]
    logger.info(f"Загружено заявок: {sum(len(v) for v in leads.values())}")

def now_kyiv() -> datetime:
    return datetime.now(KYIV_TZ).replace(tzinfo=None)

CITY_MAP = {
    "київ": "Київ", "киев": "Київ", "kyiv": "Київ", "kiev": "Київ",
    "ірпінь": "Ірпінь", "ирпень": "Ірпінь", "irpin": "Ірпінь",
    "буча": "Буча", "bucha": "Буча",
    "бровари": "Бровари", "бровары": "Бровари", "brovary": "Бровари",
    "вишневе": "Вишневе", "вишневое": "Вишневе",
    "бориспіль": "Бориспіль", "борисполь": "Бориспіль",
    "інше місто": "Інше місто", "інше_місто": "Інше місто",
    "другой город": "Інше місто", "other": "Інше місто",
    "другой_город": "Інше місто",
}

def normalize_city(raw: str) -> str:
    key = re.sub(r"[_\-]", " ", raw).lower().strip()
    return CITY_MAP.get(key, raw.strip().title())

def esc(text: str) -> str:
    for ch in ["_", "*", "[", "]", "`"]:
        text = text.replace(ch, "\\" + ch)
    return text

def parse_lead(text: str) -> dict | None:
    def extract(pattern, default="—"):
        m = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        return m.group(1).strip() if m else default

    if "Новый лид из META Ads" in text:
        date_m = re.search(r"Дата Заявки[:\s]+(\d{2}\.\d{2}\.\d{4})", text)
        lead_date = datetime.strptime(date_m.group(1), "%d.%m.%Y") if date_m else now_kyiv()
        return {
            "name":     extract(r"Имя[:\s]+(.+)"),
            "phone":    extract(r"Номер телефона[:\s]+(.+)"),
            "area":     extract(r"Площадь помещения[:\s]+(.+)"),
            "location": extract(r"Локация[:\s]+(.+)"),
            "timing":   extract(r"Когда планируете установку[?\s]*\n?(.+)"),
            "platform": extract(r"Платформа[:\s]+(.+)"),
            "source":   "META Ads",
            "date":     lead_date,
        }

    if "Request details" in text or "Номер_телефону" in text:
        date_m = re.search(r"Дата Заявки[:\s]+(\d{2}\.\d{2}\.\d{4})", text)
        lead_date = datetime.strptime(date_m.group(1), "%d.%m.%Y") if date_m else now_kyiv()
        return {
            "name":     extract(r"Name[:\s]+(.+)"),
            "phone":    extract(r"Номер_телефону[:\s]+(.+)"),
            "area":     extract(r"Площа_приміщення[\w_]*[:\s]+(.+)"),
            "location": extract(r"Локація[:\s]+(.+)"),
            "timing":   extract(r"Коли_плануєте_встановлення[\w_]*[:\s]+(.+)"),
            "platform": "Сайт",
            "source":   "Сайт",
            "date":     lead_date,
        }

    return None

def build_report(leads_list: list[dict], label: str) -> str:
    if not leads_list:
        return f"📭 За {label} заявок не поступало."

    seen, unique, duplicates = set(), [], 0
    for l in leads_list:
        phone = l["phone"].strip()
        if phone not in seen:
            seen.add(phone)
            unique.append(l)
        else:
            duplicates += 1

    total = len(leads_list)
    leads_list = unique

    cities: dict[str, int] = defaultdict(int)
    for l in leads_list:
        cities[normalize_city(l["location"])] += 1
    cities_str = "\n".join(
        f"  • {esc(c)} — {n} ({n/total*100:.0f}%)"
        for c, n in sorted(cities.items(), key=lambda x: -x[1])
    )

    areas: dict[str, int] = defaultdict(int)
    for l in leads_list:
        areas[re.sub(r"[_]+$", "", l["area"]).strip()] += 1
    areas_str = "\n".join(
        f"  • {esc(a)} — {n} ({n/total*100:.0f}%)"
        for a, n in sorted(areas.items(), key=lambda x: -x[1])
    )

    platforms: dict[str, int] = defaultdict(int)
    for l in leads_list:
        platforms[l["platform"]] += 1
    platforms_str = " | ".join(f"{esc(p)}: {c}" for p, c in sorted(platforms.items(), key=lambda x: -x[1]))

    sources: dict[str, int] = defaultdict(int)
    for l in leads_list:
        sources[l.get("source", "META Ads")] += 1
    sources_str = " | ".join(f"{esc(s)}: {c}" for s, c in sorted(sources.items(), key=lambda x: -x[1]))

    dup_str = f"\n⚠️ Дубликатов удалено: {duplicates}" if duplicates else ""

    return (
        f"📊 *Отчёт за {label}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Всего заявок: *{total}*{dup_str}\n\n"
        f"🏙 *Города:*\n{cities_str}\n\n"
        f"📐 *Площадь:*\n{areas_str}\n\n"
        f"📱 *Платформы:* {platforms_str}\n"
        f"🌐 *Источники:* {sources_str}"
    )

def get_leads_for_range(date_from: datetime, date_to: datetime) -> list[dict]:
    result = []
    current = date_from
    while current <= date_to:
        result.extend(leads.get(current.strftime("%Y-%m-%d"), []))
        current += timedelta(days=1)
    return result

def parse_report_args(args: list[str]):
    today = now_kyiv()
    text = " ".join(args).strip().lower()

    if not text or text == "сегодня":
        return leads.get(today.strftime("%Y-%m-%d"), []), f"сегодня ({today.strftime('%d.%m.%Y')})"

    if "месяц" in text:
        return get_leads_for_range(today.replace(day=1), today), today.strftime("%B %Y")

    period = re.search(r"(\d{2}\.\d{2}(?:\.\d{4})?)\s*[-]\s*(\d{2}\.\d{2}(?:\.\d{4})?)", text)
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

    return leads.get(today.strftime("%Y-%m-%d"), []), f"сегодня ({today.strftime('%d.%m.%Y')})"

# ── Handlers ───────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CHAT_ID
    if update.message is None:
        return
    if CHAT_ID is None:
        CHAT_ID = update.message.chat_id
        logger.info(f"Chat ID: {CHAT_ID}")
    sender = update.message.from_user
    logger.info(f"FROM: {sender.id if sender else 'None'} / {sender.username if sender else 'None'} / is_bot={sender.is_bot if sender else 'None'}")
    text = update.message.text or update.message.caption or ""
    logger.info(f"MSG [{update.message.chat_id}]: {text[:100]}")
    lead = parse_lead(text)
    if lead:
        key = lead["date"].strftime("%Y-%m-%d")
        leads[key].append(lead)
        save_leads()
        logger.info(f"Заявка сохранена: {lead['name']} / {lead['location']} / {key}")

async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    leads_list, label = parse_report_args(context.args or [])
    report = build_report(leads_list, label)
    await update.message.reply_text(report, parse_mode="Markdown")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kyiv_hour = (REPORT_HOUR + 2) % 24
    msg = "📋 *Доступные команды:*\n\n"
    msg += "/report — отчёт за сегодня\n"
    msg += "/report 22.02.2026 — за конкретный день\n"
    msg += "/report 22.02 — за день текущего года\n"
    msg += "/report 01.02-22.02 — за период\n"
    msg += "/report месяц — за текущий месяц\n\n"
    msg += f"/settime 21 — изменить время авто-отчёта\n\n"
    msg += f"🕗 Авто-отчёт каждый день в *{kyiv_hour}:00* по Киеву"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_settime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPORT_HOUR
    if not context.args:
        await update.message.reply_text("Использование: /settime 21")
        return
    try:
        new_hour = int(context.args[0])
        if not 0 <= new_hour <= 23:
            raise ValueError
        REPORT_HOUR = new_hour
        for job in context.job_queue.get_jobs_by_name("daily_report"):
            job.schedule_removal()
        context.job_queue.run_daily(
            send_daily_report,
            time=time(REPORT_HOUR, 0),
            name="daily_report",
        )
        kyiv_hour = (REPORT_HOUR + 2) % 24
        await update.message.reply_text(f"✅ Время отчёта изменено на *{kyiv_hour}:00* по Киеву", parse_mode="Markdown")
    except (ValueError, IndexError):
        await update.message.reply_text("Ошибка. Пример: /settime 21")

async def send_daily_report(context: ContextTypes.DEFAULT_TYPE):
    if CHAT_ID is None:
        return
    today = now_kyiv()
    key = today.strftime("%Y-%m-%d")
    label = f"сегодня ({today.strftime('%d.%m.%Y')})"
    report = build_report(leads.get(key, []), label)
    await context.bot.send_message(chat_id=CHAT_ID, text=report, parse_mode="Markdown")
    leads.pop(key, None)
    save_leads()
    logger.info("Авто-отчёт отправлен")

async def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не задан!")

    load_leads()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("settime", cmd_settime))
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.CAPTION) & ~filters.COMMAND,
        handle_message,
        block=False
    ))
    app.job_queue.run_daily(send_daily_report, time=time(REPORT_HOUR, 0), name="daily_report")

    logger.info("Бот запущен")
    await app.run_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    import time as time_module
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logger.error(f"Ошибка: {e}, перезапуск через 5 сек...")
            time_module.sleep(5)
