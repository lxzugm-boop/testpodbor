import os
import json
import logging
from collections import defaultdict
from typing import Dict, Any, List
from http import HTTPStatus
from contextlib import asynccontextmanager

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

import gspread
from google.oauth2.service_account import Credentials

from fastapi import FastAPI, Request, Response

# ========= НАСТРОЙКИ ЧЕРЕЗ ENV =========

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN env var is not set")
if not GOOGLE_SHEET_ID:
    raise RuntimeError("GOOGLE_SHEET_ID env var is not set")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var is not set")
if not WEBHOOK_URL:
    raise RuntimeError("WEBHOOK_URL env var is not set")

# ========= ЛОГИ =========

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ========= МУЛЬТИЯЗЫЧНЫЕ ТЕКСТЫ =========

LANG_RU = "ru"
LANG_EN = "en"
LANG_KK = "kk"  # Казахский
LANG_UZ = "uz"  # Узбекский

LANG_LABELS = {
    LANG_RU: "🇷🇺 Русский",
    LANG_EN: "🇬🇧 English",
    LANG_KK: "🇰🇿 Қазақша",
    LANG_UZ: "🇺🇿 O‘zbekcha",
}

TEXTS = {
    LANG_RU: {
        "welcome": "Привет! Я бот подбора картриджей Гейзер.\nВыберите язык:",
        "main_menu": "Выберите режим:",
        "btn_regular": "🔍 Подбор по вопросам",
        "btn_pro": "⚙️ Профессиональный подбор по артикулу системы",
        "btn_lang": "🌐 Сменить язык",
        "ask_pro_art": "Введите артикул системы Гейзер (например, 20001):",
        "ask_regular_what_known": "Что вы знаете о своей системе?",
        "btn_reg_sys_name": "🧰 Знаю название системы",
        "btn_reg_cart_art": "🔢 Знаю артикул картриджа",
        "btn_reg_cart_name": "🏷 Знаю название картриджа",
        "ask_sys_name": "Введите название системы (например, Гейзер-Престиж):",
        "ask_cart_art": "Введите артикул картриджа (например, 27008):",
        "ask_cart_name": "Введите название картриджа (например, CBC 10):",
        "no_systems_found": "Систем по вашему запросу не найдено.",
        "no_cartridges_found": "Картриджей по вашему запросу не найдено.",
        "choose_system": "Нашёл несколько систем, выберите вашу:",
        "choose_system_for_cart": "Этот картридж используется в нескольких системах. Выберите вашу:",
        "system_header": "Система: {name} (арт. {article})",
        "kits_header": "🔹 Рекомендуемые комплекты сменных картриджей:",
        "no_kits": "Для этой системы не найдено комплектов. Показаны отдельные картриджи:",
        "items_header": "🔹 Отдельные картриджи:",
        "item_line": "• {name} (арт. {article})\n{url}",
        "error": "Произошла ошибка. Попробуйте ещё раз.",
        "back_to_menu": "⬅️ В главное меню",
        "data_reloaded": "Данные из таблицы обновлены.",
    },
    LANG_EN: {
        "welcome": "Hi! I am a Geyser cartridge selection bot.\nPlease choose a language:",
        "main_menu": "Choose mode:",
        "btn_regular": "🔍 Guided selection",
        "btn_pro": "⚙️ Pro mode (by system article)",
        "btn_lang": "🌐 Change language",
        "ask_pro_art": "Enter the Geyser system article (e.g. 20001):",
        "ask_regular_what_known": "What do you know about your system?",
        "btn_reg_sys_name": "🧰 I know the system name",
        "btn_reg_cart_art": "🔢 I know the cartridge article",
        "btn_reg_cart_name": "🏷 I know the cartridge name",
        "ask_sys_name": "Enter the system name (e.g. Geyser-Prestige):",
        "ask_cart_art": "Enter the cartridge article (e.g. 27008):",
        "ask_cart_name": "Enter the cartridge name (e.g. CBC 10):",
        "no_systems_found": "No systems found for your query.",
        "no_cartridges_found": "No cartridges found for your query.",
        "choose_system": "I found several systems, choose yours:",
        "choose_system_for_cart": "This cartridge is used in several systems. Choose yours:",
        "system_header": "System: {name} (art. {article})",
        "kits_header": "🔹 Recommended cartridge sets:",
        "no_kits": "No sets found for this system. Showing separate cartridges:",
        "items_header": "🔹 Separate cartridges:",
        "item_line": "• {name} (art. {article})\n{url}",
        "error": "An error occurred. Please try again.",
        "back_to_menu": "⬅️ Back to main menu",
        "data_reloaded": "Data reloaded from sheet.",
    },
    LANG_KK: {
        "welcome": "Сәлем! Мен Geyser сүзгілеріне картридж таңдау ботымын.\nТілді таңдаңыз:",
        "main_menu": "Режимді таңдаңыз:",
        "btn_regular": "🔍 Сұрақтар арқылы таңдау",
        "btn_pro": "⚙️ Кәсіби режим (жүйе артикулы бойынша)",
        "btn_lang": "🌐 Тілді ауыстыру",
        "ask_pro_art": "Geyser жүйесінің артикулын енгізіңіз (мысалы, 20001):",
        "ask_regular_what_known": "Жүйеңіз туралы не білесіз?",
        "btn_reg_sys_name": "🧰 Жүйенің атауын білемін",
        "btn_reg_cart_art": "🔢 Картридж артикулын білемін",
        "btn_reg_cart_name": "🏷 Картридж атауын білемін",
        "ask_sys_name": "Жүйенің атауын енгізіңіз:",
        "ask_cart_art": "Картридж артикулын енгізіңіз:",
        "ask_cart_name": "Картридж атауын енгізіңіз:",
        "no_systems_found": "Сіздің сұрауыңыз бойынша жүйе табылмады.",
        "no_cartridges_found": "Сіздің сұрауыңыз бойынша картридж табылмады.",
        "choose_system": "Бірнеше жүйе табылды, өзіңізді таңдаңыз:",
        "choose_system_for_cart": "Бұл картридж бірнеше жүйеде қолданылады. Өз жүйеңізді таңдаңыз:",
        "system_header": "Жүйе: {name} (арт. {article})",
        "kits_header": "🔹 Ұсынылатын картридж жиынтықтары:",
        "no_kits": "Бұл жүйе үшін жиынтықтар табылмады. Жеке картридждер көрсетілді:",
        "items_header": "🔹 Жеке картридждер:",
        "item_line": "• {name} (арт. {article})\n{url}",
        "error": "Қате орын алды. Қайта көріңіз.",
        "back_to_menu": "⬅️ Басты мәзірге оралу",
        "data_reloaded": "Деректер кестеден жаңартылды.",
    },
    LANG_UZ: {
        "welcome": "Salom! Men Geyser filtrlari uchun kartridj tanlash botiman.\nTilni tanlang:",
        "main_menu": "Rejimni tanlang:",
        "btn_regular": "🔍 Savollar orqali tanlash",
        "btn_pro": "⚙️ Professional rejim (tizim artikuli bo‘yicha)",
        "btn_lang": "🌐 Tilni almashtirish",
        "ask_pro_art": "Geyser tizimi artikulini kiriting (masalan, 20001):",
        "ask_regular_what_known": "Tizimingiz haqida nimalarni bilasiz?",
        "btn_reg_sys_name": "🧰 Tizim nomini bilaman",
        "btn_reg_cart_art": "🔢 Kartridj artikulini bilaman",
        "btn_reg_cart_name": "🏷 Kartridj nomini bilaman",
        "ask_sys_name": "Tizim nomini kiriting:",
        "ask_cart_art": "Kartridj artikulini kiriting:",
        "ask_cart_name": "Kartridj nomini kiriting:",
        "no_systems_found": "So‘rovingiz bo‘yicha tizim topilmadi.",
        "no_cartridges_found": "So‘rovingiz bo‘yicha kartridj topilmadi.",
        "choose_system": "Bir nechta tizim топилди, o‘zingiznikini tanlang:",
        "choose_system_for_cart": "Bu kartridj bir nechta tizimda ishlatiladi. O‘zingiznikini tanlang:",
        "system_header": "Tizim: {name} (art. {article})",
        "kits_header": "🔹 Tavsiya etilgan kartridj to‘plamlari:",
        "no_kits": "Bu tizim uchun to‘plamlar topilmadi. Alohida kartridjlar ko‘rsatildi:",
        "items_header": "🔹 Alohida kartridjlar:",
        "item_line": "• {name} (art. {article})\n{url}",
        "error": "Xato yuz berdi. Yana urinib ko‘ring.",
        "back_to_menu": "⬅️ Asosiy menyuga qaytish",
        "data_reloaded": "Ma’lumotlar jadvalдан yangilandi.",
    },
}

DEFAULT_LANG = LANG_RU

# ========= СОСТОЯНИЯ ПОЛЬЗОВАТЕЛЕЙ =========

user_states: Dict[int, Dict[str, Any]] = {}

# ========= ДАННЫЕ ИЗ GOOGLE SHEETS =========

all_rows: List[Dict[str, Any]] = []
systems_by_article: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
systems_by_name: Dict[str, List[str]] = defaultdict(list)
cartridges_by_article: Dict[str, List[Dict[str, Any]]] = defaultdict(list)


def normalize_name(name: str) -> str:
    return (name or "").strip().upper()


def load_data_from_sheets() -> None:
    """Загружаем данные из Google Sheets в память."""
    global all_rows, systems_by_article, systems_by_name, cartridges_by_article

    logger.info("Loading data from Google Sheets...")

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1
    records = sheet.get_all_records()

    all_rows = records
    systems_by_article = defaultdict(list)
    systems_by_name = defaultdict(list)
    cartridges_by_article = defaultdict(list)

    for row in all_rows:
        sys_art_raw = row.get("Артикул")
        sys_name = row.get("Название системы")
        cart_art_raw = row.get("Артикул расходника")

        if sys_art_raw is None or sys_name is None:
            continue

        sys_art = str(sys_art_raw).split(".")[0]
        row["Артикул"] = sys_art

        sys_name_norm = normalize_name(sys_name)
        systems_by_article[sys_art].append(row)
        if sys_name_norm:
            if sys_art not in systems_by_name[sys_name_norm]:
                systems_by_name[sys_name_norm].append(sys_art)

        if cart_art_raw not in (None, ""):
            cart_art = str(cart_art_raw).split(".")[0]
            row["Артикул расходника"] = cart_art
            cartridges_by_article[cart_art].append(row)

    logger.info(
        "Loaded %d rows, %d systems, %d cartridge articles",
        len(all_rows),
        len(systems_by_article),
        len(cartridges_by_article),
    )


def split_kits_and_items(rows: List[Dict[str, Any]]) -> (List[Dict[str, Any]], List[Dict[str, Any]]):
    kits = []
    singles = []
    for r in rows:
        cart_name = str(r.get("Название расходника", "")).lower()
        if any(word in cart_name for word in ["комплект", "набор", "kit", "set"]):
            kits.append(r)
        else:
            singles.append(r)
    return kits, singles


def get_system_name_from_rows(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    return str(rows[0].get("Название системы", "")).strip()


# ========= УТИЛИТЫ =========

def get_user_lang(user_id: int) -> str:
    state = user_states.get(user_id, {})
    return state.get("lang", DEFAULT_LANG)


def set_user_lang(user_id: int, lang: str) -> None:
    state = user_states.setdefault(user_id, {})
    state["lang"] = lang


def set_user_step(user_id: int, step: str) -> None:
    state = user_states.setdefault(user_id, {})
    state["step"] = step


def get_user_step(user_id: int) -> str:
    state = user_states.get(user_id, {})
    return state.get("step", "")


def lang_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(LANG_LABELS[LANG_RU], callback_data="lang_ru"),
            InlineKeyboardButton(LANG_LABELS[LANG_EN], callback_data="lang_en"),
        ],
        [
            InlineKeyboardButton(LANG_LABELS[LANG_KK], callback_data="lang_kk"),
            InlineKeyboardButton(LANG_LABELS[LANG_UZ], callback_data="lang_uz"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def main_menu_keyboard(lang: str) -> InlineKeyboardMarkup:
    t = TEXTS[lang]
    buttons = [
        [InlineKeyboardButton(t["btn_regular"], callback_data="menu_regular")],
        [InlineKeyboardButton(t["btn_pro"], callback_data="menu_pro")],
        [InlineKeyboardButton(t["btn_lang"], callback_data="menu_lang")],
    ]
    return InlineKeyboardMarkup(buttons)


def regular_menu_keyboard(lang: str) -> InlineKeyboardMarkup:
    t = TEXTS[lang]
    buttons = [
        [InlineKeyboardButton(t["btn_reg_sys_name"], callback_data="reg_sys_name")],
        [InlineKeyboardButton(t["btn_reg_cart_art"], callback_data="reg_cart_art")],
        [InlineKeyboardButton(t["btn_reg_cart_name"], callback_data="reg_cart_name")],
        [InlineKeyboardButton(t["back_to_menu"], callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(buttons)


# ========= ХЕНДЛЕРЫ PTB =========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_id = user.id
    set_user_step(user_id, "choose_language")
    set_user_lang(user_id, DEFAULT_LANG)
    t = TEXTS[DEFAULT_LANG]
    await update.message.reply_text(t["welcome"], reply_markup=lang_keyboard())


async def reload_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    try:
        load_data_from_sheets()
        lang = get_user_lang(user.id)
        t = TEXTS[lang]
        await update.message.reply_text(t["data_reloaded"])
    except Exception as e:
        logger.exception("Reload error: %s", e)
        await update.message.reply_text("Error reloading data.")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    user_id = user.id
    lang = get_user_lang(user_id)
    t = TEXTS[lang]
    data = query.data

    try:
        if data.startswith("lang_"):
            lang_code = data.split("_", 1)[1]
            if lang_code in [LANG_RU, LANG_EN, LANG_KK, LANG_UZ]:
                set_user_lang(user_id, lang_code)
                lang = lang_code
                t = TEXTS[lang]
            set_user_step(user_id, "main_menu")
            await query.message.edit_text(
                t["main_menu"], reply_markup=main_menu_keyboard(lang)
            )
            return

        if data == "menu_regular":
            set_user_step(user_id, "regular_menu")
            await query.message.edit_text(
                t["ask_regular_what_known"], reply_markup=regular_menu_keyboard(lang)
            )
            return

        if data == "menu_pro":
            set_user_step(user_id, "await_pro_article")
            await query.message.edit_text(t["ask_pro_art"])
            return

        if data == "menu_lang":
            set_user_step(user_id, "choose_language")
            await query.message.edit_text(t["welcome"], reply_markup=lang_keyboard())
            return

        if data == "back_main":
            set_user_step(user_id, "main_menu")
            await query.message.edit_text(
                t["main_menu"], reply_markup=main_menu_keyboard(lang)
            )
            return

        if data == "reg_sys_name":
            set_user_step(user_id, "await_sys_name")
            await query.message.edit_text(t["ask_sys_name"])
            return

        if data == "reg_cart_art":
            set_user_step(user_id, "await_cart_article")
            await query.message.edit_text(t["ask_cart_art"])
            return

        if data == "reg_cart_name":
            set_user_step(user_id, "await_cart_name")
            await query.message.edit_text(t["ask_cart_name"])
            return

        if data.startswith("sys_"):
            _, sys_article = data.split("_", 1)
            rows = systems_by_article.get(sys_article, [])
            await send_system_info(
                query.message.chat_id, lang, sys_article, rows, context
            )
            set_user_step(user_id, "main_menu")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=t["main_menu"],
                reply_markup=main_menu_keyboard(lang),
            )
            return

        if data.startswith("sysfromcart_"):
            _, sys_article, cart_art = data.split("_", 2)
            rows = systems_by_article.get(sys_article, [])
            await send_system_info(
                query.message.chat_id, lang, sys_article, rows, context
            )
            set_user_step(user_id, "main_menu")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=t["main_menu"],
                reply_markup=main_menu_keyboard(lang),
            )
            return

    except Exception as e:
        logger.exception("Callback error: %s", e)
        await query.message.reply_text(t["error"])


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_id = user.id
    lang = get_user_lang(user_id)
    t = TEXTS[lang]
    text = (update.message.text or "").strip()

    step = get_user_step(user_id)

    try:
        if not step:
            set_user_step(user_id, "choose_language")
            await update.message.reply_text(t["welcome"], reply_markup=lang_keyboard())
            return

        if step == "await_pro_article":
            sys_article = text.split()[0]
            rows = systems_by_article.get(sys_article, [])
            if not rows:
                await update.message.reply_text(t["no_systems_found"])
            else:
                await send_system_info(
                    update.message.chat_id, lang, sys_article, rows, context
                )
            set_user_step(user_id, "main_menu")
            await update.message.reply_text(
                t["main_menu"], reply_markup=main_menu_keyboard(lang)
            )
            return

        if step == "await_sys_name":
            sys_name_norm = normalize_name(text)
            candidate_articles = systems_by_name.get(sys_name_norm, [])

            if not candidate_articles:
                for name_norm, arts in systems_by_name.items():
                    if sys_name_norm in name_norm:
                        for a in arts:
                            if a not in candidate_articles:
                                candidate_articles.append(a)

            if not candidate_articles:
                await update.message.reply_text(t["no_systems_found"])
                set_user_step(user_id, "main_menu")
                await update.message.reply_text(
                    t["main_menu"], reply_markup=main_menu_keyboard(lang)
                )
                return

            if len(candidate_articles) == 1:
                sys_article = candidate_articles[0]
                rows = systems_by_article.get(sys_article, [])
                await send_system_info(
                    update.message.chat_id, lang, sys_article, rows, context
                )
                set_user_step(user_id, "main_menu")
                await update.message.reply_text(
                    t["main_menu"], reply_markup=main_menu_keyboard(lang)
                )
                return

            buttons = []
            for a in candidate_articles:
                rows = systems_by_article.get(a, [])
                sys_name = get_system_name_from_rows(rows)
                label = f"{sys_name} (арт. {a})"
                buttons.append(
                    [InlineKeyboardButton(label, callback_data=f"sys_{a}")]
                )
            buttons.append(
                [InlineKeyboardButton(t["back_to_menu"], callback_data="back_main")]
            )
            set_user_step(user_id, "await_choose_system_for_name")
            await update.message.reply_text(
                t["choose_system"], reply_markup=InlineKeyboardMarkup(buttons)
            )
            return

        if step == "await_cart_article":
            cart_art = text.split()[0]
            rows = cartridges_by_article.get(cart_art, [])
            if not rows:
                await update.message.reply_text(t["no_cartridges_found"])
                set_user_step(user_id, "main_menu")
                await update.message.reply_text(
                    t["main_menu"], reply_markup=main_menu_keyboard(lang)
                )
                return

            sys_articles = []
            for r in rows:
                a = str(r.get("Артикул", "")).split(".")[0]
                if a and a not in sys_articles:
                    sys_articles.append(a)

            if len(sys_articles) == 1:
                sys_article = sys_articles[0]
                sys_rows = systems_by_article.get(sys_article, [])
                await send_system_info(
                    update.message.chat_id, lang, sys_article, sys_rows, context
                )
                set_user_step(user_id, "main_menu")
                await update.message.reply_text(
                    t["main_menu"], reply_markup=main_menu_keyboard(lang)
                )
                return

            buttons = []
            for a in sys_articles:
                sys_rows = systems_by_article.get(a, [])
                sys_name = get_system_name_from_rows(sys_rows)
                label = f"{sys_name} (арт. {a})"
                buttons.append(
                    [
                        InlineKeyboardButton(
                            label, callback_data=f"sysfromcart_{a}_{cart_art}"
                        )
                    ]
                )
            buttons.append(
                [InlineKeyboardButton(t["back_to_menu"], callback_data="back_main")]
            )
            set_user_step(user_id, "await_choose_system_for_cart")
            await update.message.reply_text(
                t["choose_system_for_cart"], reply_markup=InlineKeyboardMarkup(buttons)
            )
            return

        if step == "await_cart_name":
            cart_name_norm = normalize_name(text)
            matched_cart_art = set()
            for r in all_rows:
                name = normalize_name(str(r.get("Название расходника", "")))
                cart_art = str(r.get("Артикул расходника", "")).split(".")[0]
                if not cart_art:
                    continue
                if cart_name_norm in name:
                    matched_cart_art.add(cart_art)

            if not matched_cart_art:
                await update.message.reply_text(t["no_cartridges_found"])
                set_user_step(user_id, "main_menu")
                await update.message.reply_text(
                    t["main_menu"], reply_markup=main_menu_keyboard(lang)
                )
                return

            if len(matched_cart_art) == 1:
                cart_art = list(matched_cart_art)[0]
                rows = cartridges_by_article.get(cart_art, [])
                sys_articles = []
                for r in rows:
                    a = str(r.get("Артикул", "")).split(".")[0]
                    if a and a not in sys_articles:
                        sys_articles.append(a)

                if len(sys_articles) == 1:
                    sys_article = sys_articles[0]
                    sys_rows = systems_by_article.get(sys_article, [])
                    await send_system_info(
                        update.message.chat_id, lang, sys_article, sys_rows, context
                    )
                    set_user_step(user_id, "main_menu")
                    await update.message.reply_text(
                        t["main_menu"], reply_markup=main_menu_keyboard(lang)
                    )
                    return

                buttons = []
                for a in sys_articles:
                    sys_rows = systems_by_article.get(a, [])
                    sys_name = get_system_name_from_rows(sys_rows)
                    label = f"{sys_name} (арт. {a})"
                    buttons.append(
                        [
                            InlineKeyboardButton(
                                label, callback_data=f"sysfromcart_{a}_{cart_art}"
                            )
                        ]
                    )
                buttons.append(
                    [InlineKeyboardButton(t["back_to_menu"], callback_data="back_main")]
                )
                set_user_step(user_id, "await_choose_system_for_cart")
                await update.message.reply_text(
                    t["choose_system_for_cart"],
                    reply_markup=InlineKeyboardMarkup(buttons),
                )
                return

            await update.message.reply_text(t["no_cartridges_found"])
            set_user_step(user_id, "main_menu")
            await update.message.reply_text(
                t["main_menu"], reply_markup=main_menu_keyboard(lang)
            )
            return

        if step in ("main_menu", "regular_menu", "choose_language"):
            set_user_step(user_id, "main_menu")
            await update.message.reply_text(
                t["main_menu"], reply_markup=main_menu_keyboard(lang)
            )
            return

        await update.message.reply_text(t["error"])

    except Exception as e:
        logger.exception("Text handler error: %s", e)
        await update.message.reply_text(t["error"])


async def send_system_info(
    chat_id: int,
    lang: str,
    sys_article: str,
    rows: List[Dict[str, Any]],
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    t = TEXTS[lang]
    sys_name = get_system_name_from_rows(rows)

    kits, singles = split_kits_and_items(rows)

    lines = [t["system_header"].format(name=sys_name, article=sys_article), ""]

    if kits:
        lines.append(t["kits_header"])
        for r in kits:
            cart_name = str(r.get("Название расходника", "")).strip()
            cart_art = str(r.get("Артикул расходника", "")).split(".")[0]
            url = str(r.get("ссылка", "")).strip()
            line = t["item_line"].format(name=cart_name, article=cart_art, url=url)
            lines.append(line)
        lines.append("")
        if singles:
            lines.append(t["items_header"])
            for r in singles:
                cart_name = str(r.get("Название расходника", "")).strip()
                cart_art = str(r.get("Артикул расходника", "")).split(".")[0]
                url = str(r.get("ссылка", "")).strip()
                line = t["item_line"].format(name=cart_name, article=cart_art, url=url)
                lines.append(line)
    else:
        lines.append(t["no_kits"])
        if singles:
            for r in singles:
                cart_name = str(r.get("Название расходника", "")).strip()
                cart_art = str(r.get("Артикул расходника", "")).split(".")[0]
                url = str(r.get("ссылка", "")).strip()
                line = t["item_line"].format(name=cart_name, article=cart_art, url=url)
                lines.append(line)

    text = "\n".join(lines)
    await context.bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=False)


# ========= ИНИЦИАЛИЗАЦИЯ PTB + FASTAPI (WEBHOOK) =========

ptb_app = (
    Application.builder()
    .updater(None)   # важно для webhook-сценария
    .token(TELEGRAM_TOKEN)
    .build()
)

# регистрируем хендлеры
ptb_app.add_handler(CommandHandler("start", start))
ptb_app.add_handler(CommandHandler("reload", reload_data))
ptb_app.add_handler(CallbackQueryHandler(handle_callback))
ptb_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Жизненный цикл FastAPI-приложения: загрузка данных, запуск/остановка PTB, установка webhook."""
    load_data_from_sheets()  # подтягиваем таблицу при старте
    logger.info("Setting Telegram webhook to %s", WEBHOOK_URL)
    await ptb_app.bot.set_webhook(WEBHOOK_URL)

    async with ptb_app:
        await ptb_app.start()
        yield
        await ptb_app.stop()


app = FastAPI(lifespan=lifespan)


@app.post("/")
async def telegram_webhook(request: Request):
    """Эндпоинт, куда Telegram шлёт обновления."""
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=HTTPStatus.OK)
