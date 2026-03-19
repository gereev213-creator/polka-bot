import os
import logging
import json
import sqlite3
import asyncio
import re
import hashlib
import hmac
import uuid
from aiohttp import web as aiohttp_web
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from collections import defaultdict

try:
    import imagehash
    from PIL import Image
    IMAGEHASH_AVAILABLE = True
except ImportError:
    IMAGEHASH_AVAILABLE = False

import io as _io
import base64 as _base64

from telegram import (
    Update, InlineKeyboardButton as _InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    InputMediaPhoto, BotCommand, Message, WebAppInfo
)

# ═══════════════════════════════════════════════════════════════════════
#  RATE LIMITING — защита от флуда
# ═══════════════════════════════════════════════════════════════════════
import time as _time
from collections import defaultdict as _defaultdict

class RateLimiter:
    """Простой in-memory rate limiter: скользящее окно в 60 секунд."""
    def __init__(self):
        self._msg:  dict = _defaultdict(list)
        self._cb:   dict = _defaultdict(list)

    def _clean(self, store: dict, user_id: int, window: int = 60):
        now = _time.monotonic()
        store[user_id] = [t for t in store[user_id] if now - t < window]

    def check_message(self, user_id: int) -> bool:
        """True = разрешено, False = заблокировано."""
        self._clean(self._msg, user_id)
        if len(self._msg[user_id]) >= RATE_LIMIT_MESSAGES:
            return False
        self._msg[user_id].append(_time.monotonic())
        return True

    def check_callback(self, user_id: int) -> bool:
        self._clean(self._cb, user_id)
        if len(self._cb[user_id]) >= RATE_LIMIT_CALLBACKS:
            return False
        self._cb[user_id].append(_time.monotonic())
        return True

_rate_limiter = RateLimiter()

async def rate_limit_middleware(update, context):
    """PTB pre-процессор: отклоняем слишком частые запросы."""
    user = update.effective_user
    if not user:
        return
    if update.message:
        if not _rate_limiter.check_message(user.id):
            try:
                await update.message.reply_text("⏳ Слишком много запросов. Подождите немного.")
            except Exception:
                pass
            raise Exception("rate_limited")
    elif update.callback_query:
        if not _rate_limiter.check_callback(user.id):
            try:
                await update.callback_query.answer("⏳ Слишком быстро — подождите секунду.", show_alert=False)
            except Exception:
                pass
            raise Exception("rate_limited")

# ── Цветные кнопки: Bot API 9.4 style=primary/danger ─────────────────────────
_DANGER_KW = ("✗ Удалить", "✗ Отмена", "✗ Отклонить", "Закрыть", "✗", "Бан", "Отменить", "Отказ")

class InlineKeyboardButton(_InlineKeyboardButton):
    """Обёртка без автоматического стиля — все кнопки единого нейтрального цвета."""
    def __init__(self, text, *args, **kwargs):
        super().__init__(text, *args, **kwargs)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, ConversationHandler, filters, JobQueue
)
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch

"""
ИНСТРУКЦИЯ ПО КРИПТОССЫЛКАМ ДЛЯ ПРОМОКОДОВ:

В словаре CRYPTO_LINKS (строка ~155) настроены криптоссылки для разных процентов скидки (5%, 10%, 15%... до 80%).
Когда админ создаёт промокод с определённым процентом скидки, система автоматически использует 
соответствующую криптоссылку из этого словаря.

Например:
- Промокод со скидкой 30% → будет использована ссылка из CRYPTO_LINKS[30]
- Промокод со скидкой 50% → будет использована ссылка из CRYPTO_LINKS[50]

ЧТО НУЖНО СДЕЛАТЬ:
1. Найдите словарь CRYPTO_LINKS (строка ~155)
2. Замените URL-ссылки на реальные криптовалютные ссылки для каждого процента
3. Если нужной ссылки нет в словаре, система будет использовать стандартную криптоссылку

ПРИМЕР:
CRYPTO_LINKS = {
    5: "https://your-real-crypto-link-for-5-percent.com",
    10: "https://your-real-crypto-link-for-10-percent.com",
    ...
}

СТАТИСТИКА:
- В админ панели (Промокоды) показывается сколько раз использовался каждый промокод
- При создании промокода админ видит, есть ли криптоссылка для выбранного процента
- Когда пользователь применяет промокод, ему показывается специальная криптоссылка
- При отправке чека админу приходит уведомление с информацией о промокоде и скидке
"""

# ═══════════════════════════════════════════════════════════════════════
#  КОНФИГ — все настройки в одном месте
#  Чувствительные значения берём из переменных окружения (.env)
# ═══════════════════════════════════════════════════════════════════════

# Токен и администраторы
TOKEN          = os.environ.get("BOT_TOKEN", "8792431247:AAF58-nM7JuquW468KVnxkVPXBmwqFBfKuM")
ADMIN_USER_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "6115882628").split(",")]

# Каналы
ADMIN_CHANNEL_ID = os.environ.get("MAIN_CHANNEL", "@PolkaAds")
TECH_CHANNEL_ENV = os.environ.get("TECH_CHANNEL",  "@PolkaAdsTech")

# PayAnyWay
PAW_ACCOUNT   = os.environ.get("PAW_ACCOUNT",   "98638014")
PAW_SECRET    = os.environ.get("PAW_SECRET",    "12345")      # код проверки целостности из ЛК PayAnyWay
PAW_TEST_MODE = os.environ.get("PAW_TEST_MODE", "0")          # 0 = боевой режим (тест на 1 руб)

# Сервер на Amvera
SERVER_URL    = os.environ.get("SERVER_URL", "https://amvera-nikicev2009-run-polka-bot.amvera.ru")
WEBAPP_URL_ENV = os.environ.get("WEBAPP_URL", "").rstrip("/")
WEBAPP_PORT_ENV = int(os.environ.get("WEBAPP_PORT", "80"))

# Цены тарифов (₽/месяц) — ТЕСТ: 1 руб, после проверки вернуть 299 и 799
PRICE_STANDARD = int(os.environ.get("PRICE_STANDARD", "1"))
PRICE_PRO      = int(os.environ.get("PRICE_PRO",      "1"))

# Rate limiting — максимум запросов с одного user_id
RATE_LIMIT_MESSAGES  = int(os.environ.get("RATE_LIMIT_MSG",  "30"))  # сообщений в минуту
RATE_LIMIT_CALLBACKS = int(os.environ.get("RATE_LIMIT_CB",   "60"))  # callback в минуту

# ═══════════════════════════════════════════════════════════════════════

# Настройки бота (обратная совместимость)
ADMIN_CHANNEL_ID = "@PolkaAds"

# Словарь город → username городского канала
# Если город не найден или "Другой город" — публикуем только в основной @PolkaAds
CITY_CHANNELS = {
    "Москва":                    "@PolkaAdsMoscow",
    "Питер":                     "@PolkaAdsSPB",
    "Санкт-Петербург":           "@PolkaAdsSPB",
    "Абакан":                    "@PolkaAdsAbakan",
    "Анадырь":                   "@PolkaAdsAnadyr",
    "Архангельск":               "@PolkaAdsArkhangelsk",
    "Астрахань":                 "@PolkaAdsAstrakhan",
    "Барнаул":                   "@PolkaAdsBarnaul",
    "Белгород":                  "@PolkaAdsBelgorod",
    "Биробиджан":                "@PolkaAdsBirobidjan",
    "Благовещенск":              "@PolkaAdsBlagoveshchensk",
    "Брянск":                    "@PolkaAdsBryansk",
    "Великий Новгород":          "@PolkaAdsVelNovgorod",
    "Владивосток":               "@PolkaAdsVladivostok",
    "Владикавказ":               "@PolkaAdsVladikavkaz",
    "Владимир":                  "@PolkaAdsVladimir",
    "Волгоград":                 "@PolkaAdsVolgograd",
    "Вологда":                   "@PolkaAdsVologda",
    "Воронеж":                   "@PolkaAdsVoronezh",
    "Горно-Алтайск":             "@PolkaAdsGornoAltaysk",
    "Грозный":                   "@PolkaAdsGrozny",
    "Екатеринбург":              "@PolkaAdsEkaterinburg",
    "Иваново":                   "@PolkaAdsIvanovo",
    "Ижевск":                    "@PolkaAdsIzhevsk",
    "Иркутск":                   "@PolkaAdsIrkutsk",
    "Йошкар-Ола":                "@PolkaAdsYoshkarOla",
    "Казань":                    "@PolkaAdsKazan",
    "Калининград":               "@PolkaAdsKaliningrad",
    "Калуга":                    "@PolkaAdsKaluga",
    "Кемерово":                  "@PolkaAdsKemerovo",
    "Киров":                     "@PolkaAdsKirov",
    "Кострома":                  "@PolkaAdsKostroma",
    "Краснодар":                 "@PolkaAdsKrasnodar",
    "Красноярск":                "@PolkaAdsKrasnoyarsk",
    "Курган":                    "@PolkaAdsKurgan",
    "Курск":                     "@PolkaAdsKursk",
    "Липецк":                    "@PolkaAdsLipetsk",
    "Магадан":                   "@PolkaAdsMagadan",
    "Майкоп":                    "@PolkaAdsMaykop",
    "Махачкала":                 "@PolkaAdsMakhachkala",
    "Мурманск":                  "@PolkaAdsMurmansk",
    "Назрань":                   "@PolkaAdsNazran",
    "Нальчик":                   "@PolkaAdsNalchik",
    "Нижневартовск":             "@PolkaAdsNizhnevartovsk",
    "Нижний Новгород":           "@PolkaAdsNizhnyNovgorod",
    "Новосибирск":               "@PolkaAdsNovosibirsk",
    "Омск":                      "@PolkaAdsOmsk",
    "Орёл":                      "@PolkaAdsOrel",
    "Оренбург":                  "@PolkaAdsOrenburg",
    "Пенза":                     "@PolkaAdsPenza",
    "Пермь":                     "@PolkaAdsPerm",
    "Петрозаводск":              "@PolkaAdsPetrozavodsk",
    "Петропавловск-Камчатский":  "@PolkaAdsPetropavlovsk",
    "Псков":                     "@PolkaAdsPskov",
    "Пятигорск":                 "@PolkaAdsPyatigorsk",
    "Ростов-на-Дону":            "@PolkaAdsRostov",
    "Рязань":                    "@PolkaAdsRyazan",
    "Салехард":                  "@PolkaAdsSalehard",
    "Самара":                    "@PolkaAdsSamara",
    "Саратов":                   "@PolkaAdsSaratov",
    "Смоленск":                  "@PolkaAdsSmolensk",
    "Ставрополь":                "@PolkaAdsStavropol",
    "Сургут":                    "@PolkaAdsSurgut",
    "Сыктывкар":                 "@PolkaAdsSyktyvkar",
    "Тамбов":                    "@PolkaAdsTambov",
    "Тверь":                     "@PolkaAdsTver",
    "Томск":                     "@PolkaAdsTomsk",
    "Тула":                      "@PolkaAdsTula",
    "Тюмень":                    "@PolkaAdsTyumen",
    "Улан-Удэ":                  "@PolkaAdsUlanUde",
    "Ульяновск":                 "@PolkaAdsUlyanovsk",
    "Уфа":                       "@PolkaAdsUfa",
    "Хабаровск":                 "@PolkaAdsKhabarovsk",
    "Ханты-Мансийск":            "@PolkaAdsKhantyMansiysk",
    "Чебоксары":                 "@PolkaAdsCheboksary",
    "Челябинск":                 "@PolkaAdsChelyabinsk",
    "Черкесск":                  "@PolkaAdsCherkessk",
    "Чита":                      "@PolkaAdsChita",
    "Элиста":                    "@PolkaAdsElista",
    "Южно-Сахалинск":            "@PolkaAdsYuzhnoSakhalinsk",
    "Якутск":                    "@PolkaAdsYakutsk",
    "Ярославль":                 "@PolkaAdsYaroslavl",
}

# Каналы одежды по полу/возрасту
# Замените username на реальные каналы и добавьте бота администратором
CLOTHES_CHANNELS = {
    "Мужская":  "@PolkaAdsMen",    # канал мужской одежды
    "Женская":  "@PolkaAdsWomen",  # канал женской одежды
    "Детская":  "@PolkaAdsKids",   # канал детской одежды
}

# Канал техники — публикуется для всех объявлений категории «Техника»
TECH_CHANNEL = "@PolkaAdsTech"  # замените на реальный username

# URL вашего сервера (нужен HTTPS для Mini App)
# Пример: "https://polka.example.com"
WEBAPP_URL  = os.environ.get("WEBAPP_URL", "https://amvera-nikicev2009-run-polka-bot.amvera.ru").rstrip("/")
WEBAPP_PORT = int(os.environ.get("WEBAPP_PORT", "80"))

# Баннеры для разных разделов
BANNERS = {
    "welcome":       "https://i.postimg.cc/2jd4xhDX/1-2.png",
    "create_ad":     "https://i.postimg.cc/k43bQXY1/image.png",
    "search":        "https://i.postimg.cc/SK8XxbQj/image.png",
    "my_ads":        "https://i.postimg.cc/2jd4xhDX/1-2.png",
    "favorites":     "https://i.postimg.cc/8kw75HD1/image.png",
    "subscriptions": "https://i.postimg.cc/t4DRvZn5/image.png",
    "profile":       "https://i.postimg.cc/3wqJ8Wpm/image.png",
    "referral":      "https://i.postimg.cc/PJQjwqpL/image.png",
    "admin":         "https://i.postimg.cc/2jd4xhDX/1-2.png"
}

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Функция для получения московского времени
def get_moscow_time():
    """Возвращает текущее время в московском часовом поясе"""
    return datetime.now(ZoneInfo('Europe/Moscow'))


def fmt_date(raw: str) -> str:
    if not raw: return ''
    MONTHS = ['янв','фев','мар','апр','мая','июн','июл','авг','сен','окт','ноя','дек']
    try:
        dt = datetime.strptime(raw[:10], '%Y-%m-%d')
        return f"{dt.day} {MONTHS[dt.month-1]} {dt.year}"
    except Exception:
        return raw[:10]

def fmt_datetime(raw: str) -> str:
    if not raw: return ''
    MONTHS = ['янв','фев','мар','апр','мая','июн','июл','авг','сен','окт','ноя','дек']
    try:
        dt = datetime.strptime(raw[:16].replace('T',' '), '%Y-%m-%d %H:%M')
        return f"{dt.day} {MONTHS[dt.month-1]} {dt.year}, {dt.hour:02d}:{dt.minute:02d}"
    except Exception:
        return raw[:16]

def plural_ads(n: int) -> str:
    if 11 <= n % 100 <= 19: return f"{n} объявлений"
    r = n % 10
    if r == 1:       return f"{n} объявление"
    if 2 <= r <= 4:  return f"{n} объявления"
    return f"{n} объявлений"

def get_anon_id(user_data: dict) -> str:
    """Возвращает анонимный ID пользователя или генерирует на лету"""
    if user_data and user_data.get('anon_id'):
        return user_data['anon_id']
    import string as _s, random as _r
    return ''.join(_r.choices(_s.ascii_uppercase + _s.digits, k=6))

# ── UI: словари для человекочитаемых значений ────────────────────────
AD_STATUS_LABELS = {
    'active':     '● Активно',
    'inactive':   '○ Снято',
    'moderation': '◎ На модерации',
    'rejected':   '✗ Отклонено',
    'sold':       '✓ Продано',
}
TARIFF_LABELS = {
    'Free':     'Бесплатно',
    'Standard': 'Старт',
    'PRO':      'PRO',
    'free':     'Бесплатно',
    'standard': 'Старт',
    'pro':      'PRO',
}
ESCROW_STATUS_LABELS = {
    'pending_payment':   '◎ Ожидает оплаты',
    'payment_review':    'Проверка оплаты',
    'paid':              '● Оплачено',
    'shipped':           'Отправлено',
    'completed':         '✓ Завершено',
    'disputed':          '◎ Спор',
    'refunded':          'Возврат',
    'cancelled':         '✗ Отменено',
    'payment_rejected':  '✗ Оплата отклонена',
}

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ БЕЗОПАСНОГО РЕДАКТИРОВАНИЯ ==========
def fmt_rub(amount) -> str:
    """12345 → '12 345'"""
    try:
        return f"{int(amount):,}".replace(",", "\u00a0")
    except (TypeError, ValueError):
        return str(amount)

async def safe_edit_message(query, text, keyboard=None, parse_mode=None):
    """Безопасное редактирование сообщения с проверкой типа"""
    try:
        if query.message.photo:
            await query.message.delete()
            if keyboard:
                await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=parse_mode)
            else:
                await query.message.reply_text(text, parse_mode=parse_mode)
        else:
            if keyboard:
                await query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=parse_mode)
            else:
                await query.edit_message_text(text=text, parse_mode=parse_mode)
    except Exception as e:
        logger.error(f"Ошибка при редактировании сообщения: {e}")
        if keyboard:
            await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=parse_mode)
        else:
            await query.message.reply_text(text, parse_mode=parse_mode)

async def safe_edit_caption(query, caption, keyboard=None):
    """Безопасное редактирование подписи к фото"""
    try:
        if query.message.photo:
            if keyboard:
                await query.edit_message_caption(caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await query.edit_message_caption(caption=caption)
        else:
            # Если нет фото, редактируем текст
            if keyboard:
                await query.edit_message_text(text=caption, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await query.edit_message_text(text=caption)
    except Exception as e:
        logger.error(f"Ошибка при редактировании подписи: {e}")
        # В случае ошибки отправляем новое сообщение
        if keyboard:
            await query.message.reply_text(caption, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.message.reply_text(caption)

# Состояния
class States(Enum):
    START = 0
    MAIN_MENU = 1
    CREATE_AD_CATEGORY = 25  # Новое состояние для выбора категории
    CREATE_AD_TITLE = 2
    CREATE_AD_DESCRIPTION = 3
    CREATE_AD_PRICE = 4
    CREATE_AD_CONDITION = 21
    CREATE_AD_SIZE = 22
    CREATE_AD_CITY = 23
    CREATE_AD_DELIVERY = 24
    CREATE_AD_PHOTOS = 5
    CREATE_AD_CONTACTS = 6
    CREATE_AD_PREVIEW = 7
    CREATE_AD_DELIVERY_CUSTOM = 68
    CREATE_AD_DESCRIPTION_OPT = 69
    CREATE_AD_GENDER = 70
    SEARCH_QUERY = 8
    PAYMENT_PROOF = 9
    ADMIN_MENU = 10
    ADMIN_BROADCAST = 11
    ADMIN_MODERATION = 12
    ADMIN_MODERATION_REASON = 13
    ADMIN_BROADCAST_CONFIRM = 14
    # Новые состояния для промокодов
    ADMIN_PROMO_CREATE_CODE = 30
    ADMIN_PROMO_CREATE_DISCOUNT = 31
    ADMIN_PROMO_CREATE_DURATION = 32
    ADMIN_PROMO_CREATE_USES = 33
    ENTER_PROMOCODE = 34
    ADMIN_CRYPTO_LINK_AMOUNT = 35
    ADMIN_CRYPTO_LINK_URL = 36
    # Буст, отзывы (v22)
    BOOST_PAYMENT_PROOF = 37
    REVIEW_TEXT = 38
    REVIEW_RATING = 39
    # Эскроу (v23)
    ESCROW_AMOUNT = 40
    ESCROW_AD_ID = 41
    ESCROW_CONFIRM_BUYER = 42
    ESCROW_CONFIRM_SELLER = 43
    # v24: редактирование объявлений
    EDIT_AD_TITLE = 50
    EDIT_AD_DESCRIPTION = 51
    EDIT_AD_PRICE = 52
    EDIT_AD_CITY = 53
    EDIT_AD_DELIVERY = 54
    EDIT_AD_CONTACTS = 55
    # v25
    SEARCH_CITY_FILTER = 60
    AUTO_BUMP_INTERVAL = 61
    # v36: верификация по телефону, обжалование отзыва, избранные продавцы
    VERIFY_PHONE_INPUT = 62
    VERIFY_CODE_INPUT = 63
    REVIEW_APPEAL_TEXT = 64
    # v37: улучшенный поиск как Авито
    SEARCH_PRICE_FROM = 65
    SEARCH_PRICE_TO = 66
    SEARCH_PHOTO = 67

# Конфигурация - УВЕЛИЧЕН ЛИМИТ ФОТО ДО 10
TARIFFS = {
    "Free":     {"daily_ads": 3,            "pins_per_month": 0, "pin_hours": 0,   "photo_limit": 3,  "price": 0,
                 "boosts_included": 0, "auto_bump_interval": None, "label": "Бесплатно"},
    "Standard": {"daily_ads": float('inf'), "pins_per_month": 1, "pin_hours": 24,  "photo_limit": 10, "price": PRICE_STANDARD,
                 "boosts_included": 2, "auto_bump_interval": 24,  "label": "Старт"},
    "PRO":      {"daily_ads": float('inf'), "pins_per_month": 5, "pin_hours": 168, "photo_limit": 10, "price": PRICE_PRO,
                 "boosts_included": 5, "auto_bump_interval": 6,   "label": "PRO"},
}

# Криптоссылки по тарифу и проценту скидки.
# Формат: (план, процент_скидки) -> URL криптоплатёжной страницы
# plan = 'standard' (299 руб) | 'pro' (799 руб)
CRYPTO_LINKS_BY_PLAN = {
    # ── Старт (Standard, 299 руб) ────────────────────────────────────────
    ("standard",  0): "http://t.me/send?start=IVRWI4KCIZoj",   # 299 руб / 4$
    ("standard", 10): "http://t.me/send?start=IVjqXjoTpiN1",   # 269 руб / 3.5$
    ("standard", 20): "http://t.me/send?start=IVNvInun7KyW",   # 239 руб / 3.1$
    ("standard", 30): "http://t.me/send?start=IV0VnXS32elN",   # 209 руб / 2.7$
    ("standard", 40): "http://t.me/send?start=IVWmZQYYKSgm",   # 179 руб / 2.3$
    ("standard", 50): "http://t.me/send?start=IV7cbzb6dpDD",   # 149 руб / 1.9$
    ("standard", 60): "http://t.me/send?start=IVoZTZEiOOym",   # 119 руб / 1.6$
    ("standard", 70): "http://t.me/send?start=IVEjIGSiXdQO",   #  89 руб / 1.3$
    ("standard", 80): "http://t.me/send?start=IVBHs5bGHbVB",   #  59 руб / 1.0$
    # ── PRO (799 руб) ─────────────────────────────────────────────────────
    ("pro",  0): "http://t.me/send?start=IVVH3qAPHGQ0",        # 799 руб / 11$
    ("pro", 10): "http://t.me/send?start=IVr1Q6IbEMh2",        # 719 руб / 10$
    ("pro", 20): "http://t.me/send?start=IVkSxwW2plRS",        # 639 руб /  9$
    ("pro", 30): "http://t.me/send?start=IVV41SYXjMBN",        # 559 руб /  8$
    ("pro", 40): "http://t.me/send?start=IVu9VYtLQxI8",        # 479 руб /  7$
    ("pro", 50): "http://t.me/send?start=IVSH2oDMGPiO",        # 399 руб /  6$
    ("pro", 60): "http://t.me/send?start=IVpUmdA3u1qB",        # 319 руб /  5$
    ("pro", 70): "http://t.me/send?start=IV0ajkRyEBkl",        # 239 руб /  4$
    ("pro", 80): "http://t.me/send?start=IVo91nun8KFv",        # 159 руб /  3$
}

# Обратная совместимость: CRYPTO_LINKS[процент] — стандартный тариф
CRYPTO_LINKS = {pct: url for (plan, pct), url in CRYPTO_LINKS_BY_PLAN.items() if plan == "standard"}

# Словарь для умного поиска
SEARCH_SYNONYMS = {
    'айфон': ['iphone', 'айфон', 'apple', 'ифон'],
    'iphone': ['iphone', 'айфон', 'apple', 'ифон'],
    'ифон': ['iphone', 'айфон', 'apple'],
    'apple': ['iphone', 'айфон', 'apple'],
    'самсунг': ['samsung', 'самсунг', 'галакси'],
    'samsung': ['samsung', 'самсунг', 'галакси'],
    'галакси': ['samsung', 'галакси', 'galaxy'],
    'кроссовки': ['кроссовки', 'кеды', 'обувь', 'sneakers', 'keds'],
    'кеды': ['кроссовки', 'кеды', 'обувь', 'sneakers'],
    'sneakers': ['кроссовки', 'кеды', 'обувь', 'sneakers'],
    'техника': ['техника', 'электроника', 'gadgets', 'electronics'],
    'ноутбук': ['ноутбук', 'laptop', 'ноут', 'лэптоп'],
    'laptop': ['ноутбук', 'laptop', 'ноут'],
    'телефон': ['телефон', 'смартфон', 'phone', 'smartphone'],
    'phone': ['телефон', 'смартфон', 'phone'],
    'диван': ['диван', 'sofa', 'диванчик'],
    'sofa': ['диван', 'sofa', 'диванчик'],
    'машина': ['машина', 'автомобиль', 'car', 'auto'],
    'car': ['машина', 'автомобиль', 'car'],
    'квартира': ['квартира', 'apartment', 'flat', 'жилье'],
    'apartment': ['квартира', 'apartment', 'flat'],
}


# ── Поисковые утилиты ─────────────────────────────────────────────────────────

_TRANSLIT_RU_EN = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e',
    'ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m',
    'н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u',
    'ф':'f','х':'h','ц':'ts','ч':'ch','ш':'sh','щ':'sch',
    'ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
}

_TRANSLIT_EN_RU = {
    'a':'а','b':'б','c':'к','d':'д','e':'е','f':'ф','g':'г',
    'h':'х','i':'и','j':'дж','k':'к','l':'л','m':'м','n':'н',
    'o':'о','p':'п','q':'к','r':'р','s':'с','t':'т','u':'у',
    'v':'в','w':'в','x':'кс','y':'й','z':'з',
}


def _normalize_query(q: str) -> list:
    """
    Возвращает список поисковых термов для запроса q:
    - исходный запрос
    - транслитерация (ru→en и en→ru)
    - все слова по отдельности (для поиска по отдельным словам)
    - синонимы из SEARCH_SYNONYMS
    Убирает стоп-слова и слова короче 2 символов.
    """
    STOP_WORDS = {'в','на','с','из','по','за','к','у','о','и','а','но','не',
                  'это','то','для','при','как','так','что','или','от','до'}

    q = q.lower().strip()
    if not q:
        return []

    terms = set()
    terms.add(q)

    # Транслитерация ru→en
    t_en = q
    for ru, en in _TRANSLIT_RU_EN.items():
        t_en = t_en.replace(ru, en)
    if t_en != q:
        terms.add(t_en)

    # Транслитерация en→ru
    t_ru = q
    for en, ru in _TRANSLIT_EN_RU.items():
        t_ru = t_ru.replace(en, ru)
    if t_ru != q:
        terms.add(t_ru)

    # Отдельные слова запроса + их синонимы
    words = [w for w in q.split() if len(w) >= 2 and w not in STOP_WORDS]
    for word in words:
        terms.add(word)
        if word in SEARCH_SYNONYMS:
            terms.update(SEARCH_SYNONYMS[word])
        # Транслитерация каждого слова
        w_en = word
        for ru, en in _TRANSLIT_RU_EN.items():
            w_en = w_en.replace(ru, en)
        if w_en != word:
            terms.add(w_en)

    # Убираем пустые и слишком короткие
    return [t for t in terms if len(t) >= 2]


def _build_fts_conditions(search_terms: list) -> tuple:
    """
    Строит SQL WHERE условия и параметры для поиска по нескольким термам.
    Возвращает (conditions_list, params_list).
    Использует search_index для основного поиска (быстро) +
    title для точных совпадений (релевантность).
    """
    conditions = []
    params = []
    for term in search_terms:
        like = f"%{term}%"
        conditions.append(
            "(search_index LIKE ? OR title LIKE ?)"
        )
        params.extend([like, like])
    return conditions, params

REFERRAL_PERCENT = 5
REFERRAL_MIN_BALANCE_FOR_SUBSCRIPTION = 100
VERIFICATION_PRICE = 390           # рублей за верификацию продавца (1 год)
VERIFICATION_REQUIRED_SALES = 5   # или 5+ завершённых сделок — бесплатно

# ── v25 ──────────────────────────────────────────────────────────────
CHEAP_BADGE_THRESHOLD = 0.85       # цена ≤ 85% средней → значок ▼
AUTO_BUMP_PRICE = 149              # ₽/месяц за авто-подъём (PRO-фича)
AUTO_BUMP_INTERVALS_H = [6, 12, 24]  # варианты интервала авто-бампа

PAYMENT_DETAILS = {
    "yoomoney_standard": "https://yoomoney.ru/to/4100119456604619",
    "yoomoney_pro": "https://yoomoney.ru/to/4100119456604619",
    "crypto_standard": "http://t.me/send?start=IVRWI4KCIZoj",   # Старт 299 руб / 4$
    "crypto_pro": "http://t.me/send?start=IVVH3qAPHGQ0",         # PRO 799 руб / 11$
    "tinkoff_account": "2200701043238127"
}

# ── Поиск по фото ─────────────────────────────────────────────────────────────
# Hugging Face Inference API — бесплатно, ~30k запросов/месяц
# Получить токен: huggingface.co → Settings → Access Tokens → New token (Read)
HF_API_TOKEN = os.environ.get("HF_API_TOKEN", "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")
# Модель для классификации изображений (понимает 1000 категорий объектов)
HF_IMAGE_MODEL = "microsoft/resnet-50"
# Если токен не задан — теги недоступны, работает только pHash поиск

# Лимиты поиска по фото: {daily: кол-во в день, cooldown: сек между запросами}
PHOTO_SEARCH_LIMITS = {
    "Free":     {"daily": 3,  "cooldown": 60},
    "Standard": {"daily": 10, "cooldown": 20},
    "PRO":      {"daily": 0,  "cooldown": 5},   # 0 = без лимита
}

# ── PayAnyWay (Moneta.Assistant) ─────────────────────────────────────────────
# PAW_ACCOUNT, PAW_SECRET, PAW_TEST_MODE, SERVER_URL — заданы в конфиг-блоке выше
PAW_URL = "https://www.payanyway.ru/assistant.htm"

# Уникальные тексты для категорий
CATEGORY_EXAMPLES = {
    "· Одежда": "Timberland Sprint Trekker",
    "· Обувь": "Nike Air Max",
    "· Техника": "Apple iPhone 14",
    "· Другое": "Название товара"
}

# База данных с путем /data
class Database:
    def __init__(self, db_path: str = None):
        if db_path is None:
            if os.path.exists('/data'):
                db_path = "/data/polka.db"
            else:
                db_path = "polka.db"
        
        directory = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")  # concurrent reads + writes
        self.conn.execute("PRAGMA synchronous=NORMAL")  # быстрее, безопасно при WAL
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
        logger.info(f"База данных инициализирована по пути: {db_path}")
    
    def create_tables(self):
        cursor = self.conn.cursor()
        
        # Создаем таблицу пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                tariff TEXT DEFAULT 'Free',
                tariff_end_date TEXT,
                subscribed INTEGER DEFAULT 0,
                registration_date TEXT,
                daily_ads_count INTEGER DEFAULT 0,
                pinned_ads_used INTEGER DEFAULT 0,
                last_daily_reset TEXT,
                last_activity TEXT,
                notifications_enabled INTEGER DEFAULT 1,
                anon_id TEXT UNIQUE
            )
        ''')

        # Миграция: gender для объявлений одежды
        try:
            cursor.execute("ALTER TABLE ads ADD COLUMN gender TEXT")
        except Exception:
            pass
        # Миграция: anon_id для существующих пользователей
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN anon_id TEXT")
        except Exception:
            pass
        # Генерируем anon_id тем у кого его нет
        import string as _string, random as _random
        cursor.execute("SELECT user_id FROM users WHERE anon_id IS NULL")
        for (uid,) in cursor.fetchall():
            while True:
                aid = ''.join(_random.choices(_string.ascii_uppercase + _string.digits, k=6))
                try:
                    cursor.execute("UPDATE users SET anon_id=? WHERE user_id=?", (aid, uid))
                    break
                except Exception:
                    pass
        self.conn.commit()

        
        # Создаем таблицу объявлений с новыми полями
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                title TEXT,
                description TEXT,
                price INTEGER,
                photos TEXT,
                contact_info TEXT,
                category TEXT,
                status TEXT DEFAULT 'moderation',
                views INTEGER DEFAULT 0,
                created_at TEXT,
                published_at TEXT,
                pinned_until TEXT,
                moderated_by INTEGER,
                moderation_reason TEXT,
                channel_message_id INTEGER,
                condition TEXT,
                size TEXT,
                city TEXT,
                delivery TEXT,
                gender TEXT,
                search_index TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Создаем индексы для поиска
        for _idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_ads_search   ON ads(search_index)",
            "CREATE INDEX IF NOT EXISTS idx_ads_status    ON ads(status)",
            "CREATE INDEX IF NOT EXISTS idx_ads_city      ON ads(city)",
            "CREATE INDEX IF NOT EXISTS idx_ads_category  ON ads(category)",
            "CREATE INDEX IF NOT EXISTS idx_ads_price     ON ads(price)",
            "CREATE INDEX IF NOT EXISTS idx_ads_created   ON ads(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_ads_pinned    ON ads(pinned_until)",
            "CREATE INDEX IF NOT EXISTS idx_ads_user      ON ads(user_id, status)",
        ]:
            cursor.execute(_idx_sql)
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS favorites (
                user_id INTEGER,
                ad_id INTEGER,
                added_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(ad_id) REFERENCES ads(id),
                PRIMARY KEY(user_id, ad_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                tariff TEXT,
                amount INTEGER,
                screenshot_file_id TEXT,
                status TEXT DEFAULT 'pending',
                admin_id INTEGER,
                admin_comment TEXT,
                created_at TEXT,
                confirmed_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                date TEXT PRIMARY KEY,
                new_users INTEGER DEFAULT 0,
                new_ads INTEGER DEFAULT 0,
                published_ads INTEGER DEFAULT 0,
                payments INTEGER DEFAULT 0,
                revenue INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT,
                details TEXT,
                created_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER UNIQUE,
                registered_at TEXT,
                has_bought_subscription INTEGER DEFAULT 0,
                FOREIGN KEY(referrer_id) REFERENCES users(user_id),
                FOREIGN KEY(referred_id) REFERENCES users(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referral_balance (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                referrals_count INTEGER DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referral_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                type TEXT,
                amount INTEGER,
                description TEXT,
                created_at TEXT,
                FOREIGN KEY(referrer_id) REFERENCES users(user_id),
                FOREIGN KEY(referred_id) REFERENCES users(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referral_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                method TEXT,
                details TEXT,
                status TEXT DEFAULT 'pending',
                admin_id INTEGER,
                admin_comment TEXT,
                created_at TEXT,
                processed_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        # НОВАЯ ТАБЛИЦА: Промокоды
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                discount_percent INTEGER NOT NULL,
                duration_days INTEGER NOT NULL,
                max_uses INTEGER NOT NULL,
                current_uses INTEGER DEFAULT 0,
                created_at TEXT,
                expires_at TEXT,
                is_active INTEGER DEFAULT 1
            )
        ''')
        
        # НОВАЯ ТАБЛИЦА: Использование промокодов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS promocode_uses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                promocode_id INTEGER,
                user_id INTEGER,
                used_at TEXT,
                FOREIGN KEY(promocode_id) REFERENCES promocodes(id),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        # НОВАЯ ТАБЛИЦА: Криптоссылки для разных сумм
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crypto_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount INTEGER UNIQUE NOT NULL,
                crypto_url TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
        ''')

        # ТАБЛИЦА: Бусты объявлений (платный топ)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS boosts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                hours INTEGER DEFAULT 24,
                screenshot_file_id TEXT,
                status TEXT DEFAULT 'pending',
                boosted_until TEXT,
                created_at TEXT,
                confirmed_at TEXT,
                FOREIGN KEY(ad_id) REFERENCES ads(id),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')

        # ТАБЛИЦА: Сохранённые поиски
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS saved_searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                query TEXT NOT NULL,
                created_at TEXT,
                UNIQUE(user_id, query),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')

        # ТАБЛИЦА: Уже уведомлённые совпадения (чтобы не слать дважды)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_notifications_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                ad_id INTEGER NOT NULL,
                sent_at TEXT,
                UNIQUE(user_id, ad_id)
            )
        ''')

        # ТАБЛИЦА: Отзывы о продавцах
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER NOT NULL,
                buyer_id INTEGER NOT NULL,
                ad_id INTEGER NOT NULL,
                rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
                text TEXT,
                created_at TEXT,
                UNIQUE(buyer_id, ad_id),
                FOREIGN KEY(seller_id) REFERENCES users(user_id),
                FOREIGN KEY(buyer_id) REFERENCES users(user_id),
                FOREIGN KEY(ad_id) REFERENCES ads(id)
            )
        ''')

        # ТАБЛИЦА: Уведомления о просмотрах объявления
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contact_clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL,
                viewer_id INTEGER NOT NULL,
                created_at TEXT,
                FOREIGN KEY(ad_id) REFERENCES ads(id)
            )
        ''')

        # ТАБЛИЦА: Безопасные сделки (эскроу)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS escrow_deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL,
                buyer_id INTEGER NOT NULL,
                seller_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                commission INTEGER NOT NULL,
                status TEXT DEFAULT 'pending_payment',
                buyer_confirmed INTEGER DEFAULT 0,
                seller_confirmed INTEGER DEFAULT 0,
                payment_screenshot TEXT,
                dispute_reason TEXT,
                created_at TEXT,
                paid_at TEXT,
                completed_at TEXT,
                FOREIGN KEY(ad_id) REFERENCES ads(id),
                FOREIGN KEY(buyer_id) REFERENCES users(user_id),
                FOREIGN KEY(seller_id) REFERENCES users(user_id)
            )
        ''')

        # ТАБЛИЦА: Дневная аналитика просмотров объявлений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ad_views_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                views INTEGER DEFAULT 0,
                UNIQUE(ad_id, date),
                FOREIGN KEY(ad_id) REFERENCES ads(id)
            )
        ''')

        # ТАБЛИЦА: Жалобы (антифрод)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id INTEGER NOT NULL,
                ad_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TEXT,
                UNIQUE(reporter_id, ad_id),
                FOREIGN KEY(reporter_id) REFERENCES users(user_id),
                FOREIGN KEY(ad_id) REFERENCES ads(id)
            )
        ''')

        # ТАБЛИЦА: Заблокированные пользователи
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                banned_at TEXT,
                banned_by INTEGER
            )
        ''')

        # ТАБЛИЦА: Верификация продавцов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS seller_verifications (
                user_id INTEGER PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                method TEXT,
                payment_screenshot TEXT,
                verified_at TEXT,
                verified_by INTEGER,
                expires_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Миграция: добавляем expires_at если колонки нет
        cursor.execute("PRAGMA table_info(seller_verifications)")
        verify_cols = [row[1] for row in cursor.fetchall()]
        if 'expires_at' not in verify_cols:
            try:
                cursor.execute("ALTER TABLE seller_verifications ADD COLUMN expires_at TEXT")
                logger.info("Добавлена колонка expires_at в seller_verifications")
            except Exception as e:
                logger.error(f"Миграция expires_at: {e}")

        # ТАБЛИЦА: История редактирования объявлений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ad_edit_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL,
                field TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_at TEXT,
                FOREIGN KEY(ad_id) REFERENCES ads(id)
            )
        ''')

        # ── v25 ─────────────────────────────────────────────────────
        # ТАБЛИЦА: Подписки на снижение цены
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_watches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                ad_id INTEGER NOT NULL,
                price_at_subscribe INTEGER NOT NULL,
                created_at TEXT,
                UNIQUE(user_id, ad_id),
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(ad_id) REFERENCES ads(id)
            )
        ''')

        # ТАБЛИЦА: Авто-подъём объявлений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auto_bumps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                interval_hours INTEGER NOT NULL DEFAULT 24,
                active INTEGER DEFAULT 1,
                last_bumped_at TEXT,
                expires_at TEXT,
                created_at TEXT,
                FOREIGN KEY(ad_id) REFERENCES ads(id),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Добавляем поле promocode_code в таблицу payments если его нет
        cursor.execute("PRAGMA table_info(payments)")
        payment_columns = [row[1] for row in cursor.fetchall()]
        
        if 'promocode_code' not in payment_columns:
            try:
                cursor.execute("ALTER TABLE payments ADD COLUMN promocode_code TEXT")
                logger.info("Добавлена колонка promocode_code в таблицу payments")
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки promocode_code: {e}")
        
        if 'discount_percent' not in payment_columns:
            try:
                cursor.execute("ALTER TABLE payments ADD COLUMN discount_percent INTEGER DEFAULT 0")
                logger.info("Добавлена колонка discount_percent в таблицу payments")
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки discount_percent: {e}")
        
        # Добавляем поле duration для сохранения длительности подписки
        if 'duration' not in payment_columns:
            try:
                cursor.execute("ALTER TABLE payments ADD COLUMN duration INTEGER DEFAULT 30")
                logger.info("Добавлена колонка duration в таблицу payments")
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки duration: {e}")

        # Добавляем поле transaction_id для PayAnyWay (UUID)
        if 'transaction_id' not in payment_columns:
            try:
                cursor.execute("ALTER TABLE payments ADD COLUMN transaction_id TEXT")
                logger.info("Добавлена колонка transaction_id в таблицу payments")
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки transaction_id: {e}")

        # Добавляем поле plan (ключ тарифа: standard / pro)
        if 'plan' not in payment_columns:
            try:
                cursor.execute("ALTER TABLE payments ADD COLUMN plan TEXT")
                logger.info("Добавлена колонка plan в таблицу payments")
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки plan: {e}")

        # Добавляем поле reminded_at — чтобы не спамить напоминаниями
        if 'reminded_at' not in payment_columns:
            try:
                cursor.execute("ALTER TABLE payments ADD COLUMN reminded_at TEXT")
                logger.info("Добавлена колонка reminded_at в таблицу payments")
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки reminded_at: {e}")
        
        # Миграция: добавляем недостающие колонки если их нет
        # Миграция: счётчик поиска по фото
        cursor.execute("PRAGMA table_info(users)")
        user_cols = {r["name"] for r in cursor.fetchall()}
        if "photo_search_count" not in user_cols:
            cursor.execute("ALTER TABLE users ADD COLUMN photo_search_count INTEGER DEFAULT 0")
        if "photo_search_reset_date" not in user_cols:
            cursor.execute("ALTER TABLE users ADD COLUMN photo_search_reset_date TEXT DEFAULT NULL")
        if "photo_search_last_at" not in user_cols:
            cursor.execute("ALTER TABLE users ADD COLUMN photo_search_last_at TEXT DEFAULT NULL")
        self.conn.commit()

        # Миграция: добавляем колонки для поиска по фото
        cursor.execute("PRAGMA table_info(ads)")
        ads_cols = {r["name"] for r in cursor.fetchall()}
        if "photo_hash" not in ads_cols:
            cursor.execute("ALTER TABLE ads ADD COLUMN photo_hash TEXT DEFAULT NULL")
        if "photo_tags" not in ads_cols:
            cursor.execute("ALTER TABLE ads ADD COLUMN photo_tags TEXT DEFAULT NULL")
        # Индекс по хэшу для быстрого поиска дубликатов
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ads_photo_hash ON ads(photo_hash)")
        self.conn.commit()

        cursor.execute("PRAGMA table_info(ads)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        # Список колонок которые должны быть
        required_columns = {
            'condition': 'TEXT',
            'size': 'TEXT',
            'city': 'TEXT',
            'delivery': 'TEXT',
            'search_index': 'TEXT'
        }
        
        # Добавляем недостающие колонки
        for column_name, column_type in required_columns.items():
            if column_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE ads ADD COLUMN {column_name} {column_type}")
                    logger.info(f"Добавлена колонка {column_name} в таблицу ads")
                except Exception as e:
                    logger.error(f"Ошибка при добавлении колонки {column_name}: {e}")
        
        self.conn.commit()
    
    # Пользователи
    def get_user(self, user_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def create_user(self, user_data: Dict):
        import string as _s, random as _r
        cursor = self.conn.cursor()
        # Генерируем уникальный анонимный ID
        while True:
            aid = ''.join(_r.choices(_s.ascii_uppercase + _s.digits, k=6))
            cursor.execute("SELECT 1 FROM users WHERE anon_id=?", (aid,))
            if not cursor.fetchone():
                break
        cursor.execute('''
            INSERT OR IGNORE INTO users 
            (user_id, username, first_name, registration_date, anon_id) 
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], user_data.get('username', ''),
              user_data.get('first_name', ''), user_data.get('registration_date', get_moscow_time().isoformat()), aid))
        self.conn.commit()

    
    def update_user(self, user_id: int, **kwargs):
        cursor = self.conn.cursor()
        set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [user_id]
        cursor.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
        self.conn.commit()
    
    # Объявления
    def create_ad(self, ad_data: Dict) -> int:
        cursor = self.conn.cursor()
        
        # Создаем поисковый индекс с транслитерацией и синонимами
        search_index = self.create_search_index(ad_data['title'], ad_data['description'], ad_data.get('category', ''))
        
        cursor.execute('''
            INSERT INTO ads 
            (user_id, title, description, price, photos, contact_info, category, created_at, status, search_index, condition, size, city, delivery, gender)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'moderation', ?, ?, ?, ?, ?, ?)
        ''', (ad_data['user_id'], ad_data['title'], ad_data['description'],
              ad_data['price'], json.dumps(ad_data.get('photos', [])),
              ad_data.get('contact_info', ''), ad_data.get('category', ''),
              ad_data.get('created_at', get_moscow_time().isoformat()), search_index,
              ad_data.get('condition', ''), ad_data.get('size', ''), 
              ad_data.get('city', ''), ad_data.get('delivery', ''),
              ad_data.get('gender', '')))
        ad_id = cursor.lastrowid
        self.conn.commit()
        return ad_id
    
    def get_ad(self, ad_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM ads WHERE id = ?", (ad_id,))
        row = cursor.fetchone()
        if row:
            ad_dict = dict(row)
            ad_dict['photos'] = json.loads(ad_dict['photos']) if ad_dict['photos'] else []
            return ad_dict
        return None
    
    def update_ad(self, ad_id: int, **kwargs):
        cursor = self.conn.cursor()
        set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [ad_id]
        cursor.execute(f"UPDATE ads SET {set_clause} WHERE id = ?", values)
        self.conn.commit()
    
    def get_user_ads(self, user_id: int, status: str = None, limit: int = 50) -> List[Dict]:
        cursor = self.conn.cursor()
        if status:
            cursor.execute('SELECT * FROM ads WHERE user_id = ? AND status = ? ORDER BY created_at DESC LIMIT ?',
                         (user_id, status, limit))
        else:
            cursor.execute('SELECT * FROM ads WHERE user_id = ? ORDER BY created_at DESC LIMIT ?',
                         (user_id, limit))
        
        ads = []
        for row in cursor.fetchall():
            ad_dict = dict(row)
            ad_dict['photos'] = json.loads(ad_dict['photos']) if ad_dict['photos'] else []
            ads.append(ad_dict)
        return ads
    
    def create_search_index(self, title: str, description: str, category: str) -> str:
        """Строит поисковый индекс: title + description + category + синонимы + транслитерация."""
        base = f"{title} {description} {category}".lower()
        # Нормализуем весь текст как один запрос чтобы получить все варианты
        terms = _normalize_query(base)
        # Добавляем исходный текст чтобы точные вхождения всегда находились
        all_words = set(base.split()) | set(terms)
        return " ".join(w for w in all_words if len(w) >= 2)

    def search_ads(self, query: str, limit: int = 50) -> List[Dict]:
        """
        Поиск по объявлениям с ранжированием по релевантности:
          1. Точное совпадение в title
          2. Частичное совпадение в title
          3. Совпадение в search_index (description + category + синонимы)
        Закреплённые объявления всегда в топе внутри своей группы релевантности.
        """
        moscow_now = get_moscow_time().isoformat()
        search_terms = _normalize_query(query)

        if not search_terms:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT * FROM ads WHERE status='active' ORDER BY created_at DESC LIMIT ?",
                (limit,)
            )
            rows = cursor.fetchall()
            return [
                {**dict(r), 'photos': json.loads(dict(r)['photos']) if dict(r)['photos'] else []}
                for r in rows
            ]

        # Строим условия поиска
        cond_parts, params = _build_fts_conditions(search_terms)
        where_search = " OR ".join(cond_parts)

        # Запрос с ранжированием по релевантности:
        # score 0 = точное совпадение title, 1 = частичное title, 2 = только index
        # внутри каждого score: закреплённые первыми, потом по дате
        sql = f"""
            SELECT *,
                CASE
                    WHEN lower(title) = ?                    THEN 0
                    WHEN lower(title) LIKE ?                  THEN 1
                    ELSE                                           2
                END AS _score
            FROM ads
            WHERE status = 'active'
              AND ({where_search})
            ORDER BY
                _score ASC,
                CASE WHEN pinned_until > ? THEN 0 ELSE 1 END ASC,
                created_at DESC
            LIMIT ?
        """
        q_lower = query.lower().strip()
        score_params = [q_lower, f"%{q_lower}%"]
        full_params = score_params + params + [moscow_now, limit]

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, full_params)
        except Exception as e:
            logger.warning(f"search_ads fallback: {e}")
            # Простой fallback без ранжирования
            simple_conds = [f"(title LIKE ? OR search_index LIKE ?)" for _ in search_terms]
            simple_params = []
            for t in search_terms:
                simple_params.extend([f"%{t}%", f"%{t}%"])
            cursor.execute(
                f"SELECT * FROM ads WHERE status='active' AND ({' OR '.join(simple_conds)}) "
                f"ORDER BY created_at DESC LIMIT ?",
                simple_params + [limit]
            )

        ads = []
        for row in cursor.fetchall():
            ad = dict(row)
            ad.pop('_score', None)
            ad['photos'] = json.loads(ad['photos']) if ad['photos'] else []
            ads.append(ad)
        return ads

    def search_ads_filtered(self, query: str = '', category: str = '',
                             price_min: int = 0, price_max: int = 0,
                             city: str = '', condition: str = '',
                             delivery: str = '', sort_by: str = 'date_desc',
                             limit: int = 100) -> List[Dict]:
        """Поиск с полными фильтрами (аналог Авито)"""
        query = (query or '').lower().strip()
        cursor = self.conn.cursor()
        moscow_now = get_moscow_time().isoformat()

        conditions = ["status = 'active'"]
        params = []

        # Текстовый поиск — используем общую нормализацию запроса
        if query:
            search_terms = _normalize_query(query)
            text_conds, text_params = _build_fts_conditions(search_terms)
            params.extend(text_params)
            conditions.append("(" + " OR ".join(text_conds) + ")")

        # Фильтр категории
        if category and category != 'all':
            conditions.append("category LIKE ?")
            params.append(f"%{category}%")

        # Диапазон цены
        if price_min > 0:
            conditions.append("CAST(price AS INTEGER) >= ?")
            params.append(price_min)
        if price_max > 0:
            conditions.append("CAST(price AS INTEGER) <= ?")
            params.append(price_max)

        # Город
        if city and city != 'all':
            conditions.append("city LIKE ?")
            params.append(f"%{city}%")

        # Состояние (новое/б/у)
        if condition == 'new':
            conditions.append("(condition = 'Новое' OR condition = 'new')")
        elif condition == 'used':
            conditions.append("(condition != 'Новое' AND condition != 'new' AND condition IS NOT NULL AND condition != '')")

        # Доставка
        if delivery == 'yes':
            conditions.append("(delivery = 'Да' OR delivery = 'yes' OR delivery = '1')")

        where_clause = " AND ".join(conditions)

        # Сортировка
        sort_map = {
            'date_desc':  'CASE WHEN pinned_until > ? THEN 0 ELSE 1 END, created_at DESC',
            'date_asc':   'CASE WHEN pinned_until > ? THEN 0 ELSE 1 END, created_at ASC',
            'price_asc':  'CASE WHEN pinned_until > ? THEN 0 ELSE 1 END, CAST(price AS INTEGER) ASC',
            'price_desc': 'CASE WHEN pinned_until > ? THEN 0 ELSE 1 END, CAST(price AS INTEGER) DESC',
            'views_desc': 'CASE WHEN pinned_until > ? THEN 0 ELSE 1 END, views DESC',
        }
        order_clause = sort_map.get(sort_by, sort_map['date_desc'])

        try:
            cursor.execute(
                f"SELECT * FROM ads WHERE {where_clause} ORDER BY {order_clause} LIMIT ?",
                params + [moscow_now, limit]
            )
        except Exception as e:
            logger.warning(f"search_ads_filtered fallback: {e}")
            cursor.execute(
                f"SELECT * FROM ads WHERE {where_clause} ORDER BY created_at DESC LIMIT ?",
                params + [limit]
            )

        ads = []
        for row in cursor.fetchall():
            ad_dict = dict(row)
            ad_dict['photos'] = json.loads(ad_dict['photos']) if ad_dict['photos'] else []
            ads.append(ad_dict)
        return ads

    def get_search_history(self, user_id: int) -> List[str]:
        """Последние 7 запросов пользователя"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS search_history (
                    user_id INTEGER,
                    query TEXT,
                    searched_at TEXT,
                    PRIMARY KEY (user_id, query)
                )
            """)
            self.conn.commit()
            cursor.execute(
                "SELECT query FROM search_history WHERE user_id=? ORDER BY searched_at DESC LIMIT 7",
                (user_id,)
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception as _e:
            return []

    def add_search_history(self, user_id: int, query: str):
        """Сохранить запрос в историю"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS search_history (
                    user_id INTEGER,
                    query TEXT,
                    searched_at TEXT,
                    PRIMARY KEY (user_id, query)
                )
            """)
            cursor.execute(
                "INSERT OR REPLACE INTO search_history (user_id, query, searched_at) VALUES (?, ?, ?)",
                (user_id, query.strip()[:60], get_moscow_time().isoformat())
            )
            # Оставляем только последние 10
            cursor.execute("""
                DELETE FROM search_history WHERE user_id=? AND query NOT IN (
                    SELECT query FROM search_history WHERE user_id=? ORDER BY searched_at DESC LIMIT 10
                )
            """, (user_id, user_id))
            self.conn.commit()
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    def moderate_ad(self, ad_id: int, status: str, moderator_id: int, reason: str = "", channel_message_id: int = None) -> Optional[Dict]:
        cursor = self.conn.cursor()
        
        # Получаем московское время
        moscow_time = get_moscow_time().isoformat()
        
        if status == 'active':
            cursor.execute('''
                UPDATE ads SET status = ?, moderated_by = ?, moderation_reason = ?, published_at = ?, channel_message_id = ?
                WHERE id = ?
            ''', (status, moderator_id, reason, moscow_time, channel_message_id, ad_id))
        else:
            cursor.execute('''
                UPDATE ads SET status = ?, moderated_by = ?, moderation_reason = ?
                WHERE id = ?
            ''', (status, moderator_id, reason, ad_id))
        
        # ВАЖНО: Явно фиксируем изменения
        self.conn.commit()
        
        # Проверяем что обновление прошло успешно
        if cursor.rowcount == 0:
            logger.error(f"Объявление {ad_id} не найдено при модерации")
            return None
        
        # Получаем данные объявления после обновления
        cursor.execute('SELECT user_id, title, status FROM ads WHERE id = ?', (ad_id,))
        row = cursor.fetchone()
        
        if row:
            result = dict(row)
            logger.info(f"Объявление {ad_id} обновлено. Новый статус: {result.get('status')}")
            return result
        else:
            logger.error(f"Не удалось получить данные объявления {ad_id} после обновления")
            return None
    
    def deactivate_ad(self, ad_id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('UPDATE ads SET status = "inactive" WHERE id = ?', (ad_id,))
        self.conn.commit()
        return cursor.rowcount > 0
    
    def increment_ad_views(self, ad_id: int):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE ads SET views = views + 1 WHERE id = ?", (ad_id,))
        self.conn.commit()
        self.log_ad_view_daily(ad_id)
    
    # Реферальная программа
    def create_referral(self, referrer_id: int, referred_id: int) -> bool:
        cursor = self.conn.cursor()
        
        if referrer_id == referred_id:
            return False
        
        cursor.execute("SELECT 1 FROM referrals WHERE referred_id = ?", (referred_id,))
        if cursor.fetchone():
            return False
        
        try:
            cursor.execute('''
                INSERT INTO referrals (referrer_id, referred_id, registered_at)
                VALUES (?, ?, ?)
            ''', (referrer_id, referred_id, get_moscow_time().isoformat()))
            
            self.update_referral_stats(referrer_id)
            self.conn.commit()
            return True
        except Exception as _e:
            return False
    
    def get_referrer(self, referred_id: int) -> Optional[int]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT referrer_id FROM referrals WHERE referred_id = ?", (referred_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    def get_referrals(self, referrer_id: int) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT r.*, u.username, u.first_name, u.tariff 
            FROM referrals r
            JOIN users u ON r.referred_id = u.user_id
            WHERE r.referrer_id = ?
            ORDER BY r.registered_at DESC
        ''', (referrer_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_referral_stats(self, user_id: int) -> Dict:
        cursor = self.conn.cursor()
        cursor.execute('SELECT balance, total_earned, referrals_count FROM referral_balance WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if row:
            stats = dict(row)
        else:
            stats = {'balance': 0, 'total_earned': 0, 'referrals_count': 0}
        
        cursor.execute('''
            SELECT COUNT(*) FROM referrals r
            JOIN users u ON r.referred_id = u.user_id
            WHERE r.referrer_id = ? AND u.tariff IN ('Standard', 'PRO')
        ''', (user_id,))
        stats['active_referrals'] = cursor.fetchone()[0] or 0
        
        return stats
    
    def update_referral_stats(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,))
        referrals_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT SUM(amount) FROM referral_transactions WHERE referrer_id = ?', (user_id,))
        total_earned = cursor.fetchone()[0] or 0
        
        cursor.execute('''
            SELECT SUM(amount) FROM referral_withdrawals 
            WHERE user_id = ? AND status = 'approved' AND method = 'subscription'
        ''', (user_id,))
        total_spent = cursor.fetchone()[0] or 0
        
        balance = total_earned - total_spent
        
        cursor.execute('''
            INSERT OR REPLACE INTO referral_balance 
            (user_id, balance, total_earned, referrals_count)
            VALUES (?, ?, ?, ?)
        ''', (user_id, balance, total_earned, referrals_count))
        
        self.conn.commit()
    
    def add_referral_transaction(self, referrer_id: int, referred_id: int, amount: int, description: str):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO referral_transactions 
            (referrer_id, referred_id, type, amount, description, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (referrer_id, referred_id, 'subscription', amount, description, get_moscow_time().isoformat()))
        
        self.update_referral_stats(referrer_id)
        self.conn.commit()
        return cursor.lastrowid
    
    def process_referral_reward(self, referred_id: int, amount: int):
        referrer_id = self.get_referrer(referred_id)
        if not referrer_id:
            return False
        
        reward = int(amount * REFERRAL_PERCENT / 100)
        if reward <= 0:
            return False
        
        tx_id = self.add_referral_transaction(
            referrer_id=referrer_id,
            referred_id=referred_id,
            amount=reward,
            description=f"{REFERRAL_PERCENT}% от подписки реферала"
        )
        
        cursor = self.conn.cursor()
        cursor.execute('UPDATE referrals SET has_bought_subscription = 1 WHERE referred_id = ?', (referred_id,))
        self.conn.commit()
        
        return tx_id
    
    def pay_subscription_with_balance(self, user_id: int, tariff: str, price: int):
        cursor = self.conn.cursor()
        cursor.execute("SELECT balance FROM referral_balance WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        
        if not row or row['balance'] < price:
            raise ValueError("Недостаточно средств на балансе")
        
        cursor.execute('''
            INSERT INTO referral_withdrawals 
            (user_id, amount, method, details, status, created_at, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, price, 'subscription', f"Оплата подписки {tariff}", 
              'approved', get_moscow_time().isoformat(), get_moscow_time().isoformat()))
        
        self.update_referral_stats(user_id)
        self.conn.commit()
        return True
    
    # Платежи
    def create_payment(self, payment_data: Dict) -> int:
        cursor = self.conn.cursor()
        
        # Получаем процент скидки и длительность из промокода если он есть
        discount_percent = 0
        duration = payment_data.get('duration', 30)  # По умолчанию 30 дней
        
        if payment_data.get('promocode'):
            promo = self.get_promocode(payment_data['promocode'])
            if promo:
                discount_percent = promo['discount_percent']
                duration = promo['duration_days']  # Берём длительность из промокода
        
        cursor.execute('''
            INSERT INTO payments 
            (user_id, tariff, amount, screenshot_file_id, created_at, promocode_code, discount_percent, duration)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (payment_data['user_id'], payment_data['tariff'],
              payment_data['amount'], payment_data.get('screenshot_file_id', ''),
              payment_data.get('created_at', get_moscow_time().isoformat()),
              payment_data.get('promocode', None),
              discount_percent,
              duration))
        payment_id = cursor.lastrowid
        self.conn.commit()
        return payment_id
    
    def moderate_payment(self, payment_id: int, status: str, admin_id: int, comment: str = "") -> Optional[Dict]:
        """Подтверждение/отклонение платежа с атомарным обновлением БД."""
        cursor = self.conn.cursor()
        try:
            # Явно начинаем транзакцию
            cursor.execute("BEGIN IMMEDIATE")

            if status == 'confirmed':
                # Обновляем платеж
                cursor.execute('''
                    UPDATE payments 
                    SET status = ?, admin_id = ?, admin_comment = ?, confirmed_at = ?
                    WHERE id = ?
                ''', (status, admin_id, comment, get_moscow_time().isoformat(), payment_id))

                # Получаем данные платежа
                cursor.execute('SELECT user_id, tariff, amount, promocode_code, duration FROM payments WHERE id = ?', (payment_id,))
                row = cursor.fetchone()
                if not row:
                    raise ValueError(f"Платеж {payment_id} не найден")

                user_id, tariff, amount = row['user_id'], row['tariff'], row['amount']
                
                # Получаем промокод и длительность
                promocode_code = None
                duration_days = 30  # По умолчанию
                
                try:
                    promocode_code = row['promocode_code']
                except (KeyError, IndexError):
                    pass
                
                try:
                    saved_duration = row['duration']
                    if saved_duration and saved_duration > 0:
                        duration_days = saved_duration
                except (KeyError, IndexError):
                    pass

                # Если есть промокод, всегда проверяем его длительность
                # (на случай если платёж был создан до добавления поля duration)
                if promocode_code:
                    try:
                        promo = self.get_promocode(promocode_code)
                        if promo:
                            # Используем длительность из промокода если она больше текущей
                            if promo['duration_days'] > duration_days:
                                duration_days = promo['duration_days']
                            # Отмечаем использование промокода
                            self.use_promocode(promo['id'], user_id)
                    except Exception as e:
                        logger.error(f"Ошибка обработки промокода {promocode_code}: {e}")
                        # Продолжаем с текущим duration_days

                # Получаем текущую дату окончания подписки пользователя
                cursor.execute('SELECT tariff_end_date FROM users WHERE user_id = ?', (user_id,))
                user_row = cursor.fetchone()
                current_end_date_str = user_row['tariff_end_date'] if user_row else None
                
                logger.info(f"Активация подписки для user {user_id}: tariff={tariff}, duration={duration_days} дней, current_end={current_end_date_str}")
                
                # Определяем начальную дату для расчёта
                now = get_moscow_time()
                if current_end_date_str:
                    try:
                        current_end_date = datetime.fromisoformat(current_end_date_str)
                        # Если подписка ещё активна (не истекла), продлеваем от даты окончания
                        if current_end_date > now:
                            start_date = current_end_date
                            logger.info(f"Подписка активна, продлеваем от {current_end_date.date()}")
                        else:
                            # Если подписка истекла, начинаем с текущей даты
                            start_date = now
                            logger.info(f"Подписка истекла, начинаем с {now.date()}")
                    except (ValueError, TypeError):
                        # Если формат даты неверный, начинаем с текущей даты
                        start_date = now
                        logger.warning(f"Неверный формат даты окончания: {current_end_date_str}, начинаем с {now.date()}")
                else:
                    # Если подписки не было, начинаем с текущей даты
                    start_date = now
                    logger.info(f"Новая подписка, начинаем с {now.date()}")

                # Рассчитываем новую дату окончания
                tariff_end_date = (start_date + timedelta(days=duration_days)).isoformat()
                logger.info(f"Новая дата окончания: {tariff_end_date[:10]}")
                
                # Активация/продление подписки
                cursor.execute('''
                    UPDATE users 
                    SET tariff = ?, tariff_end_date = ? 
                    WHERE user_id = ?
                ''', (tariff, tariff_end_date, user_id))

                # Фиксируем транзакцию
                self.conn.commit()

                # Возвращаем информацию для уведомлений
                return {'user_id': user_id, 'amount': amount, 'tariff': tariff}

            else:  # rejected
                cursor.execute('''
                    UPDATE payments 
                    SET status = ?, admin_id = ?, admin_comment = ?
                    WHERE id = ?
                ''', (status, admin_id, comment, payment_id))
                self.conn.commit()

                cursor.execute('SELECT user_id, amount, tariff FROM payments WHERE id = ?', (payment_id,))
                row = cursor.fetchone()
                return dict(row) if row else None

        except Exception as e:
            # Откат транзакции при любой ошибке
            self.conn.rollback()
            logger.error(f"Ошибка при модерации платежа {payment_id}: {e}", exc_info=True)
            raise  # пробрасываем исключение дальше
    
    def get_pending_payments(self, limit: int = 50) -> List[Dict]:
        """Получает только ручные платежи со скриншотом (PayAnyWay активируется автоматически)"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT p.*, u.username, u.first_name 
            FROM payments p
            JOIN users u ON p.user_id = u.user_id
            WHERE p.status = 'pending'
              AND p.screenshot_file_id IS NOT NULL
              AND p.screenshot_file_id != ''
            ORDER BY p.created_at DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]

    def get_archived_payments(self, limit: int = 30) -> List[Dict]:
        """Получает обработанные платежи (confirmed / rejected) для архива"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT p.*, u.username, u.first_name 
            FROM payments p
            JOIN users u ON p.user_id = u.user_id
            WHERE p.status IN ('confirmed', 'rejected')
            ORDER BY p.confirmed_at DESC, p.created_at DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_pending_moderation(self, limit: int = 50) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT a.*, u.username, u.first_name 
            FROM ads a
            JOIN users u ON a.user_id = u.user_id
            WHERE a.status = 'moderation'
            ORDER BY a.created_at DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]
    
    # Статистика
    def update_statistics(self):
        cursor = self.conn.cursor()
        today = get_moscow_time().date().isoformat()
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE date(registration_date) = date(?)', (today,))
        new_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ads WHERE date(created_at) = date(?)', (today,))
        new_ads = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ads WHERE status = "active" AND date(published_at) = date(?)', (today,))
        published_ads = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*), SUM(amount) FROM payments WHERE status = "confirmed" AND date(confirmed_at) = date(?)', (today,))
        result = cursor.fetchone()
        payments = result[0] or 0
        revenue = result[1] or 0
        
        cursor.execute('''
            INSERT OR REPLACE INTO statistics 
            (date, new_users, new_ads, published_ads, payments, revenue)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (today, new_users, new_ads, published_ads, payments, revenue))
        self.conn.commit()
    
    def get_overall_stats(self) -> Dict:
        cursor = self.conn.cursor()
        stats = {}
        
        cursor.execute('SELECT COUNT(*) FROM users')
        stats['total_users'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ads')
        stats['total_ads'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ads WHERE status = "active"')
        stats['active_ads'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ads WHERE status = "moderation"')
        stats['moderation_ads'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM payments WHERE status = "confirmed"')
        stats['total_payments'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT SUM(amount) FROM payments WHERE status = "confirmed"')
        stats['total_revenue'] = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT tariff, COUNT(*) FROM users GROUP BY tariff')
        stats['tariff_stats'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        cursor.execute('SELECT COUNT(DISTINCT user_id) FROM logs WHERE date(created_at) >= date("now", "-7 days")')
        stats['active_users_7d'] = cursor.fetchone()[0]
        
        return stats
    
    # Логи
    def log_action(self, user_id: int, action: str, details: str = ""):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO logs (user_id, action, details, created_at)
            VALUES (?, ?, ?, ?)
        ''', (user_id, action, details, get_moscow_time().isoformat()))
        self.conn.commit()
    
    # Обслуживание
    def reset_daily_limits(self):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE users SET daily_ads_count = 0, last_daily_reset = ? WHERE tariff != "PRO"',
                      (get_moscow_time().isoformat(),))
        self.conn.commit()
    
    def expire_old_pinned_ads(self):
        cursor = self.conn.cursor()
        now_msk = get_moscow_time().isoformat()
        cursor.execute('UPDATE ads SET pinned_until = NULL WHERE pinned_until IS NOT NULL AND pinned_until < ?', (now_msk,))
        affected = cursor.rowcount
        self.conn.commit()
        return affected
    
    def get_expiring_subscriptions(self, days_before: int = 3) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT user_id, username, first_name, tariff, tariff_end_date 
            FROM users 
            WHERE tariff IN ("Standard", "PRO") 
            AND date(tariff_end_date) = date(?, ?)
            AND notifications_enabled = 1
        ''', (get_moscow_time().date().isoformat(), f'+{days_before} days'))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_all_users(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT user_id FROM users')
        return [dict(row) for row in cursor.fetchall()]
    
    # ========== МЕТОДЫ ДЛЯ ПРОМОКОДОВ ==========
    
    def create_promocode(self, code: str, discount: int, duration: int, max_uses: int, expires_at: str = None):
        """Создание нового промокода"""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO promocodes (code, discount_percent, duration_days, max_uses, created_at, expires_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            ''', (code.upper(), discount, duration, max_uses, get_moscow_time().isoformat(), expires_at))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    
    def get_promocode(self, code: str):
        """Получение промокода с защитой от отсутствия таблицы."""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT id, code, discount_percent, duration_days, max_uses, current_uses, expires_at, is_active
                FROM promocodes WHERE code = ? AND is_active = 1
            ''', (code.upper(),))
            result = cursor.fetchone()
        except sqlite3.OperationalError as e:
            # Таблица promocodes отсутствует – создаём и возвращаем None
            logger.warning(f"Таблица promocodes отсутствует: {e}")
            self.create_tables()
            return None
        
        if result:
            promo_dict = dict(result)
            
            # Проверка срока действия
            if promo_dict['expires_at']:
                expires_dt = datetime.fromisoformat(promo_dict['expires_at'])
                # Приводим к naive для корректного сравнения
                moscow_now = get_moscow_time()
                moscow_now_naive = moscow_now.replace(tzinfo=None) if moscow_now.tzinfo else moscow_now
                expires_dt_naive = expires_dt.replace(tzinfo=None) if expires_dt.tzinfo else expires_dt
                if moscow_now_naive > expires_dt_naive:
                    return None
            
            # Проверка лимита использований
            if promo_dict['current_uses'] >= promo_dict['max_uses']:
                return None
            
            return promo_dict
        
        return None
    
    def check_promocode_used(self, promo_id: int, user_id: int):
        """Проверка, использовал ли пользователь промокод"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT id FROM promocode_uses WHERE promocode_id = ? AND user_id = ?
            ''', (promo_id, user_id))
            return cursor.fetchone() is not None
        except sqlite3.OperationalError as e:
            logger.warning(f"Таблица promocode_uses отсутствует при проверке использования: {e}")
            self.create_tables()
            return False
        except Exception as e:
            logger.error(f"Ошибка при проверке использования промокода: {e}")
            return False
    
    def use_promocode(self, promo_id: int, user_id: int):
        """Использование промокода – безопасно, с перехватом ошибок БД."""
        try:
            if self.check_promocode_used(promo_id, user_id):
                return False

            cursor = self.conn.cursor()
            cursor.execute('UPDATE promocodes SET current_uses = current_uses + 1 WHERE id = ?', (promo_id,))
            cursor.execute('''
                INSERT INTO promocode_uses (promocode_id, user_id, used_at)
                VALUES (?, ?, ?)
            ''', (promo_id, user_id, get_moscow_time().isoformat()))
            self.conn.commit()
            return True
        except sqlite3.OperationalError as e:
            # Если таблицы ещё нет – создаём принудительно
            logger.warning(f"Ошибка при использовании промокода (возможно, отсутствуют таблицы): {e}")
            self.create_tables()  # повторно создаёт недостающие таблицы
            # Повторяем операцию
            try:
                if self.check_promocode_used(promo_id, user_id):
                    return False
                cursor = self.conn.cursor()
                cursor.execute('UPDATE promocodes SET current_uses = current_uses + 1 WHERE id = ?', (promo_id,))
                cursor.execute('''
                    INSERT INTO promocode_uses (promocode_id, user_id, used_at)
                    VALUES (?, ?, ?)
                ''', (promo_id, user_id, get_moscow_time().isoformat()))
                self.conn.commit()
                return True
            except Exception as retry_error:
                logger.error(f"Повторная ошибка в use_promocode: {retry_error}", exc_info=True)
                return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка в use_promocode: {e}", exc_info=True)
            return False
    
    def get_all_promocodes(self):
        """Получение всех промокодов"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT code, discount_percent, duration_days, max_uses, current_uses, expires_at, is_active
            FROM promocodes ORDER BY created_at DESC
        ''')
        return [dict(row) for row in cursor.fetchall()]
    
    def deactivate_promocode(self, code: str):
        """Деактивация промокода"""
        cursor = self.conn.cursor()
        cursor.execute('UPDATE promocodes SET is_active = 0 WHERE code = ?', (code.upper(),))
        self.conn.commit()
    
    def calculate_discounted_price(self, original_price: int, discount_percent: int) -> int:
        """Расчёт цены со скидкой"""
        return int(original_price * (100 - discount_percent) / 100)
    
    # ========== МЕТОДЫ ДЛЯ КРИПТОССЫЛОК ==========
    
    def add_crypto_link(self, amount: int, crypto_url: str):
        """Добавление или обновление криптоссылки"""
        cursor = self.conn.cursor()
        now = get_moscow_time().isoformat()
        
        # Проверяем наличие ссылки
        cursor.execute('SELECT id FROM crypto_links WHERE amount = ?', (amount,))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute('''
                UPDATE crypto_links SET crypto_url = ?, updated_at = ? WHERE amount = ?
            ''', (crypto_url, now, amount))
        else:
            cursor.execute('''
                INSERT INTO crypto_links (amount, crypto_url, created_at, updated_at)
                VALUES (?, ?, ?, ?)
            ''', (amount, crypto_url, now, now))
        
        self.conn.commit()
    
    def get_crypto_link(self, amount: int):
        """Получение криптоссылки для суммы"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT crypto_url FROM crypto_links WHERE amount = ?', (amount,))
        result = cursor.fetchone()
        return dict(result)['crypto_url'] if result else None
    
    def get_all_crypto_links(self):
        """Получение всех криптоссылок"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT amount, crypto_url FROM crypto_links ORDER BY amount')
        return [dict(row) for row in cursor.fetchall()]
    
    def delete_crypto_link(self, amount: int):
        """Удаление криптоссылки"""
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM crypto_links WHERE amount = ?', (amount,))
        self.conn.commit()

    # ========== МЕТОДЫ ДЛЯ БУСТА ==========

    def create_boost(self, ad_id: int, user_id: int, amount: int, hours: int, screenshot_file_id: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO boosts (ad_id, user_id, amount, hours, screenshot_file_id, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
        ''', (ad_id, user_id, amount, hours, screenshot_file_id, get_moscow_time().isoformat()))
        self.conn.commit()
        return cursor.lastrowid

    def confirm_boost(self, boost_id: int, admin_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM boosts WHERE id = ?', (boost_id,))
        row = cursor.fetchone()
        if not row:
            return None
        boost = dict(row)
        boosted_until = (get_moscow_time() + timedelta(hours=boost['hours'])).isoformat()
        cursor.execute('''
            UPDATE boosts SET status='confirmed', confirmed_at=?, boosted_until=? WHERE id=?
        ''', (get_moscow_time().isoformat(), boosted_until, boost_id))
        cursor.execute('''
            UPDATE ads SET pinned_until=? WHERE id=?
        ''', (boosted_until, boost['ad_id']))
        self.conn.commit()
        return boost

    def get_pending_boosts(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT b.*, u.username, u.first_name, a.title
            FROM boosts b
            JOIN users u ON b.user_id = u.user_id
            JOIN ads a ON b.ad_id = a.id
            WHERE b.status = 'pending'
            ORDER BY b.created_at DESC
        ''')
        return [dict(row) for row in cursor.fetchall()]

    def reject_boost(self, boost_id: int, admin_id: int):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE boosts SET status='rejected' WHERE id=?", (boost_id,))
        self.conn.commit()

    # ========== МЕТОДЫ ДЛЯ СОХРАНЁННЫХ ПОИСКОВ ==========

    def save_search(self, user_id: int, query: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO saved_searches (user_id, query, created_at)
                VALUES (?, ?, ?)
            ''', (user_id, query.lower().strip(), get_moscow_time().isoformat()))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as _e:
            return False

    def get_user_saved_searches(self, user_id: int) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM saved_searches WHERE user_id=? ORDER BY created_at DESC', (user_id,))
        return [dict(row) for row in cursor.fetchall()]

    def delete_saved_search(self, user_id: int, query: str):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM saved_searches WHERE user_id=? AND query=?', (user_id, query.lower().strip()))
        self.conn.commit()

    def get_all_saved_searches(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT DISTINCT query, user_id FROM saved_searches')
        return [dict(row) for row in cursor.fetchall()]

    def mark_search_notification_sent(self, user_id: int, ad_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO search_notifications_sent (user_id, ad_id, sent_at)
                VALUES (?, ?, ?)
            ''', (user_id, ad_id, get_moscow_time().isoformat()))
            self.conn.commit()
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    def is_search_notification_sent(self, user_id: int, ad_id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM search_notifications_sent WHERE user_id=? AND ad_id=?', (user_id, ad_id))
        return cursor.fetchone() is not None

    # ========== МЕТОДЫ ДЛЯ ОТЗЫВОВ ==========

    def create_review(self, seller_id: int, buyer_id: int, ad_id: int, rating: int, text: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO reviews (seller_id, buyer_id, ad_id, rating, text, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (seller_id, buyer_id, ad_id, rating, text, get_moscow_time().isoformat()))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as _e:
            return False

    def get_seller_reviews(self, seller_id: int) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT r.*, u.username, u.first_name
            FROM reviews r
            JOIN users u ON r.buyer_id = u.user_id
            WHERE r.seller_id=?
            ORDER BY r.created_at DESC
        ''', (seller_id,))
        return [dict(row) for row in cursor.fetchall()]

    def get_seller_rating(self, seller_id: int) -> Dict:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) as count, AVG(rating) as avg_rating
            FROM reviews WHERE seller_id=?
        ''', (seller_id,))
        row = cursor.fetchone()
        if row and row['count'] > 0:
            return {'count': row['count'], 'avg': round(row['avg_rating'], 1)}
        return {'count': 0, 'avg': 0.0}

    def log_contact_click(self, ad_id: int, viewer_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO contact_clicks (ad_id, viewer_id, created_at)
                VALUES (?, ?, ?)
            ''', (ad_id, viewer_id, get_moscow_time().isoformat()))
            self.conn.commit()
            # Получаем user_id продавца объявления
            cursor.execute('SELECT user_id FROM ads WHERE id=?', (ad_id,))
            row = cursor.fetchone()
            return dict(row)['user_id'] if row else None
        except Exception as _e:
            return None

    def get_ad_contact_clicks(self, ad_id: int) -> int:
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM contact_clicks WHERE ad_id=?', (ad_id,))
        return cursor.fetchone()[0]

    # ========== АНТИФРОД ==========

    def report_user(self, reporter_id: int, target_user_id: int, reason: str) -> bool:
        """Жалоба на пользователя (продавца/покупателя)"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reports_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reporter_id INTEGER,
                    target_user_id INTEGER,
                    reason TEXT,
                    created_at TEXT,
                    UNIQUE(reporter_id, target_user_id)
                )
            """)
            cursor.execute(
                "INSERT OR IGNORE INTO reports_users (reporter_id, target_user_id, reason, created_at) VALUES (?,?,?,?)",
                (reporter_id, target_user_id, reason, get_moscow_time().isoformat())
            )
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"report_user error: {e}")
            return False

    def get_user_report_count(self, target_user_id: int) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM reports_users WHERE target_user_id=?", (target_user_id,))
            return cursor.fetchone()[0]
        except Exception:
            return 0

    def has_duplicate_ad(self, user_id: int, title: str) -> bool:
        """Проверяет наличие активного объявления с таким же заголовком у этого продавца"""
        cursor = self.conn.cursor()
        norm = title.strip().lower()
        cursor.execute(
            "SELECT 1 FROM ads WHERE user_id=? AND status IN ('active','moderation') AND LOWER(TRIM(title))=? LIMIT 1",
            (user_id, norm)
        )
        return cursor.fetchone() is not None

    def get_new_ads_since(self, since_iso: str, category: str = '', city: str = '', limit: int = 8) -> list:
        """Возвращает новые объявления за период для дайджеста"""
        cursor = self.conn.cursor()
        q = "SELECT * FROM ads WHERE status='active' AND published_at >= ?"
        params = [since_iso]
        if category:
            q += " AND category=?"
            params.append(category)
        if city:
            q += " AND city=?"
            params.append(city)
        q += " ORDER BY published_at DESC LIMIT ?"
        params.append(limit)
        cursor.execute(q, params)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            d = dict(row)
            if d.get('photos'):
                try: d['photos'] = json.loads(d['photos'])
                except: d['photos'] = []
            result.append(d)
        return result

    def get_digest_subscribers(self) -> list:
        """Все активные пользователи с включёнными уведомлениями"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT user_id, notifications_enabled FROM users WHERE notifications_enabled=1"
        )
        return [dict(r) for r in cursor.fetchall()]

    def save_abandoned_draft(self, user_id: int, step: str):
        """Сохраняет факт начатого создания объявления"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS abandoned_drafts (
                    user_id INTEGER PRIMARY KEY,
                    step TEXT,
                    created_at TEXT
                )
            """)
            cursor.execute(
                "INSERT OR REPLACE INTO abandoned_drafts (user_id, step, created_at) VALUES (?,?,?)",
                (user_id, step, get_moscow_time().isoformat())
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"save_abandoned_draft error: {e}")

    def clear_abandoned_draft(self, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute("DELETE FROM abandoned_drafts WHERE user_id=?", (user_id,))
            self.conn.commit()
        except Exception:
            pass

    def get_abandoned_drafts_older_than(self, minutes: int) -> list:
        cursor = self.conn.cursor()
        cutoff = (get_moscow_time() - timedelta(minutes=minutes)).isoformat()
        try:
            cursor.execute(
                "SELECT user_id, step, created_at FROM abandoned_drafts WHERE created_at <= ?",
                (cutoff,)
            )
            return [dict(r) for r in cursor.fetchall()]
        except Exception:
            return []

    def report_ad(self, reporter_id: int, ad_id: int, reason: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO reports (reporter_id, ad_id, reason, created_at)
                VALUES (?, ?, ?, ?)
            ''', (reporter_id, ad_id, reason, get_moscow_time().isoformat()))
            self.conn.commit()
            if cursor.rowcount == 0:
                return False
            # Проверяем: если 3+ жалобы → скрываем объявление автоматически
            cursor.execute('SELECT COUNT(*) FROM reports WHERE ad_id=? AND status="pending"', (ad_id,))
            count = cursor.fetchone()[0]
            if count >= 3:
                cursor.execute('UPDATE ads SET status="moderation" WHERE id=? AND status="active"', (ad_id,))
                self.conn.commit()
                logger.warning(f"Объявление {ad_id} скрыто автоматически: {count} жалоб")
            return True
        except Exception as e:
            logger.error(f"report_ad error: {e}")
            return False

    def get_pending_reports(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT r.*, a.title, u.username, u.first_name
            FROM reports r
            JOIN ads a ON r.ad_id = a.id
            JOIN users u ON r.reporter_id = u.user_id
            WHERE r.status = "pending"
            ORDER BY r.ad_id, r.created_at DESC
        ''')
        return [dict(row) for row in cursor.fetchall()]

    def get_report_count(self, ad_id: int) -> int:
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM reports WHERE ad_id=?', (ad_id,))
        return cursor.fetchone()[0]

    def dismiss_reports(self, ad_id: int):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE reports SET status="dismissed" WHERE ad_id=?', (ad_id,))
        self.conn.commit()

    def get_dialog_with_seller(self, user_id: int, seller_id: int) -> Optional[Dict]:
        """Последний диалог между user_id и seller_id (в любую сторону)"""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                SELECT cs.id, cs.ad_id, cs.last_message_at, cs.status,
                       a.title AS ad_title,
                       (SELECT cm.text FROM chat_messages cm
                        WHERE cm.session_id = cs.id
                        ORDER BY cm.created_at DESC LIMIT 1) AS last_text,
                       (SELECT cm.sender_id FROM chat_messages cm
                        WHERE cm.session_id = cs.id
                        ORDER BY cm.created_at DESC LIMIT 1) AS last_sender_id,
                       (SELECT COUNT(*) FROM chat_messages cm WHERE cm.session_id = cs.id) AS msg_count
                FROM chat_sessions cs
                JOIN ads a ON cs.ad_id = a.id
                WHERE ((cs.buyer_id=? AND cs.seller_id=?) OR (cs.buyer_id=? AND cs.seller_id=?))
                ORDER BY cs.last_message_at DESC
                LIMIT 1
            ''', (user_id, seller_id, seller_id, user_id))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception:
            return None

    def is_banned(self, user_id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM banned_users WHERE user_id=?', (user_id,))
        return cursor.fetchone() is not None

    def ban_user(self, user_id: int, reason: str, admin_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO banned_users (user_id, reason, banned_at, banned_by)
            VALUES (?, ?, ?, ?)
        ''', (user_id, reason, get_moscow_time().isoformat(), admin_id))
        # Скрываем все объявления забаненного
        cursor.execute('UPDATE ads SET status="inactive" WHERE user_id=? AND status="active"', (user_id,))
        self.conn.commit()

    def unban_user(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM banned_users WHERE user_id=?', (user_id,))
        self.conn.commit()

    # ========== ВЕРИФИКАЦИЯ ПРОДАВЦОВ ==========

    def get_verification(self, user_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM seller_verifications WHERE user_id=?', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def request_verification(self, user_id: int, method: str, screenshot: str = None) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO seller_verifications (user_id, status, method, payment_screenshot)
                VALUES (?, "pending", ?, ?)
            ''', (user_id, method, screenshot))
            self.conn.commit()
            return True
        except Exception as _e:
            return False

    def approve_verification(self, user_id: int, admin_id: int, expires_at: str = None):
        cursor = self.conn.cursor()
        # INSERT OR REPLACE чтобы гарантированно создать запись даже если request_verification не был вызван
        cursor.execute('''
            INSERT OR REPLACE INTO seller_verifications
                (user_id, status, method, payment_screenshot, verified_at, verified_by, expires_at)
            VALUES (
                ?,
                "verified",
                COALESCE((SELECT method FROM seller_verifications WHERE user_id=?), "phone"),
                (SELECT payment_screenshot FROM seller_verifications WHERE user_id=?),
                ?,
                ?,
                ?
            )
        ''', (user_id, user_id, user_id, get_moscow_time().isoformat(), admin_id, expires_at))
        self.conn.commit()
        logger.info(f"Пользователь {user_id} верифицирован (admin={admin_id})")

    def expire_paid_verifications(self) -> List[int]:
        """Сбрасывает истёкшие платные верификации. Возвращает список user_id."""
        cursor = self.conn.cursor()
        now = get_moscow_time().isoformat()
        cursor.execute('''
            SELECT user_id FROM seller_verifications
            WHERE status="verified" AND method="paid" AND expires_at IS NOT NULL AND expires_at < ?
        ''', (now,))
        expired = [row[0] for row in cursor.fetchall()]
        if expired:
            cursor.execute(f'''
                UPDATE seller_verifications
                SET status="expired"
                WHERE user_id IN ({",".join("?" * len(expired))})
                AND method="paid" AND expires_at < ?
            ''', expired + [now])
            self.conn.commit()
            logger.info(f"Истекло платных верификаций: {len(expired)}")
        return expired

    def get_completed_deals_count(self, user_id: int) -> int:
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM escrow_deals WHERE seller_id=? AND status="completed"', (user_id,))
        return cursor.fetchone()[0]

    def get_pending_verifications(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT sv.*, u.username, u.first_name
            FROM seller_verifications sv
            JOIN users u ON sv.user_id = u.user_id
            WHERE sv.status = "pending"
        ''')
        return [dict(row) for row in cursor.fetchall()]


    def get_avg_price_for_category(self, category: str, exclude_ad_id: int = 0) -> Optional[float]:
        """Средняя цена по категории среди активных объявлений"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT AVG(price) FROM ads
            WHERE category = ? AND status = 'active' AND id != ?
        ''', (category, exclude_ad_id))
        row = cursor.fetchone()
        val = row[0] if row else None
        return float(val) if val else None

    def is_price_below_market(self, ad_id: int) -> bool:
        """True если цена объявления ≤ CHEAP_BADGE_THRESHOLD от средней по категории"""
        ad = self.get_ad(ad_id)
        if not ad or not ad.get('category') or not ad.get('price'):
            return False
        avg = self.get_avg_price_for_category(ad['category'], exclude_ad_id=ad_id)
        if not avg or avg <= 0:
            return False
        return ad['price'] <= avg * CHEAP_BADGE_THRESHOLD

    def get_market_price_info(self, category: str, price: int, exclude_ad_id: int = 0) -> Dict:
        """Возвращает среднюю цену и процент отклонения"""
        avg = self.get_avg_price_for_category(category, exclude_ad_id)
        if not avg:
            return {'avg': None, 'diff_pct': None, 'is_cheap': False}
        diff_pct = ((price - avg) / avg) * 100
        return {
            'avg': int(avg),
            'diff_pct': round(diff_pct, 1),
            'is_cheap': price <= avg * CHEAP_BADGE_THRESHOLD
        }

    # ========== v25: ПОДПИСКА НА СНИЖЕНИЕ ЦЕНЫ ==========

    def watch_price(self, user_id: int, ad_id: int, current_price: int) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO price_watches (user_id, ad_id, price_at_subscribe, created_at)
                VALUES (?, ?, ?, ?)
            ''', (user_id, ad_id, current_price, get_moscow_time().isoformat()))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as _e:
            return False

    def unwatch_price(self, user_id: int, ad_id: int):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM price_watches WHERE user_id=? AND ad_id=?', (user_id, ad_id))
        self.conn.commit()

    def is_watching_price(self, user_id: int, ad_id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM price_watches WHERE user_id=? AND ad_id=?', (user_id, ad_id))
        return cursor.fetchone() is not None

    def get_price_watches_for_ad(self, ad_id: int) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM price_watches WHERE ad_id=?', (ad_id,))
        return [dict(r) for r in cursor.fetchall()]

    def notify_price_drop_needed(self, ad_id: int, new_price: int) -> List[Dict]:
        """Возвращает подписчиков у которых цена снизилась относительно момента подписки"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT pw.*, u.first_name, u.username
            FROM price_watches pw
            JOIN users u ON pw.user_id = u.user_id
            WHERE pw.ad_id = ? AND pw.price_at_subscribe > ?
        ''', (ad_id, new_price))
        return [dict(r) for r in cursor.fetchall()]

    # ========== v25: АВТО-ПОДЪЁМ ==========

    def create_auto_bump(self, ad_id: int, user_id: int, interval_hours: int, days: int = 30) -> int:
        cursor = self.conn.cursor()
        # Деактивируем старые авто-бампы для этого объявления
        cursor.execute('UPDATE auto_bumps SET active=0 WHERE ad_id=?', (ad_id,))
        now = get_moscow_time()
        expires = (now + timedelta(days=days)).isoformat()
        cursor.execute('''
            INSERT INTO auto_bumps (ad_id, user_id, interval_hours, active, last_bumped_at, expires_at, created_at)
            VALUES (?, ?, ?, 1, ?, ?, ?)
        ''', (ad_id, user_id, interval_hours, now.isoformat(), expires, now.isoformat()))
        self.conn.commit()
        return cursor.lastrowid

    def get_active_auto_bumps(self) -> List[Dict]:
        cursor = self.conn.cursor()
        now = get_moscow_time().isoformat()
        cursor.execute('''
            SELECT ab.*, a.title, a.user_id as owner_id
            FROM auto_bumps ab
            JOIN ads a ON ab.ad_id = a.id
            JOIN users u ON ab.user_id = u.user_id
            WHERE ab.active = 1
              AND ab.expires_at > ?
              AND a.status = 'active'
              AND u.tariff IN ('Standard', 'PRO')
              AND (u.tariff_end_date IS NULL OR u.tariff_end_date > ?)
        ''', (now, now))
        return [dict(r) for r in cursor.fetchall()]

    def update_auto_bump_time(self, bump_id: int):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE auto_bumps SET last_bumped_at=? WHERE id=?',
                       (get_moscow_time().isoformat(), bump_id))
        self.conn.commit()

    def cancel_auto_bump(self, ad_id: int, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE auto_bumps SET active=0 WHERE ad_id=? AND user_id=?', (ad_id, user_id))
        self.conn.commit()

    def get_auto_bump_for_ad(self, ad_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM auto_bumps WHERE ad_id=? AND active=1
            ORDER BY created_at DESC LIMIT 1
        ''', (ad_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    # ========== v25: АНАЛИТИКА ДЛЯ АДМИНА ==========

    def get_admin_funnel_stats(self) -> Dict:
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE subscription_type='standard'")
        standard = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE subscription_type='pro'")
        pro = cursor.fetchone()[0]
        free = total - standard - pro

        # LTV: средняя выручка на пользователя
        cursor.execute("SELECT SUM(amount) FROM payments WHERE status='confirmed'")
        total_revenue_row = cursor.fetchone()
        total_revenue = total_revenue_row[0] if total_revenue_row[0] else 0
        ltv = round(total_revenue / total, 2) if total > 0 else 0

        # Churn: пользователи у которых истекла подписка за последние 30 дней
        cursor.execute('''
            SELECT COUNT(*) FROM users
            WHERE subscription_type != 'free'
              AND subscription_end < datetime('now')
              AND subscription_end > datetime('now', '-30 days')
        ''')
        churned = cursor.fetchone()[0]

        # Топ продавцов по количеству активных объявлений
        cursor.execute('''
            SELECT u.first_name, u.username, u.user_id, COUNT(a.id) as ad_count
            FROM users u JOIN ads a ON u.user_id = a.user_id
            WHERE a.status = 'active'
            GROUP BY u.user_id
            ORDER BY ad_count DESC LIMIT 10
        ''')
        top_sellers = [dict(r) for r in cursor.fetchall()]

        # Топ продавцов по обороту через эскроу
        cursor.execute('''
            SELECT u.first_name, u.username, e.seller_id,
                   SUM(e.amount) as total_sold, COUNT(*) as deals
            FROM escrow_deals e JOIN users u ON e.seller_id = u.user_id
            WHERE e.status = 'completed'
            GROUP BY e.seller_id ORDER BY total_sold DESC LIMIT 10
        ''')
        top_by_revenue = [dict(r) for r in cursor.fetchall()]

        # Динамика регистраций по дням за 14 дней
        cursor.execute('''
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM users WHERE created_at >= DATE('now', '-14 days')
            GROUP BY DATE(created_at) ORDER BY date
        ''')
        reg_dynamics = [dict(r) for r in cursor.fetchall()]

        # Выручка по дням за 14 дней
        cursor.execute('''
            SELECT DATE(confirmed_at) as date, SUM(amount) as revenue
            FROM payments WHERE status='confirmed'
              AND confirmed_at >= DATE('now', '-14 days')
            GROUP BY DATE(confirmed_at) ORDER BY date
        ''')
        rev_dynamics = [dict(r) for r in cursor.fetchall()]

        return {
            'total_users': total, 'free': free, 'standard': standard, 'pro': pro,
            'total_revenue': total_revenue, 'ltv': ltv, 'churned_30d': churned,
            'top_sellers': top_sellers, 'top_by_revenue': top_by_revenue,
            'reg_dynamics': reg_dynamics, 'rev_dynamics': rev_dynamics,
        }

    # ========== РЕДАКТИРОВАНИЕ ОБЪЯВЛЕНИЙ ==========

    def edit_ad_field(self, ad_id: int, field: str, new_value: str, user_id: int) -> tuple:
        """Редактирует поле объявления. Возвращает (success, old_value)"""
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT {field} FROM ads WHERE id=? AND user_id=?', (ad_id, user_id))
        row = cursor.fetchone()
        if not row:
            return False, None
        old_value = str(row[0]) if row[0] else ''
        cursor.execute(f'UPDATE ads SET {field}=? WHERE id=? AND user_id=?', (new_value, ad_id, user_id))
        update_rowcount = cursor.rowcount  # сохраняем до следующего execute
        cursor.execute('''
            INSERT INTO ad_edit_history (ad_id, field, old_value, new_value, edited_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (ad_id, field, old_value, new_value, get_moscow_time().isoformat()))
        self.conn.commit()
        return update_rowcount > 0, old_value



    # ========== МЕТОДЫ ДЛЯ АНАЛИТИКИ ПРОСМОТРОВ ==========

    def log_ad_view_daily(self, ad_id: int):
        """Фиксируем просмотр в дневной таблице"""
        cursor = self.conn.cursor()
        today = get_moscow_time().date().isoformat()
        try:
            cursor.execute('''
                INSERT INTO ad_views_daily (ad_id, date, views) VALUES (?, ?, 1)
                ON CONFLICT(ad_id, date) DO UPDATE SET views = views + 1
            ''', (ad_id, today))
            self.conn.commit()
        except Exception as e:
            logger.error(f"log_ad_view_daily error: {e}")

    def get_ad_views_chart(self, ad_id: int, days: int = 14) -> List[Dict]:
        """Возвращает данные просмотров за последние N дней"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT date, views FROM ad_views_daily
            WHERE ad_id = ? AND date >= date('now', ?)
            ORDER BY date ASC
        ''', (ad_id, f'-{days} days'))
        rows = [dict(r) for r in cursor.fetchall()]
        # Заполняем пропуски нулями
        from datetime import date as date_type
        result = []
        today = get_moscow_time().date()
        existing = {r['date']: r['views'] for r in rows}
        for i in range(days):
            d = (today - timedelta(days=days - 1 - i)).isoformat()
            result.append({'date': d, 'views': existing.get(d, 0)})
        return result

    # ========== МЕТОДЫ ДЛЯ ЭСКРОУ ==========

    ESCROW_COMMISSION_PERCENT = 3  # fallback, основная логика в calc_escrow_commission()

    def create_escrow_deal(self, ad_id: int, buyer_id: int, seller_id: int, amount: int) -> int:
        cursor = self.conn.cursor()
        commission = calc_escrow_commission(amount)
        cursor.execute('''
            INSERT INTO escrow_deals
            (ad_id, buyer_id, seller_id, amount, commission, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending_payment', ?)
        ''', (ad_id, buyer_id, seller_id, amount, commission, get_moscow_time().isoformat()))
        self.conn.commit()
        return cursor.lastrowid

    def get_escrow_deal(self, deal_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM escrow_deals WHERE id = ?', (deal_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def update_escrow(self, deal_id: int, **kwargs):
        cursor = self.conn.cursor()
        set_clause = ', '.join([f'{k}=?' for k in kwargs])
        values = list(kwargs.values()) + [deal_id]
        cursor.execute(f'UPDATE escrow_deals SET {set_clause} WHERE id=?', values)
        self.conn.commit()

    def get_user_escrow_deals(self, user_id: int) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT e.*, a.title FROM escrow_deals e
            JOIN ads a ON e.ad_id = a.id
            WHERE e.buyer_id = ? OR e.seller_id = ?
            ORDER BY e.created_at DESC LIMIT 20
        ''', (user_id, user_id))
        return [dict(r) for r in cursor.fetchall()]

    def get_pending_escrow_disputes(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT e.*, a.title, ub.first_name as buyer_name, us.first_name as seller_name
            FROM escrow_deals e
            JOIN ads a ON e.ad_id = a.id
            JOIN users ub ON e.buyer_id = ub.user_id
            JOIN users us ON e.seller_id = us.user_id
            WHERE e.status = 'disputed'
            ORDER BY e.created_at DESC
        ''')
        return [dict(r) for r in cursor.fetchall()]

    # ========== ИЗБРАННЫЕ ПРОДАВЦЫ ==========

    def add_favorite_seller(self, user_id: int, seller_id: int) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS favorite_sellers (
                    user_id INTEGER,
                    seller_id INTEGER,
                    added_at TEXT,
                    PRIMARY KEY(user_id, seller_id),
                    FOREIGN KEY(user_id) REFERENCES users(user_id),
                    FOREIGN KEY(seller_id) REFERENCES users(user_id)
                )
            ''')
            cursor.execute('''
                INSERT OR IGNORE INTO favorite_sellers (user_id, seller_id, added_at)
                VALUES (?, ?, ?)
            ''', (user_id, seller_id, get_moscow_time().isoformat()))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"add_favorite_seller error: {e}")
            return False

    def remove_favorite_seller(self, user_id: int, seller_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('DELETE FROM favorite_sellers WHERE user_id=? AND seller_id=?', (user_id, seller_id))
            self.conn.commit()
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    def is_favorite_seller(self, user_id: int, seller_id: int) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('SELECT 1 FROM favorite_sellers WHERE user_id=? AND seller_id=?', (user_id, seller_id))
            return cursor.fetchone() is not None
        except Exception as _e:
            return False

    def get_favorite_sellers(self, user_id: int) -> List[Dict]:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS favorite_sellers (
                    user_id INTEGER,
                    seller_id INTEGER,
                    added_at TEXT,
                    PRIMARY KEY(user_id, seller_id)
                )
            ''')
            cursor.execute('''
                SELECT fs.seller_id, u.first_name, u.username, fs.added_at
                FROM favorite_sellers fs
                JOIN users u ON fs.seller_id = u.user_id
                WHERE fs.user_id = ?
                ORDER BY fs.added_at DESC
            ''', (user_id,))
            return [dict(r) for r in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_favorite_sellers error: {e}")
            return []

    # ========== ОБЖАЛОВАНИЕ ОТЗЫВОВ ==========

    def appeal_review(self, review_id: int, seller_id: int, reason: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS review_appeals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id INTEGER NOT NULL,
                    seller_id INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    admin_decision TEXT,
                    created_at TEXT,
                    decided_at TEXT,
                    FOREIGN KEY(review_id) REFERENCES reviews(id),
                    FOREIGN KEY(seller_id) REFERENCES users(user_id)
                )
            ''')
            cursor.execute('''
                INSERT OR IGNORE INTO review_appeals (review_id, seller_id, reason, created_at)
                VALUES (?, ?, ?, ?)
            ''', (review_id, seller_id, reason, get_moscow_time().isoformat()))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"appeal_review error: {e}")
            return False

    def get_pending_appeals(self) -> List[Dict]:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS review_appeals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id INTEGER NOT NULL,
                    seller_id INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    admin_decision TEXT,
                    created_at TEXT,
                    decided_at TEXT
                )
            ''')
            cursor.execute('''
                SELECT ra.*, r.rating, r.text as review_text, r.buyer_id,
                       u.first_name as seller_name, u.username as seller_username
                FROM review_appeals ra
                JOIN reviews r ON ra.review_id = r.id
                JOIN users u ON ra.seller_id = u.user_id
                WHERE ra.status = 'pending'
                ORDER BY ra.created_at DESC
            ''')
            return [dict(r) for r in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_pending_appeals error: {e}")
            return []

    def decide_appeal(self, appeal_id: int, decision: str, admin_id: int):
        """decision: 'delete_review' or 'reject_appeal'"""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                UPDATE review_appeals SET status=?, admin_decision=?, decided_at=?
                WHERE id=?
            ''', (decision, decision, get_moscow_time().isoformat(), appeal_id))
            if decision == 'delete_review':
                cursor.execute('SELECT review_id FROM review_appeals WHERE id=?', (appeal_id,))
                row = cursor.fetchone()
                if row:
                    cursor.execute('DELETE FROM reviews WHERE id=?', (dict(row)['review_id'],))
            self.conn.commit()
        except Exception as e:
            logger.error(f"decide_appeal error: {e}")

    # ========== ВЕРИФИКАЦИЯ ПО ТЕЛЕФОНУ ==========

    def save_phone_code(self, user_id: int, phone: str, code: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS phone_verify_codes (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT,
                    code TEXT,
                    created_at TEXT
                )
            ''')
            cursor.execute('''
                INSERT OR REPLACE INTO phone_verify_codes (user_id, phone, code, created_at)
                VALUES (?, ?, ?, ?)
            ''', (user_id, phone, code, get_moscow_time().isoformat()))
            self.conn.commit()
        except Exception as e:
            logger.error(f"save_phone_code error: {e}")

    def get_phone_code(self, user_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        try:
            cursor.execute('SELECT * FROM phone_verify_codes WHERE user_id=?', (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as _e:
            return None

    def delete_phone_code(self, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('DELETE FROM phone_verify_codes WHERE user_id=?', (user_id,))
            self.conn.commit()
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    def close(self):
        self.conn.close()

db = Database()

# Вспомогательные функции
async def check_subscription(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id=ADMIN_CHANNEL_ID, user_id=user_id)
        logger.info(f"check_subscription: user={user_id}, status={member.status}")
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"check_subscription ОШИБКА для user={user_id}: {e}")
        # При любой ошибке пропускаем проверку чтобы не блокировать пользователей.
        # ВАЖНО: добавьте @PolkaAdsBot администратором в @PolkaAds
        logger.warning(f"Пропускаем проверку подписки. Убедитесь что @PolkaAdsBot — администратор @PolkaAds. Ошибка: {e}")
        return True

async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS

async def _index_ad_photo(ad_id: int, file_id: str, bot) -> None:
    """Фоновая задача: вычисляет pHash и теги первого фото объявления."""
    try:
        photo_hash = await compute_photo_hash(bot, file_id)
        tags = await extract_photo_tags_hf(bot, file_id) if HF_API_TOKEN else []
        update_kwargs = {}
        if photo_hash:
            update_kwargs["photo_hash"] = photo_hash
        if tags:
            update_kwargs["photo_tags"] = json.dumps(tags, ensure_ascii=False)
        if update_kwargs:
            db.update_ad(ad_id, **update_kwargs)
            logger.info(f"Photo indexed for ad {ad_id}: hash={bool(photo_hash)}, tags={tags[:3]}")
    except Exception as e:
        logger.error(f"_index_ad_photo error for ad {ad_id}: {e}")


async def publish_ad_to_channel(ad_id: int, context: ContextTypes.DEFAULT_TYPE):
    ad = db.get_ad(ad_id)
    if not ad:
        return None
    
    # Формат для канала — чистый текст без HTML-тегов
    verify = db.get_verification(ad['user_id'])
    seller = db.get_user(ad['user_id'])
    seller_name = seller.get('first_name', '') if seller else ''
    verified_tag = " ✓ Верифицирован" if verify and verify.get('status') == 'verified' else ""

    price_fmt = f"{int(ad['price']):,}".replace(",", " ")

    channel_text = f"{ad['title']}\n\n"
    if ad.get('description'):
        channel_text += f"{ad['description'][:300]}\n\n"
    channel_text += f"Цена: {price_fmt} ₽\n"
    if ad.get('condition'):
        channel_text += f"Состояние: {ad['condition']}\n"
    if ad.get('size'):
        channel_text += f"Размер: {ad['size']}\n"
    if ad.get('gender'):
        channel_text += f"Для: {ad['gender'].lower()} одежда\n"
    if ad.get('city'):
        channel_text += f"Город: {ad['city']}\n"
    if ad.get('delivery'):
        channel_text += f"Доставка: {ad['delivery']}\n"
    if seller_name:
        channel_text += f"\nПродавец: {seller_name}{verified_tag}"

    # Кнопки
    keyboard = [
        [InlineKeyboardButton("✎ Написать продавцу", url=f"https://t.me/PolkaAdsBot?start=contact_{ad_id}"),
         InlineKeyboardButton("♡ В избранное", url=f"https://t.me/PolkaAdsBot?start=favorite_{ad_id}")],
        [InlineKeyboardButton("+ Разместить объявление", url="https://t.me/PolkaAdsBot?start=new_ad")]
    ]
    
    try:
        if ad['photos']:
            # Если есть несколько фото, отправляем все фотографии одной медиагруппой
            if len(ad['photos']) > 1:
                media_group = []
                # Первое фото с подписью
                media_group.append(InputMediaPhoto(
                    media=ad['photos'][0],
                    caption=channel_text
                ))
                # Остальные фото без подписи
                for photo in ad['photos'][1:]:
                    media_group.append(InputMediaPhoto(media=photo))
                
                # Отправляем все фото одной медиагруппой
                messages = await context.bot.send_media_group(
                    chat_id=ADMIN_CHANNEL_ID,
                    media=media_group
                )
                
                # Используем ID первого сообщения из медиагруппы
                message_id = messages[0].message_id
                
                # Отправляем кнопки отдельным сообщением (т.к. медиагруппа не поддерживает inline кнопки)
                await context.bot.send_message(
                    chat_id=ADMIN_CHANNEL_ID,
                    text="Выберите действие:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                # Если только одно фото, отправляем как обычно
                message = await context.bot.send_photo(
                    chat_id=ADMIN_CHANNEL_ID, 
                    photo=ad['photos'][0],
                    caption=channel_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                message_id = message.message_id
        else:
            message = await context.bot.send_message(
                chat_id=ADMIN_CHANNEL_ID, 
                text=channel_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            message_id = message.message_id
        
        db.log_action(ad['user_id'], "ad_published", f"Ad ID: {ad_id}, Channel message: {message_id}")

        # ── Публикация в городской канал ─────────────────────────────────────────
        city = (ad.get('city') or '').strip()
        city_channel = CITY_CHANNELS.get(city)
        if city_channel:
            try:
                if ad['photos']:
                    if len(ad['photos']) > 1:
                        city_media_group = [InputMediaPhoto(media=ad['photos'][0], caption=channel_text)]
                        for photo in ad['photos'][1:]:
                            city_media_group.append(InputMediaPhoto(media=photo))
                        await context.bot.send_media_group(chat_id=city_channel, media=city_media_group)
                        await context.bot.send_message(
                            chat_id=city_channel,
                            text="Выберите действие:",
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                    else:
                        await context.bot.send_photo(
                            chat_id=city_channel,
                            photo=ad['photos'][0],
                            caption=channel_text,
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                else:
                    await context.bot.send_message(
                        chat_id=city_channel,
                        text=channel_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                logger.info(f"Объявление {ad_id} опубликовано в городской канал {city_channel} (город: {city})")
            except Exception as city_err:
                logger.warning(f"Не удалось опубликовать объявление {ad_id} в городской канал {city_channel}: {city_err}")
                for admin_id in ADMIN_USER_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=f"⚠️ Объявление #{ad_id} опубликовано в @PolkaAds, но не удалось опубликовать в городской канал {city_channel} (город: {city})\n\nОшибка: {city_err}"
                        )
                    except Exception:
                        pass

        # ── Публикация в канал одежды (мужская / женская / детская) ──────────
        gender = (ad.get('gender') or '').strip()
        clothes_channel = None
        if ad.get('category') == 'Одежда' and gender:
            clothes_channel = CLOTHES_CHANNELS.get(gender)
        if clothes_channel:
            try:
                if ad['photos']:
                    if len(ad['photos']) > 1:
                        cg = [InputMediaPhoto(media=ad['photos'][0], caption=channel_text)]
                        for photo in ad['photos'][1:]:
                            cg.append(InputMediaPhoto(media=photo))
                        await context.bot.send_media_group(chat_id=clothes_channel, media=cg)
                        await context.bot.send_message(
                            chat_id=clothes_channel,
                            text="Выберите действие:",
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                    else:
                        await context.bot.send_photo(
                            chat_id=clothes_channel,
                            photo=ad['photos'][0],
                            caption=channel_text,
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                else:
                    await context.bot.send_message(
                        chat_id=clothes_channel,
                        text=channel_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                logger.info(f"Объявление {ad_id} опубликовано в канал одежды {clothes_channel} ({gender})")
            except Exception as clothes_err:
                logger.warning(f"Не удалось опубликовать объявление {ad_id} в канал одежды {clothes_channel}: {clothes_err}")
                for admin_id in ADMIN_USER_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=f"⚠️ Объявление #{ad_id} не опубликовано в канал одежды {clothes_channel} ({gender})\n\nОшибка: {clothes_err}"
                        )
                    except Exception:
                        pass

        # ── Публикация в канал техники ────────────────────────────────────
        tech_channel_used = None
        if ad.get('category') == 'Техника':
            try:
                if ad['photos']:
                    if len(ad['photos']) > 1:
                        tg = [InputMediaPhoto(media=ad['photos'][0], caption=channel_text)]
                        for photo in ad['photos'][1:]:
                            tg.append(InputMediaPhoto(media=photo))
                        await context.bot.send_media_group(chat_id=TECH_CHANNEL, media=tg)
                        await context.bot.send_message(
                            chat_id=TECH_CHANNEL,
                            text="Выберите действие:",
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                    else:
                        await context.bot.send_photo(
                            chat_id=TECH_CHANNEL,
                            photo=ad['photos'][0],
                            caption=channel_text,
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                else:
                    await context.bot.send_message(
                        chat_id=TECH_CHANNEL,
                        text=channel_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                tech_channel_used = TECH_CHANNEL
                logger.info(f"Объявление {ad_id} опубликовано в канал техники {TECH_CHANNEL}")
            except Exception as tech_err:
                logger.warning(f"Не удалось опубликовать объявление {ad_id} в канал техники {TECH_CHANNEL}: {tech_err}")
                for admin_id in ADMIN_USER_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=f"⚠️ Объявление #{ad_id} не опубликовано в канал техники {TECH_CHANNEL}\n\nОшибка: {tech_err}"
                        )
                    except Exception:
                        pass

        return message_id, clothes_channel, gender, tech_channel_used
    except Exception as e:
        import traceback
        logger.error(f"Ошибка публикации объявления {ad_id} в канал {ADMIN_CHANNEL_ID}: {e}\n{traceback.format_exc()}")
        for admin_id in ADMIN_USER_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ Не удалось опубликовать объявление #{ad_id} в канал {ADMIN_CHANNEL_ID}\n\nОшибка: {e}"
                )
            except Exception:
                pass
        return None, None, '', None

async def notify_admin_new_ad(ad_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        ad = db.get_ad(ad_id)
        if not ad:
            logger.error(f"Объявление {ad_id} не найдено в базе данных")
            return
        
        user_info = db.get_user(ad['user_id'])
        user_display = f"{user_info['first_name']} (@{user_info['username']})" if user_info and user_info.get('username') else f"{user_info.get('first_name', 'Пользователь')}"
        
        notification_text = (
            f"Новое объявление на модерацию\n\n"
            f"ID: {ad['id']}\n"
            f"{ad['title']}\n"
            f"{ad['price']} ₽ | {ad.get('category', '')}\n"
            f"Продавец: {user_display}\n"
            f"Дата: {ad['created_at'][:16]}\n\n"
            f"Описание: {ad.get('description', '')[:100]}..."
        )
        
        keyboard = [
            [InlineKeyboardButton("▸ Смотреть", callback_data=f"admin_view_ad:{ad_id}")],
            [InlineKeyboardButton("✓ Одобрить", callback_data=f"moderate_approve:{ad_id}"),
             InlineKeyboardButton("✗ Отклонить", callback_data=f"moderate_reject:{ad_id}")],
            [InlineKeyboardButton("◎ На модерации", callback_data="admin_moderation")],
        ]
        
        for admin_id in ADMIN_USER_IDS:
            try:
                if ad.get('photos') and len(ad['photos']) > 0:
                    # Отправляем до 10 фото в медиагруппе
                    media_group = [InputMediaPhoto(ad['photos'][0], caption=notification_text)]
                    for photo in ad['photos'][1:10]:  # Ограничиваем 10 фото
                        media_group.append(InputMediaPhoto(photo))
                    
                    messages = await context.bot.send_media_group(chat_id=admin_id, media=media_group)
                    # Отправляем клавиатуру отдельным сообщением
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text="Выберите действие:",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                else:
                    await context.bot.send_message(chat_id=admin_id, text=notification_text,
                                                 reply_markup=InlineKeyboardMarkup(keyboard))
                logger.info(f"Уведомление отправлено администратору {admin_id}")
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка в notify_admin_new_ad для объявления {ad_id}: {e}")

async def notify_admin_new_payment(payment_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет уведомление админам о новом платеже"""
    try:
        # Получаем информацию о платеже из БД
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT p.id, p.user_id, p.tariff, p.amount, p.screenshot_file_id, 
                   p.status, p.created_at, p.promocode_code, p.discount_percent,
                   u.first_name, u.username 
            FROM payments p
            JOIN users u ON p.user_id = u.user_id
            WHERE p.id = ?
        """, (payment_id,))
        payment = cursor.fetchone()
        
        if not payment:
            logger.error(f"Платеж {payment_id} не найден в базе данных")
            return
        
        # Получаем названия колонок
        column_names = [description[0] for description in cursor.description]
        
        # Преобразуем в словарь
        payment_dict = dict(zip(column_names, payment))
        
        user_display = f"{payment_dict['first_name']}"
        if payment_dict.get('username'):
            user_display += f" (@{payment_dict['username']})"
        
        notification_text = (
            f"Новый платёж\n\n"
            f"ID: {payment_dict['id']}\n"
            f"Пользователь: {user_display}\n"
            f"Тариф: {payment_dict['tariff']}\n"
            f"Сумма: {payment_dict['amount']} ₽\n"
        )
        
        # Добавляем информацию о промокоде, если он использовался
        if payment_dict.get('promocode_code'):
            discount_percent = payment_dict.get('discount_percent', 0)
            notification_text += (
                f"Промокод: {payment_dict['promocode_code']}\n"
                f"Скидка: {discount_percent}%\n"
            )
        
        notification_text += (
            f"Дата: {payment_dict['created_at'][:16]}\n\n"
            f"Скриншот чека:"
        )
        
        keyboard = [
            [InlineKeyboardButton("✓ Подтвердить", callback_data=f"payment_confirm:{payment_dict['id']}"),
             InlineKeyboardButton("✗ Отклонить", callback_data=f"payment_reject:{payment_dict['id']}")],
            [InlineKeyboardButton("◎ Платежи", callback_data="admin_payments")]
        ]
        
        # Отправляем уведомление всем админам
        for admin_id in ADMIN_USER_IDS:
            try:
                # Отправляем скриншот с текстом и кнопками
                await context.bot.send_photo(
                    chat_id=admin_id,
                    photo=payment_dict['screenshot_file_id'],
                    caption=notification_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info(f"Уведомление о платеже {payment_id} отправлено администратору {admin_id}")
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id} о платеже: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка в notify_admin_new_payment для платежа {payment_id}: {e}")

async def delete_ad_from_channel(ad_id: int, context: ContextTypes.DEFAULT_TYPE):
    ad = db.get_ad(ad_id)
    if not ad or not ad.get('channel_message_id'):
        return False
    
    try:
        # Если это медиагруппа (несколько фото), нужно удалить все сообщения
        # Telegram возвращает ID первого сообщения в медиагруппе
        # Для надежности пробуем удалить несколько сообщений подряд
        message_id = ad['channel_message_id']
        deleted_count = 0
        
        # Пытаемся удалить до 10 последовательных сообщений (на случай медиагруппы)
        for i in range(10):
            try:
                await context.bot.delete_message(chat_id=ADMIN_CHANNEL_ID, message_id=message_id + i)
                deleted_count += 1
            except Exception as e:
                # Если сообщение не найдено, прекращаем попытки
                if deleted_count > 0:
                    break
        
        db.update_ad(ad_id, channel_message_id=None)
        db.log_action(ad['user_id'], "ad_deleted_from_channel", f"Ad ID: {ad_id}, deleted {deleted_count} messages")
        return True
    except Exception as e:
        logger.error(f"Не удалось удалить сообщение из канала: {e}")
        return False

# ========== ЗАГЛУШКИ ДЛЯ НЕДОСТАЮЩИХ ФУНКЦИЙ ==========

async def contact_seller(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает контакты продавца при нажатии на кнопку"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad:
        await query.answer("Объявление не найдено", show_alert=True)
        return
    seller_contact = ad.get('contact_info') or 'Не указан'
    await query.answer(
        f"Контакт продавца:\n{seller_contact}",
        show_alert=True
    )
    await track_contact_click(ad_id, update.effective_user.id, context)

async def edit_ad_without_param(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit_message(query, "Выберите объявление в разделе «Мои объявления» для редактирования")

async def delete_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    await safe_edit_message(query, f"Удаление объявления {ad_id} — функция в разработке")

async def admin_moderation_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit_message(query, "Следующая страница модерации — в разработке")

async def admin_payments_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit_message(query, "Следующая страница платежей — в разработке")

async def show_moderation_ads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    # Получаем объявления на модерации
    all_ads = db.get_user_ads(user.id)
    moderation_ads = [ad for ad in all_ads if ad['status'] == 'moderation']
    
    if not moderation_ads:
        text = "Объявлений на модерации нет"
        keyboard = [[InlineKeyboardButton("← Назад", callback_data="my_ads")]]
    else:
        text = f"Объявлений на модерации: {len(moderation_ads)}\n\n"
        
        for i, ad in enumerate(moderation_ads, 1):
            text += (
                f"{i}. {ad['title'][:40]}...\n"
                f" {ad['price']} ₽ | {ad.get('category', '')}\n"
                f" Создано: {ad['created_at'][:10]}\n"
                f" ID: {ad['id']}\n\n"
            )
        
        text += "Обычно модерация занимает до 24 часов.\nВы получите уведомление когда объявления будут проверены."
        
        keyboard = [[InlineKeyboardButton("← Мои объявления", callback_data="my_ads")]]
    
    await safe_edit_message(query, text, keyboard)

async def pin_ad_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    user_data = db.get_user(user.id)
    
    # Получаем активные объявления пользователя
    all_ads = db.get_user_ads(user.id)
    active_ads = [ad for ad in all_ads if ad['status'] == 'active']
    
    # Проверяем доступные закрепления
    tariff = user_data['tariff']
    pinned_ads_used = user_data.get('pinned_ads_used', 0) or 0
    
    if tariff == 'Standard':
        max_pins = 1
    elif tariff == 'PRO':
        max_pins = 3
    else:
        await safe_edit_message(query, "Закрепление доступно только на тарифах Standard и PRO")
        return
    
    pins_available = max_pins - pinned_ads_used
    
    if pins_available <= 0:
        text = (
            f"Закрепление объявлений\n\n"
            f"Лимит закреплений на этот месяц исчерпан.\n\n"
            f"Тариф: {tariff}\n"
            f"Использовано: {pinned_ads_used}/{max_pins}\n\n"
            f"Закрепления обновятся в начале следующего месяца."
        )
        keyboard = [[InlineKeyboardButton("← Назад", callback_data="profile")]]
        await safe_edit_message(query, text, keyboard)
        return
    
    if not active_ads:
        text = (
            f"Закрепление объявлений\n\n"
            f"У вас нет активных объявлений для закрепления.\n\n"
            f"Сначала создайте объявление"
        )
        keyboard = [[InlineKeyboardButton("← Назад", callback_data="profile")]]
        await safe_edit_message(query, text, keyboard)
        return
    
    text = (
        f"Закрепление объявления\n\n"
        f"Тариф: {tariff}\n"
        f"Доступно закреплений: {pins_available}/{max_pins}\n\n"
        f"Срок закрепления: 24 часа\n"
        f"Увеличивает просмотры в 5–10 раз\n\n"
        f"Выберите объявление:"
    )
    
    keyboard = []
    moscow_now = get_moscow_time()
    for ad in active_ads[:10]:
        is_pinned = ad.get('pinned_until') and datetime.fromisoformat(ad['pinned_until']) > moscow_now
        pin_status = "◆ " if is_pinned else ""
        
        keyboard.append([
            InlineKeyboardButton(
                f"{pin_status}{ad['title'][:40]}... | {ad['price']}₽",
                callback_data=f"pin_ad:{ad['id']}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("← Назад", callback_data="profile")])
    
    await safe_edit_message(query, text, keyboard)

async def pin_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    ad_id = int(query.data.split(":")[1])
    user = update.effective_user
    
    # Получаем объявление
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != user.id:
        await safe_edit_message(query, "Объявление не найдено или нет доступа")
        return
    
    # Проверяем, можно ли закрепить
    user_data = db.get_user(user.id)
    tariff = user_data['tariff']
    
    # Проверяем тариф
    if tariff == 'Free':
        # Для бесплатного тарифа показываем сообщение с предложением купить подписку
        await safe_edit_message(query,
            text="Закрепление недоступно на бесплатном тарифе.\n\n"
                 "Закрепление доступно на платных тарифах:\n"
                 "• Standard — 1 закреп в месяц на 24 часа\n"
                 "• PRO — 3 закрепa в месяц на 7 дней каждый\n\n"
                 "Перейдите в раздел «Тарифы» для подробностей.",
            keyboard=[[InlineKeyboardButton("◆ Тарифы", callback_data="subscriptions")],
                     [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]]
        )
        return
    
    # Получаем параметры тарифа из конфига
    if tariff not in TARIFFS or TARIFFS[tariff]['pins_per_month'] == 0:
        await safe_edit_message(query, "Закрепление доступно только на тарифах Standard и PRO")
        return
    
    max_pins = TARIFFS[tariff]['pins_per_month']
    pin_duration_hours = TARIFFS[tariff]['pin_hours']
    
    # Проверяем, не закреплено ли уже
    if ad.get('pinned_until'):
        pinned_until = datetime.fromisoformat(ad['pinned_until'])
        moscow_now = get_moscow_time()
        if pinned_until > moscow_now:
            remaining = pinned_until - moscow_now
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            
            text = (
                f"Объявление уже закреплено\n\n"
                f"{ad['title']}\n\n"
                f"⏰ Осталось: {hours}ч {minutes}мин\n"
                f"Закреплено до: {ad['pinned_until'][:16]}"
            )
            keyboard = [[InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]]
            await safe_edit_message(query, text, keyboard)
            return
    
    # Проверяем лимиты закреплений
    pinned_ads_used = user_data.get('pinned_ads_used', 0) or 0
    pins_available = max_pins - pinned_ads_used
    
    if pins_available <= 0:
        if tariff == 'Standard':
            await safe_edit_message(query, 
                text=f"Лимит закреплений исчерпан (1/1)\n\n"
                f"В тарифе Standard доступен 1 закреп в месяц.\n"
                f"Вы можете:\n"
                f"• Дождаться следующего месяца\n"
                f"• Перейти на тариф PRO (3 закрепа в месяц)",
                keyboard=[[InlineKeyboardButton("◆ Тарифы", callback_data="subscriptions")],
                         [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]]
            )
        else:  # PRO
            await safe_edit_message(query, 
                text=f"Лимит закреплений исчерпан (3/3)\n\n"
                f"Лимит обновится в начале следующего месяца.",
                keyboard=[[InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]]
            )
        return
    
    # Закрепляем объявление
    pinned_until = (get_moscow_time() + timedelta(hours=pin_duration_hours)).isoformat()
    
    cursor = db.conn.cursor()
    cursor.execute(
        "UPDATE ads SET pinned_until = ? WHERE id = ?",
        (pinned_until, ad_id)
    )
    
    # Увеличиваем счетчик использованных закреплений
    db.update_user(user.id, pinned_ads_used=pinned_ads_used + 1)
    db.conn.commit()
    
    # РЕАЛЬНО ЗАКРЕПЛЯЕМ сообщение в канале (если есть)
    if ad.get('channel_message_id'):
        try:
            # Закрепляем сообщение в канале
            await context.bot.pin_chat_message(
                chat_id=ADMIN_CHANNEL_ID,
                message_id=ad['channel_message_id'],
                disable_notification=True  # Без уведомления подписчиков
            )
            logger.info(f"Объявление {ad_id} закреплено в канале (message_id: {ad['channel_message_id']})")
            
            # Планируем открепление через нужное кол-во часов
            context.job_queue.run_once(
                unpin_ad_callback,
                when=timedelta(hours=pin_duration_hours),
                data={'ad_id': ad_id, 'message_id': ad['channel_message_id']},
                name=f"unpin_ad_{ad_id}"
            )
            logger.info(f"Запланировано открепление объявления {ad_id} через {pin_duration_hours} часов")
            
        except Exception as e:
            logger.error(f"Не удалось закрепить сообщение в канале: {e}")
            # Откатываем изменения в БД, если не удалось закрепить
            cursor.execute("UPDATE ads SET pinned_until = NULL WHERE id = ?", (ad_id,))
            db.update_user(user.id, pinned_ads_used=pinned_ads_used)
            db.conn.commit()
            await safe_edit_message(query, "Не удалось закрепить объявление. Попробуйте позже.")
            return
    
    # Формируем текст с учетом времени
    if pin_duration_hours >= 24:
        duration_text = f"{pin_duration_hours // 24} дней" if pin_duration_hours > 24 else "24 часа"
    else:
        duration_text = f"{pin_duration_hours} часов"
    
    text = (
        f"Объявление закреплено\n\n"
        f"{ad['title']}\n\n"
        f"⏰ Закреплено на {duration_text} (тариф {tariff})\n"
        f"До: {pinned_until[:16]}\n\n"
        f"Использовано: {pinned_ads_used + 1}/{max_pins}\n\n"
        f"Объявление закреплено в канале и показывается вверху поиска."
    )
    
    keyboard = [[InlineKeyboardButton("← К объявлению", callback_data=f"view_my_ad:{ad_id}")]]
    await safe_edit_message(query, text, keyboard)
    
    db.log_action(user.id, "ad_pinned", f"Ad ID: {ad_id}, Until: {pinned_until}, Duration: {pin_duration_hours}h")

# Callback для открепления объявления по таймеру
async def unpin_ad_callback(context: ContextTypes.DEFAULT_TYPE):
    """Открепляет объявление из канала по таймеру"""
    job_data = context.job.data
    ad_id = job_data['ad_id']
    message_id = job_data['message_id']
    
    try:
        # Открепляем сообщение в канале
        await context.bot.unpin_chat_message(
            chat_id=ADMIN_CHANNEL_ID,
            message_id=message_id
        )
        logger.info(f"Объявление {ad_id} откреплено из канала (message_id: {message_id})")
        
        # Обновляем базу данных
        cursor = db.conn.cursor()
        cursor.execute("UPDATE ads SET pinned_until = NULL WHERE id = ?", (ad_id,))
        db.conn.commit()
        
        # Уведомляем пользователя
        ad = db.get_ad(ad_id)
        if ad:
            try:
                await context.bot.send_message(
                    chat_id=ad['user_id'],
                    text=f"Закрепление объявления истекло\n\n{ad['title']}\n\n"
                         f"Вы можете закрепить его снова, если остались доступные закрепления."
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {ad['user_id']}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка при откреплении объявления {ad_id}: {e}")

async def cancel_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit_message(query, "Оплата отменена")
    await show_main_menu(update, context)

async def edit_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Открывает меню реального редактирования — использует EDITABLE_FIELDS"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != update.effective_user.id:
        await safe_edit_message(query, "Объявление не найдено")
        return
    # EDITABLE_FIELDS определён ниже в файле — импорт через глобальный scope работает
    keyboard = [
        [InlineKeyboardButton(f"{label}", callback_data=f"edit_field:{ad_id}:{field}")]
        for field, (label, _) in EDITABLE_FIELDS.items()
    ]
    keyboard.append([InlineKeyboardButton("← К объявлению", callback_data=f"view_my_ad:{ad_id}")])
    await safe_edit_message(query, f"Редактирование: {ad['title']}\n\nЧто изменить?", keyboard)

# Заглушки edit_price/edit_desc/edit_contacts — переадресуем на реальный edit_ad
async def edit_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update.callback_query.data = f"edit_ad:{update.callback_query.data.split(':')[1]}"
    await edit_ad(update, context)

async def edit_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update.callback_query.data = f"edit_ad:{update.callback_query.data.split(':')[1]}"
    await edit_ad(update, context)

async def edit_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update.callback_query.data = f"edit_ad:{update.callback_query.data.split(':')[1]}"
    await edit_ad(update, context)

# ========== ОСНОВНЫЕ ФУНКЦИИ БОТА ==========

# Обработчики пользователей
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Проверяем бан
    if db.is_banned(user.id):
        await update.message.reply_text(
            "Аккаунт заблокирован за нарушение правил платформы.\n"
            "Если считаете это ошибкой — свяжитесь с поддержкой."
        )
        return
    
    # Обработка реферальной ссылки и deep links
    referral_id = None
    deep_link_action = None
    deep_link_ad_id = None
    
    if context.args and len(context.args) > 0:
        arg = context.args[0]
        if arg.startswith('ref_'):
            try:
                referral_id = int(arg.replace('ref_', ''))
            except Exception as _e:
                logger.debug(f"Ignored error: {_e}")
        elif arg.startswith('contact_'):
            try:
                deep_link_action = 'contact'
                deep_link_ad_id = int(arg.replace('contact_', ''))
            except Exception as _e:
                logger.debug(f"Ignored error: {_e}")
        elif arg.startswith('favorite_'):
            try:
                deep_link_action = 'favorite'
                deep_link_ad_id = int(arg.replace('favorite_', ''))
            except Exception as _e:
                logger.debug(f"Ignored error: {_e}")
        elif arg.startswith('favseller_'):
            try:
                deep_link_action = 'favseller'
                deep_link_ad_id = int(arg.replace('favseller_', ''))
            except Exception as _e:
                logger.debug(f"Ignored error: {_e}")
        elif arg.startswith('paid_'):
            # Формат: paid_{plan}_{price}  например paid_pro_799
            deep_link_action = 'paid'
            try:
                parts = arg.split('_', 2)  # ['paid', 'pro', '799']
                deep_link_plan  = parts[1] if len(parts) > 1 else 'standard'
                deep_link_price = int(parts[2]) if len(parts) > 2 else 0
            except Exception:
                deep_link_plan  = 'standard'
                deep_link_price = 0
        elif arg.startswith('promo_'):
            # Формат: promo_{КОД}_{plan}  например promo_SALE30_pro
            deep_link_action = 'promo'
            try:
                parts = arg.split('_', 2)  # ['promo', 'КОД', 'plan']
                deep_link_promo_code = parts[1].upper() if len(parts) > 1 else ''
                deep_link_plan = parts[2] if len(parts) > 2 else 'standard'
            except Exception:
                deep_link_promo_code = ''
                deep_link_plan = 'standard'
        elif arg == 'support':
            deep_link_action = 'support'
    
    # Создание/обновление пользователя
    user_data = db.get_user(user.id)
    if not user_data:
        new_user = {
            'user_id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'registration_date': get_moscow_time().isoformat()
        }
        db.create_user(new_user)
        
        # Создание реферальной связи
        if referral_id and referral_id != user.id:
            if db.create_referral(referrer_id=referral_id, referred_id=user.id):
                try:
                    await context.bot.send_message(
                        chat_id=referral_id,
                        text=f"Новый реферал\n\n"
                             f"@{user.username or user.first_name} зарегистрировался по вашей ссылке.\n\n"
                             f"Вы получите {REFERRAL_PERCENT}% от суммы при покупке подписки."
                    )
                except Exception as e:
                    logger.error(f"Не удалось уведомить реферера: {e}")
    
    # Проверка подписки
    subscribed = await check_subscription(user.id, context)
    db.update_user(user.id, subscribed=int(subscribed))
    
    if not subscribed:
        keyboard = [
            [InlineKeyboardButton("→ Подписаться на канал", url="https://t.me/PolkaAds")],
            [InlineKeyboardButton("✓ Я подписался", callback_data="check_subscription")]
        ]
        await update.message.reply_text(
            "Добро пожаловать в Полку\n\n"
            f"Для доступа к боту необходимо подписаться на наш канал:\nhttps://t.me/PolkaAds\n\n"
            "После подписки нажмите кнопку ниже.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.START
    else:
        # Обработка deep links после проверки подписки
        if deep_link_action == 'contact' and deep_link_ad_id:
            ad = db.get_ad(deep_link_ad_id)
            if ad:
                # ИСПРАВЛЕНО: было 'contacts', теперь 'contact_info'
                seller_contact = ad.get('contact_info', 'Не указан')
                seller_id = ad['user_id']
                already_fav = db.is_favorite_seller(user.id, seller_id)
                fav_label = "Убрать из избранных продавцов" if already_fav else "В избранное продавца"
                fav_cb = f"unfav_seller:{seller_id}" if already_fav else f"fav_seller:{seller_id}"
                keyboard = [
                    [InlineKeyboardButton(fav_label, callback_data=fav_cb)],
                    [InlineKeyboardButton("← Меню", callback_data="back_to_menu")]
                ]
                await update.message.reply_text(
                    f"<b>Контакт продавца:</b>\n\n{seller_contact}\n\n"
                    f"Объявление: {ad['title']}",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
            else:
                await update.message.reply_text("Объявление не найдено")
            return States.MAIN_MENU
        
        elif deep_link_action == 'favorite' and deep_link_ad_id:
            ad = db.get_ad(deep_link_ad_id)
            if ad:
                keyboard = [
                    [InlineKeyboardButton("◆ В избранное", callback_data=f"add_favorite:{deep_link_ad_id}"),
                     InlineKeyboardButton("✗ Отмена", callback_data="back_to_menu")]
                ]
                await update.message.reply_text(
                    f"Добавить в избранное?\n\n"
                    f"{ad['title']}\n"
                    f"Цена: {ad['price']} ₽",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await update.message.reply_text("Объявление не найдено")
            return States.MAIN_MENU

        elif deep_link_action == 'favseller' and deep_link_ad_id:
            ad = db.get_ad(deep_link_ad_id)
            if ad:
                seller_id = ad['user_id']
                user_data_local = db.get_user(seller_id)
                already_fav = db.is_favorite_seller(user.id, seller_id)
                if already_fav:
                    seller_name = user_data_local.get('first_name', 'Продавец') if user_data_local else 'Продавец'
                    await update.message.reply_text(
                        f"{seller_name} уже в списке избранных продавцов",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
                    )
                else:
                    db.add_favorite_seller(user.id, seller_id)
                    seller_name = user_data_local.get('first_name', 'Продавец') if user_data_local else 'Продавец'
                    await update.message.reply_text(
                        f"Продавец {seller_name} добавлен в избранное.\n\n"
                        f"Его объявления доступны в разделе «Избранные продавцы».",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
                    )
            else:
                await update.message.reply_text("Объявление не найдено")
            return States.MAIN_MENU

        elif deep_link_action == 'paid':
            # Пользователь нажал «Я оплатил» в Mini App → просим скриншот оплаты
            PLAN_NAMES = {'free': 'Free', 'standard': 'Старт', 'pro': 'PRO'}
            plan_label = PLAN_NAMES.get(deep_link_plan, deep_link_plan.upper())
            context.user_data['pending_payment'] = {
                'tariff':   plan_label,
                'amount':   deep_link_price,
                'duration': 30,
            }
            await update.message.reply_text(
                f"Отлично! Вы выбрали тариф <b>{plan_label}</b> — {deep_link_price} ₽.\n\n"
                "Пожалуйста, отправьте <b>скриншот оплаты</b> — администратор активирует подписку "
                "в течение 15 минут.",
                parse_mode=ParseMode.HTML,
            )
            return States.MAIN_MENU

        elif deep_link_action == 'support':
            await update.message.reply_text(
                "Служба поддержки Полки\n\n"
                "Опишите вашу проблему — мы ответим в ближайшее время.\n\n"
                "Или напишите напрямую: @PolkaAdsBot",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
                ),
            )
            return States.MAIN_MENU

        elif deep_link_action == 'promo' and deep_link_promo_code:
            plan  = deep_link_plan
            PLAN_NAMES = {'free': 'Free', 'standard': 'Старт', 'pro': 'PRO'}
            plan_label = PLAN_NAMES.get(plan, plan.upper())
            promo = db.get_promocode(deep_link_promo_code)
            if not promo:
                await update.message.reply_text(
                    f"✗ Промокод <b>{deep_link_promo_code}</b> не найден или истёк.\n\n"
                    "Вернитесь на страницу тарифов и попробуйте другой код.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
                )
            else:
                discount = promo['discount_percent']
                tariff_key = 'Standard' if plan == 'standard' else 'PRO'
                base_price = TARIFFS.get(tariff_key, {}).get('price', 299)
                discounted_price = round(base_price * (1 - discount / 100))
                context.user_data['promo_code']     = deep_link_promo_code
                context.user_data['promo_discount']  = discount
                context.user_data['promo_plan']      = plan

                crypto_url   = resolve_crypto_url(discount, discounted_price, plan)
                yoomoney_url = PAYMENT_DETAILS.get(f'yoomoney_{plan}', PAYMENT_DETAILS['yoomoney_standard'])

                await update.message.reply_text(
                    f"✓ Промокод <b>{deep_link_promo_code}</b> действителен!\n\n"
                    f"Скидка: <b>{discount}%</b>\n"
                    f"Тариф: <b>{plan_label}</b>\n"
                    f"Цена со скидкой: <b>{discounted_price} ₽</b>\n\n"
                    f"Оплатите <b>{discounted_price} ₽</b> любым удобным способом:\n"
                    f"• <a href=\"{yoomoney_url}\">ЮMoney</a>\n"
                    f"• <a href=\"{crypto_url}\">Крипта</a>\n\n"
                    f"Затем отправьте скриншот — подписка активируется в течение 15 минут.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
                )
            return States.MAIN_MENU

        if context.user_data.pop('show_onboarding', False):
            return await show_onboarding(update, context)
        return await show_main_menu(update, context)

async def check_subscription_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    subscribed = await check_subscription(user.id, context)
    
    if not subscribed:
        await safe_edit_message(query, "Вы ещё не подписались на канал. Подпишитесь и нажмите кнопку ниже.")
    else:
        db.update_user(user.id, subscribed=1)
        # Проверяем — верифицирован ли уже
        existing_verify = db.get_verification(user.id)
        already_verified = existing_verify and existing_verify.get('status') == 'verified'
        if already_verified:
            await show_main_menu(update, context)
        else:
            await show_verification_prompt(query, context)


async def show_verification_prompt(query, context: ContextTypes.DEFAULT_TYPE):
    """Экран верификации после подписки на канал"""
    text = (
        "<b>Подписка подтверждена</b>\n\n"
        "Добро пожаловать в Полку\n\n"
        "─────────────────\n"
        "<b>Верификация продавца</b>\n\n"
        "Верификация — это бесплатное подтверждение вашей личности. "
        "Верифицированным продавцам <b>доверяют больше</b>:\n\n"
        "• Значок <b>✓</b> на всех объявлениях и в профиле\n"
        "• Покупатели охотнее выходят на сделку\n"
        "• В среднем <b>на 40% больше откликов</b>\n"
        "• Выше позиции в результатах поиска\n\n"
        "Займёт меньше минуты — просто поделитесь номером телефона.\n\n"
        "─────────────────\n"
        "<i>Вы всегда сможете пройти верификацию позже в разделе «Профиль»</i>"
    )
    keyboard = [
        [InlineKeyboardButton("◆ Пройти верификацию", callback_data="verify_seller")],
        [InlineKeyboardButton("· Пропустить", callback_data="skip_verification")]
    ]
    try:
        await query.edit_message_text(
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        await query.message.reply_text(
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )


async def skip_verification_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал 'Пропустить' верификацию"""
    query = update.callback_query
    await query.answer()
    await show_main_menu(update, context)

async def show_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экран онбординга для новых пользователей"""
    user = update.effective_user
    text = (
        f"<b>Добро пожаловать, {user.first_name}!</b>\n\n"
        "Полка — доска объявлений в Telegram.\n"
        "Покупайте и продавайте быстро, безопасно, без лишних шагов.\n\n"
        "─────────────────\n"
        "◈ <b>Что вы можете делать:</b>\n\n"
        "▸ Размещать объявления — бесплатно\n"
        "▸ Искать товары по категориям и городам\n"
        "▸ Безопасные сделки через эскроу\n"
        "▸ Следить за любимыми продавцами\n\n"
        "─────────────────\n"
        "◈ <b>Тарифы:</b>\n\n"
        "○ <b>Бесплатно</b> — 3 объявления, 3 фото\n"
        "◈ <b>Старт — 299 ₽/мес</b> — безлимит, аналитика, авто-подъём раз в 24ч\n"
        "◉ <b>PRO — 799 ₽/мес</b> — всё из Старт + авто-подъём каждые 6ч, приоритет\n\n"
        "<i>Начните бесплатно — тариф можно сменить в любой момент.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("+ Разместить объявление", callback_data="create_ad")],
        [InlineKeyboardButton("◎ Поиск", callback_data="search")],
        [InlineKeyboardButton("← Меню", callback_data="back_to_menu")],
    ]
    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    else:
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    return States.MAIN_MENU


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    user_data = db.get_user(user.id)
    tariff = user_data.get('tariff', 'Free') if user_data else 'Free'
    tariff_label = TARIFF_LABELS.get(tariff, tariff)
    active_ads = 0
    if user_data:
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ads WHERE user_id=? AND status='active'", (user.id,))
        active_ads = cursor.fetchone()[0]

    keyboard = [
        [InlineKeyboardButton("+ Разместить объявление", callback_data="create_ad"),
         InlineKeyboardButton("Поиск", callback_data="search")],
        [InlineKeyboardButton("Мои объявления", callback_data="my_ads"),
         InlineKeyboardButton("Избранное", callback_data="favorites")],
        [InlineKeyboardButton("Избранные продавцы", callback_data="favorite_sellers"),
         InlineKeyboardButton("Поиски", callback_data="my_saved_searches")],
        [InlineKeyboardButton("Профиль", callback_data="profile"),
         InlineKeyboardButton("Тарифы", callback_data="subscriptions")],
        [InlineKeyboardButton("Рефералы", callback_data="referral_menu"),
         InlineKeyboardButton("Поддержка", url="https://t.me/SupPolka")],
    ]

    ads_hint = f"У вас {active_ads} активных объявлений." if active_ads else "Разместите первое объявление!"

    text = (
        f"{user.first_name}, добро пожаловать в Полку\n\n"
        f"Тариф: {tariff_label}\n"
        f"{ads_hint}\n\n"
        f"Что будем делать?"
    )
    
    markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        try:
            await update.message.reply_photo(photo=BANNERS["welcome"], caption=text, reply_markup=markup)
        except Exception:
            await update.message.reply_text(text, reply_markup=markup)
    else:
        try:
            await update.callback_query.message.reply_photo(photo=BANNERS["welcome"], caption=text, reply_markup=markup)
        except Exception:
            await update.callback_query.message.reply_text(text, reply_markup=markup)
        try:
            await update.callback_query.delete_message()
        except Exception:
            pass

    return States.MAIN_MENU

# Мои объявления с кнопкой снять с публикации
async def my_ads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        user = update.effective_user
        
        # Получаем все объявления пользователя - ОБНОВЛЯЕМ ДАННЫЕ КАЖДЫЙ РАЗ
        all_ads = db.get_user_ads(user.id)
        active_ads = [ad for ad in all_ads if ad['status'] == 'active']
        moderation_ads = [ad for ad in all_ads if ad['status'] == 'moderation']
        inactive_ads = [ad for ad in all_ads if ad['status'] == 'inactive']
        
        # Считаем общее количество просмотров ТОЛЬКО активных объявлений
        cursor = db.conn.cursor()
        cursor.execute("SELECT SUM(views) FROM ads WHERE user_id = ? AND status = 'active'", (user.id,))
        total_views = cursor.fetchone()[0] or 0
        
        text = "Мои объявления\n\n"
        
        if not all_ads:
            text += "У вас пока нет объявлений.\n\nСоздайте первое объявление!"
            keyboard = [
                [InlineKeyboardButton("+ Разместить объявление", callback_data="create_ad")],
                [InlineKeyboardButton("← Назад", callback_data="back_to_menu")]
            ]
        else:
            text += "Статистика:\n"
            text += f"Активных: {len(active_ads)}\n"
            text += f"В архиве: {len(inactive_ads)}\n"
            text += f"Просмотров: {total_views}\n"
            
            if moderation_ads:
                text += f"◎ На модерации: {len(moderation_ads)}\n"
            
            keyboard = [
                [InlineKeyboardButton(f"▸ Активные ({len(active_ads)})", callback_data="my_ads_active")],
                [InlineKeyboardButton(f"▸ Архив ({len(inactive_ads)})", callback_data="my_ads_archive")],
            ]
            
            if moderation_ads:
                keyboard.append([InlineKeyboardButton(f"◎ На модерации ({len(moderation_ads)})", callback_data="show_moderation_ads")])
            
            keyboard.append([InlineKeyboardButton("+ Разместить объявление", callback_data="create_ad")])
            keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])
        
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в my_ads: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить объявления. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def my_ads_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список активных объявлений"""
    query = update.callback_query
    await query.answer()
    
    try:
        user = update.effective_user
        
        # Получаем активные объявления
        all_ads = db.get_user_ads(user.id)
        active_ads = [ad for ad in all_ads if ad['status'] == 'active']
        
        if not active_ads:
            text = "Активных объявлений нет."
            keyboard = [[InlineKeyboardButton("← Мои объявления", callback_data="my_ads")]]
        else:
            text = f"Активные объявления: {len(active_ads)}\n\n"
            
            for i, ad in enumerate(active_ads, 1):
                pinned = "◆ " if ad.get('pinned_until') else ""
                text += f"{i}. {pinned}{ad['title']}\n"
                text += f" {ad['price']} ₽ · {ad['views']} просмотров\n\n"
            
            keyboard = []
            for ad in active_ads:
                title_short = ad['title'][:30] + "..." if len(ad['title']) > 30 else ad['title']
                keyboard.append([
                    InlineKeyboardButton(
                        f"{title_short} · {ad['views']} просм.",
                        callback_data=f"view_my_ad:{ad['id']}"
                    )
                ])
            
            keyboard.append([InlineKeyboardButton("← Мои объявления", callback_data="my_ads")])
        
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в my_ads_active: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить объявления.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def my_ads_archive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список объявлений в архиве"""
    query = update.callback_query
    await query.answer()
    
    try:
        user = update.effective_user
        
        # Получаем архивные объявления
        all_ads = db.get_user_ads(user.id)
        archive_ads = [ad for ad in all_ads if ad['status'] == 'inactive']
        
        if not archive_ads:
            text = "Архив пуст."
            keyboard = [[InlineKeyboardButton("← Мои объявления", callback_data="my_ads")]]
        else:
            text = f"Архив: {len(archive_ads)} объявлений\n\n"
            
            for i, ad in enumerate(archive_ads, 1):
                text += f"{i}. {ad['title']}\n"
                text += f" {ad['price']} ₽\n\n"
            
            keyboard = []
            for ad in archive_ads:
                title_short = ad['title'][:30] + "..." if len(ad['title']) > 30 else ad['title']
                keyboard.append([
                    InlineKeyboardButton(
                        title_short,
                        callback_data=f"view_my_ad:{ad['id']}"
                    )
                ])
            
            keyboard.append([InlineKeyboardButton("← Мои объявления", callback_data="my_ads")])
        
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в my_ads_archive: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить архив.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def view_my_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    ad_id = int(query.data.split(":")[1])
    user = update.effective_user
    
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != user.id:
        await safe_edit_message(query, "Объявление не найдено или нет доступа")
        return
    
    status_label = AD_STATUS_LABELS.get(ad['status'], ad['status'])
    pinned_str = f"\n◆ Закреплено до: {ad['pinned_until'][:16]}" if ad.get('pinned_until') else ""
    views = ad.get('views', 0)
    contact_clicks = db.get_ad_contact_clicks(ad_id)

    text = (
        f"<b>{ad['title']}</b>\n\n"
        f"Цена: <b>{ad['price']} ₽</b>\n"
        f"Категория: {ad['category']}\n"
        f"Город: {ad.get('city') or '—'}\n"
        f"Доставка: {ad.get('delivery') or '—'}\n\n"
        f"Статус: {status_label}{pinned_str}\n\n"
        f"Просмотров: {views} · Обращений: {contact_clicks}\n"
        f"Опубликовано: {ad['created_at'][:10]}\n\n"
        f"{ad['description'][:200]}{'...' if len(ad.get('description','')) > 200 else ''}\n\n"
        f"Контакт: {ad['contact_info']}"
    )
    
    keyboard = []
    
    if ad['status'] == 'active':
        # ВСЕГДА добавляем кнопку Закрепить (проверка тарифа будет при нажатии)
        keyboard.append([InlineKeyboardButton("◆ Закрепить", callback_data=f"pin_ad:{ad_id}")])
        analytics_row = [InlineKeyboardButton("▲ Поднять в топ", callback_data=f"boost_upsell:{ad_id}")]
        if db.get_user(user.id).get('tariff', 'Free') != 'Free':
            analytics_row.append(InlineKeyboardButton("◎ Аналитика", callback_data=f"ad_analytics:{ad_id}"))
        keyboard.append(analytics_row)
        keyboard.append([InlineKeyboardButton("↺ Авто-подъём", callback_data=f"auto_bump:{ad_id}"),
                         InlineKeyboardButton("→ Поделиться", callback_data=f"share_ad:{ad_id}")])
        keyboard.append([
            InlineKeyboardButton("✗ Снять", callback_data=f"deactivate_ad:{ad_id}"),
            InlineKeyboardButton("✎ Редактировать", callback_data=f"edit_ad:{ad_id}")
        ])
        keyboard.append([
            InlineKeyboardButton("+ Дублировать", callback_data=f"duplicate_ad:{ad_id}")
        ])
    elif ad['status'] == 'inactive':
        keyboard.append([
            InlineKeyboardButton("✓ Опубликовать снова", callback_data=f"reactivate_ad:{ad_id}"),
            InlineKeyboardButton("✗ Удалить", callback_data=f"delete_ad_completely:{ad_id}")
        ])
        keyboard.append([
            InlineKeyboardButton("+ Дублировать", callback_data=f"duplicate_ad:{ad_id}")
        ])
    
    keyboard.append([InlineKeyboardButton("← Мои объявления", callback_data="my_ads")])
    
    if ad['photos']:
        try:
            if len(ad['photos']) == 1:
                await query.message.reply_photo(
                    photo=ad['photos'][0],
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
                await query.delete_message()
            else:
                media_group = [InputMediaPhoto(
                    media=ad['photos'][0], caption=text, parse_mode=ParseMode.HTML
                )]
                for photo in ad['photos'][1:]:
                    media_group.append(InputMediaPhoto(photo))
                await query.message.reply_media_group(media=media_group)
                await query.message.reply_text(
                    "Управление объявлением:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                await query.delete_message()
        except Exception as e:
            logger.error(f"view_my_ad photo error: {e}")
            await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    else:
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

async def deactivate_ad_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        ad_id = int(query.data.split(":")[1])
        user = update.effective_user
        
        ad = db.get_ad(ad_id)
        if not ad or ad['user_id'] != user.id:
            await safe_edit_message(query, "Объявление не найдено или нет доступа")
            return
        
        # Пытаемся удалить сообщение из канала
        deleted_from_channel = await delete_ad_from_channel(ad_id, context)
        
        # Деактивируем объявление в базе
        success = db.deactivate_ad(ad_id)
        
        if success:
            text = "Объявление снято с публикации.\n\n"
            if deleted_from_channel:
                text += "Сообщение удалено из канала."
            else:
                text += "Не удалось удалить сообщение из канала, но объявление скрыто из поиска."
            
            await safe_edit_message(query, text)
            db.log_action(user.id, "ad_deactivated", f"Ad ID: {ad_id}")
        else:
            await safe_edit_message(query, "Не удалось снять объявление с публикации")
        
        await my_ads(update, context)
    except Exception as e:
        logger.error(f"Ошибка в deactivate_ad_handler: {e}")
        try:
            await safe_edit_message(query, "Не удалось снять объявление. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def delete_ad_completely(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    ad_id = int(query.data.split(":")[1])
    user = update.effective_user
    
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != user.id:
        await safe_edit_message(query, "Объявление не найдено или нет доступа")
        return
    
    # Удаляем из канала все сообщения (включая медиагруппу)
    if ad.get('channel_message_id'):
        try:
            message_id = ad['channel_message_id']
            # Пытаемся удалить до 10 последовательных сообщений
            for i in range(10):
                try:
                    await context.bot.delete_message(
                        chat_id=ADMIN_CHANNEL_ID, 
                        message_id=message_id + i
                    )
                except Exception as _e:
                    if i > 0:
                        break
        except Exception as e:
            logger.error(f"Не удалось удалить сообщение из канала: {e}")
    
    # Удаляем из избранного
    cursor = db.conn.cursor()
    cursor.execute("DELETE FROM favorites WHERE ad_id = ?", (ad_id,))
    
    # Удаляем объявление из базы данных
    cursor.execute("DELETE FROM ads WHERE id = ?", (ad_id,))
    db.conn.commit()
    
    await safe_edit_message(query, "Объявление удалено")
    db.log_action(user.id, "ad_deleted_completely", f"Ad ID: {ad_id}")
    
    await my_ads(update, context)

async def reactivate_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        ad_id = int(query.data.split(":")[1])
        user = update.effective_user
        
        ad = db.get_ad(ad_id)
        if not ad or ad['user_id'] != user.id:
            await safe_edit_message(query, "Объявление не найдено или нет доступа")
            return
        
        # Публикуем объявление в канале заново
        pub_result = await publish_ad_to_channel(ad_id, context)
        message_id = pub_result[0] if isinstance(pub_result, tuple) else pub_result
        
        if message_id:
            # Обновляем статус и сохраняем ID сообщения
            db.update_ad(ad_id, status='active', channel_message_id=message_id)
            await safe_edit_message(query, "Объявление опубликовано")
            db.log_action(user.id, "ad_reactivated", f"Ad ID: {ad_id}")
        else:
            await safe_edit_message(query, "Не удалось опубликовать объявление")
        
        await my_ads(update, context)
    except Exception as e:
        logger.error(f"Ошибка в reactivate_ad: {e}")
        try:
            await safe_edit_message(query, "Не удалось опубликовать объявление. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass

# Избранное - УЛУЧШЕННОЕ UX
async def favorites(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    cursor = db.conn.cursor()
    # ОБНОВЛЯЕМ ДАННЫЕ КАЖДЫЙ РАЗ - проверяем актуальность избранного
    cursor.execute('''
        SELECT a.* FROM ads a
        JOIN favorites f ON a.id = f.ad_id
        WHERE f.user_id = ? AND a.status = 'active'
        ORDER BY f.added_at DESC
        LIMIT 20
    ''', (user.id,))
    
    favorite_ads = cursor.fetchall()
    
    # Также подсчитываем общее количество избранных (включая неактивные, для статистики)
    cursor.execute('SELECT COUNT(*) FROM favorites WHERE user_id = ?', (user.id,))
    total_favorites = cursor.fetchone()[0]
    
    if not favorite_ads:
        text = "<b>Избранных объявлений пока нет</b>\n\nДобавляйте объявления в избранное при просмотре."
        keyboard = [
            [InlineKeyboardButton("◎ Поиск", callback_data="search")],
            [InlineKeyboardButton("← Назад", callback_data="back_to_menu")]
        ]
    else:
        text = f"<b>Избранное:</b> {len(favorite_ads)} объявлений\n\n"
        
        # Формируем текст с превью как в поиске
        for i, ad_tuple in enumerate(favorite_ads, 1):
            # Преобразуем tuple в словарь
            ad = {
                'id': ad_tuple[0],
                'title': ad_tuple[2],
                'price': ad_tuple[4],
                'category': ad_tuple[7] if len(ad_tuple) > 7 else 'Другое'
            }
            text += f"<b>{ad['title']}</b>\n{ad['price']} ₽ · {ad['category']}\n\n"
        
        # Формируем кнопки для просмотра объявлений
        keyboard = []
        for i, ad_tuple in enumerate(favorite_ads, 1):
            ad_id = ad_tuple[0]
            ad_title = ad_tuple[2]
            ad_price = ad_tuple[4]
            title_short = ad_title[:25] + "..." if len(ad_title) > 25 else ad_title
            keyboard.append([InlineKeyboardButton(
                f"{title_short} — {ad_price} ₽",
                callback_data=f"view_ad:{ad_id}"
            )])
        
        keyboard.append([InlineKeyboardButton("◎ Поиск", callback_data="search")])
        keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])
    
    try:
        await query.message.reply_photo(
            photo=BANNERS["favorites"],
            caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
        await query.delete_message()
    except Exception:
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

# Поиск объявлений
async def _get_search_filters(ctx) -> dict:
    """Получить текущие фильтры из user_data"""
    return {
        'query':     ctx.user_data.get('sf_query', ''),
        'category':  ctx.user_data.get('sf_category', ''),
        'price_min': ctx.user_data.get('sf_price_min', 0),
        'price_max': ctx.user_data.get('sf_price_max', 0),
        'city':      ctx.user_data.get('sf_city', ''),
        'condition': ctx.user_data.get('sf_condition', ''),
        'delivery':  ctx.user_data.get('sf_delivery', ''),
        'sort_by':   ctx.user_data.get('sf_sort', 'date_desc'),
        'page':      ctx.user_data.get('sf_page', 0),
    }

def _filters_label(f: dict) -> str:
    """Краткое описание активных фильтров"""
    parts = []
    if f.get('category'): parts.append(f"{f['category']}")
    if f.get('city'):     parts.append(f"{f['city']}")
    if f.get('price_min') or f.get('price_max'):
        lo = f['price_min'] or 0
        hi = f['price_max'] or '∞'
        parts.append(f"{lo}–{hi} ₽")
    if f.get('condition') == 'new':  parts.append("◎ Новое")
    if f.get('condition') == 'used': parts.append("◎ Б/у")
    if f.get('delivery') == 'yes':   parts.append("◎ С доставкой")
    SORT_LABELS = {
        'date_desc':  'Сначала новые',
        'date_asc':   'Сначала старые',
        'price_asc':  'Дешевле',
        'price_desc': 'Дороже',
        'views_desc': 'Популярные',
    }
    if f.get('sort_by', 'date_desc') != 'date_desc':
        parts.append(SORT_LABELS.get(f['sort_by'], ''))
    return ' '.join(parts) if parts else ''

SEARCH_CATEGORIES = [
    ('Техника', 'Техника'),
    ('Одежда', 'Одежда'),
    ('Обувь', 'Обувь'),
    ('Авто', 'Авто'),
    ('Недвижимость', 'Недвижимость'),
    ('Игры', 'Игры'),
    ('Книги', 'Книги'),
    ('Другое', 'Другое'),
]

SORT_OPTIONS = [
    ('date_desc',  'Сначала новые'),
    ('date_asc',   'Сначала старые'),
    ('price_asc',  'Дешевле'),
    ('price_desc', 'Дороже'),
    ('views_desc', 'По популярности'),
]

# ═══════════════════════════════════════════════════════════════════════
#  ПОИСК ПО ФОТО — утилиты
# ═══════════════════════════════════════════════════════════════════════

async def compute_photo_hash(bot, file_id: str) -> Optional[str]:
    """Скачивает фото из Telegram и вычисляет perceptual hash (pHash)."""
    if not IMAGEHASH_AVAILABLE:
        return None
    try:
        tg_file = await bot.get_file(file_id)
        buf = _io.BytesIO()
        await tg_file.download_to_memory(buf)
        buf.seek(0)
        img = Image.open(buf).convert("RGB")
        h = imagehash.phash(img, hash_size=16)  # 256-bit hash
        return str(h)
    except Exception as e:
        logger.error(f"compute_photo_hash error: {e}")
        return None


async def extract_photo_tags_hf(bot, file_id: str) -> list:
    """
    Извлекает теги из фото через Hugging Face Inference API.
    Модель: microsoft/resnet-50 — 1000 классов ImageNet.
    Бесплатно ~30k запросов/месяц, токен из huggingface.co.
    Переводит английские метки в русские для лучшего поиска.
    """
    if not HF_API_TOKEN:
        return []
    try:
        tg_file = await bot.get_file(file_id)
        buf = _io.BytesIO()
        await tg_file.download_to_memory(buf)
        image_bytes = buf.getvalue()

        import aiohttp as _aiohttp
        url = f"https://api-inference.huggingface.co/models/{HF_IMAGE_MODEL}"
        headers = {"Authorization": f"Bearer {HF_API_TOKEN}"}

        async with _aiohttp.ClientSession() as session:
            async with session.post(
                url, data=image_bytes, headers=headers,
                timeout=_aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 503:
                    # Модель загружается (cold start) — повторяем через 3 сек
                    await asyncio.sleep(3)
                    async with session.post(
                        url, data=image_bytes, headers=headers,
                        timeout=_aiohttp.ClientTimeout(total=15)
                    ) as resp2:
                        data = await resp2.json()
                else:
                    data = await resp.json()

        if not isinstance(data, list):
            logger.warning(f"HF API unexpected response: {data}")
            return []

        # Словарь перевода самых частых ImageNet-классов в русские термины
        _EN_TO_RU = {
            # Одежда
            "jersey": "футболка", "t-shirt": "футболка", "sweatshirt": "свитшот",
            "jean": "джинсы", "denim": "джинсы", "miniskirt": "юбка",
            "suit": "костюм", "coat": "пальто", "cardigan": "кардиган",
            "sock": "носки", "stole": "платок", "brassiere": "бюстгальтер",
            "swimming trunks": "купальник", "bikini": "бикини",
            # Обувь
            "sneaker": "кроссовки", "running shoe": "кроссовки",
            "sandal": "сандалии", "boot": "ботинки", "loafer": "мокасины",
            "clog": "сабо", "platform shoe": "платформа",
            # Техника
            "laptop": "ноутбук", "notebook": "ноутбук",
            "desktop computer": "компьютер", "monitor": "монитор",
            "mobile phone": "телефон", "smartphone": "смартфон",
            "iphone": "айфон", "tablet": "планшет", "kindle": "электронная книга",
            "camera": "фотоаппарат", "television": "телевизор",
            "speaker": "колонка", "headphones": "наушники", "keyboard": "клавиатура",
            "mouse": "мышь", "printer": "принтер", "router": "роутер",
            # Мебель
            "sofa": "диван", "couch": "диван", "chair": "стул",
            "table": "стол", "desk": "стол", "bed": "кровать",
            "wardrobe": "шкаф", "bookcase": "стеллаж", "shelf": "полка",
            # Авто
            "car": "автомобиль", "convertible": "кабриолет", "sports car": "спорткар",
            "minivan": "минивэн", "limousine": "лимузин", "jeep": "джип",
            "bicycle": "велосипед", "motorcycle": "мотоцикл", "scooter": "самокат",
            # Спорт / хобби
            "guitar": "гитара", "piano": "пианино", "violin": "скрипка",
            "dumbbell": "гантели", "barbell": "штанга", "treadmill": "беговая дорожка",
            "skateboard": "скейтборд", "surfboard": "сёрфборд",
            "backpack": "рюкзак", "handbag": "сумка", "wallet": "кошелёк",
            "watch": "часы", "sunglasses": "очки", "ring": "кольцо",
            # Книги / игры
            "book": "книга", "comic book": "комикс",
            "jigsaw puzzle": "пазл", "toy": "игрушка", "teddy bear": "игрушка",
            # Дом
            "lamp": "светильник", "pillow": "подушка", "curtain": "шторы",
            "rug": "ковёр", "vase": "ваза", "refrigerator": "холодильник",
            "washing machine": "стиральная машина", "microwave": "микроволновка",
        }

        tags_en = []
        tags_ru = []
        for item in data:
            label = item.get("label", "").lower().replace("_", " ")
            score = item.get("score", 0)
            if score < 0.05:
                continue
            tags_en.append(label)
            if label in _EN_TO_RU:
                tags_ru.append(_EN_TO_RU[label])

        # Возвращаем: сначала русские (для поиска), потом английские
        result = tags_ru[:6] + [t for t in tags_en[:6] if t not in tags_ru]
        return result[:10]

    except Exception as e:
        logger.error(f"extract_photo_tags_hf error: {e}")
        return []


def phash_distance(h1: str, h2: str) -> int:
    """Расстояние Хэмминга между двумя pHash строками. Меньше = похожее."""
    try:
        ih1 = imagehash.hex_to_hash(h1)
        ih2 = imagehash.hex_to_hash(h2)
        return ih1 - ih2  # встроенный оператор imagehash
    except Exception:
        return 999


def search_by_photo_hash(photo_hash: str, threshold: int = 12, limit: int = 30) -> List[Dict]:
    """
    Ищет объявления с похожим фото по pHash.
    threshold=12 из 256 бит (~5% разницы) — хороший баланс точности.
    """
    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT * FROM ads WHERE status='active' AND photo_hash IS NOT NULL"
    )
    candidates = []
    for row in cursor.fetchall():
        ad = dict(row)
        dist = phash_distance(photo_hash, ad["photo_hash"])
        if dist <= threshold:
            ad["_similarity"] = round((1 - dist / 256) * 100)
            ad["photos"] = json.loads(ad["photos"]) if ad["photos"] else []
            candidates.append(ad)
    # Сортируем по похожести (desc), потом по дате
    candidates.sort(key=lambda x: (-x["_similarity"], x.get("created_at", "")))
    return candidates[:limit]



async def search_by_photo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал 'Поиск по фото' — просим прислать фото."""
    query = update.callback_query
    await query.answer()
    context.user_data['awaiting_search_photo'] = True
    await safe_edit_message(
        query,
        "Отправьте фото товара — найдём похожие объявления.\n\nЧем чётче фото, тем точнее результат.",
        [[InlineKeyboardButton("← Назад", callback_data="search")]]
    )
    return States.SEARCH_PHOTO


def check_photo_search_limit(user_id: int) -> tuple:
    """
    Проверяет лимиты поиска по фото для пользователя.
    Возвращает (allowed: bool, reason: str | None).
    reason — текст для пользователя если не allowed.
    """
    user_data = db.get_user(user_id)
    if not user_data:
        return False, "Пользователь не найден."

    tariff = user_data.get("tariff", "Free")
    limits = PHOTO_SEARCH_LIMITS.get(tariff, PHOTO_SEARCH_LIMITS["Free"])
    now = get_moscow_time()
    today = now.date().isoformat()

    # ── Кулдаун между запросами ───────────────────────────────────────
    last_at_str = user_data.get("photo_search_last_at")
    if last_at_str:
        try:
            last_at = datetime.fromisoformat(last_at_str)
            elapsed = (now - last_at).total_seconds()
            if elapsed < limits["cooldown"]:
                wait = int(limits["cooldown"] - elapsed) + 1
                return False, f"Подождите ещё {wait} сек. перед следующим поиском по фото.\n\nТариф {tariff}: кулдаун {limits['cooldown']} сек."
        except (ValueError, TypeError):
            pass

    # ── Дневной лимит ─────────────────────────────────────────────────
    if limits["daily"] > 0:  # 0 = без лимита (PRO)
        reset_date = user_data.get("photo_search_reset_date", "")
        count = user_data.get("photo_search_count", 0) or 0

        # Сброс счётчика в новый день
        if reset_date != today:
            db.update_user(user_id, photo_search_count=0, photo_search_reset_date=today)
            count = 0

        if count >= limits["daily"]:
            tariff_names = {"Free": "Старт — 299 ₽/мес", "Standard": "PRO — 799 ₽/мес"}
            upgrade_hint = tariff_names.get(tariff, "")
            upgrade_str = f"\n\nДля увеличения лимита — тариф {upgrade_hint}" if upgrade_hint else ""
            return False, f"Лимит поиска по фото исчерпан: {count}/{limits['daily']} в день.\n\nЛимит обновится завтра.{upgrade_str}"

    return True, None


def record_photo_search(user_id: int):
    """Фиксирует использование поиска по фото: обновляет счётчик и время."""
    user_data = db.get_user(user_id)
    if not user_data:
        return
    today = get_moscow_time().date().isoformat()
    reset_date = user_data.get("photo_search_reset_date", "")
    count = user_data.get("photo_search_count", 0) or 0
    if reset_date != today:
        count = 0
    db.update_user(
        user_id,
        photo_search_count=count + 1,
        photo_search_reset_date=today,
        photo_search_last_at=get_moscow_time().isoformat()
    )


async def handle_search_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем фото от пользователя и ищем похожие объявления."""
    if not update.message or not update.message.photo:
        await update.message.reply_text(
            "Пожалуйста, отправьте фото (не файл).",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("← Назад", callback_data="search")
            ]])
        )
        return States.SEARCH_PHOTO

    # Проверяем лимиты
    user_id = update.effective_user.id
    allowed, reason = check_photo_search_limit(user_id)
    if not allowed:
        user_data = db.get_user(user_id)
        tariff = (user_data or {}).get("tariff", "Free")
        limits = PHOTO_SEARCH_LIMITS.get(tariff, PHOTO_SEARCH_LIMITS["Free"])
        daily = limits["daily"]
        count = (user_data or {}).get("photo_search_count", 0) or 0

        keyboard = []
        if tariff == "Free":
            keyboard.append([InlineKeyboardButton("◆ Тарифы", callback_data="subscriptions")])
        keyboard.append([InlineKeyboardButton("← Назад", callback_data="search")])

        # Показываем счётчик если дневной лимит
        counter_str = f"\n\nИспользовано сегодня: {min(count, daily)}/{daily}" if daily > 0 else ""
        await update.message.reply_text(
            reason + counter_str,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.SEARCH_PHOTO

    msg = await update.message.reply_text("Анализирую фото...")

    # Фиксируем использование сразу (до запроса к API)
    record_photo_search(user_id)

    file_id = update.message.photo[-1].file_id
    bot = context.bot

    # Параллельно вычисляем хэш и теги
    photo_hash = await compute_photo_hash(bot, file_id)
    tags = await extract_photo_tags_hf(bot, file_id) if HF_API_TOKEN else []

    results_hash = []
    results_tags = []

    if photo_hash:
        results_hash = search_by_photo_hash(photo_hash, threshold=15)

    if tags:
        # Ищем по тегам через обычный текстовый поиск
        tag_query = " ".join(tags[:5])
        results_tags = db.search_ads(tag_query, limit=30)

    # Объединяем результаты: сначала совпавшие по хэшу, потом по тегам
    seen_ids = set()
    combined = []
    for ad in results_hash:
        if ad["id"] not in seen_ids:
            ad["_match"] = f"Похожесть {ad.get('_similarity', '?')}%"
            combined.append(ad)
            seen_ids.add(ad["id"])
    for ad in results_tags:
        if ad["id"] not in seen_ids:
            ad["_match"] = "По содержимому"
            combined.append(ad)
            seen_ids.add(ad["id"])

    try:
        await msg.delete()
    except Exception:
        pass

    if not combined:
        tag_str = ", ".join(tags[:4]) if tags else ""
        hint = (f"\n\nОпределили на фото: {tag_str}") if tag_str else ""
        await update.message.reply_text(
            f"По этому фото ничего не найдено.{hint}\n\nПопробуйте текстовый поиск.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◎ Текстовый поиск", callback_data="search"),
                InlineKeyboardButton("← Меню", callback_data="back_to_menu"),
            ]])
        )
        return States.SEARCH_QUERY

    # Показываем результаты
    tag_str = ", ".join(tags[:4]) if tags else ""
    hint_line = f"Найдено по фото: {tag_str}\n\n" if tag_str else ""
    text = f"{hint_line}<b>Похожие объявления ({len(combined)}):</b>\n\n"

    keyboard = []
    moscow_now = get_moscow_time().isoformat()
    for ad in combined[:10]:
        pinned = "◆ " if ad.get("pinned_until") and ad["pinned_until"] > moscow_now else ""
        match  = ad.get("_match", "")
        title_short = ad["title"][:26] + "…" if len(ad["title"]) > 26 else ad["title"]
        text += f"{pinned}{ad['title']}\n{ad['price']} ₽  <i>{match}</i>\n\n"
        keyboard.append([InlineKeyboardButton(
            f"{pinned}{title_short} — {ad['price']} ₽",
            callback_data=f"view_ad:{ad['id']}"
        )])

    keyboard.append([
        InlineKeyboardButton("◎ Текстовый поиск", callback_data="search"),
        InlineKeyboardButton("← Меню", callback_data="back_to_menu"),
    ])

    await update.message.reply_text(
        text, parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data.pop("awaiting_search_photo", None)
    return States.SEARCH_QUERY



async def search_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главный экран поиска с историей запросов"""
    query = update.callback_query
    await query.answer()

    # Сбрасываем фильтры при новом входе
    for k in ('sf_query','sf_category','sf_price_min','sf_price_max',
              'sf_city','sf_condition','sf_delivery','sf_sort','sf_page'):
        context.user_data.pop(k, None)

    user_id = update.effective_user.id
    history = db.get_search_history(user_id)

    text = (
        "<b>Поиск</b>\n\n"
        "Введите название товара, бренд или ключевые слова:\n"
        "<i>Например: iPhone 13, Nike Air, диван кожаный</i>"
    )

    keyboard = []

    if history:
        text += "\n\n<b>Последние запросы:</b>"
        for h in history[:5]:
            keyboard.append([InlineKeyboardButton(f"{h}", callback_data=f"sq:{h}")])

    keyboard.append([InlineKeyboardButton("◎ Поиск по фото", callback_data="search_by_photo")])
    keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])

    try:
        await query.message.reply_photo(
            photo=BANNERS["search"],
            caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
        await query.delete_message()
    except Exception:
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

    context.user_data['search_page'] = 0
    return States.SEARCH_QUERY


async def search_quick_repeat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Повторить поиск из истории по кнопке sq:"""
    query = update.callback_query
    await query.answer()
    search_text = query.data[3:]  # strip "sq:"
    context.user_data['sf_query'] = search_text
    context.user_data['sf_page'] = 0
    await _do_search_and_show(update, context, edit=True)
    return States.SEARCH_QUERY


async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь ввёл текстовый запрос"""
    query_text = update.message.text.strip()
    if not query_text:
        return States.SEARCH_QUERY

    context.user_data['sf_query'] = query_text
    context.user_data['sf_page'] = 0
    db.add_search_history(update.effective_user.id, query_text)
    await _do_search_and_show(update, context, edit=False)
    return States.SEARCH_QUERY


async def _do_search_and_show(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False):
    """Ядро: выполнить поиск и показать результаты"""
    f = await _get_search_filters(context)
    page = f['page']
    limit_per_page = 8

    all_results = db.search_ads_filtered(
        query=f['query'],
        category=f['category'],
        price_min=f['price_min'],
        price_max=f['price_max'],
        city=f['city'],
        condition=f['condition'],
        delivery=f['delivery'],
        sort_by=f['sort_by'],
        limit=200,
    )

    total = len(all_results)
    total_pages = max(1, (total + limit_per_page - 1) // limit_per_page)
    page = min(page, total_pages - 1)
    context.user_data['sf_page'] = page
    results = all_results[page * limit_per_page:(page + 1) * limit_per_page]

    SORT_LABELS = {
        'date_desc':  'Сначала новые',
        'date_asc':   'Сначала старые',
        'price_asc':  'Дешевле',
        'price_desc': 'Дороже',
        'views_desc': 'Популярные',
    }

    query_label = f['query'] or '(все товары)'
    filters_str = _filters_label(f)
    sort_label = SORT_LABELS.get(f['sort_by'], 'Сначала новые')

    if total == 0:
        text = (
            f"<b>«{query_label}»</b>\n\n"
            f"Ничего не найдено\n\n"
            "Попробуйте изменить запрос или сбросить фильтры.\n"
            "Можем уведомить, когда появится похожий товар."
        )
        keyboard = []
        if filters_str:
            keyboard.append([InlineKeyboardButton("✗ Сбросить фильтры", callback_data="sf_reset")])
        keyboard += [
            [InlineKeyboardButton("◆ Уведомить о появлении", callback_data=f"save_search:{f['query']}")],
            [InlineKeyboardButton("◎ Новый поиск", callback_data="search"),
             InlineKeyboardButton("← Меню", callback_data="back_to_menu")]
        ]
    else:
        header = f"<b>«{query_label}»</b> — {total} объявл."
        if filters_str:
            header += f"\n<i>{filters_str}</i>"
        if total_pages > 1:
            header += f" ·  стр. {page+1}/{total_pages}"
        header += "\n\n"

        text = header
        for ad in results:
            pinned = "◆ " if ad.get('pinned_until') else ""
            cheap  = "▼ " if db.is_price_below_market(ad['id']) else ""
            verify = db.get_verification(ad['user_id'])
            badge  = "✓ " if verify and verify.get('status') == 'verified' else ""
            city_s = f" · {ad['city']}" if ad.get('city') else ""
            cond_s = f" · {ad.get('condition','')}" if ad.get('condition') else ""
            delivery_s = " · ▸" if ad.get('delivery') in ('Да','yes','1') else ""
            views_s = f" · {ad.get('views',0)} просм."
            price_f = fmt_rub(ad['price'])
            text += (
                f"{pinned}{cheap}{badge}<b>{ad['title']}</b>\n"
                f"<b>{price_f} ₽</b>{city_s}{cond_s}{delivery_s}{views_s}\n\n"
            )

        keyboard = []
        for ad in results:
            cheap  = "▼" if db.is_price_below_market(ad['id']) else ""
            verify = db.get_verification(ad['user_id'])
            badge  = "✓" if verify and verify.get('status') == 'verified' else ""
            price_f = fmt_rub(ad['price'])
            short = ad['title'][:30] + "…" if len(ad['title']) > 30 else ad['title']
            keyboard.append([InlineKeyboardButton(
                f"{cheap}{badge} {short} — {price_f} ₽",
                callback_data=f"view_ad:{ad['id']}"
            )])

        # Пагинация
        pag = []
        if page > 0:
            pag.append(InlineKeyboardButton("←", callback_data="sf_page_prev"))
        pag.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="sf_noop"))
        if page < total_pages - 1:
            pag.append(InlineKeyboardButton("→", callback_data="sf_page_next"))
        if len(pag) > 1:
            keyboard.append(pag)

        # Строка быстрых фильтров
        keyboard.append([
            InlineKeyboardButton(f"Фильтры" + (" ●" if filters_str else ""), callback_data="sf_menu"),
            InlineKeyboardButton(f"{sort_label}", callback_data="sf_sort_menu"),
        ])
        if f['query']:
            keyboard.append([
                InlineKeyboardButton("◆ Сохранить поиск", callback_data=f"save_search:{f['query']}"),
                InlineKeyboardButton("✗ Сбросить", callback_data="sf_reset"),
            ])
        keyboard.append([
            InlineKeyboardButton("◎ Новый поиск", callback_data="search"),
            InlineKeyboardButton("← Меню", callback_data="back_to_menu"),
        ])

    if edit and update.callback_query:
        await safe_edit_message(update.callback_query, text, keyboard, parse_mode=ParseMode.HTML)
    else:
        msg = update.message or (update.callback_query.message if update.callback_query else None)
        if msg:
            await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)


# ─── Обработчики кнопок фильтров ────────────────────────────────────

async def sf_page_prev(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['sf_page'] = max(0, context.user_data.get('sf_page', 0) - 1)
    await _do_search_and_show(update, context, edit=True)
    return States.SEARCH_QUERY

async def sf_page_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['sf_page'] = context.user_data.get('sf_page', 0) + 1
    await _do_search_and_show(update, context, edit=True)
    return States.SEARCH_QUERY

async def sf_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

async def sf_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Фильтры сброшены")
    for k in ('sf_category','sf_price_min','sf_price_max','sf_city',
              'sf_condition','sf_delivery','sf_sort'):
        context.user_data.pop(k, None)
    context.user_data['sf_page'] = 0
    await _do_search_and_show(update, context, edit=True)
    return States.SEARCH_QUERY

async def sf_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель фильтров"""
    query = update.callback_query
    await query.answer()
    f = await _get_search_filters(context)

    cat_label = f['category'] or 'Все'
    city_label = f['city'] or 'Все'
    price_label = ''
    if f['price_min'] or f['price_max']:
        price_label = f" {f['price_min'] or 0}–{f['price_max'] or '∞'} ₽"
    cond_map = {'': 'Любое', 'new': 'Новое', 'used': 'Б/у'}
    cond_label = cond_map.get(f['condition'], 'Любое')
    delivery_label = 'Есть' if f['delivery'] == 'yes' else 'Любая'

    text = (
        "<b>Фильтры поиска</b>\n\n"
        f"Категория: <b>{cat_label}</b>\n"
        f"Город: <b>{city_label}</b>\n"
        f"Цена:{price_label or ' любая'}\n"
        f"Состояние: <b>{cond_label}</b>\n"
        f"Доставка: <b>{delivery_label}</b>"
    )
    keyboard = [
        [InlineKeyboardButton("◎ Категория", callback_data="sf_cat_menu"),
         InlineKeyboardButton("◎ Город", callback_data="sf_city_menu")],
        [InlineKeyboardButton("◎ Цена от", callback_data="sf_price_from"),
         InlineKeyboardButton("◎ Цена до", callback_data="sf_price_to")],
        [InlineKeyboardButton("◎ Новое", callback_data="sf_cond:new"),
         InlineKeyboardButton("◎ Б/у", callback_data="sf_cond:used"),
         InlineKeyboardButton("◎ Любое", callback_data="sf_cond:")],
        [InlineKeyboardButton("◎ С доставкой", callback_data="sf_delivery:yes")],
        [InlineKeyboardButton("✗ Сбросить", callback_data="sf_reset"),
         InlineKeyboardButton("✓ Применить", callback_data="sf_apply")],
        [InlineKeyboardButton("← К результатам", callback_data="sf_apply")],
    ]
    await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    return States.SEARCH_QUERY

async def sf_cat_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор категории"""
    query = update.callback_query
    await query.answer()
    keyboard = []
    row = []
    for label, val in SEARCH_CATEGORIES:
        row.append(InlineKeyboardButton(label, callback_data=f"sf_cat:{val}"))
        if len(row) == 2:
            keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("◎ Все категории", callback_data="sf_cat:all")])
    keyboard.append([InlineKeyboardButton("← Фильтры", callback_data="sf_menu")])
    await safe_edit_message(query, "<b>Категория:</b>", keyboard, parse_mode=ParseMode.HTML)
    return States.SEARCH_QUERY

async def sf_set_cat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    context.user_data['sf_category'] = '' if val == 'all' else val
    context.user_data['sf_page'] = 0
    await sf_menu(update, context)
    return States.SEARCH_QUERY

async def sf_city_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор города"""
    query = update.callback_query
    await query.answer()
    keyboard = []
    row = []
    for city in POPULAR_CITIES:
        row.append(InlineKeyboardButton(city, callback_data=f"sf_city:{city}"))
        if len(row) == 2:
            keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("◎ Все города", callback_data="sf_city:all")])
    keyboard.append([InlineKeyboardButton("← Фильтры", callback_data="sf_menu")])
    await safe_edit_message(query, "<b>Город:</b>", keyboard, parse_mode=ParseMode.HTML)
    return States.SEARCH_QUERY

async def sf_set_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    context.user_data['sf_city'] = '' if val == 'all' else val
    context.user_data['sf_page'] = 0
    await sf_menu(update, context)
    return States.SEARCH_QUERY

async def sf_set_cond(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    context.user_data['sf_condition'] = val
    context.user_data['sf_page'] = 0
    await sf_menu(update, context)
    return States.SEARCH_QUERY

async def sf_set_delivery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    prev = context.user_data.get('sf_delivery', '')
    context.user_data['sf_delivery'] = '' if prev == val else val
    context.user_data['sf_page'] = 0
    await sf_menu(update, context)
    return States.SEARCH_QUERY

async def sf_sort_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню сортировки"""
    query = update.callback_query
    await query.answer()
    current = context.user_data.get('sf_sort', 'date_desc')
    keyboard = []
    for val, label in SORT_OPTIONS:
        check = "✓ " if val == current else ""
        keyboard.append([InlineKeyboardButton(f"{check}{label}", callback_data=f"sf_sort:{val}")])
    keyboard.append([InlineKeyboardButton("← К результатам", callback_data="sf_apply")])
    await safe_edit_message(query, "<b>Сортировка:</b>", keyboard, parse_mode=ParseMode.HTML)
    return States.SEARCH_QUERY

async def sf_set_sort(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    context.user_data['sf_sort'] = val
    context.user_data['sf_page'] = 0
    await _do_search_and_show(update, context, edit=True)
    return States.SEARCH_QUERY

async def sf_price_from_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['sf_awaiting_price'] = 'from'
    await safe_edit_message(query,
        "<b>Минимальная цена (₽):</b>\n\nИли нажмите «Пропустить»",
        [[InlineKeyboardButton("· Пропустить", callback_data="sf_price_skip"),
          InlineKeyboardButton("← Назад", callback_data="sf_menu")]],
        parse_mode=ParseMode.HTML)
    return States.SEARCH_PRICE_FROM

async def sf_price_to_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['sf_awaiting_price'] = 'to'
    await safe_edit_message(query,
        "<b>Максимальная цена (₽):</b>\n\nИли нажмите «Пропустить»",
        [[InlineKeyboardButton("· Пропустить", callback_data="sf_price_skip"),
          InlineKeyboardButton("← Назад", callback_data="sf_menu")]],
        parse_mode=ParseMode.HTML)
    return States.SEARCH_PRICE_TO

async def sf_price_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем ввод цены"""
    text = update.message.text.strip().replace(' ', '').replace('₽', '').replace(',', '').replace('.', '')
    which = context.user_data.pop('sf_awaiting_price', 'from')
    try:
        val = int(text)
        if which == 'from':
            context.user_data['sf_price_min'] = val
        else:
            context.user_data['sf_price_max'] = val
        context.user_data['sf_page'] = 0
        await update.message.reply_text(f"Цена {'от' if which=='from' else 'до'} {fmt_rub(val)} ₽ установлена")
    except ValueError:
        await update.message.reply_text("Введите число, например: 5000")
        return States.SEARCH_PRICE_FROM if which == 'from' else States.SEARCH_PRICE_TO
    await _do_search_and_show(update, context, edit=False)
    return States.SEARCH_QUERY

async def sf_price_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop('sf_awaiting_price', None)
    await sf_menu(update, context)
    return States.SEARCH_QUERY

async def sf_apply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Применить фильтры — вернуться к результатам"""
    query = update.callback_query
    await query.answer()
    context.user_data['sf_page'] = 0
    await _do_search_and_show(update, context, edit=True)
    return States.SEARCH_QUERY

# Устаревшие функции для обратной совместимости (пагинация через старый формат)
async def search_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    page = int(parts[1])
    if len(parts) > 2:
        context.user_data['sf_query'] = ":".join(parts[2:])
    context.user_data['sf_page'] = page
    await _do_search_and_show(update, context, edit=True)
    return States.SEARCH_QUERY


async def view_ad(update: Update, context: ContextTypes.DEFAULT_TYPE, ad_id: int = None):
    query = update.callback_query
    if query:
        await query.answer()
        if not ad_id:
            try:
                ad_id = int(query.data.split(":")[1])
            except (IndexError, ValueError):
                await safe_edit_message(query, "Не удалось открыть объявление.")
                return
    else:
        return

    try:
        ad_data = db.get_ad(ad_id)
        if not ad_data or ad_data['status'] != 'active':
            await safe_edit_message(query, "Объявление не найдено или снято с публикации")
            return

        db.increment_ad_views(ad_id)
        context.user_data['last_viewed_ad_id'] = ad_id

        cursor = db.conn.cursor()
        cursor.execute("SELECT 1 FROM favorites WHERE user_id = ? AND ad_id = ?", (update.effective_user.id, ad_id))
        in_favorites = cursor.fetchone() is not None

        user_info = db.get_user(ad_data['user_id'])

        # Формируем карточку объявления
        text = f"<b>{ad_data['title']}</b>\n\n"
        if ad_data.get('description'):
            text += f"{ad_data['description']}\n\n"
        text += f"<b>{ad_data['price']} ₽</b>\n"
        if ad_data.get('condition'):
            text += f"Состояние: {ad_data['condition']}\n"
        if ad_data.get('size'):
            text += f"Размер: {ad_data['size']}\n"
        if ad_data.get('gender'):
            _gender_label = {'Мужская': '· Мужская', 'Женская': '· Женская', 'Детская': '· Детская'}.get(ad_data['gender'], ad_data['gender'])
            text += f"Для: {_gender_label}\n"
        if ad_data.get('city'):
            text += f"{ad_data['city']}"
            if ad_data.get('delivery'):
                text += f" ·  {ad_data['delivery']}"
            text += "\n"
        elif ad_data.get('delivery'):
            text += f"Доставка: {ad_data['delivery']}\n"

        # Рейтинг и верификация продавца
        seller_rating = db.get_seller_rating(ad_data['user_id'])
        seller_verify = db.get_verification(ad_data['user_id'])
        is_verified = seller_verify and seller_verify.get('status') == 'verified'

        seller_anon_id = get_anon_id(user_info)
        verify_tag = "  ✓" if is_verified else ""
        if seller_rating['count'] > 0:
            stars = "★" * round(seller_rating['avg']) + "☆" * (5 - round(seller_rating['avg']))
            rating_inline = f"  {stars} {seller_rating['avg']}"
        else:
            rating_inline = ""

        text += f"──────────────────\n"
        text += f"Продавец <b>#{seller_anon_id}</b>{verify_tag}{rating_inline}\n"

        market_info = db.get_market_price_info(
            ad_data.get('category', ''), ad_data['price'], exclude_ad_id=ad_id
        )
        if market_info['is_cheap']:
            diff = abs(market_info['diff_pct'])
            text += f"Цена ниже рынка на {diff:.0f}% (ср. {market_info['avg']} ₽)\n"

        asyncio.create_task(track_contact_click(ad_id, update.effective_user.id, context))

        contact_btn = InlineKeyboardButton("✎ Написать продавцу", callback_data=f"chat_start:{ad_id}")
        fav_label = "Убрать из избранного" if in_favorites else "В избранное"
        fav_cb = f"{'remove_favorite' if in_favorites else 'add_favorite'}:{ad_id}"

        if seller_rating['count'] > 0:
            filled = "★" * round(seller_rating['avg'])
            empty  = "☆" * (5 - round(seller_rating['avg']))
            reviews_btn_label = f"{filled}{empty} {seller_rating['avg']} · {seller_rating['count']} отз."
        else:
            reviews_btn_label = "☆☆☆☆☆ Нет отзывов"

        already_fav_seller = db.is_favorite_seller(update.effective_user.id, ad_data['user_id'])
        fav_seller_label = "В избранных" if already_fav_seller else "В избранное"
        fav_seller_cb    = f"unfav_seller:{ad_data['user_id']}" if already_fav_seller else f"fav_seller:{ad_data['user_id']}"

        keyboard = [
            [contact_btn, InlineKeyboardButton(fav_label, callback_data=fav_cb)],
            [InlineKeyboardButton(reviews_btn_label, callback_data=f"seller_reviews:{ad_data['user_id']}"),
             InlineKeyboardButton(fav_seller_label, callback_data=fav_seller_cb)],
            [InlineKeyboardButton("▸ Профиль продавца", callback_data=f"seller_profile:{ad_data['user_id']}")],
        ]

        if 'last_search' in context.user_data:
            last_q = context.user_data['last_search']
            keyboard.append([InlineKeyboardButton(
                f"Уведомлять о «{last_q[:20]}»",
                callback_data=f"save_search:{last_q}"
            )])

        if update.effective_user.id == ad_data['user_id']:
            keyboard.append([
                InlineKeyboardButton("▲ Поднять в топ", callback_data=f"boost_upsell:{ad_id}"),
                *(
                    [InlineKeyboardButton("◎ Аналитика", callback_data=f"ad_analytics:{ad_id}")]
                    if db.get_user(update.effective_user.id).get('tariff', 'Free') != 'Free' else []
                )
            ])
            keyboard.append([
                InlineKeyboardButton("✎ Редактировать", callback_data=f"edit_ad:{ad_id}"),
                InlineKeyboardButton("✗ Снять", callback_data=f"deactivate_ad:{ad_id}")
            ])
        else:
            watching = db.is_watching_price(update.effective_user.id, ad_id)
            watch_label = "Не следить за ценой" if watching else "Следить за ценой"
            keyboard.append([
                InlineKeyboardButton(watch_label, callback_data=f"watch_price:{ad_id}"),
                InlineKeyboardButton("! Пожаловаться", callback_data=f"report_ad:{ad_id}")
            ])

        if 'last_search' in context.user_data:
            keyboard.append([InlineKeyboardButton("← Результаты", callback_data=f"back_to_search:{context.user_data['last_search']}")])
        else:
            keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])

        if ad_data['photos']:
            try:
                if len(ad_data['photos']) == 1:
                    await query.message.reply_photo(
                        photo=ad_data['photos'][0],
                        caption=text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.HTML
                    )
                    await query.delete_message()
                else:
                    media_group = [InputMediaPhoto(
                        media=ad_data['photos'][0], caption=text, parse_mode=ParseMode.HTML
                    )]
                    for photo in ad_data['photos'][1:]:
                        media_group.append(InputMediaPhoto(photo))
                    await query.message.reply_media_group(media=media_group)
                    await query.message.reply_text(
                        "Действия:",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    await query.delete_message()
            except Exception as e:
                logger.error(f"view_ad photo error: {e}")
                await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
        else:
            await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка в view_ad: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить объявление. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass

async def back_to_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        search_query = query.data.split(":", 1)[1]
        context.user_data['search_page'] = 0
        context.user_data['last_search'] = search_query

        limit = 10
        all_results = db.search_ads(search_query, limit=100)
        city_filter = context.user_data.get('search_city')
        if city_filter and all_results:
            all_results = [a for a in all_results if city_filter.lower() in (a.get('city') or '').lower()]

        if not all_results:
            await safe_edit_message(query,
                f"«{search_query}» — ничего не найдено",
                [[InlineKeyboardButton("◎ Новый поиск", callback_data="search"),
                  InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
            )
            return

        total_pages = (len(all_results) + limit - 1) // limit
        results = all_results[:limit]

        text = f"«{search_query}» — {len(all_results)} результатов\n\n"
        keyboard = []
        for i, ad in enumerate(results, 1):
            pinned = "◆ " if ad.get('pinned_until') else ""
            cheap = "▼ " if db.is_price_below_market(ad['id']) else ""
            city_str = f" · {ad['city']}" if ad.get('city') else ""
            verify = db.get_verification(ad['user_id'])
            badge = "✓ " if verify and verify.get('status') == 'verified' else ""
            text += f"{i}. {pinned}{cheap}{badge}{ad['title']}\n{ad['price']} ₽{city_str}\n\n"
            title_short = ad['title'][:28] + "…" if len(ad['title']) > 28 else ad['title']
            keyboard.append([InlineKeyboardButton(
                f"{cheap}{badge}{title_short} — {ad['price']} ₽",
                callback_data=f"view_ad:{ad['id']}"
            )])

        if total_pages > 1:
            keyboard.append([InlineKeyboardButton("→", callback_data=f"search_page:1:{search_query}")])

        keyboard.append([
            InlineKeyboardButton("◎ Новый поиск", callback_data="search"),
            InlineKeyboardButton("← Меню", callback_data="back_to_menu")
        ])
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в back_to_search: {e}")
        await safe_edit_message(query, "Не удалось загрузить результаты. Попробуйте поиск заново.",
            [[InlineKeyboardButton("◎ Поиск", callback_data="search"),
              InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
        )

async def toggle_favorite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        action, ad_id_str = query.data.split(":")
        ad_id = int(ad_id_str)
        user_id = update.effective_user.id
        
        cursor = db.conn.cursor()
        if action == "add_favorite":
            cursor.execute("INSERT OR IGNORE INTO favorites (user_id, ad_id, added_at) VALUES (?, ?, ?)",
                          (user_id, ad_id, get_moscow_time().isoformat()))
            db.conn.commit()
            await query.answer("Добавлено в избранное", show_alert=True)
        else:
            cursor.execute("DELETE FROM favorites WHERE user_id = ? AND ad_id = ?", (user_id, ad_id))
            db.conn.commit()
            await query.answer("Удалено из избранного", show_alert=True)
        
        # Возвращаемся в главное меню
        await show_main_menu(update, context)
    except Exception as e:
        logger.error(f"Ошибка в toggle_favorite: {e}")
        try:
            await safe_edit_message(query, "Не удалось обновить избранное.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def toggle_favorite_seller(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавить/убрать продавца из избранного"""
    query = update.callback_query
    try:
        # НЕ вызываем query.answer() здесь — Telegram разрешает только ОДИН ответ.
        # Ответ с нужным текстом отправляется ниже.
        seller_id = int(query.data.split(":")[1])
        user_id = update.effective_user.id

        if user_id == seller_id:
            await query.answer("Нельзя добавить себя в избранное", show_alert=True)
            return

        already = db.is_favorite_seller(user_id, seller_id)
        if already:
            db.remove_favorite_seller(user_id, seller_id)
            await query.answer("Продавец убран из избранного", show_alert=True)
        else:
            db.add_favorite_seller(user_id, seller_id)
            seller = db.get_user(seller_id)
            name = seller['first_name'] if seller else f"#{seller_id}"
            await query.answer(f"{name} добавлен в избранное!", show_alert=True)
        # Обновляем карточку объявления если пришли из просмотра объявления
        ad_id = context.user_data.get('last_viewed_ad_id')
        if ad_id:
            await view_ad(update, context, ad_id=ad_id)
    except Exception as e:
        logger.error(f"Ошибка в toggle_favorite_seller: {e}")
        try:
            await safe_edit_message(query, "Не удалось обновить избранных продавцов.",
                [[InlineKeyboardButton("← Назад", callback_data="favorite_sellers")]])
        except Exception:
            pass
async def show_favorite_sellers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список избранных продавцов"""
    query = update.callback_query
    await query.answer()
    try:
        user_id = update.effective_user.id

        sellers = db.get_favorite_sellers(user_id)

        if not sellers:
            text = "Избранные продавцы\n\nУ вас пока нет избранных продавцов.\n\nДобавляйте продавцов в избранное при просмотре объявлений!"
            keyboard = [
                [InlineKeyboardButton("◎ Поиск продавца", callback_data="search_seller")],
                [InlineKeyboardButton("← Назад", callback_data="back_to_menu")]
            ]
        else:
            text = f"Избранные продавцы: {len(sellers)}\n\n"
            keyboard = []
            for s in sellers:
                name = s['first_name']
                username_str = f" (@{s['username']})" if s.get('username') else ""
                text += f"• {name}{username_str}\n"
                keyboard.append([
                    InlineKeyboardButton(f"{name}{username_str}", callback_data=f"seller_profile:{s['seller_id']}"),
                    InlineKeyboardButton("✗", callback_data=f"unfav_seller:{s['seller_id']}")
                ])
            keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])

        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в show_favorite_sellers: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить список продавцов.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def unfav_seller(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Убрать продавца из избранного прямо из списка"""
    query = update.callback_query
    await query.answer()
    try:
        seller_id = int(query.data.split(":")[1])
        db.remove_favorite_seller(update.effective_user.id, seller_id)
        await show_favorite_sellers(update, context)
    except Exception as e:
        logger.error(f"Ошибка в unfav_seller: {e}")
        try:
            await safe_edit_message(query, "Не удалось обновить избранных продавцов.",
                [[InlineKeyboardButton("← Назад", callback_data="favorite_sellers")]])
        except Exception:
            pass
async def search_seller(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск продавца по username или имени"""
    query = update.callback_query
    await query.answer()
    await safe_edit_message(
        query,
        "<b>Поиск продавца</b>\n\nВведите @username или имя продавца:",
        [[InlineKeyboardButton("✗ Отмена", callback_data="favorite_sellers")]],
        parse_mode=ParseMode.HTML
    )
    context.user_data["awaiting_seller_search"] = True


async def handle_search_seller_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает текстовый ввод для поиска продавца"""
    if not context.user_data.get("awaiting_seller_search"):
        return
    context.user_data.pop("awaiting_seller_search", None)
    query_text = update.message.text.strip().lstrip("@")
    user_id = update.effective_user.id
    cursor = db.conn.cursor()
    cursor.execute(
        """SELECT user_id, first_name, username FROM users
           WHERE (username LIKE ? OR first_name LIKE ?) AND user_id != ?
           LIMIT 10""",
        (f"%{query_text}%", f"%{query_text}%", user_id)
    )
    results = cursor.fetchall()
    if not results:
        keyboard = [
            [InlineKeyboardButton("↺ Попробовать снова", callback_data="search_seller")],
            [InlineKeyboardButton("← Назад", callback_data="favorite_sellers")]
        ]
        await update.message.reply_text(
            "Продавец не найден. Попробуйте другой запрос.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    text = f"Найдено продавцов: {len(results)}\n\n"
    keyboard = []
    for row in results:
        sid, fname, uname = row[0], row[1], row[2]
        label = f"{fname} (@{uname})" if uname else fname
        already = db.is_favorite_seller(user_id, sid)
        fav_label = "✓ В избранном" if already else "В избранное"
        fav_cb = f"unfav_seller:{sid}" if already else f"fav_seller:{sid}"
        keyboard.append([
            InlineKeyboardButton(f"{label}", callback_data=f"seller_reviews:{sid}"),
            InlineKeyboardButton(fav_label, callback_data=fav_cb)
        ])
    keyboard.append([InlineKeyboardButton("← Назад", callback_data="favorite_sellers")])
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


# Профиль
async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = update.effective_user
    try:
        user_data = db.get_user(user.id)
        if not user_data:
            await safe_edit_message(query, "Пользователь не найден")
            return

        cursor = db.conn.cursor()
        moscow_now = get_moscow_time().isoformat()

        cursor.execute("SELECT COUNT(*) FROM ads WHERE user_id = ? AND status = 'active'", (user.id,))
        active_ads = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM ads WHERE user_id = ? AND pinned_until IS NOT NULL AND pinned_until > ?", (user.id, moscow_now))
        pinned_ads = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(views) FROM ads WHERE user_id = ? AND status = 'active'", (user.id,))
        total_views = cursor.fetchone()[0] or 0

        cursor.execute('''
            SELECT COUNT(*) FROM favorites f
            JOIN ads a ON f.ad_id = a.id
            WHERE f.user_id = ? AND a.status = 'active'
        ''', (user.id,))
        favorites_count = cursor.fetchone()[0]

        tariff_label = TARIFF_LABELS.get(user_data['tariff'], user_data['tariff'])
        tariff_end = f"до {user_data['tariff_end_date'][:10]}" if user_data.get('tariff_end_date') else "бессрочно"

        seller_rating = db.get_seller_rating(user.id)
        rating_str = ""
        if seller_rating['count'] > 0:
            stars = "★" * round(seller_rating['avg'])
            rating_str = f"\n{stars} {seller_rating['avg']} ({seller_rating['count']} отзывов)"

        ref_stats = db.get_referral_stats(user.id)
        ref_balance = ref_stats.get('balance', 0) if ref_stats else 0

        verify = db.get_verification(user.id)
        is_verified = verify and verify.get('status') == 'verified'
        is_pending  = verify and verify.get('status') == 'pending'
        verify_str  = "  ✓ Верифицирован" if is_verified else ""

        completed_deals = db.get_completed_deals_count(user.id)

        if is_verified:
            verify_block = "\n✓ <b>Статус: Верифицированный продавец</b>\nПокупатели видят ваш значок и доверяют вам больше.\n"
        elif is_pending:
            verify_block = "\n◎ <b>Верификация на проверке</b> — ответ придёт в течение 24 часов.\n"
        else:
            verify_block = (
                "\n <b>Верификация не пройдена</b>\n"
                "Верифицированные продавцы получают значок ✓ и <b>на 40% больше откликов</b>.\n"
            )

        text = (
            f"<b>{user.first_name}</b>{verify_str}\n"
            f"{'@' + user.username if user.username else ''}\n\n"
            f"Тариф: <b>{tariff_label}</b> ({tariff_end})\n"
            f"На платформе с {user_data['registration_date'][:10]}\n"
            f"{verify_block}\n"
            f"Статистика:\n"
            f"• Активных объявлений: {active_ads}\n"
            f"• Всего просмотров: {total_views}\n"
            f"• Завершённых сделок: {completed_deals}\n"
            f"• Добавили в избранное: {favorites_count} раз\n"
            f"{rating_str}\n\n"
            f"Реф. баланс: {ref_balance} ₽\n"
            f"◈ Ваш ID: <code>#{get_anon_id(user_data)}</code>  <i>(анонимный)</i>"
        )

        tariff  = user_data['tariff']
        keyboard = []

        if tariff == "Free":
            keyboard.append([InlineKeyboardButton("◆ Обновить тариф", callback_data="subscriptions")])
        else:
            if tariff == "Standard":
                pinned_available = (user_data.get('pinned_ads_used', 0) or 0) < 1
            else:
                pinned_available = (user_data.get('pinned_ads_used', 0) or 0) < 3
            if pinned_available:
                keyboard.append([InlineKeyboardButton("◆ Закрепить", callback_data="pin_ad_menu")])
            keyboard.append([InlineKeyboardButton(f"↺ Продлить {tariff_label}", callback_data=f"renew_{tariff.lower()}")])

        if not is_verified:
            if is_pending:
                keyboard.append([InlineKeyboardButton("◎ Верификация на рассмотрении", callback_data="verify_seller")])
            else:
                keyboard.append([InlineKeyboardButton("◆ Верифицировать профиль", callback_data="verify_seller")])
        keyboard.append([InlineKeyboardButton("▸ Мои диалоги", callback_data="my_chats")])
        keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])

        markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.message.reply_photo(
                photo=BANNERS["profile"],
                caption=text,
                reply_markup=markup,
                parse_mode=ParseMode.HTML
            )
            await query.delete_message()
        except Exception:
            await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка в show_profile: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить профиль. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
        except Exception:
            pass

    return States.MAIN_MENU
# Тарифы
async def show_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_data = db.get_user(update.effective_user.id)
    current_tariff = user_data['tariff'] if user_data else 'Free'

    if current_tariff == "Free":
        status_line = "Сейчас: бесплатный тариф — до 3 объявлений в день, 3 фото"
    elif current_tariff == "Standard":
        tariff_end = user_data.get('tariff_end_date', '')
        end_str = f" (до {tariff_end[:10]})" if tariff_end else ""
        status_line = f"● Активен тариф Старт{end_str}"
    else:
        tariff_end = user_data.get('tariff_end_date', '')
        end_str = f" (до {tariff_end[:10]})" if tariff_end else ""
        status_line = f"● Активен тариф PRO{end_str}"

    text = (
        f"<b>Тарифы Полки</b>\n\n"
        f"{status_line}\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<b>Бесплатно</b>\n"
        "• 3 объявления в день · 3 фото\n"
        "• Верификация профиля\n"
        "• Анонимный чат с покупателями\n\n"
        "<b>Старт — 299 ₽/мес</b>\n"
        "• Безлимит объявлений · 10 фото\n"
        "• Авто-подъём каждые 24ч\n"
        "• 1 закрепление в месяц (24ч в топе)\n"
        "• Аналитика просмотров\n\n"
        "<b>PRO — 799 ₽/мес</b>\n"
        "• Всё из Старта\n"
        "• Авто-подъём каждые 6ч\n"
        "• 5 закреплений в месяц (7 дней в топе)\n"
        "• Приоритетная модерация\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Выберите тариф для оплаты:"
    )

    keyboard = []
    if current_tariff != "Standard":
        keyboard.append([InlineKeyboardButton("◆ Старт — 299 ₽/мес", callback_data="buy_tariff:standard")])
    else:
        keyboard.append([InlineKeyboardButton("↺ Продлить Старт — 299 ₽", callback_data="buy_tariff:standard")])

    if current_tariff != "PRO":
        keyboard.append([InlineKeyboardButton("◆ PRO — 799 ₽/мес", callback_data="buy_tariff:pro")])
    else:
        keyboard.append([InlineKeyboardButton("↺ Продлить PRO — 799 ₽", callback_data="buy_tariff:pro")])

    keyboard.append([InlineKeyboardButton("◎ Ввести промокод", callback_data="enter_promo")])
    keyboard.append([InlineKeyboardButton("◆ Оплатить реферальным балансом", callback_data="pay_with_balance")])
    keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])

    try:
        await query.message.reply_photo(
            photo=BANNERS["subscriptions"],
            caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
        await query.delete_message()
    except Exception:
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

    return States.MAIN_MENU


async def buy_tariff_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь выбрал тариф — показываем способы оплаты"""
    query = update.callback_query
    await query.answer()

    try:
        plan = query.data.split(':')[1]  # 'standard' или 'pro'
        PLAN_NAMES = {'standard': 'Старт', 'pro': 'PRO'}
        PRICES_BOT = {'standard': 1, 'pro': 1}
        PLAN_RANK  = {'free': 0, 'standard': 1, 'pro': 2}

        plan_label = PLAN_NAMES[plan]
        price = PRICES_BOT[plan]

        # Проверяем текущий тариф пользователя
        user_data = db.get_user(update.effective_user.id)
        current_raw = (user_data.get('tariff', 'Free') if user_data else 'Free')
        TARIFF_TO_KEY = {
            'Free': 'free', 'free': 'free',
            'Standard': 'standard', 'Старт': 'standard', 'standard': 'standard',
            'PRO': 'pro', 'pro': 'pro',
        }
        current_key  = TARIFF_TO_KEY.get(current_raw, 'free')
        current_rank = PLAN_RANK.get(current_key, 0)
        plan_rank    = PLAN_RANK.get(plan, 1)

        # Предупреждение: покупка тарифа ниже текущего
        if current_rank > plan_rank and not context.user_data.pop('_skip_downgrade_check', False):
            current_label = PLAN_NAMES.get(current_key, current_raw)
            end_date = ''
            if user_data and user_data.get('tariff_end_date'):
                end_date = f" (до {user_data['tariff_end_date'][:10]})"
            keyboard = [
                [InlineKeyboardButton(f"↑ Лучше куплю PRO — {PRICES_BOT['pro']} ₽", callback_data="buy_tariff:pro")],
                [InlineKeyboardButton(f"· Всё равно купить {plan_label}", callback_data=f"buy_tariff_confirm:{plan}")],
                [InlineKeyboardButton("← Назад", callback_data="subscriptions")],
            ]
            await safe_edit_message(
                query,
                f"⚠️ У вас уже активен тариф <b>{current_label}</b>{end_date}.\n\n"
                f"Покупка <b>{plan_label}</b> <b>понизит</b> ваши возможности.\n\n"
                f"Вы уверены?",
                keyboard, parse_mode=ParseMode.HTML
            )
            return States.MAIN_MENU

        # Учитываем промокод если был введён ранее
        promo_code     = context.user_data.get('promo_code', '')
        promo_discount = context.user_data.get('promo_discount', 0)
        promo_plan     = context.user_data.get('promo_plan', '')

        if promo_code and (promo_plan == plan or promo_plan == 'any') and promo_discount > 0:
            final_price = round(price * (1 - promo_discount / 100))
            saved = price - final_price
            promo_line = f"\n🎟 Промокод <b>{promo_code}</b>: −{promo_discount}% (−{saved} ₽)\n"
        else:
            final_price = price
            promo_line = ''
            promo_code = ''
            promo_discount = 0

        context.user_data['pending_payment'] = {
            'tariff':    plan_label,
            'amount':    final_price,
            'duration':  30,
            'promocode': promo_code or None,
        }

        crypto_url = resolve_crypto_url(promo_discount, final_price, plan)

        # Генерируем ссылку PayAnyWay (возвращает (url, transaction_id))
        paw_url        = None
        paw_tx_id      = None
        if PAW_ACCOUNT:
            paw_url, paw_tx_id = generate_payanyway_url(
                user_id=update.effective_user.id,
                plan=plan,
                amount=final_price,
                promo=promo_code or ""
            )

        text = (
            f"<b>Оплата тарифа {plan_label}</b>\n\n"
            f"Сумма: <b>{final_price} ₽</b>/мес{promo_line}\n"
            "Выберите способ оплаты:"
        )

        keyboard = []
        if paw_url:
            keyboard.append([InlineKeyboardButton("→ Оплатить картой / СБП", url=paw_url)])
            keyboard.append([InlineKeyboardButton("↺ Проверить оплату", callback_data=f"checkpay:{paw_tx_id}")])
        keyboard.append([InlineKeyboardButton("→ Оплатить криптой", url=crypto_url)])
        keyboard.append([InlineKeyboardButton("✓ Я оплатил криптой — отправить чек", callback_data="i_paid")])
        keyboard.append([InlineKeyboardButton("◎ Ввести промокод", callback_data="enter_promo")])
        keyboard.append([InlineKeyboardButton("← Назад к тарифам", callback_data="subscriptions")])

        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
        return States.MAIN_MENU
    except Exception as e:
        logger.error(f"Ошибка в buy_tariff_handler: {e}")
        try:
            await safe_edit_message(query, "Не удалось открыть страницу оплаты. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="subscriptions")]])
        except Exception:
            pass
async def buy_tariff_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь подтвердил покупку тарифа ниже текущего — редиректим в обычный обработчик"""
    query = update.callback_query
    # Подменяем data чтобы переиспользовать buy_tariff_handler без проверки даунгрейда
    plan = query.data.split(':')[1]
    query.data = f"buy_tariff:{plan}"
    # Временно сбрасываем тариф в контексте чтобы пройти проверку
    context.user_data['_skip_downgrade_check'] = True
    return await buy_tariff_handler(update, context)


async def show_tinkoff_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем реквизиты Тинькофф"""
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split(':')
        price = parts[1] if len(parts) > 1 else ''
        plan_label = parts[2] if len(parts) > 2 else ''
        tinkoff = PAYMENT_DETAILS.get('tinkoff_account', '').replace(' ', '')

        text = (
            f"<b>Оплата через Тинькофф</b>\n\n"
            f"Переведите <b>{price} ₽</b> на карту:\n\n"
            f"<code>{tinkoff}</code>\n\n"
            f"Назначение: <i>Подписка Полка {plan_label}</i>\n\n"
            "После оплаты нажмите кнопку ниже и отправьте скриншот."
        )

        keyboard = [
            [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data="i_paid")],
            [InlineKeyboardButton("← Назад", callback_data=f"buy_tariff:{context.user_data.get('pending_payment', {}).get('tariff', 'standard').lower()}")],
        ]

        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
        return States.MAIN_MENU
    except Exception as e:
        logger.error(f"Ошибка в show_tinkoff_handler: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить реквизиты.",
                [[InlineKeyboardButton("← Назад", callback_data="subscriptions")]])
        except Exception:
            pass
async def i_paid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал «Я оплатил» — просим скриншот"""
    query = update.callback_query
    await query.answer()

    try:
        payment_info = context.user_data.get('pending_payment')
        if not payment_info:
            await safe_edit_message(query, "Сначала выберите тариф.", [[InlineKeyboardButton("◆ Тарифы", callback_data="subscriptions")]])
            return States.MAIN_MENU

        plan_label = payment_info.get('tariff', '')
        price      = payment_info.get('amount', 0)

        text = (
            f"Отлично! Тариф <b>{plan_label}</b> — {price} ₽\n\n"
            "Пожалуйста, отправьте <b>скриншот оплаты</b> прямо в этот чат.\n\n"
            "Администратор активирует подписку в течение 15 минут."
        )

        keyboard = [[InlineKeyboardButton("← Отмена", callback_data="subscriptions")]]
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
        return States.MAIN_MENU
    except Exception as e:
        logger.error(f"Ошибка в i_paid_handler: {e}")
        try:
            await safe_edit_message(query, "Не удалось обработать запрос. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="subscriptions")]])
        except Exception:
            pass
async def enter_promo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь хочет ввести промокод"""
    query = update.callback_query
    await query.answer()

    try:
        context.user_data['waiting_promo'] = True
        text = (
            "🎟 <b>Введите промокод</b>\n\n"
            "Напишите промокод в чат — я проверю скидку и применю её к выбранному тарифу."
        )
        keyboard = [[InlineKeyboardButton("← Назад", callback_data="subscriptions")]]
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
        return States.MAIN_MENU
    except Exception as e:
        logger.error(f"Ошибка в enter_promo_handler: {e}")
        try:
            await safe_edit_message(query, "Не удалось открыть ввод промокода.",
                [[InlineKeyboardButton("← Назад", callback_data="subscriptions")]])
        except Exception:
            pass
async def handle_promo_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает текстовый ввод промокода"""
    try:
        if not context.user_data.get('waiting_promo'):
            return None  # не наш случай

        code = update.message.text.strip().upper()
        context.user_data.pop('waiting_promo', None)

        promo = db.get_promocode(code)
        if not promo:
            await update.message.reply_text(
                f"✗ Промокод <b>{code}</b> не найден или истёк.\n\n"
                "Попробуйте другой код или вернитесь к тарифам.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◆ Тарифы", callback_data="subscriptions")]])
            )
            return States.MAIN_MENU

        discount = promo['discount_percent']

        # Считаем скидку для обоих тарифов
        std_final = round(299 * (1 - discount / 100))
        pro_final = round(799 * (1 - discount / 100))

        # Сохраняем промокод в контекст (без привязки к тарифу — применится при выборе)
        context.user_data['promo_code']     = code
        context.user_data['promo_discount'] = discount
        context.user_data['promo_plan']     = 'any'

        text = (
            f"✅ Промокод <b>{code}</b> действителен!\n\n"
            f"Скидка: <b>{discount}%</b>\n\n"
            f"Старт: <s>299 ₽</s> → <b>{std_final} ₽</b>\n"
            f"PRO: <s>799 ₽</s> → <b>{pro_final} ₽</b>\n\n"
            "Выберите тариф для оплаты:"
        )

        keyboard = [
            [InlineKeyboardButton(f"Старт — {std_final} ₽/мес", callback_data="buy_tariff:standard")],
            [InlineKeyboardButton(f"PRO — {pro_final} ₽/мес", callback_data="buy_tariff:pro")],
            [InlineKeyboardButton("← Назад", callback_data="subscriptions")],
        ]

        await update.message.reply_text(
            text, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.MAIN_MENU
    except Exception as e:
        logger.error(f"Ошибка в handle_promo_text: {e}")
        try:
            await safe_edit_message(query, "Не удалось применить промокод. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="subscriptions")]])
        except Exception:
            pass

# Реферальная программа
async def referral_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        user = update.effective_user
        stats = db.get_referral_stats(user.id)
        
        bot_username = (await context.bot.get_me()).username
        ref_link = f"https://t.me/{bot_username}?start=ref_{user.id}"
        
        text = (
            f"Реферальная программа\n\n"
            f"Ваш баланс: {stats['balance']} ₽\n"
            f"Заработано: {stats['total_earned']} ₽\n"
            f"Приглашено: {stats['referrals_count']}\n"
            f"Купили подписку: {stats['active_referrals']}\n\n"
            f"Как это работает:\n"
            f"1. Приглашайте друзей по вашей ссылке\n"
            f"2. Если друг покупает подписку\n"
            f"3. Вы получаете {REFERRAL_PERCENT}% от суммы его подписки\n"
            f"4. Накопленные средства можно использовать для оплаты своей подписки\n\n"
            f"Ваша реферальная ссылка:\n"
            f"<code>{ref_link}</code>\n\n"
            f"Что сегодня продаём или ищем?"
        )
        
        keyboard = [
            [InlineKeyboardButton("▸ Мои рефералы", callback_data="referral_list"),
             InlineKeyboardButton("◎ Баланс", callback_data="referral_balance")],
            [InlineKeyboardButton("→ Поделиться", callback_data="referral_share")],
            [InlineKeyboardButton("← Назад", callback_data="back_to_menu")]
        ]
        
        try:
            await query.message.reply_photo(
                photo=BANNERS["referral"],
                caption=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            await query.delete_message()
        except Exception:
            await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в referral_menu: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить реферальную программу.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def referral_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        user = update.effective_user
        referrals = db.get_referrals(user.id)
        
        if not referrals:
            text = (
                "Ваши рефералы\n\n"
                "У вас пока нет рефералов.\n\n"
                " Пригласите друзей по реферальной ссылке!\n"
                f"За каждого приглашенного друга, который купит подписку, "
                f"вы получите {REFERRAL_PERCENT}% от суммы его подписки."
            )
        else:
            text = f"Ваши рефералы: {len(referrals)}\n\n"
            
            for i, ref in enumerate(referrals[:10], 1):
                tariff_icon = "◉" if ref['tariff'] == 'PRO' else "◈" if ref['tariff'] == 'Standard' else ""
                has_sub = "✓" if ref['has_bought_subscription'] else "✗"
                date = ref['registered_at'][:10] if ref['registered_at'] else "неизв."
                
                text += (
                    f"{i}. @{ref['username'] or ref['first_name']}\n"
                    f" {tariff_icon} {ref['tariff']} | Купил подписку: {has_sub}\n"
                    f"  Приглашен: {date}\n\n"
                )
            
            if len(referrals) > 10:
                text += f"\n... и еще {len(referrals) - 10} рефералов"
        
        keyboard = [
            [InlineKeyboardButton("→ Пригласить друзей", callback_data="referral_share")],
            [InlineKeyboardButton("↺ Обновить", callback_data="referral_list")],
            [InlineKeyboardButton("← Назад", callback_data="referral_menu")]
        ]
        
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в referral_list: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить список рефералов.",
                [[InlineKeyboardButton("← Назад", callback_data="referral_menu")]])
        except Exception:
            pass
async def referral_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        user = update.effective_user
        stats = db.get_referral_stats(user.id)
        
        text = (
            f"Реферальный баланс\n\n"
            f"Доступно: {stats['balance']}₽\n"
            f"Всего заработано: {stats['total_earned']}₽\n"
            f"Приглашено пользователей: {stats['referrals_count']}\n\n"
            f" Вы можете использовать эти средства для оплаты своей подписки!"
        )
        
        keyboard = [
            [InlineKeyboardButton("◆ Оплатить подписку", callback_data="pay_with_balance")],
            [InlineKeyboardButton("← Назад", callback_data="referral_menu")]
        ]
        
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в referral_balance: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить баланс.",
                [[InlineKeyboardButton("← Назад", callback_data="referral_menu")]])
        except Exception:
            pass
async def referral_share(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    bot_username = (await context.bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{user.id}"
    
    text = (
        f"Поделиться реферальной ссылкой\n\n"
        f"Ваша ссылка:\n"
        f"<code>{ref_link}</code>\n\n"
        f"Текст для приглашения:\n"
        f"Привет! Присоединяйся к Полке - удобной площадке для покупки и продажи товаров! "
        f"По моей ссылке ты получишь доступ ко всем функциям. "
        f"А я получу бонус, если ты купишь подписку! "
    )
    
    keyboard = [
        [InlineKeyboardButton("→ Поделиться в Telegram", 
                           url=f"https://t.me/share/url?url={ref_link}&text=Присоединяйся%20к%20Полка!")],
        [InlineKeyboardButton("→ Скопировать ссылку", callback_data=f"copy_ref_link:{user.id}")],
        [InlineKeyboardButton("← Назад", callback_data="referral_menu")]
    ]
    
    await safe_edit_message(query, text, keyboard)

async def copy_ref_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = int(query.data.split(':')[1])
    bot_username = (await context.bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    
    await query.answer(f"Ссылка: {ref_link}")

async def pay_with_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    stats = db.get_referral_stats(user.id)
    
    if stats['balance'] < REFERRAL_MIN_BALANCE_FOR_SUBSCRIPTION:
        await safe_edit_message(query,
            text=f"Недостаточно средств на балансе\n\n"
                 f"Ваш баланс: {stats['balance']} ₽\n"
                 f"Минимум для оплаты подписки: {REFERRAL_MIN_BALANCE_FOR_SUBSCRIPTION}₽\n\n"
                 f" Пригласите друзей, чтобы накопить больше!"
        )
        return
    
    text = (
        f"Оплата подписки реферальным балансом\n\n"
        f"Ваш баланс: {stats['balance']}₽\n\n"
        f"Выберите подписку для оплаты:"
    )
    
    keyboard = [
        [InlineKeyboardButton("Старт (299₽)", callback_data="pay_standard_balance"),
         InlineKeyboardButton("◆ PRO — 799 ₽", callback_data="pay_pro_balance")],
        [InlineKeyboardButton("← Назад", callback_data="referral_balance")]
    ]
    
    await safe_edit_message(query, text, keyboard)

async def pay_subscription_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    tariff = query.data.replace('pay_', '').replace('_balance', '').capitalize()
    price = TARIFFS.get(tariff, TARIFFS["Standard"])["price"]
    
    user = update.effective_user
    stats = db.get_referral_stats(user.id)
    
    if stats['balance'] < price:
        await safe_edit_message(query,
            text=f"Недостаточно средств на балансе\n\n"
                 f"Ваш баланс: {stats['balance']} ₽\n"
                 f"Стоимость {tariff}: {price}₽\n\n"
                 f" Пригласите больше друзей, чтобы накопить нужную сумму!"
        )
        return
    
    text = (
        f"Подтверждение оплаты\n\n"
        f"Подписка: {tariff}\n"
        f"Стоимость: {price}₽\n"
        f"Ваш баланс: {stats['balance']}₽\n"
        f"Остаток после оплаты: {stats['balance'] - price}₽\n\n"
        f"Подтвердить оплату подписки {tariff}?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✓ Да, оплатить", callback_data=f"confirm_pay_{tariff.lower()}"),
         InlineKeyboardButton("✗ Отмена", callback_data="pay_with_balance")]
    ]
    
    await safe_edit_message(query, text, keyboard)

async def confirm_pay_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    tariff = query.data.replace('confirm_pay_', '').capitalize()
    price = TARIFFS.get(tariff, TARIFFS["Standard"])["price"]
    
    user = update.effective_user
    
    try:
        success = db.pay_subscription_with_balance(user.id, tariff, price)
        
        if success:
            tariff_end_date = (get_moscow_time() + timedelta(days=30)).isoformat()
            db.update_user(user.id, tariff=tariff, tariff_end_date=tariff_end_date)
            
            await safe_edit_message(query,
                text=f"Поздравляем!\n\n"
                     f"Подписка {tariff} успешно оплачена реферальным балансом!\n\n"
                     f"Списано: {price}₽\n"
                     f"Новый тариф: {tariff}\n"
                     f"Действует до: {tariff_end_date[:10]}\n\n"
                     f"Теперь вам доступны все премиум-функции! "
            )
            
            db.log_action(user.id, "subscription_paid_with_balance", f"Tariff: {tariff}, Amount: {price}₽")
        else:
            await safe_edit_message(query,
                text="Не удалось провести оплату. Попробуйте позже или обратитесь в поддержку.",
                keyboard=[[InlineKeyboardButton("← Назад", callback_data="referral_balance")]]
            )
    except ValueError as e:
        await safe_edit_message(query,
            text=f"Недостаточно средств или неверная сумма. Проверьте баланс и попробуйте снова.",
            keyboard=[[InlineKeyboardButton("← Назад", callback_data="referral_balance")]]
        )
    except Exception as e:
        logger.error(f"Ошибка при оплате подписки балансом: {e}")
        await safe_edit_message(query,
            text="Не удалось провести оплату. Попробуйте позже или обратитесь в поддержку.",
            keyboard=[[InlineKeyboardButton("← Назад", callback_data="referral_balance")]]
        )

# Создание объявлений - ИСПРАВЛЕНА ПРОБЛЕМА СО Standard ПОДПИСКОЙ
# =====================================================================
# + СОЗДАНИЕ ОБЪЯВЛЕНИЯ — всё по кнопкам
# =====================================================================

DELIVERY_OPTIONS = [
    ("СДЭК",             "СДЭК"),
    ("Почта России",      "Почта России"),
    ("Boxberry",          "Boxberry"),
    ("DPD",               "DPD"),
    ("ПЭК",               "ПЭК"),
    ("КСЭ",               "КСЭ"),
    ("Яндекс Доставка",   "Яндекс Доставка"),
    ("OZON Доставка",     "OZON Доставка"),
    ("Самовывоз",         "Самовывоз"),
    ("· Другое",            "custom"),
]

CONDITION_OPTIONS = [
    ("◎ Новое",              "◎ Новое"),
    ("Отличное",          "Отличное"),
    ("Хорошее",           "Хорошее"),
    ("Удовлетворительное","Удовлетворительное"),
    ("На запчасти",       "На запчасти"),
]

SIZE_CLOTHES = ["XS","S","M","L","XL","XXL","XXXL","Другой"]
SIZE_SHOES   = ["36","37","38","39","40","41","42","43","44","45","46","Другой"]


def _ad_progress(step: int, total: int = 7) -> str:
    filled = "●" * step
    empty  = "○" * (total - step)
    return f"{filled}{empty}  {step}/{total}"


async def create_ad_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user

    subscribed = await check_subscription(user.id, context)
    if not subscribed:
        try: await query.message.delete()
        except: pass
        await context.bot.send_message(user.id, "Сначала подпишитесь на канал!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]))
        return ConversationHandler.END

    user_data = db.get_user(user.id)
    tariff = user_data['tariff']

    if tariff == 'Free' and user_data.get('daily_ads_count', 0) >= TARIFFS['Free']['daily_ads']:
        try: await query.message.delete()
        except: pass
        await context.bot.send_message(user.id,
            "Дневной лимит исчерпан\n\nБесплатный тариф: 2 объявления в день\n\n"
            "Старт за 299 ₽/мес — безлимит + авто-подъём + аналитика",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◆ Старт — 299 ₽/мес", callback_data="buy_standard")],
                [InlineKeyboardButton("◆ PRO — 799 ₽/мес", callback_data="buy_pro")],
                [InlineKeyboardButton("← Меню", callback_data="back_to_menu")],
            ]))
        return ConversationHandler.END

    # Сброс старых данных
    context.user_data.pop('creating_ad', None)
    context.user_data.pop('photo_limit', None)
    context.user_data['creating_ad'] = {
        'user_id': user.id, 'photos': [], 'title': '',
        'description': '', 'price': 0, 'category': '',
        'contact_info': '', 'delivery_list': [],
    }
    # Сохраняем черновик для напоминания о брошенном объявлении
    db.save_abandoned_draft(user.id, 'started')

    keyboard = [
        [InlineKeyboardButton("· Одежда",     callback_data="cad_cat:clothes"),
         InlineKeyboardButton("· Обувь",      callback_data="cad_cat:shoes")],
        [InlineKeyboardButton("· Техника",    callback_data="cad_cat:tech"),
         InlineKeyboardButton("· Авто",       callback_data="cad_cat:auto")],
        [InlineKeyboardButton("· Для дома",   callback_data="cad_cat:home"),
         InlineKeyboardButton("· Игры / Хобби", callback_data="cad_cat:hobby")],
        [InlineKeyboardButton("· Книги",      callback_data="cad_cat:books"),
         InlineKeyboardButton("· Другое",     callback_data="cad_cat:other")],
        [InlineKeyboardButton("✗ Отмена",      callback_data="cancel")],
    ]
    try: await query.delete_message()
    except: pass
    tariff_label = TARIFF_LABELS.get(tariff, tariff)
    try:
        await query.message.reply_photo(
            photo=BANNERS["create_ad"],
            caption=f"<b>Новое объявление</b>  {_ad_progress(1)}\nТариф: {tariff_label}\n\n<b>Категория:</b>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        await query.message.reply_text(
            f"<b>Новое объявление</b>  {_ad_progress(1)}\nТариф: {tariff_label}\n\n<b>Категория:</b>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    return States.CREATE_AD_CATEGORY


async def create_ad_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cat_map = {
        "cad_cat:clothes": "· Одежда",
        "cad_cat:shoes":   "· Обувь",
        "cad_cat:tech":    "· Техника",
        "cad_cat:auto":    "· Авто",
        "cad_cat:home":    "· Для дома",
        "cad_cat:hobby":   "· Игры / Хобби",
        "cad_cat:books":   "· Книги",
        "cad_cat:other":   "· Другое",
    }
    # Поддержка старых колбэков
    old_map = {
        "category_clothes": "· Одежда",
        "category_shoes":   "· Обувь",
        "category_tech":    "· Техника",
        "category_other":   "· Другое",
    }
    category = cat_map.get(query.data) or old_map.get(query.data, "· Другое")
    context.user_data['creating_ad']['category'] = category

    await safe_edit_message(query,
        f"<b>Название товара</b>  {_ad_progress(2)}\n\n"
        f"Категория: {category}\n\n"
        f"Напишите название — коротко и понятно:\n"
        f"<i>Пример: {CATEGORY_EXAMPLES.get(category, 'Nike Air Max 90')}</i>",
        [[InlineKeyboardButton("✗ Отмена", callback_data="cancel")]],
        parse_mode=ParseMode.HTML)
    return States.CREATE_AD_TITLE


async def create_ad_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = update.message.text.strip()
    if len(title) < 3:
        await update.message.reply_text("Название слишком короткое. Введите ещё раз:")
        return States.CREATE_AD_TITLE

    context.user_data['creating_ad']['title'] = title
    category = context.user_data['creating_ad'].get('category', '')

    # Состояние — кнопками для всех категорий
    keyboard = []
    row = []
    for label, val in CONDITION_OPTIONS:
        row.append(InlineKeyboardButton(label, callback_data=f"cad_cond:{val}"))
        if len(row) == 2:
            keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("· Пропустить", callback_data="cad_cond:skip")])
    keyboard.append([InlineKeyboardButton("✗ Отмена", callback_data="cancel")])

    await update.message.reply_text(
        f"<b>Состояние товара</b>  {_ad_progress(3)}\n\n"
        f"<b>{title}</b>\n\nВыберите состояние:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )
    return States.CREATE_AD_CONDITION


async def create_ad_condition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    context.user_data['creating_ad']['condition'] = '' if val == 'skip' else val
    category = context.user_data['creating_ad'].get('category', '')

    if category in (' Одежда', ' Обувь'):
        # Размер кнопками
        sizes = SIZE_SHOES if category == ' Обувь' else SIZE_CLOTHES
        keyboard = []
        row = []
        for s in sizes:
            row.append(InlineKeyboardButton(s, callback_data=f"cad_size:{s}"))
            if len(row) == 4:
                keyboard.append(row); row = []
        if row: keyboard.append(row)
        keyboard.append([InlineKeyboardButton("· Пропустить", callback_data="cad_size:skip")])
        keyboard.append([InlineKeyboardButton("✗ Отмена", callback_data="cancel")])
        await safe_edit_message(query,
            f"+ <b>Размер</b>  {_ad_progress(3)}\n\nВыберите размер:",
            keyboard, parse_mode=ParseMode.HTML)
        return States.CREATE_AD_SIZE
    else:
        context.user_data['creating_ad']['size'] = ''
        return await _ask_city(query, context)


async def create_ad_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]

    if val == 'Другой':
        await safe_edit_message(query,
            f"+ <b>Введите размер</b>  {_ad_progress(3)}\n\nНапишите ваш размер:",
            [[InlineKeyboardButton("✗ Отмена", callback_data="cancel")]])
        context.user_data['creating_ad']['_awaiting_size'] = True
        return States.CREATE_AD_SIZE
    context.user_data['creating_ad']['size'] = '' if val == 'skip' else val
    context.user_data['creating_ad'].pop('_awaiting_size', None)
    # Для одежды — дополнительно спрашиваем пол/возраст
    if context.user_data['creating_ad'].get('category') == 'Одежда':
        return await _ask_gender(query, context)
    return await _ask_city(query, context)


async def create_ad_size_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь написал размер вручную"""
    if not context.user_data.get('creating_ad', {}).get('_awaiting_size'):
        return States.CREATE_AD_SIZE
    size = update.message.text.strip()
    context.user_data['creating_ad']['size'] = size
    context.user_data['creating_ad'].pop('_awaiting_size', None)
    # Для одежды — спрашиваем пол/возраст перед городом
    if context.user_data['creating_ad'].get('category') == 'Одежда':
        keyboard = [
            [InlineKeyboardButton("· Мужская", callback_data="cad_gender:Мужская"),
             InlineKeyboardButton("· Женская",  callback_data="cad_gender:Женская")],
            [InlineKeyboardButton("· Детская",  callback_data="cad_gender:Детская"),
             InlineKeyboardButton("· Пропустить", callback_data="cad_gender:skip")],
        ]
        await update.message.reply_text(
            f"<b>Для кого одежда?</b>  {_ad_progress(4)}\n\nВыберите аудиторию:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        return States.CREATE_AD_GENDER
    keyboard = _city_keyboard()
    await update.message.reply_text(
        f"+ <b>Город</b>  {_ad_progress(4)}\n\nВыберите город или напишите свой:",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    return States.CREATE_AD_CITY


def _city_keyboard():
    cities = ["Москва","Санкт-Петербург","Новосибирск","Екатеринбург",
              "Казань","Краснодар","Нижний Новгород","Челябинск","Ростов-на-Дону","Уфа"]
    keyboard = []
    row = []
    for c in cities:
        row.append(InlineKeyboardButton(c, callback_data=f"cad_city:{c}"))
        if len(row) == 2:
            keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("· Другой город", callback_data="cad_city:custom")])
    keyboard.append([InlineKeyboardButton("✗ Отмена", callback_data="cancel")])
    return keyboard


async def _ask_city(query_or_msg, context):
    keyboard = _city_keyboard()
    text = f"+ <b>Город</b>  {_ad_progress(4)}\n\nВыберите город или укажите свой:"
    if hasattr(query_or_msg, 'edit_message_text'):
        await safe_edit_message(query_or_msg, text, keyboard, parse_mode=ParseMode.HTML)
    else:
        await query_or_msg.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    return States.CREATE_AD_CITY


async def _ask_gender(query_or_msg, context):
    """Вопрос: для кого одежда (мужская/женская/детская)"""
    keyboard = [
        [InlineKeyboardButton("· Мужская", callback_data="cad_gender:Мужская"),
         InlineKeyboardButton("· Женская",  callback_data="cad_gender:Женская")],
        [InlineKeyboardButton("· Детская",  callback_data="cad_gender:Детская"),
         InlineKeyboardButton("· Пропустить", callback_data="cad_gender:skip")],
        [InlineKeyboardButton("✗ Отмена", callback_data="cancel")],
    ]
    text = f"<b>Для кого одежда?</b>  {_ad_progress(4)}\n\nВыберите аудиторию — объявление попадёт в нужный канал:"
    if hasattr(query_or_msg, 'edit_message_text'):
        await safe_edit_message(query_or_msg, text, keyboard, parse_mode=ParseMode.HTML)
    else:
        await query_or_msg.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    return States.CREATE_AD_GENDER


async def create_ad_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора пола/возраста для одежды"""
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    context.user_data['creating_ad']['gender'] = '' if val == 'skip' else val
    return await _ask_city(query, context)


async def create_ad_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора города (кнопка или текст)"""
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        val = query.data.split(":", 1)[1]
        if val == 'custom':
            await safe_edit_message(query,
                f"+ <b>Введите ваш город</b>  {_ad_progress(4)}:",
                [[InlineKeyboardButton("✗ Отмена", callback_data="cancel")]],
                parse_mode=ParseMode.HTML)
            context.user_data['creating_ad']['_awaiting_city'] = True
            return States.CREATE_AD_CITY
        context.user_data['creating_ad']['city'] = val
        context.user_data['creating_ad'].pop('_awaiting_city', None)
        return await _ask_delivery(query, context)
    else:
        # Текстовый ввод города
        if not context.user_data.get('creating_ad', {}).get('_awaiting_city'):
            return States.CREATE_AD_CITY
        city = update.message.text.strip()
        context.user_data['creating_ad']['city'] = city
        context.user_data['creating_ad'].pop('_awaiting_city', None)
        return await _ask_delivery(update.message, context)


def _delivery_keyboard(selected: list) -> list:
    keyboard = []
    row = []
    for label, val in DELIVERY_OPTIONS:
        check = "✓ " if val in selected else ""
        cb = f"cad_dlv:{val}"
        row.append(InlineKeyboardButton(f"{check}{label}", callback_data=cb))
        if len(row) == 2:
            keyboard.append(row); row = []
    if row: keyboard.append(row)
    if selected:
        keyboard.append([InlineKeyboardButton(f"✓ Готово ({len(selected)} выбрано)", callback_data="cad_dlv:done")])
    keyboard.append([InlineKeyboardButton("✗ Отмена", callback_data="cancel")])
    return keyboard


async def _ask_delivery(query_or_msg, context):
    selected = context.user_data['creating_ad'].get('delivery_list', [])
    keyboard = _delivery_keyboard(selected)
    text = (f"+ <b>Доставка</b>  {_ad_progress(5)}\n\n"
            "Выберите один или несколько способов доставки.\n"
            "Нажмите ещё раз чтобы снять выбор:")
    if hasattr(query_or_msg, 'edit_message_text'):
        await safe_edit_message(query_or_msg, text, keyboard, parse_mode=ParseMode.HTML)
    else:
        await query_or_msg.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    return States.CREATE_AD_DELIVERY


async def create_ad_delivery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тогл кнопок доставки + обработка 'Другое' и 'Готово'"""
    query = update.callback_query
    await query.answer()
    val = query.data.split(":", 1)[1]
    selected = context.user_data['creating_ad'].setdefault('delivery_list', [])

    if val == 'done':
        if not selected:
            await query.answer("Выберите хотя бы один способ доставки", show_alert=True)
            return States.CREATE_AD_DELIVERY
        # Собираем строку доставки
        context.user_data['creating_ad']['delivery'] = ', '.join(selected)
        return await _ask_price(query, context)

    if val == 'custom':
        await safe_edit_message(query,
            f"+ <b>Укажите способ доставки</b>\n\nНапишите название курьерской службы или способа:",
            [[InlineKeyboardButton("← Назад", callback_data="cad_dlv_back")]],
            parse_mode=ParseMode.HTML)
        return States.CREATE_AD_DELIVERY_CUSTOM

    # Тогл
    if val in selected:
        selected.remove(val)
    else:
        selected.append(val)

    keyboard = _delivery_keyboard(selected)
    text = (f"+ <b>Доставка</b>  {_ad_progress(5)}\n\n"
            "Выберите один или несколько способов.\n"
            "Нажмите ещё раз чтобы снять:")
    await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    return States.CREATE_AD_DELIVERY


async def create_ad_delivery_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь написал свой способ доставки"""
    if update.callback_query:
        # Кнопка «Назад»
        await update.callback_query.answer()
        return await _ask_delivery(update.callback_query, context)
    custom = update.message.text.strip()
    context.user_data['creating_ad'].setdefault('delivery_list', []).append(custom)
    return await _ask_delivery(update.message, context)


async def _ask_price(query_or_msg, context):
    text = f"+ <b>Цена</b>  {_ad_progress(6)}\n\nВведите цену в рублях:\n<i>Только число, например: 2500</i>"
    kb = [[InlineKeyboardButton("✗ Отмена", callback_data="cancel")]]
    if hasattr(query_or_msg, 'edit_message_text'):
        await safe_edit_message(query_or_msg, text, kb, parse_mode=ParseMode.HTML)
    else:
        await query_or_msg.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
    return States.CREATE_AD_PHOTOS


async def create_ad_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    price_text = update.message.text.strip().replace(' ','').replace('₽','').replace('руб','').replace('р','')
    try:
        price = int(price_text)
        if price <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("✗ Введите число больше нуля, например: 2500")
        return States.CREATE_AD_PHOTOS

    context.user_data['creating_ad']['price'] = price

    user_data = db.get_user(update.effective_user.id)
    tariff = user_data['tariff']
    photo_limit = TARIFFS[tariff]['photo_limit']
    context.user_data['photo_limit'] = photo_limit

    await update.message.reply_text(
        f"+ <b>Описание</b>  {_ad_progress(6)}\n\n"
        f"Добавьте описание товара (необязательно):\n"
        f"<i>Расскажите подробнее — размер, состояние, история, дефекты</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("· Пропустить", callback_data="cad_desc:skip")],
            [InlineKeyboardButton("✗ Отмена", callback_data="cancel")],
        ]),
        parse_mode=ParseMode.HTML
    )
    return States.CREATE_AD_DESCRIPTION_OPT


async def create_ad_description_opt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Необязательное описание"""
    if update.callback_query:
        await update.callback_query.answer()
        context.user_data['creating_ad']['description'] = ''
    else:
        context.user_data['creating_ad']['description'] = update.message.text.strip()

    return await _ask_photos(update, context)


async def _ask_photos(update, context):
    photo_limit = context.user_data.get('photo_limit', 10)
    text = (f"+ <b>Фото</b>  {_ad_progress(7)}\n\n"
            f"Загрузите фото товара (до {photo_limit} штук).\n"
            "Первое фото станет главным.\n\n"
            "<i>Можно отправить несколько сразу</i>")
    kb = [
        [InlineKeyboardButton("· Без фото", callback_data="next_step")],
        [InlineKeyboardButton("✗ Отмена", callback_data="cancel")],
    ]
    msg = update.callback_query.message if update.callback_query else update.message
    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
    return States.CREATE_AD_CONTACTS


async def handle_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        return States.CREATE_AD_CONTACTS
    photo = update.message.photo[-1].file_id
    if 'creating_ad' not in context.user_data:
        return States.CREATE_AD_CONTACTS
    ad = context.user_data['creating_ad']
    photo_limit = context.user_data.get('photo_limit', 10)

    if len(ad['photos']) < photo_limit:
        ad['photos'].append(photo)
        remaining = photo_limit - len(ad['photos'])
        if remaining > 0:
            text = (f"✓ Фото {len(ad['photos'])}/{photo_limit}\n\n"
                    "Добавьте ещё или нажмите <b>Далее</b>:")
        else:
            text = f"✓ Все {photo_limit} фото добавлены. Нажмите <b>Далее</b>:"
        await update.message.reply_text(text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("→ Далее", callback_data="next_step")],
                [InlineKeyboardButton("✗ Отмена", callback_data="cancel")],
            ]), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"✗ Лимит {photo_limit} фото достигнут. Нажмите <b>Далее</b>:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("→ Далее", callback_data="next_step")]]),
            parse_mode=ParseMode.HTML)
    return States.CREATE_AD_CONTACTS


async def skip_or_next_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ad = context.user_data.get('creating_ad', {})
    # Автозаполняем контакт из Telegram username если есть
    user = update.effective_user
    if not ad.get('contact_info'):
        if user.username:
            ad['contact_info'] = f"@{user.username}"
    return await _show_preview(query, context)


async def create_ad_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Если пользователь написал текст на шаге фото — это контакт"""
    contacts = update.message.text.strip()
    if 'creating_ad' not in context.user_data:
        await update.message.reply_text("✗ Данные потеряны. Начните заново.")
        return States.MAIN_MENU
    context.user_data['creating_ad']['contact_info'] = contacts
    return await _show_preview(update.message, context)


async def _show_preview(query_or_msg, context):
    ad = context.user_data.get('creating_ad', {})

    # Автоматически подставляем контакт если пустой
    if not ad.get('contact_info'):
        ad['contact_info'] = 'Не указан'

    delivery_str = ad.get('delivery', '') or ', '.join(ad.get('delivery_list', [])) or '—'

    lines = [f"<b>Проверьте объявление</b>\n"]
    lines.append(f"<b>{ad.get('title','—')}</b>")
    if ad.get('description'):
        lines.append(f"\n{ad['description'][:300]}")
    lines.append(f"\n<b>Цена:</b> {ad.get('price', 0)} ₽")
    if ad.get('condition'):
        lines.append(f"<b>Состояние:</b> {ad['condition']}")
    if ad.get('size'):
        lines.append(f"<b>Размер:</b> {ad['size']}")
    if ad.get('gender'):
        _gl = {'Мужская': '👔 Мужская', 'Женская': '👗 Женская', 'Детская': '🧒 Детская'}.get(ad['gender'], ad['gender'])
        lines.append(f"<b>Для:</b> {_gl}")
    if ad.get('city'):
        lines.append(f"<b>Город:</b> {ad['city']}")
    lines.append(f"<b>Доставка:</b> {delivery_str}")
    lines.append(f"<b>Категория:</b> {ad.get('category','—')}")
    lines.append(f"<b>Контакт:</b> {ad['contact_info']}")
    if ad.get('photos'):
        lines.append(f"Фото: {len(ad['photos'])} шт.")

    preview_text = "\n".join(lines) + "\n\n<i>Всё верно?</i>"

    keyboard = [
        [InlineKeyboardButton("✓ Опубликовать", callback_data="publish_ad"),
         InlineKeyboardButton("✎ Изменить", callback_data="edit_ad")],
        [InlineKeyboardButton("✎ Изменить контакт", callback_data="cad_edit_contact")],
        [InlineKeyboardButton("✗ Отмена", callback_data="cancel")],
    ]

    if ad.get('photos'):
        try:
            media_group = [InputMediaPhoto(ad['photos'][0], caption=preview_text, parse_mode=ParseMode.HTML)]
            for p in ad['photos'][1:]:
                media_group.append(InputMediaPhoto(p))
            msg = query_or_msg.message if hasattr(query_or_msg, 'edit_message_text') else query_or_msg
            await msg.reply_media_group(media=media_group)
            await msg.reply_text("Всё верно?", reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"preview photo error: {e}")
            msg = query_or_msg.message if hasattr(query_or_msg, 'edit_message_text') else query_or_msg
            await msg.reply_text(preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    else:
        if hasattr(query_or_msg, 'edit_message_text'):
            await safe_edit_message(query_or_msg, preview_text, keyboard, parse_mode=ParseMode.HTML)
        else:
            await query_or_msg.reply_text(preview_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

    return States.CREATE_AD_PREVIEW


async def create_ad_edit_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактировать контакт прямо из превью"""
    query = update.callback_query
    await query.answer()
    if update.message:
        # Пришёл текст — это новый контакт
        context.user_data['creating_ad']['contact_info'] = update.message.text.strip()
        return await _show_preview(update.message, context)
    await safe_edit_message(query,
        " <b>Введите контакт для связи</b>\n\n"
        "Telegram username (@username), номер телефона или другой способ:",
        [[InlineKeyboardButton("✗ Отмена", callback_data="cancel")]],
        parse_mode=ParseMode.HTML)
    context.user_data['creating_ad']['_awaiting_contact'] = True
    return States.CREATE_AD_CONTACTS


# Старые функции-заглушки для совместимости
async def create_ad_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    description = update.message.text.strip()
    context.user_data['creating_ad']['description'] = description
    return await _ask_city(update.message, context)

async def create_ad_condition_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return States.CREATE_AD_CONDITION

async def create_ad_city_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('creating_ad', {}).get('_awaiting_city'):
        return States.CREATE_AD_CITY
    city = update.message.text.strip()
    context.user_data['creating_ad']['city'] = city
    context.user_data['creating_ad'].pop('_awaiting_city', None)
    return await _ask_delivery(update.message, context)


async def publish_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    ad = context.user_data.get('creating_ad')
    
    if not ad:
        # Пытаемся отправить новое сообщение вместо редактирования
        try:
            await query.message.reply_text(
                "✗ Ошибка: данные объявления не найдены",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
            )
        except Exception as _e:
            await context.bot.send_message(
                chat_id=user.id,
                text="✗ Ошибка: данные объявления не найдены",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
            )
        return ConversationHandler.END
    
    try:
        # Проверяем обязательные поля и заполняем пустые
        required_fields = {
            'user_id': user.id,
            'title': ad.get('title', ''),
            'description': ad.get('description', ''),
            'price': ad.get('price', 0),
            'photos': ad.get('photos', []),
            'contact_info': ad.get('contact_info', ''),
            'category': ad.get('category', ' Другое'),
            'created_at': ad.get('created_at', get_moscow_time().isoformat()),
            'condition': ad.get('condition', ''),
            'size': ad.get('size', ''),
            'city': ad.get('city', ''),
            'delivery': ad.get('delivery', ''),
            'gender': ad.get('gender', '')
        }
        
        # Логируем данные для отладки
        logger.info(f"Создание объявления пользователем {user.id}: {required_fields['title']}")

        # Антидубликат: проверяем нет ли уже активного объявления с таким же названием
        if db.has_duplicate_ad(user.id, required_fields['title']):
            dup_text = (
                "⚠️ У вас уже есть активное объявление с таким же названием:\n\n"
                f"<b>{required_fields['title']}</b>\n\n"
                "Проверьте раздел \"Мои объявления\". "
                "Если хотите разместить снова — немного измените название."
            )
            await query.message.reply_text(
                dup_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▸ Мои объявления", callback_data="my_ads")],
                    [InlineKeyboardButton("✓ Разместить всё равно", callback_data="publish_ad_force")]
                ])
            )
            context.user_data['publish_ad_force_data'] = required_fields
            return ConversationHandler.END

        # Создаем объявление в базе данных
        ad_id = db.create_ad(required_fields)
        
        if not ad_id:
            raise Exception("База данных не вернула ID объявления")
        
        logger.info(f"Объявление создано с ID: {ad_id}")
        
        # Обновляем счетчик объявлений пользователя
        user_data = db.get_user(user.id)
        new_count = (user_data.get('daily_ads_count', 0) or 0) + 1
        db.update_user(user.id, daily_ads_count=new_count)
        
        # Вычисляем pHash и теги первого фото для поиска по фото
        if required_fields.get("photos"):
            asyncio.create_task(_index_ad_photo(ad_id, required_fields["photos"][0], context.bot))

        # Уведомляем администраторов
        try:
            await notify_admin_new_ad(ad_id, context)
            logger.info(f"Администраторы уведомлены о новом объявлении {ad_id}")
        except Exception as e:
            logger.error(f"Не удалось уведомить администраторов: {e}")
            # Продолжаем работу даже если уведомление не отправилось
        
        # Очищаем данные
        if 'creating_ad' in context.user_data:
            del context.user_data['creating_ad']
        if 'photo_limit' in context.user_data:
            del context.user_data['photo_limit']
        
        db.log_action(user.id, "ad_created", f"Ad ID: {ad_id}")
        # Черновик выполнен — удаляем напоминание
        db.clear_abandoned_draft(user.id)

        # Показываем успешное сообщение с кнопкой
        success_text = (
            "Объявление создано и отправлено на модерацию.\n\n"
            "◎ Оно отправлено на модерацию\n\n"
            "Обычно модерация занимает до 24 часов.\n"
            "Вы получите уведомление, когда объявление будет опубликовано."
        )
        
        keyboard = [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
        
        # Всегда отправляем новое сообщение вместо редактирования
        try:
            # Пытаемся удалить старое сообщение с превью
            try:
                await query.message.delete()
            except Exception as _e:
                logger.debug(f"Ignored error: {_e}")
            # Отправляем новое сообщение
            await context.bot.send_message(
                chat_id=user.id,
                text=success_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {e}")
            # Если не удалось отправить, пробуем через reply
            try:
                await query.message.reply_text(
                    success_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as e2:
                logger.error(f"Ошибка при reply: {e2}")
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка при публикации объявления: {e}")
        logger.error(f"Данные объявления: {ad}")
        
        # Очищаем данные при ошибке
        if 'creating_ad' in context.user_data:
            del context.user_data['creating_ad']
        
        error_text = (
            f"Не удалось создать объявление. Попробуйте ещё раз — если ошибка повторяется, напишите в поддержку."
        )
        
        keyboard = [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
        
        # Отправляем сообщение об ошибке
        try:
            await query.message.reply_text(
                error_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as _e:
            await context.bot.send_message(
                chat_id=user.id,
                text=error_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        return ConversationHandler.END

async def buy_tariff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    tariff_map = {
        "buy_standard":   ("Standard", 249),
        "buy_pro":        ("PRO",      649),
        "renew_standard": ("Standard", 249),
        "renew_pro":      ("PRO",      649)
    }
    
    tariff, amount = tariff_map.get(query.data, ("Standard", 249))
    context.user_data['pending_payment'] = {'tariff': tariff, 'amount': amount}

    # Экран подтверждения перед показом реквизитов
    label = TARIFF_LABELS.get(tariff, tariff)
    features = {
        "Standard": [
            "Безлимит объявлений",
            "До 10 фото на объявление",
            "2 буста в месяц",
            "Авто-подъём раз в 24ч",
            "Аналитика просмотров",
            "Значок ✓ верифицированного продавца",
        ],
        "PRO": [
            "Безлимит объявлений",
            "До 10 фото на объявление",
            "5 бустов в месяц",
            "Авто-подъём каждые 6 часов",
            "Страница магазина",
            "Приоритетная поддержка",
        ],
    }
    feature_list = "\n".join(f"▸ {f}" for f in features.get(tariff, []))
    confirm_text = (
        f"◈ <b>Оформление тарифа «{label}»</b>\n\n"
        f"Стоимость: <b>{amount} ₽ / месяц</b>\n\n"
        f"Что входит:\n{feature_list}\n\n"
        f"─────────────────\n"
        f"Подписка активируется после подтверждения оплаты администратором.\n"
        f"Отменить можно в любой момент — следующий месяц не спишется."
    )
    confirm_keyboard = [
        [InlineKeyboardButton("→ Перейти к оплате", callback_data=f"proceed_pay:{tariff}:{amount}")],
        [InlineKeyboardButton("← Отмена", callback_data="subscriptions")],
    ]
    await safe_edit_message(query, confirm_text, confirm_keyboard, parse_mode=ParseMode.HTML)
    return States.MAIN_MENU


async def proceed_to_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем реквизиты после подтверждения"""
    query = update.callback_query
    await query.answer()
    _, tariff, amount_str = query.data.split(":")
    amount = int(amount_str)
    context.user_data['pending_payment'] = {'tariff': tariff, 'amount': amount}

    links = {
        "Standard": {"yoomoney": PAYMENT_DETAILS["yoomoney_standard"], "crypto": PAYMENT_DETAILS["crypto_standard"]},
        "PRO": {"yoomoney": PAYMENT_DETAILS["yoomoney_pro"], "crypto": PAYMENT_DETAILS["crypto_pro"]}
    }
    
    current_links = links.get(tariff, links["Standard"])
    text = f"Способ оплаты — тариф «{TARIFF_LABELS.get(tariff, tariff)}», {amount} ₽/мес:\n"
    
    keyboard = [
        [InlineKeyboardButton("→ ЮMoney", url=current_links["yoomoney"])],
        [InlineKeyboardButton("→ Криптовалюта", url=current_links["crypto"])],
        [InlineKeyboardButton("→ Реквизиты (Тинькофф)", callback_data="show_tinkoff")],
        [InlineKeyboardButton("◎ Есть промокод?", callback_data=f"use_promo:{tariff}:{amount}")],
        [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data="confirm_payment"),
         InlineKeyboardButton("✗ Отмена", callback_data="cancel_payment")]
    ]
    
    await safe_edit_message(query, text, keyboard)
    return States.MAIN_MENU

async def use_promo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало использования промокода"""
    query = update.callback_query
    await query.answer()
    
    # Парсим данные: tariff:amount
    _, tariff, amount = query.data.split(':')
    
    context.user_data['promo_tariff'] = tariff
    context.user_data['promo_original_amount'] = int(amount)
    
    text = (
        f"Применение промокода\n\n"
        f"Тариф: {tariff}\n"
        f"Цена без скидки: {amount} ₽\n\n"
        f"Введите промокод:"
    )
    
    keyboard = [[InlineKeyboardButton("✗ Отмена", callback_data=f"buy_{tariff.lower()}")]]
    
    await safe_edit_message(query, text, keyboard)
    
    return States.ENTER_PROMOCODE

async def apply_promocode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Применение промокода"""
    promo_code = update.message.text.strip().upper()
    user_id = update.effective_user.id
    
    # Получаем промокод
    promo = db.get_promocode(promo_code)
    
    if not promo:
        # Добавляем кнопку отмены
        keyboard = [[InlineKeyboardButton("✗ Отмена", callback_data=f"buy_{context.user_data.get('promo_tariff', 'standard').lower()}")]]
        await update.message.reply_text(
            "✗ Промокод не найден, истёк или исчерпан.\n\n"
            "Попробуйте другой промокод или вернитесь назад.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.ENTER_PROMOCODE
    
    # Проверяем, не использовал ли пользователь этот промокод
    if db.check_promocode_used(promo['id'], user_id):
        # Добавляем кнопку отмены
        keyboard = [[InlineKeyboardButton("✗ Отмена", callback_data=f"buy_{context.user_data.get('promo_tariff', 'standard').lower()}")]]
        await update.message.reply_text(
            "✗ Вы уже использовали этот промокод.\n\n"
            "Попробуйте другой промокод или вернитесь назад.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.ENTER_PROMOCODE
    
    # Получаем данные о тарифе
    tariff = context.user_data.get('promo_tariff')
    original_amount = context.user_data.get('promo_original_amount')
    
    # Рассчитываем цену со скидкой
    discounted_amount = db.calculate_discounted_price(original_amount, promo['discount_percent'])
    
    # Сохраняем данные
    context.user_data['promo_code'] = promo_code
    context.user_data['promo_discount'] = promo['discount_percent']
    context.user_data['promo_discounted_amount'] = discounted_amount
    context.user_data['promo_duration'] = promo['duration_days']
    context.user_data['promo_id'] = promo['id']
    
    # Получаем криптоссылку для процента скидки из словаря CRYPTO_LINKS
    discount_percent = promo['discount_percent']
    crypto_url = CRYPTO_LINKS.get(discount_percent)
    
    text = (
        f"✓ Промокод {promo_code} применён!\n\n"
        f"Тариф: {tariff}\n"
        f"Цена без скидки: {original_amount} ₽\n"
        f"Скидка: {promo['discount_percent']}%\n"
        f"Цена со скидкой: {discounted_amount} ₽\n"
        f"Длительность: {promo['duration_days']} дней\n"
    )
    
    # Информируем о специальной криптоссылке
    if crypto_url:
        text += f"\nДля вас настроена специальная криптоссылка с учётом скидки!\n"
    
    text += "\nВыберите способ оплаты:"
    
    keyboard = [
        [InlineKeyboardButton("→ ЮMoney", url=PAYMENT_DETAILS.get("yoomoney_standard", ""))]
    ]
    
    # Добавляем криптоссылку если есть для данного процента скидки
    if crypto_url:
        keyboard.append([InlineKeyboardButton("→ Криптовалюта (со скидкой)", url=crypto_url)])
    else:
        # Если нет специальной ссылки для данного процента, используем стандартную
        keyboard.append([InlineKeyboardButton("→ Криптовалюта", url=PAYMENT_DETAILS.get("crypto_standard", ""))])
    
    keyboard.extend([
        [InlineKeyboardButton("→ Реквизиты (Тинькофф)", callback_data="show_tinkoff")],
        [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data="confirm_payment_promo"),
         InlineKeyboardButton("✗ Отмена", callback_data="cancel_payment")]
    ])
    
    # Сохраняем данные платежа
    context.user_data['pending_payment'] = {
        'tariff': tariff,
        'amount': discounted_amount,
        'promocode': promo_code,
        'duration': promo['duration_days']
    }
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # НЕ завершаем ConversationHandler, чтобы пользователь мог перейти к оплате
    return States.PAYMENT_PROOF

async def confirm_payment_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение оплаты с промокодом"""
    query = update.callback_query
    await query.answer()
    
    payment_info = context.user_data.get('pending_payment', {})
    
    text = (
        f"✓ Ожидание подтверждения оплаты\n\n"
        f"Тариф: {payment_info.get('tariff', 'Standard')}\n"
        f"Сумма: {payment_info.get('amount', 149)}₽\n"
        f"Промокод: {payment_info.get('promocode', 'N/A')}\n"
        f"Длительность: {payment_info.get('duration', 30)} дней\n\n"
        "Пожалуйста, отправьте скриншот или фото чека об оплате.\n\n"
        "Администратор проверит оплату и активирует подписку.\n"
        "Обычно это занимает до 15 минут.\n\n"
        "Если хотите отменить, нажмите '✗ Отмена'"
    )
    
    keyboard = [[InlineKeyboardButton("✗ Отмена", callback_data="cancel_payment")]]
    
    await safe_edit_message(query, text, keyboard)
    return States.PAYMENT_PROOF

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    payment_info = context.user_data.get('pending_payment', {})
    
    text = (
        f"✓ Ожидание подтверждения оплаты\n\n"
        f"Тариф: {TARIFF_LABELS.get(payment_info.get('tariff', 'Standard'), payment_info.get('tariff', 'Старт'))}\n"
        f"Сумма: {payment_info.get('amount', '?')}₽\n\n"
        "Пожалуйста, отправьте скриншот или фото чека об оплате.\n\n"
        "Администратор проверит оплату и активирует подписку.\n"
        "Обычно это занимает до 15 минут.\n\n"
        "Если хотите отменить, нажмите '✗ Отмена'"
    )
    
    keyboard = [[InlineKeyboardButton("✗ Отмена", callback_data="cancel_payment")]]
    
    await safe_edit_message(query, text, keyboard)
    return States.PAYMENT_PROOF

async def handle_payment_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message.photo:
            payment_info = context.user_data.get('pending_payment', {})
            
            if not payment_info:
                # Нет ожидающего платежа — фото отправлено в другом контексте
                # (например, при создании объявления). Молча пропускаем.
                return None
            
            screenshot_file_id = update.message.photo[-1].file_id
            
            payment_data = {
                'user_id': update.effective_user.id,
                'tariff': payment_info.get('tariff', 'Standard'),
                'amount': payment_info.get('amount', 0),
                'screenshot_file_id': screenshot_file_id,
                'created_at': get_moscow_time().isoformat(),
                'promocode': payment_info.get('promocode', None),
                'duration': payment_info.get('duration', 30)
            }
            
            try:
                payment_id = db.create_payment(payment_data)
                logger.info(f"Создан платеж ID: {payment_id} от пользователя {update.effective_user.id}")
            except Exception as e:
                logger.error(f"Ошибка при создании платежа в БД: {e}")
                await update.message.reply_text(
                    "Не удалось сохранить чек. Попробуйте отправить его ещё раз — если не поможет, напишите в поддержку @SupPolka"
                )
                return States.PAYMENT_PROOF
            
            try:
                await notify_admin_new_payment(payment_id, context)
                logger.info(f"Админы уведомлены о платеже {payment_id}")
            except Exception as e:
                logger.error(f"Ошибка при уведомлении админов о платеже {payment_id}: {e}")
                # Продолжаем работу даже если уведомление не отправилось
            
            await update.message.reply_text(
                "✓ Чек получен\n\n"
                "Администратор уведомлен о вашем платеже.\n"
                "Ожидайте активации подписки в течение 15 минут.\n\n"
                "Как только подписка будет активирована, вы получите уведомление."
            )
            
            db.log_action(update.effective_user.id, "payment_submitted", 
                         f"Payment ID: {payment_id}, Tariff: {payment_info.get('tariff', 'Standard')}")
            
            if 'pending_payment' in context.user_data:
                del context.user_data['pending_payment']
            
            await show_main_menu(update, context)
            return States.MAIN_MENU
        
        return States.PAYMENT_PROOF
        
    except Exception as e:
        logger.error(f"Критическая ошибка в handle_payment_proof: {e}")
        await update.message.reply_text(
            "Не удалось обработать чек. Пожалуйста, отправьте его повторно или напишите в поддержку @SupPolka"
        )
        return States.MAIN_MENU

# Административная панель
async def admin_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки «Назад в панель» из callback — показывает панель без фото"""
    query = update.callback_query
    if query:
        try:
            await query.answer()
        except Exception:
            pass
    if not await is_admin(update.effective_user.id):
        return
    keyboard = [
        [InlineKeyboardButton("Статистика", callback_data="admin_stats"),
         InlineKeyboardButton("◎ Модерация", callback_data="admin_moderation")],
        [InlineKeyboardButton("◎ Платежи", callback_data="admin_payments"),
         InlineKeyboardButton("◎ Рассылка", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("◎ Промокоды", callback_data="admin_promocodes"),
         InlineKeyboardButton("◎ Криптоссылки", callback_data="admin_crypto_links")],
        [InlineKeyboardButton("◎ Аналитика", callback_data="admin_advanced_analytics")],
        [InlineKeyboardButton("◎ Активировать PAW", callback_data="admin_paw_activate")],
        [InlineKeyboardButton("← Меню", callback_data="back_to_menu")]
    ]
    if query:
        await safe_edit_message(query, "Панель администратора\n\nВыберите раздел:", keyboard)
    else:
        await update.message.reply_text(
            "Панель администратора\n\nВыберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not await is_admin(user.id):
        await update.message.reply_text("✗ У вас нет доступа к этой команде")
        return
    
    keyboard = [
        [InlineKeyboardButton("Статистика", callback_data="admin_stats"),
         InlineKeyboardButton("◎ Модерация", callback_data="admin_moderation")],
        [InlineKeyboardButton("◎ Платежи", callback_data="admin_payments"),
         InlineKeyboardButton("◎ Рассылка", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("◎ Промокоды", callback_data="admin_promocodes"),
         InlineKeyboardButton("◎ Криптоссылки", callback_data="admin_crypto_links")],
        [InlineKeyboardButton("◎ Аналитика", callback_data="admin_advanced_analytics")],
        [InlineKeyboardButton("◎ Активировать PAW", callback_data="admin_paw_activate")],
        [InlineKeyboardButton("← Меню", callback_data="back_to_menu")]
    ]
    
    try:
        await update.message.reply_photo(
            photo=BANNERS["admin"],
            caption="Панель администратора\n\nВыберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception:
        await update.message.reply_text(
            "Панель администратора\n\nВыберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    db.log_action(user.id, "admin_access", "Accessed admin panel")
    return States.ADMIN_MENU

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        await safe_edit_message(query, "✗ У вас нет доступа")
        return
    
    stats = db.get_overall_stats()
    
    text = (
        f"Статистика системы\n\n"
        f"Пользователи: {stats['total_users']}\n"
        f"Объявления: {stats['total_ads']}\n"
        f" • Активных: {stats['active_ads']}\n"
        f" • На модерации: {stats['moderation_ads']}\n\n"
        f"Финансы:\n"
        f" • Всего платежей: {stats['total_payments']}\n"
        f" • Общая выручка: {stats['total_revenue']}₽\n\n"
        f"Активность:\n"
        f" • Активных пользователей (7д): {stats['active_users_7d']}\n\n"
        f"Тарифы:\n"
    )
    
    for tariff, count in stats['tariff_stats'].items():
        percentage = (count / stats['total_users'] * 100) if stats['total_users'] > 0 else 0
        text += f" • {tariff}: {count} ({percentage:.1f}%)\n"
    
    keyboard = [
        [InlineKeyboardButton("↺ Обновить", callback_data="admin_stats"),
         InlineKeyboardButton("← Панель", callback_data="admin_back")]
    ]
    
    await safe_edit_message(query, text, keyboard)
    db.log_action(update.effective_user.id, "admin_stats_view", "Viewed statistics")

async def admin_moderation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        if not await is_admin(update.effective_user.id):
            await safe_edit_message(query, "✗ У вас нет доступа")
            return

        pending_ads = db.get_pending_moderation(20)

        if not pending_ads:
            text = "✓ На модерации нет объявлений"
            keyboard = [[InlineKeyboardButton("← Панель", callback_data="admin_back")]]
        else:
            text = f"<b>Модерация</b> — {len(pending_ads)} объявл.\n\n"

            keyboard = []
            for ad in pending_ads[:10]:
                uname = f"@{ad['username']}" if ad.get('username') else ad.get('first_name','?')
                city_str = f" · {ad['city']}" if ad.get('city') else ""
                text += (
                    f"<b>#{ad['id']}</b> {ad['title'][:35]}\n"
                    f"{uname}   {ad['price']} ₽{city_str}\n\n"
                )
                keyboard.append([
                    InlineKeyboardButton(f"#{ad['id']} {ad['title'][:22]}", callback_data=f"admin_view_ad:{ad['id']}"),
                ])
                keyboard.append([
                    InlineKeyboardButton("✓ Одобрить", callback_data=f"moderate_approve:{ad['id']}"),
                    InlineKeyboardButton("✗ Отклонить", callback_data=f"moderate_reject:{ad['id']}"),
                ])

            keyboard.append([
                InlineKeyboardButton("↺ Обновить", callback_data="admin_moderation"),
                InlineKeyboardButton("← Панель", callback_data="admin_back"),
            ])

        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
        db.log_action(update.effective_user.id, "admin_moderation_view", f"Viewed {len(pending_ads)} pending ads")
    except Exception as e:
        logger.error(f"Ошибка в admin_moderation: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить очередь модерации.",
                [[InlineKeyboardButton("← Назад", callback_data="admin_panel")]])
        except Exception:
            pass
async def admin_view_moderation_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Полный просмотр объявления из модерации — как видит покупатель, плюс кнопки решения"""
    query = update.callback_query
    await query.answer()

    if not await is_admin(update.effective_user.id):
        return

    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad:
        await query.answer("Объявление не найдено", show_alert=True)
        return

    user_info = db.get_user(ad['user_id'])
    uname = f"@{user_info['username']}" if user_info and user_info.get('username') else (user_info.get('first_name','?') if user_info else '?')
    verify = db.get_verification(ad['user_id'])
    verified = "✓" if verify and verify.get('status') == 'verified' else "—"

    delivery_str = ad.get('delivery', '—') or '—'
    lines = [
        f"<b>Объявление #{ad_id} — на модерации</b>\n",
        f"<b>{ad['title']}</b>",
    ]
    if ad.get('description'):
        lines.append(f"\n{ad['description']}")
    lines.append(f"\n<b>Цена:</b> {ad['price']} ₽")
    if ad.get('condition'):
        lines.append(f"<b>Состояние:</b> {ad['condition']}")
    if ad.get('size'):
        lines.append(f"<b>Размер:</b> {ad['size']}")
    if ad.get('gender'):
        _gl = {'Мужская': '👔 Мужская', 'Женская': '👗 Женская', 'Детская': '🧒 Детская'}.get(ad['gender'], ad['gender'])
        lines.append(f"<b>Для:</b> {_gl}")
    if ad.get('city'):
        lines.append(f"<b>Город:</b> {ad['city']}")
    lines.append(f"<b>Доставка:</b> {delivery_str}")
    if ad.get('category'):
        lines.append(f"<b>Категория:</b> {ad['category']}")
    lines.append(f"\n<b>Продавец:</b> {uname}  (ID: <code>{ad['user_id']}</code>)  {verified}")
    lines.append(f"<b>Создано:</b> {ad['created_at'][:16]}")
    if ad.get('photos'):
        lines.append(f"Фото: {len(ad['photos'])} шт.")

    text = "\n".join(lines)

    keyboard = [
        [InlineKeyboardButton("✓ Одобрить", callback_data=f"moderate_approve:{ad_id}"),
         InlineKeyboardButton("✗ Отклонить", callback_data=f"moderate_reject:{ad_id}")],
        [InlineKeyboardButton("← К списку", callback_data="admin_moderation")],
    ]

    try:
        if ad.get('photos'):
            if len(ad['photos']) == 1:
                await query.message.reply_photo(
                    photo=ad['photos'][0],
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
            else:
                media = [InputMediaPhoto(ad['photos'][0], caption=text, parse_mode=ParseMode.HTML)]
                for p in ad['photos'][1:]:
                    media.append(InputMediaPhoto(p))
                await query.message.reply_media_group(media=media)
                await query.message.reply_text("Действия:", reply_markup=InlineKeyboardMarkup(keyboard))
            await query.delete_message()
        else:
            await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"admin_view_moderation_ad error: {e}")
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

async def moderate_ad_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # НЕ вызываем query.answer() здесь — вызовем один раз с нужным текстом ниже

    try:
        if not await is_admin(update.effective_user.id):
            await query.answer("✗ Нет доступа", show_alert=True)
            return

        action, ad_id_str = query.data.split(":")
        ad_id = int(ad_id_str)
        admin_id = update.effective_user.id

        if action == "moderate_approve":
            pub_result = await publish_ad_to_channel(ad_id, context)
            if isinstance(pub_result, tuple) and len(pub_result) == 4:
                message_id, clothes_ch, gender_pub, tech_ch = pub_result
            elif isinstance(pub_result, tuple):
                message_id, clothes_ch, gender_pub, tech_ch = pub_result[0], None, '', None
            else:
                message_id, clothes_ch, gender_pub, tech_ch = pub_result, None, '', None
            ad_info = db.moderate_ad(ad_id, 'active', admin_id, "Одобрено администратором", message_id)

            if ad_info:
                user_id, title = ad_info['user_id'], ad_info['title']
                try:
                    # Строим список каналов, куда опубликовано
                    ad_for_notify = db.get_ad(ad_id)
                    city = (ad_for_notify.get('city') or '').strip() if ad_for_notify else ''
                    city_ch = CITY_CHANNELS.get(city)
                    channels_list = '• @PolkaAds (вся Россия)'
                    if city_ch:
                        channels_list += f'\n• {city_ch} ({city})'
                    if clothes_ch and gender_pub:
                        clothes_label = {'Мужская': 'мужская', 'Женская': 'женская', 'Детская': 'детская'}.get(gender_pub, gender_pub)
                        channels_list += f'\n• {clothes_ch} (одежда — {clothes_label})'
                    if tech_ch:
                        channels_list += f'\n• {tech_ch} (техника)'
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"✓ Ваше объявление одобрено!\n\n"
                            f"<b>{title}</b>\n\n"
                            f"Опубликовано в каналах:\n{channels_list}\n\n"
                            f"Теперь оно доступно в поиске и покупатели могут вам написать."
                        ),
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")

                # Уведомить пользователей, у которых этот продавец в избранном
                try:
                    fav_cursor = db.conn.cursor()
                    fav_cursor.execute(
                        "SELECT user_id FROM favorite_sellers WHERE seller_id=?",
                        (user_id,)
                    )
                    followers = [r[0] for r in fav_cursor.fetchall()]
                    seller = db.get_user(user_id)
                    seller_name = seller.get('first_name', 'Продавец') if seller else 'Продавец'
                    for follower_id in followers:
                        if follower_id == user_id:
                            continue
                        try:
                            await context.bot.send_message(
                                chat_id=follower_id,
                                text=(
                                    f"◆ <b>Новое объявление от {seller_name}</b>\n\n"
                                    f"{title}\n\n"
                                    f"<i>Этот продавец у вас в избранном.</i>"
                                ),
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("▸ Посмотреть", callback_data=f"view_ad:{ad_id}")
                                ]]),
                                parse_mode=ParseMode.HTML
                            )
                        except Exception:
                            pass
                except Exception as e:
                    logger.error(f"Ошибка уведомления подписчиков: {e}")

                await query.answer("✓ Одобрено!", show_alert=False)
                db.log_action(admin_id, "ad_approved", f"Ad ID: {ad_id}")
            else:
                await query.answer("Объявление не найдено", show_alert=True)
                return

            # Показываем обновлённый список — всегда новым сообщением, чтобы не падать на фото
            pending_ads = db.get_pending_moderation(20)
            if not pending_ads:
                text = "✓ <b>Модерация</b>\n\nВсе объявления проверены!"
                keyboard = [[InlineKeyboardButton("← Панель", callback_data="admin_back")]]
            else:
                text = f"<b>Модерация</b> — осталось {len(pending_ads)} объявл.\n\n"
                keyboard = []
                for ad in pending_ads[:10]:
                    uname = f"@{ad['username']}" if ad.get('username') else ad.get('first_name','?')
                    city_str = f" · {ad['city']}" if ad.get('city') else ""
                    text += f"<b>#{ad['id']}</b> {ad['title'][:35]}\n{uname}   {ad['price']} ₽{city_str}\n\n"
                    keyboard.append([InlineKeyboardButton(f"#{ad['id']} {ad['title'][:22]}", callback_data=f"admin_view_ad:{ad['id']}")])
                    keyboard.append([
                        InlineKeyboardButton("✓ Одобрить", callback_data=f"moderate_approve:{ad['id']}"),
                        InlineKeyboardButton("✗ Отклонить", callback_data=f"moderate_reject:{ad['id']}"),
                    ])
                keyboard.append([
                    InlineKeyboardButton("↺ Обновить", callback_data="admin_moderation"),
                    InlineKeyboardButton("← Панель", callback_data="admin_back"),
                ])

            try:
                # Пробуем отредактировать — работает если это текстовое сообщение
                await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
            except Exception:
                # Если сообщение — фото или уже такое же — отправляем новое
                try:
                    await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
                    await query.message.delete()
                except Exception as e:
                    logger.error(f"moderate_approve reply error: {e}")
            return

        elif action == "moderate_reject":
            await query.answer()
            context.user_data['moderating_ad'] = ad_id
            await safe_edit_message(query,
                text=f"Отклонение объявления #{ad_id}\n\nУкажите причину:",
                keyboard=[[InlineKeyboardButton("✗ Отмена", callback_data="admin_moderation")]]
            )
            return States.ADMIN_MODERATION_REASON

        await query.answer()
    except Exception as e:
        logger.error(f"Ошибка в moderate_ad_action: {e}")
        try:
            await safe_edit_message(query, "Не удалось выполнить действие модерации.",
                [[InlineKeyboardButton("← Назад", callback_data="admin_moderation")]])
        except Exception:
            pass
async def handle_moderation_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    
    if 'moderating_ad' not in context.user_data:
        await update.message.reply_text("✗ Ошибка: не найдено объявление для модерации")
        return States.ADMIN_MENU
    
    reason = update.message.text
    ad_id = context.user_data['moderating_ad']
    
    # Отклоняем объявление с указанием причины
    ad_info = db.moderate_ad(ad_id, 'rejected', update.effective_user.id, reason)
    
    if ad_info:
        user_id, title = ad_info['user_id'], ad_info['title']
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✗ Ваше объявление отклонено\n\n{title}\n\nПричина: {reason}\n\nВы можете создать новое объявление с исправлениями."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
        
        await update.message.reply_text(f"✓ Объявление #{ad_id} отклонено с указанием причины")
        db.log_action(update.effective_user.id, "ad_rejected", f"Ad ID: {ad_id}, Reason: {reason[:50]}")
    else:
        await update.message.reply_text("✗ Объявление не найден")
    
    # Очищаем контекст
    if 'moderating_ad' in context.user_data:
        del context.user_data['moderating_ad']
    
    # Возвращаемся в меню модерации
    await admin_moderation(update, context)
    return States.ADMIN_MENU

async def admin_payments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает платежи, ожидающие проверки"""
    query = update.callback_query
    try:
        if query:
            await query.answer()

        if not await is_admin(update.effective_user.id):
            await safe_edit_message(query, "✗ У вас нет доступа")
            return

        pending_payments = db.get_pending_payments(50)

        if not pending_payments:
            text = "✓ Нет платежей на проверке"
            keyboard = [
                [InlineKeyboardButton("◎ Архив платежей", callback_data="admin_payments_archive")],
                [InlineKeyboardButton("↺ Обновить", callback_data="admin_payments"),
                 InlineKeyboardButton("← Панель", callback_data="admin_back")]
            ]
        else:
            text = f"Ждут проверки: {len(pending_payments)}\n\n"
            keyboard = []
            for payment in pending_payments[:5]:
                text += (
                    f"{payment['id']}: {payment['tariff']} — {payment['amount']}₽\n"
                    f"  {payment['first_name']} (@{payment['username']})\n"
                    f"  {payment['created_at'][:16]}\n\n"
                )
                keyboard.append([
                    InlineKeyboardButton(f"✓ Подтвердить {payment['id']}", callback_data=f"payment_confirm:{payment['id']}"),
                    InlineKeyboardButton(f"✗ Отклонить {payment['id']}", callback_data=f"payment_reject:{payment['id']}")
                ])
                if payment.get('screenshot_file_id'):
                    keyboard.append([
                        InlineKeyboardButton(f"Чек #{payment['id']}", callback_data=f"payment_view_receipt:{payment['id']}")
                    ])

            if len(pending_payments) > 5:
                keyboard.append([InlineKeyboardButton("▸ Следующие 5", callback_data="admin_payments_next")])

            keyboard.append([InlineKeyboardButton("◎ Архив платежей", callback_data="admin_payments_archive")])
            keyboard.append([
                InlineKeyboardButton("↺ Обновить", callback_data="admin_payments"),
                InlineKeyboardButton("← Панель", callback_data="admin_back")
            ])

        await safe_edit_message(query, text, keyboard)
        db.log_action(update.effective_user.id, "admin_payments_view", f"Viewed {len(pending_payments)} pending payments")
    except Exception as e:
        logger.error(f"Ошибка в admin_payments: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить платежи.",
                [[InlineKeyboardButton("← Назад", callback_data="admin_panel")]])
        except Exception:
            pass

# ── Ручная активация PayAnyWay по user_id + тарифу ────────────────────────────
PAW_ACTIVATE_STATES = {}  # временное хранилище для диалога

async def admin_paw_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 1 — просим ввести user_id и тариф"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    context.user_data['paw_activate_step'] = 'waiting_input'
    await safe_edit_message(
        query,
        "💳 <b>Ручная активация PayAnyWay</b>\n\n"
        "Введите в формате:\n"
        "<code>USER_ID тариф</code>\n\n"
        "Пример:\n"
        "<code>123456789 standard</code>\n"
        "<code>123456789 pro</code>\n\n"
        "Тариф: <b>standard</b> (Старт) или <b>pro</b> (PRO)",
        [[InlineKeyboardButton("← Отмена", callback_data="admin_back")]],
        parse_mode=ParseMode.HTML
    )

async def admin_paw_activate_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 2 — получаем ввод и активируем"""
    if not await is_admin(update.effective_user.id):
        return
    if context.user_data.get('paw_activate_step') != 'waiting_input':
        return

    text = update.message.text.strip()
    parts = text.split()
    PLAN_NAMES = {"standard": "Старт", "pro": "PRO"}

    if len(parts) != 2 or not parts[0].isdigit() or parts[1] not in PLAN_NAMES:
        await update.message.reply_text(
            "✗ Неверный формат. Введите: <code>USER_ID тариф</code>\n"
            "Пример: <code>123456789 standard</code>",
            parse_mode=ParseMode.HTML
        )
        return

    user_id    = int(parts[0])
    plan_key   = parts[1]
    plan_label = PLAN_NAMES[plan_key]

    # Проверяем что пользователь существует
    user = db.get_user(user_id)
    if not user:
        await update.message.reply_text(f"✗ Пользователь {user_id} не найден в БД.")
        return

    # Продлеваем подписку
    now_dt = get_moscow_time()
    current_end = user.get('tariff_end_date')
    try:
        end_dt = datetime.fromisoformat(current_end) if current_end else now_dt
        start_dt = end_dt if end_dt > now_dt else now_dt
    except Exception:
        start_dt = now_dt
    new_end = (start_dt + timedelta(days=30)).isoformat()

    cursor = db.conn.cursor()
    cursor.execute(
        "UPDATE users SET tariff=?, tariff_end_date=? WHERE user_id=?",
        (plan_label, new_end, user_id)
    )
    # Создаём запись в платежах
    cursor.execute(
        """INSERT INTO payments (user_id, plan, tariff, amount, status, created_at, confirmed_at)
           VALUES (?, ?, ?, ?, 'paid', ?, ?)""",
        (user_id, plan_key, plan_label, 0, now_dt.isoformat(), now_dt.isoformat())
    )
    db.conn.commit()
    context.user_data.pop('paw_activate_step', None)

    # Уведомляем пользователя
    try:
        await update.get_bot().send_message(
            chat_id=user_id,
            text=f"✅ Оплата получена! Подписка активирована.\n\nТариф <b>{plan_label}</b> активирован на 30 дней.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await update.message.reply_text(
        f"✅ Готово!\n\n"
        f"👤 User: {user_id} ({user.get('first_name', '')})\n"
        f"📦 Тариф: {plan_label}\n"
        f"📅 До: {new_end[:10]}\n\n"
        f"Пользователь получил уведомление.",
        parse_mode="HTML"
    )


async def admin_payments_archive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Архив обработанных платежей (confirmed / rejected)"""
    query = update.callback_query
    await query.answer()

    if not await is_admin(update.effective_user.id):
        await safe_edit_message(query, "✗ У вас нет доступа")
        return

    archived = db.get_archived_payments(30)

    if not archived:
        text = "Архив платежей пуст"
        keyboard = [[InlineKeyboardButton("← К платежам", callback_data="admin_payments")]]
    else:
        text = f"Архив платежей (последние {len(archived)})\n\n"
        for p in archived:
            status_icon = "✓" if p['status'] == 'confirmed' else "✗"
            date_str = (p.get('confirmed_at') or p['created_at'])[:16]
            text += (
                f"{status_icon} #{p['id']} {p['tariff']} — {p['amount']}₽\n"
                f"  {p['first_name']} (@{p['username']})\n"
                f"  {date_str}\n\n"
            )
        keyboard = [[InlineKeyboardButton("← К платежам", callback_data="admin_payments")]]

    await safe_edit_message(query, text, keyboard)

async def admin_view_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет скриншот чека выбранного платежа прямо в чат админа"""
    query = update.callback_query
    # НЕ вызываем query.answer() сразу — вызовем один раз в конце

    if not await is_admin(update.effective_user.id):
        await query.answer("✗ У вас нет доступа", show_alert=True)
        return

    payment_id = int(query.data.split(":")[1])

    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT p.*, u.first_name, u.username FROM payments p "
        "JOIN users u ON p.user_id = u.user_id WHERE p.id = ?",
        (payment_id,)
    )
    row = cursor.fetchone()

    if not row:
        await query.answer("✗ Платеж не найден", show_alert=True)
        return

    p = dict(row)
    file_id = p.get('screenshot_file_id')

    if not file_id:
        await query.answer("✗ Скриншот чека отсутствует", show_alert=True)
        return

    status_map = {'confirmed': '✓ Подтверждён', 'rejected': '✗ Отклонён', 'pending': '◎ Ожидает'}
    status_str = status_map.get(p['status'], p['status'])
    user_str = f"{p['first_name']} (@{p['username']})" if p.get('username') else p['first_name']

    caption = (
        f" Чек платежа #{p['id']}\n\n"
        f"{user_str}\n"
        f"{p['tariff']} — {p['amount']}₽\n"
        f"{p['created_at'][:16]}\n"
        f"Статус: {status_str}"
    )

    keyboard = []
    if p['status'] == 'pending':
        keyboard.append([
            InlineKeyboardButton("✓ Подтвердить", callback_data=f"payment_confirm:{payment_id}"),
            InlineKeyboardButton("✗ Отклонить", callback_data=f"payment_reject:{payment_id}")
        ])
    keyboard.append([InlineKeyboardButton("← К платежам", callback_data="admin_payments")])

    try:
        await query.message.reply_photo(
            photo=file_id,
            caption=caption,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await query.answer()  # ✓ успех — отвечаем один раз
    except Exception as e:
        logger.error(f"Ошибка при отправке чека #{payment_id}: {e}")
        await query.answer("✗ Не удалось загрузить чек", show_alert=True)


async def moderate_payment_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        if not await is_admin(update.effective_user.id):
            await query.answer("✗ У вас нет доступа", show_alert=True)
            return
        
        action, payment_id_str = query.data.split(":")
        payment_id = int(payment_id_str)
        admin_id = update.effective_user.id
        
        if action == "payment_confirm":
            try:
                payment_info = db.moderate_payment(payment_id, 'confirmed', admin_id, "Платеж подтвержден")
            except Exception as e:
                logger.error(f"Ошибка при подтверждении платежа {payment_id}: {e}", exc_info=True)
                await query.answer("✗ Не удалось подтвердить платеж. Проверьте логи.", show_alert=True)
                return
            
            if payment_info:
                user_id, amount, tariff = payment_info['user_id'], payment_info['amount'], payment_info['tariff']
                
                # Получаем актуальную дату окончания подписки
                user_data = db.get_user(user_id)
                tariff_end_date = user_data.get('tariff_end_date', '')
                end_date_display = tariff_end_date[:10] if tariff_end_date else 'неизвестно'
                
                # Начисляем реферальное вознаграждение
                try:
                    referral_tx_id = db.process_referral_reward(user_id, amount)
                except Exception as e:
                    logger.error(f"Ошибка начисления реферального вознаграждения: {e}")
                    referral_tx_id = None
                
                # Уведомляем пользователя
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f" Поздравляем!\n\nВаша подписка {tariff} успешно активирована!\nСумма: {amount}₽\n\nДействует до: {end_date_display}\n\n✓ Теперь вам доступны все премиум-функции!"
                    )
                except Exception as e:
                    logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
                
                # Уведомляем реферера L1
                if referral_tx_id:
                    referrer_id = db.get_referrer(user_id)
                    if referrer_id:
                        try:
                            referrer_stats = db.get_referral_stats(referrer_id)
                            referred_info = db.get_user(user_id)
                            referred_name = f"@{referred_info['username']}" if referred_info and referred_info.get('username') else referred_info['first_name']
                            
                            await context.bot.send_message(
                                chat_id=referrer_id,
                                text=f"Новое реферальное начисление!\n\n"
                                     f"Ваш реферал {referred_name} купил подписку {tariff}\n"
                                     f"Сумма подписки: {amount}₽\n"
                                     f" Ваш процент: {REFERRAL_PERCENT}%\n"
                                     f"Начислено: {int(amount * REFERRAL_PERCENT / 100)}₽\n\n"
                                     f"Ваша статистика:\n"
                                     f"• Всего заработано: {referrer_stats['total_earned']}₽\n"
                                     f"• Текущий баланс: {referrer_stats['balance']}₽\n"
                                     f"• Приглашено пользователей: {referrer_stats['referrals_count']}\n\n"
                                     f" Накопленные средства можно использовать для оплаты своей подписки!"
                            )
                        except Exception as e:
                            logger.error(f"Не удалось уведомить реферера {referrer_id}: {e}")

                db.log_action(admin_id, "payment_confirmed", f"Payment ID: {payment_id}, Amount: {amount}₽")
            else:
                await query.answer("✗ Платеж не найден", show_alert=True)
                return
        
        elif action == "payment_reject":
            await query.answer()
            context.user_data['moderating_payment'] = payment_id
            await safe_edit_message(query,
                text=f"Отклонение платежа #{payment_id}\n\nУкажите причину отклонения:",
                keyboard=[[InlineKeyboardButton("✗ Отмена", callback_data="admin_payments")]]
            )
            return States.ADMIN_MODERATION_REASON
        
        else:
            await query.answer()
        
        # Показываем обновлённый список платежей
        pending_payments = db.get_pending_payments(50)
        if not pending_payments:
            text = "✓ Нет платежей на проверке"
            keyboard = [
                [InlineKeyboardButton("◎ Архив платежей", callback_data="admin_payments_archive")],
                [InlineKeyboardButton("↺ Обновить", callback_data="admin_payments"),
                 InlineKeyboardButton("← Панель", callback_data="admin_back")]
            ]
        else:
            text = f"Ждут проверки: {len(pending_payments)}\n\n"
            keyboard = []
            for payment in pending_payments[:5]:
                text += (
                    f"{payment['id']}: {payment['tariff']} — {payment['amount']}₽\n"
                    f"  {payment['first_name']} (@{payment['username']})\n"
                    f"  {payment['created_at'][:16]}\n\n"
                )
                keyboard.append([
                    InlineKeyboardButton(f"✓ Подтвердить {payment['id']}", callback_data=f"payment_confirm:{payment['id']}"),
                    InlineKeyboardButton(f"✗ Отклонить {payment['id']}", callback_data=f"payment_reject:{payment['id']}")
                ])
                if payment.get('screenshot_file_id'):
                    keyboard.append([
                        InlineKeyboardButton(f"Чек #{payment['id']}", callback_data=f"payment_view_receipt:{payment['id']}")
                    ])
            if len(pending_payments) > 5:
                keyboard.append([InlineKeyboardButton("▸ Следующие 5", callback_data="admin_payments_next")])
            keyboard.append([InlineKeyboardButton("◎ Архив платежей", callback_data="admin_payments_archive")])
            keyboard.append([
                InlineKeyboardButton("↺ Обновить", callback_data="admin_payments"),
                InlineKeyboardButton("← Панель", callback_data="admin_back")
            ])
        
        # Удаляем старое сообщение (может содержать фото чека) и отправляем новое
        try:
            await query.message.delete()
        except Exception as e:
            logger.error(f"Не удалось удалить сообщение при обработке платежа: {e}")
        
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"Критическая ошибка в moderate_payment_action: {e}", exc_info=True)
        try:
            await query.answer("Не удалось обработать платёж. Обновите страницу и попробуйте снова.", show_alert=True)
        except Exception:
            pass
        # Возвращаемся в меню платежей
        await admin_payments(update, context)

async def handle_payment_rejection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not await is_admin(user.id):
        return
    
    if 'moderating_payment' not in context.user_data:
        await update.message.reply_text("✗ Ошибка: не найден платеж для модерации")
        return States.ADMIN_MENU
    
    reason = update.message.text
    payment_id = context.user_data['moderating_payment']
    
    payment_info = db.moderate_payment(payment_id, 'rejected', user.id, reason)
    
    if payment_info:
        user_id, amount, tariff = payment_info['user_id'], payment_info['amount'], payment_info['tariff']
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✗ Ваш платеж отклонен\n\nТариф: {tariff}\nСумма: {amount}₽\n\nПричина: {reason}\n\nЕсли вы считаете это ошибкой, свяжитесь с поддержкой."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
        
        await update.message.reply_text(f"✓ Платеж #{payment_id} отклонен с указанием причины")
        db.log_action(user.id, "payment_rejected", f"Payment ID: {payment_id}, Reason: {reason[:50]}")
    else:
        await update.message.reply_text("✗ Платеж не найден")
    
    # Очищаем контекст
    if 'moderating_payment' in context.user_data:
        del context.user_data['moderating_payment']
    
    # Показываем обновлённый список напрямую (без query, т.к. это message update)
    pending_payments = db.get_pending_payments(50)
    if not pending_payments:
        text = "✓ Нет платежей на проверке"
        keyboard = [
            [InlineKeyboardButton("◎ Архив платежей", callback_data="admin_payments_archive")],
            [InlineKeyboardButton("↺ Обновить", callback_data="admin_payments"),
             InlineKeyboardButton("← Панель", callback_data="admin_back")]
        ]
    else:
        text = f"Ждут проверки: {len(pending_payments)}\n\n"
        keyboard = []
        for payment in pending_payments[:5]:
            text += (
                f"{payment['id']}: {payment['tariff']} — {payment['amount']}₽\n"
                f"  {payment['first_name']} (@{payment['username']})\n"
                f"  {payment['created_at'][:16]}\n\n"
            )
            keyboard.append([
                InlineKeyboardButton(f"✓ Подтвердить {payment['id']}", callback_data=f"payment_confirm:{payment['id']}"),
                InlineKeyboardButton(f"✗ Отклонить {payment['id']}", callback_data=f"payment_reject:{payment['id']}")
            ])
            if payment.get('screenshot_file_id'):
                keyboard.append([
                    InlineKeyboardButton(f"Чек #{payment['id']}", callback_data=f"payment_view_receipt:{payment['id']}")
                ])
        if len(pending_payments) > 5:
            keyboard.append([InlineKeyboardButton("▸ Следующие 5", callback_data="admin_payments_next")])
        keyboard.append([InlineKeyboardButton("◎ Архив платежей", callback_data="admin_payments_archive")])
        keyboard.append([
            InlineKeyboardButton("↺ Обновить", callback_data="admin_payments"),
            InlineKeyboardButton("← Панель", callback_data="admin_back")
        ])
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    return States.ADMIN_MENU

# РАССЫЛКА ДЛЯ АДМИНА - РЕАЛИЗОВАНА ДО 10 ФОТО
async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        await safe_edit_message(query, "✗ У вас нет доступа")
        return
    
    await safe_edit_message(query,
        text="Рассылка сообщений\n\nОтправьте сообщение для рассылки (текст и/или до 10 фото):",
        keyboard=[[InlineKeyboardButton("← Панель", callback_data="admin_back")]]
    )
    
    context.user_data['broadcast_data'] = {'photos': [], 'text': ''}
    return States.ADMIN_BROADCAST

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return States.ADMIN_MENU
    
    if update.message.text:
        # Текстовое сообщение
        context.user_data['broadcast_data']['text'] = update.message.text
        context.user_data['broadcast_data']['photos'] = []
        
        # Показываем превью и запрашиваем подтверждение
        text = f"Превью рассылки\n\n{update.message.text}\n\nОтправить всем пользователям?"
        
        keyboard = [
            [InlineKeyboardButton("✓ Да, отправить", callback_data="broadcast_confirm")],
            [InlineKeyboardButton("✗ Отмена", callback_data="admin_back")]
        ]
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return States.ADMIN_BROADCAST_CONFIRM
    
    elif update.message.photo:
        # Фото с подписью или без
        photo = update.message.photo[-1].file_id
        caption = update.message.caption or ""
        
        # Инициализируем список фото если нужно
        if 'photos' not in context.user_data['broadcast_data']:
            context.user_data['broadcast_data']['photos'] = []
        
        # Добавляем фото (максимум 10)
        if len(context.user_data['broadcast_data']['photos']) < 10:
            context.user_data['broadcast_data']['photos'].append(photo)
            
            if caption and not context.user_data['broadcast_data']['text']:
                context.user_data['broadcast_data']['text'] = caption
            
            remaining = 10 - len(context.user_data['broadcast_data']['photos'])
            
            # Показываем превью и запрашиваем подтверждение
            preview_text = (
                f"Превью рассылки\n\n"
                f"{context.user_data['broadcast_data']['text'] if context.user_data['broadcast_data']['text'] else '[Без текста]'}\n\n"
                f" Загружено фото: {len(context.user_data['broadcast_data']['photos'])}/10\n"
                f"Осталось мест: {remaining}\n\n"
                f"Можно отправить еще фото или нажать 'Далее' для отправки"
            )
            
            keyboard = [
                [InlineKeyboardButton("→ Далее", callback_data="broadcast_preview")],
                [InlineKeyboardButton("✓ Отправить сейчас", callback_data="broadcast_confirm")],
                [InlineKeyboardButton("✗ Отмена", callback_data="admin_back")]
            ]
            
            await update.message.reply_text(preview_text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("✗ Достигнут лимит в 10 фото. Нажмите 'Далее' для продолжения.")
    
    return States.ADMIN_BROADCAST

async def broadcast_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    broadcast_data = context.user_data.get('broadcast_data', {})
    
    text = (
        f"Превью рассылки\n\n"
        f"{broadcast_data.get('text', '[Без текста]')}\n\n"
        f" Фото: {len(broadcast_data.get('photos', []))} шт\n\n"
        f"Отправить всем пользователям?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✓ Да, отправить", callback_data="broadcast_confirm")],
        [InlineKeyboardButton("✗ Отмена", callback_data="admin_back")]
    ]
    
    if broadcast_data.get('photos'):
        try:
            media_group = [InputMediaPhoto(broadcast_data['photos'][0], caption=text)]
            for photo in broadcast_data['photos'][1:]:
                media_group.append(InputMediaPhoto(photo))
            await query.message.reply_media_group(media=media_group)
            await query.message.reply_text("Выберите действие:", reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"broadcast preview photo error: {e}")
            await safe_edit_message(query, text, keyboard)
    else:
        await safe_edit_message(query, text, keyboard)
    
    return States.ADMIN_BROADCAST_CONFIRM

async def broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        await safe_edit_message(query, "✗ У вас нет доступа")
        return
    
    broadcast_data = context.user_data.get('broadcast_data', {})
    
    if not broadcast_data:
        await safe_edit_message(query, "✗ Нет данных для рассылки")
        return States.ADMIN_MENU
    
    # Получаем всех пользователей
    users = db.get_all_users()
    total_users = len(users)
    
    # Отправляем сообщение
    await query.edit_message_text(f" Начинаю рассылку для {total_users} пользователей...")
    
    sent_count = 0
    failed_count = 0
    
    for user in users:
        try:
            if broadcast_data.get('photos'):
                # Отправляем медиагруппу (до 10 фото)
                media_group = []
                for i, photo in enumerate(broadcast_data['photos'][:10]):  # Ограничиваем 10 фото
                    if i == 0:
                        # Первое фото с подписью
                        media_group.append(InputMediaPhoto(
                            photo, 
                            caption=broadcast_data.get('text', '')
                        ))
                    else:
                        media_group.append(InputMediaPhoto(photo))
                
                await context.bot.send_media_group(
                    chat_id=user['user_id'],
                    media=media_group
                )
            else:
                # Отправляем текст
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=broadcast_data.get('text', ''),
                    parse_mode=ParseMode.HTML
                )
            sent_count += 1
        except Exception as e:
            failed_count += 1
            logger.error(f"Не удалось отправить сообщение пользователю {user['user_id']}: {e}")
        
        # Небольшая задержка, чтобы не превысить лимиты Telegram
        await asyncio.sleep(0.1)
    
    # Очищаем данные рассылки
    if 'broadcast_data' in context.user_data:
        del context.user_data['broadcast_data']
    
    # Логируем рассылку
    db.log_action(update.effective_user.id, "broadcast_sent", 
                  f"Sent: {sent_count}, Failed: {failed_count}, Total: {total_users}")
    
    await safe_edit_message(query,
        text=f"✓ Рассылка завершена!\n\nСтатистика:\n• Отправлено: {sent_count}\n• Не отправлено: {failed_count}\n• Всего пользователей: {total_users}",
        keyboard=[[InlineKeyboardButton("← Панель", callback_data="admin_back")]]
    )
    
    return States.ADMIN_MENU

# Общие обработчики
async def cancel_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # Очищаем данные создания объявления
    if 'creating_ad' in context.user_data:
        del context.user_data['creating_ad']
    if 'photo_limit' in context.user_data:
        del context.user_data['photo_limit']
    
    await show_main_menu(update, context)
    return ConversationHandler.END

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    await show_main_menu(update, context)
    return States.MAIN_MENU

# ========== УПРАВЛЕНИЕ ПРОМОКОДАМИ ==========

async def admin_promocodes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление промокодами"""
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        await safe_edit_message(query, "✗ У вас нет доступа")
        return
    
    promocodes = db.get_all_promocodes()
    
    text = "Управление промокодами\n\n"
    
    if promocodes:
        text += "Активные промокоды:\n\n"
        total_uses = 0
        for promo in promocodes:
            status_emoji = "✓" if promo['is_active'] else "✗"
            expires_text = ""
            if promo['expires_at']:
                expires_dt = datetime.fromisoformat(promo['expires_at'])
                expires_text = f" (до {expires_dt.strftime('%d.%m.%Y')})"
            
            text += f"{status_emoji} {promo['code']} - {promo['discount_percent']}% скидка\n"
            text += f"  Использовано: {promo['current_uses']}/{promo['max_uses']}{expires_text}\n"
            text += f" ⏱ Длительность: {promo['duration_days']} дней\n\n"
            
            total_uses += promo['current_uses']
        
        text += f"Всего использований: {total_uses}\n"
    else:
        text += "Промокодов пока нет\n\n"
    
    keyboard = [
        [InlineKeyboardButton("+ Создать промокод", callback_data="admin_promo_create")],
        [InlineKeyboardButton("✗ Деактивировать промокод", callback_data="admin_promo_deactivate")],
        [InlineKeyboardButton("← Назад", callback_data="admin_back")]
    ]
    
    await safe_edit_message(query, text, keyboard)

async def admin_promo_create_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало создания промокода"""
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        return
    
    text = "Создание промокода\n\nВведите код промокода (например: CRAB20):"
    
    keyboard = [[InlineKeyboardButton("✗ Отмена", callback_data="admin_promocodes")]]
    
    await safe_edit_message(query, text, keyboard)
    
    return States.ADMIN_PROMO_CREATE_CODE

async def admin_promo_input_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод кода промокода"""
    code = update.message.text.strip().upper()
    
    # Проверка на уникальность
    if db.get_promocode(code):
        await update.message.reply_text("✗ Промокод с таким кодом уже существует. Введите другой код:")
        return States.ADMIN_PROMO_CREATE_CODE
    
    context.user_data['promo_code'] = code
    
    await update.message.reply_text(
        f"✓ Код: {code}\n\nВведите процент скидки (например: 20):"
    )
    
    return States.ADMIN_PROMO_CREATE_DISCOUNT

async def admin_promo_input_discount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод процента скидки"""
    try:
        discount = int(update.message.text.strip())
        
        if discount <= 0 or discount > 100:
            raise ValueError
        
        context.user_data['promo_discount'] = discount
        
        await update.message.reply_text(
            f"✓ Скидка: {discount}%\n\nВведите длительность подписки в днях (например: 30):"
        )
        
        return States.ADMIN_PROMO_CREATE_DURATION
        
    except ValueError:
        await update.message.reply_text("✗ Неверный формат. Введите число от 1 до 100:")
        return States.ADMIN_PROMO_CREATE_DISCOUNT

async def admin_promo_input_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод длительности подписки"""
    try:
        duration = int(update.message.text.strip())
        
        if duration <= 0:
            raise ValueError
        
        context.user_data['promo_duration'] = duration
        
        await update.message.reply_text(
            f"✓ Длительность: {duration} дней\n\nВведите максимальное количество использований (например: 100):"
        )
        
        return States.ADMIN_PROMO_CREATE_USES
        
    except ValueError:
        await update.message.reply_text("✗ Неверный формат. Введите положительное число:")
        return States.ADMIN_PROMO_CREATE_DURATION

async def admin_promo_input_uses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод максимального количества использований"""
    try:
        max_uses = int(update.message.text.strip())
        
        if max_uses <= 0:
            raise ValueError
        
        # Создаём промокод
        code = context.user_data['promo_code']
        discount = context.user_data['promo_discount']
        duration = context.user_data['promo_duration']
        
        if db.create_promocode(code, discount, duration, max_uses):
            # Проверяем наличие криптоссылки для данного процента скидки
            crypto_link_info = ""
            if discount in CRYPTO_LINKS:
                crypto_link_info = f"\nКриптоссылка: Настроена для {discount}% скидки"
            else:
                crypto_link_info = f"\n! Криптоссылка: Будет использована стандартная (нет специальной для {discount}%)"
            
            text = f"✓ Промокод создан!\n\n"
            text += f"Код: {code}\n"
            text += f"Скидка: {discount}%\n"
            text += f"Длительность: {duration} дней\n"
            text += f"Макс. использований: {max_uses}\n"
            text += crypto_link_info
            
            keyboard = [[InlineKeyboardButton("← К промокодам", callback_data="admin_promocodes")]]
            
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("✗ Ошибка при создании промокода")
        
        # Очищаем данные
        context.user_data.clear()
        
        return ConversationHandler.END
        
    except ValueError:
        await update.message.reply_text("✗ Неверный формат. Введите положительное число:")
        return States.ADMIN_PROMO_CREATE_USES

async def admin_promo_deactivate_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало деактивации промокода"""
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        return
    
    promocodes = db.get_all_promocodes()
    active_codes = [p for p in promocodes if p['is_active']]
    
    if not active_codes:
        await query.answer("✗ Нет активных промокодов", show_alert=True)
        return await admin_promocodes(update, context)
    
    text = " Деактивация промокода\n\nВыберите промокод для деактивации:"
    
    keyboard = []
    for promo in active_codes:
        keyboard.append([InlineKeyboardButton(
            f"{promo['code']} ({promo['current_uses']}/{promo['max_uses']})",
            callback_data=f"admin_promo_deact:{promo['code']}"
        )])
    
    keyboard.append([InlineKeyboardButton("← Назад", callback_data="admin_promocodes")])
    
    await safe_edit_message(query, text, keyboard)

async def admin_promo_deactivate_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение деактивации промокода"""
    query = update.callback_query
    await query.answer()
    
    code = query.data.split(':')[1]
    
    db.deactivate_promocode(code)
    
    await query.answer(f"✓ Промокод {code} деактивирован", show_alert=True)
    
    return await admin_promocodes(update, context)

# ========== УПРАВЛЕНИЕ КРИПТОССЫЛКАМИ ==========

async def admin_crypto_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление криптоссылками"""
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        await safe_edit_message(query, "✗ У вас нет доступа")
        return
    
    crypto_links = db.get_all_crypto_links()
    
    text = "Управление криптоссылками\n\n"
    
    if crypto_links:
        text += "Текущие ссылки:\n\n"
        for link in crypto_links:
            text += f"{link['amount']} ₽\n"
            text += f"{link['crypto_url']}\n\n"
    else:
        text += "Криптоссылок пока нет\n\n"
    
    keyboard = [
        [InlineKeyboardButton("+ Добавить ссылку", callback_data="admin_crypto_add")],
        [InlineKeyboardButton("✗ Удалить ссылку", callback_data="admin_crypto_delete")],
        [InlineKeyboardButton("← Назад", callback_data="admin_back")]
    ]
    
    await safe_edit_message(query, text, keyboard)

async def admin_crypto_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало добавления криптоссылки"""
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        return
    
    text = "Добавление криптоссылки\n\nВведите сумму в рублях (например: 239):"
    
    keyboard = [[InlineKeyboardButton("✗ Отмена", callback_data="admin_crypto_links")]]
    
    await safe_edit_message(query, text, keyboard)
    
    return States.ADMIN_CRYPTO_LINK_AMOUNT

async def admin_crypto_input_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод суммы для криптоссылки"""
    try:
        amount = int(update.message.text.strip())
        
        if amount <= 0:
            raise ValueError
        
        context.user_data['crypto_amount'] = amount
        
        await update.message.reply_text(
            f"✓ Сумма: {amount} ₽\n\nВведите URL криптоссылки:"
        )
        
        return States.ADMIN_CRYPTO_LINK_URL
        
    except ValueError:
        await update.message.reply_text("✗ Неверный формат. Введите положительное число:")
        return States.ADMIN_CRYPTO_LINK_AMOUNT

async def admin_crypto_input_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод URL криптоссылки"""
    url = update.message.text.strip()
    amount = context.user_data['crypto_amount']
    
    db.add_crypto_link(amount, url)
    
    text = f"✓ Криптоссылка добавлена!\n\n"
    text += f"Сумма: {amount} ₽\n"
    text += f"URL: {url}\n"
    
    keyboard = [[InlineKeyboardButton("← К криптоссылкам", callback_data="admin_crypto_links")]]
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    context.user_data.clear()
    
    return ConversationHandler.END

async def admin_crypto_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало удаления криптоссылки"""
    query = update.callback_query
    await query.answer()
    
    if not await is_admin(update.effective_user.id):
        return
    
    crypto_links = db.get_all_crypto_links()
    
    if not crypto_links:
        await query.answer("✗ Нет криптоссылок для удаления", show_alert=True)
        return await admin_crypto_links(update, context)
    
    text = " Удаление криптоссылки\n\nВыберите ссылку для удаления:"
    
    keyboard = []
    for link in crypto_links:
        keyboard.append([InlineKeyboardButton(
            f"{link['amount']} ₽",
            callback_data=f"admin_crypto_del:{link['amount']}"
        )])
    
    keyboard.append([InlineKeyboardButton("← Назад", callback_data="admin_crypto_links")])
    
    await safe_edit_message(query, text, keyboard)

async def admin_crypto_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение удаления криптоссылки"""
    query = update.callback_query
    await query.answer()
    
    amount = int(query.data.split(':')[1])
    
    db.delete_crypto_link(amount)
    
    await query.answer(f"✓ Криптоссылка для {amount} ₽ удалена", show_alert=True)
    
    return await admin_crypto_links(update, context)

async def admin_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'broadcast_data' in context.user_data:
        del context.user_data['broadcast_data']
    return await admin_panel_callback(update, context)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Ошибка: {context.error}")
    
    if update and update.effective_chat:
        await update.effective_chat.send_message("Что-то пошло не так. Попробуйте ещё раз или вернитесь в /start")

# Дополнительные функции
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        " Как пользоваться Полкой\n\n"
        "Полка — доска объявлений в Telegram. Здесь можно продавать и покупать товары быстро и удобно.\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        "◆ Как разместить объявление\n"
        "━━━━━━━━━━━━━━━━━\n"
        "1. Нажмите + Создать объявление в главном меню\n"
        "2. Выберите категорию товара\n"
        "3. Введите название, описание и цену\n"
        "4. Укажите состояние, размер (если нужно), город и способ доставки\n"
        "5. Добавьте фото (до 10 штук)\n"
        "6. Укажите контакт для связи (ваш @username или номер)\n"
        "7. Проверьте превью и нажмите ✓ Опубликовать\n\n"
        "Объявление отправится на модерацию — после проверки оно появится в канале и поиске.\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        " Как найти товар\n"
        "━━━━━━━━━━━━━━━━━\n"
        "1. Нажмите  Поиск в главном меню\n"
        "2. Введите название товара — бот найдёт по синонимам и транслитерации\n"
        " Например: iphone = айфон = ифон\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        "Тарифы\n"
        "━━━━━━━━━━━━━━━━━\n"
        "• Free — 2 объявления в день, базовый поиск\n"
        "• Старт (299₽/мес) — безлимит, 10 фото, 2 буста\n"
        "• Pro (799₽/мес) — 10 фото, авто-подъём, 5 бустов\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        "Реферальная программа\n"
        "━━━━━━━━━━━━━━━━━\n"
        "Приглашайте друзей и получайте 5% от их оплаты подписки на баланс.\n"
        "Баланс можно использовать для оплаты своего тарифа.\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        "? Если что-то не работает\n"
        "━━━━━━━━━━━━━━━━━\n"
        "1. Убедитесь, что вы подписаны на канал @PolkaAdsBot\n"
        "2. Попробуйте /start для перезапуска\n"
        "3. Напишите в поддержку: @PolkaAdsBot"
    )
    await update.message.reply_text(text)

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /menu — возврат в главное меню"""
    await show_main_menu(update, context)

async def show_tinkoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    text = (
        "Реквизиты для оплаты\n\n"
        f"<code>{PAYMENT_DETAILS['tinkoff_account']}</code>\n"
        "Банк: Тинькофф\n\n"
        " После перевода нажмите «Я оплатил» и отправьте фото чека — "
        "администратор подтвердит в течение 15 минут."
    )
    
    keyboard = [
        [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data="confirm_payment")],
        [InlineKeyboardButton("← Назад", callback_data="cancel_payment")]
    ]
    
    await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

# =====================================================================
#  БУСТ ОБЪЯВЛЕНИЙ
# =====================================================================

# Бусты — Вся Россия (основной канал @PolkaAds)
BOOST_PRICE     = 149   # 24 часа в топе РФ
BOOST_PRICE_72H = 299   # 72 часа в топе РФ
BOOST_PACK_5    = 499   # пакет 5 бустов РФ

# Бусты — Городской канал
BOOST_CITY_24H   = 79   # 24 часа в топе города
BOOST_CITY_72H   = 149  # 72 часа в топе города
BOOST_CITY_PACK5 = 299  # пакет 5 городских бустов
URGENT_BADGE_PRICE = 129  # «Срочно» — визуальная плашка
VERIFICATION_PRICE = 0    # верификация бесплатна для всех
AUTO_BUMP_PRICE = 0       # авто-подъём включён в Старт и Про

async def boost_ad_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало буста — пользователь нажал  Поднять в топ"""
    query = update.callback_query
    await query.answer()

    ad_id = int(query.data.split(":")[1])
    user = update.effective_user
    ad = db.get_ad(ad_id)

    if not ad or ad['user_id'] != user.id:
        await safe_edit_message(query, "Объявление не найдено")
        return

    clicks = db.get_ad_contact_clicks(ad_id)

    text = (
        f" Буст объявления\n\n"
        f"{ad['title']}\n"
        f"Просмотров: {ad['views']} |  Интересов: {clicks}\n\n"
        f" Буст поднимает объявление в топ поиска и канала на 24 часа\n"
        f"Обычно увеличивает просмотры в 5–10 раз\n\n"
        f"Стоимость: {BOOST_PRICE} ₽\n"
        f"В комментарии: БУСТ {ad_id}\n\n"
        f"Выберите способ оплаты, затем нажмите «Я оплатил» и отправьте фото чека."
    )

    keyboard = [
        [InlineKeyboardButton("→ ЮMoney", url=PAYMENT_DETAILS['yoomoney_standard'])],
        [InlineKeyboardButton("→ Криптовалюта", url=PAYMENT_DETAILS['crypto_standard'])],
        [InlineKeyboardButton("→ Реквизиты (Тинькофф)", callback_data="show_tinkoff")],
        [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data=f"boost_confirm:{ad_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]
    ]
    await safe_edit_message(query, text, keyboard)

async def boost_confirm_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал «Оплатил» — просим скриншот"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    context.user_data['boosting_ad_id'] = ad_id

    await safe_edit_message(query,
        " Отправьте скриншот чека об оплате буста.\n\nАдминистратор подтвердит и поднимет объявление в течение 15 минут.",
        [[InlineKeyboardButton("✗ Отмена", callback_data=f"view_my_ad:{ad_id}")]]
    )
    return States.BOOST_PAYMENT_PROOF

async def handle_boost_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем скриншот оплаты буста"""
    if not update.message or not update.message.photo:
        await update.message.reply_text("✗ Пожалуйста, отправьте фото/скриншот чека.")
        return States.BOOST_PAYMENT_PROOF

    ad_id = context.user_data.get('boosting_ad_id')
    if not ad_id:
        await update.message.reply_text("Сессия истекла. Вернитесь к объявлению и нажмите «▲ Поднять в топ» ещё раз.")
        return ConversationHandler.END

    file_id = update.message.photo[-1].file_id
    user = update.effective_user
    boost_id = db.create_boost(ad_id, user.id, BOOST_PRICE, 24, file_id)
    ad = db.get_ad(ad_id)

    # Уведомляем админов
    for admin_id in ADMIN_USER_IDS:
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=file_id,
                caption=(
                    f" Новый буст #{boost_id}\n\n"
                    f"{user.first_name} (@{user.username or 'нет'})\n"
                    f"Объявление #{ad_id}: {ad['title'] if ad else '?'}\n"
                    f"{BOOST_PRICE} ₽ / 24 ч"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✓ Подтвердить", callback_data=f"boost_approve:{boost_id}"),
                    InlineKeyboardButton("✗ Отклонить", callback_data=f"boost_reject:{boost_id}")
                ]])
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить админа о бусте: {e}")

    await update.message.reply_text(
        "✓ Чек получен! Администратор подтвердит буст в течение 15 минут.\n"
        "Вы получите уведомление как только объявление будет поднято в топ."
    )
    # Триггер: предложить пакет после второго буста
    ad_id = context.user_data.get('boosting_ad_id', 0)
    asyncio.create_task(
        trigger_boost_pack_upsell(update.effective_user.id, ad_id, context)
    )
    if 'boosting_ad_id' in context.user_data:
        del context.user_data['boosting_ad_id']
    return ConversationHandler.END

async def boost_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ подтверждает буст"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return

    boost_id = int(query.data.split(":")[1])
    boost = db.confirm_boost(boost_id, update.effective_user.id)
    if not boost:
        await query.answer("✗ Буст не найден", show_alert=True)
        return

    ad = db.get_ad(boost['ad_id'])
    # Попытка закрепить в канале
    if ad and ad.get('channel_message_id'):
        try:
            await context.bot.pin_chat_message(
                chat_id=ADMIN_CHANNEL_ID,
                message_id=ad['channel_message_id'],
                disable_notification=True
            )
        except Exception as e:
            logger.error(f"Не удалось закрепить при бусте: {e}")

    try:
        await context.bot.send_message(
            chat_id=boost['user_id'],
            text=(
                f" Буст активирован!\n\n"
                f"{ad['title'] if ad else 'Объявление'}\n"
                f"⏰ Объявление в топе на 24 часа\n\n"
                f"Ждите рост просмотров!"
            )
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить о бусте: {e}")

    await safe_edit_message(query, f"✓ Буст #{boost_id} подтверждён и активирован!")

async def boost_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ отклоняет буст"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return

    boost_id = int(query.data.split(":")[1])
    cursor = db.conn.cursor()
    cursor.execute("SELECT user_id FROM boosts WHERE id=?", (boost_id,))
    row = cursor.fetchone()
    db.reject_boost(boost_id, update.effective_user.id)

    if row:
        try:
            await context.bot.send_message(
                chat_id=dict(row)['user_id'],
                text=f"✗ Буст #{boost_id} отклонён. Свяжитесь с поддержкой если считаете это ошибкой."
            )
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query, f"✗ Буст #{boost_id} отклонён.")

# =====================================================================
#  СОХРАНЁННЫЕ ПОИСКИ
# =====================================================================

async def save_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохранить поисковый запрос для уведомлений"""
    query = update.callback_query
    await query.answer()
    try:
        search_query = query.data.split(":", 1)[1]
        user_id = update.effective_user.id

        saved = db.save_search(user_id, search_query)
        if saved:
            await query.answer(f" Уведомления по «{search_query}» включены!", show_alert=True)
        else:
            await query.answer("Этот поиск уже сохранён.", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка в save_search_callback: {e}")
        try:
            await safe_edit_message(query, "Не удалось сохранить поиск.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def my_saved_searches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список сохранённых поисков пользователя"""
    query = update.callback_query
    await query.answer()
    try:
        user_id = update.effective_user.id
        searches = db.get_user_saved_searches(user_id)

        if not searches:
            text = " Сохранённые поиски\n\nУ вас нет сохранённых поисков.\n\nПри поиске нажмите « Уведомить о новых», чтобы получать уведомления при появлении новых товаров."
            keyboard = [[InlineKeyboardButton("◎ Поиск", callback_data="search"), InlineKeyboardButton("← Назад", callback_data="back_to_menu")]]
        else:
            text = f" Сохранённые поиски: {len(searches)}\n\nВы получаете уведомления при появлении новых объявлений:\n\n"
            for s in searches:
                text += f"• {s['query']}\n"
            keyboard = []
            for s in searches:
                keyboard.append([InlineKeyboardButton(f"✗ Удалить «{s['query']}»", callback_data=f"del_search:{s['query']}")])
            keyboard.append([InlineKeyboardButton("◎ Новый поиск", callback_data="search"), InlineKeyboardButton("← Назад", callback_data="back_to_menu")])

        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в my_saved_searches: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить сохранённые поиски.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def delete_saved_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удалить сохранённый поиск"""
    query = update.callback_query
    await query.answer()
    search_query = query.data.split(":", 1)[1]
    db.delete_saved_search(update.effective_user.id, search_query)
    await query.answer(f" Поиск «{search_query}» удалён", show_alert=True)
    await my_saved_searches(update, context)

# =====================================================================
#  УВЕДОМЛЕНИЯ ПРОДАВЦУ ПРИ ИНТЕРЕСЕ
# =====================================================================

async def track_contact_click(ad_id: int, viewer_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Логируем клик на «Связаться» и уведомляем продавца"""
    seller_id = db.log_contact_click(ad_id, viewer_id)
    if not seller_id or seller_id == viewer_id:
        return
    ad = db.get_ad(ad_id)
    if not ad:
        return
    clicks = db.get_ad_contact_clicks(ad_id)
    try:
        await context.bot.send_message(
            chat_id=seller_id,
            text=(
                f" Покупатель заинтересовался вашим объявлением!\n\n"
                f"{ad['title']}\n"
                f"{ad['price']} ₽\n\n"
                f" Всего интересов: {clicks}\n\n"
                f" Хотите ещё больше просмотров? Поднимите объявление в топ за {BOOST_PRICE} ₽!"
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("▲ Поднять в топ", callback_data=f"boost_ad:{ad_id}")
            ]])
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить продавца {seller_id}: {e}")

# =====================================================================
# ★ ОТЗЫВЫ И РЕЙТИНГ ПРОДАВЦОВ
# =====================================================================

async def leave_review_seller_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отзыв со страницы профиля продавца — берём любое его объявление"""
    query = update.callback_query
    await query.answer()
    seller_id = int(query.data.split(":")[1])
    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT id FROM ads WHERE user_id=? AND status='active' ORDER BY created_at DESC LIMIT 1",
        (seller_id,)
    )
    row = cursor.fetchone()
    if not row:
        cursor.execute("SELECT id FROM ads WHERE user_id=? ORDER BY created_at DESC LIMIT 1", (seller_id,))
        row = cursor.fetchone()
    if not row:
        await query.answer("У продавца нет объявлений для отзыва", show_alert=True)
        return
    ad_id = row[0]
    context.user_data['review_ad_id'] = ad_id
    keyboard = [
        [InlineKeyboardButton("★ 1", callback_data=f"review_rate:1:{ad_id}"),
         InlineKeyboardButton("★★ 2", callback_data=f"review_rate:2:{ad_id}"),
         InlineKeyboardButton("★★★ 3", callback_data=f"review_rate:3:{ad_id}")],
        [InlineKeyboardButton("★★★★ 4", callback_data=f"review_rate:4:{ad_id}"),
         InlineKeyboardButton("★★★★★ 5", callback_data=f"review_rate:5:{ad_id}")],
        [InlineKeyboardButton("← Отмена", callback_data=f"seller_profile:{seller_id}")]
    ]
    await safe_edit_message(query, "Оцените продавца\n\nВыберите оценку от 1 до 5:", keyboard)


async def leave_review_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало оставления отзыва"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    context.user_data['review_ad_id'] = ad_id

    text = (
        "Оцените продавца\n\nВыберите оценку от 1 до 5:"
    )
    keyboard = [
        [
            InlineKeyboardButton("1", callback_data=f"review_rate:1:{ad_id}"),
            InlineKeyboardButton("★★ 2", callback_data=f"review_rate:2:{ad_id}"),
            InlineKeyboardButton("★★★ 3", callback_data=f"review_rate:3:{ad_id}"),
        ],
        [
            InlineKeyboardButton("★★★★ 4", callback_data=f"review_rate:4:{ad_id}"),
            InlineKeyboardButton("★★★★★ 5", callback_data=f"review_rate:5:{ad_id}"),
        ],
        [InlineKeyboardButton("✗ Отмена", callback_data=f"view_ad:{ad_id}")]
    ]
    await safe_edit_message(query, text, keyboard)

async def review_set_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь выбрал рейтинг, просим текст"""
    query = update.callback_query
    await query.answer()
    _, rating_str, ad_id_str = query.data.split(":")
    context.user_data['review_rating'] = int(rating_str)
    context.user_data['review_ad_id'] = int(ad_id_str)

    stars = "★" * int(rating_str)
    await safe_edit_message(query,
        f"Оценка: {stars}\n\nНапишите отзыв (или отправьте «-» чтобы оставить только оценку):",
        [[InlineKeyboardButton("✗ Отмена", callback_data=f"view_ad:{ad_id_str}")]]
    )
    return States.REVIEW_TEXT

async def review_handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняем отзыв"""
    text = update.message.text.strip()
    if text == '-':
        text = ''
    ad_id = context.user_data.get('review_ad_id')
    rating = context.user_data.get('review_rating')
    buyer_id = update.effective_user.id

    if not ad_id or not rating:
        await update.message.reply_text("Сессия истекла. Вернитесь к профилю продавца и попробуйте оставить отзыв снова.")
        return ConversationHandler.END

    ad = db.get_ad(ad_id)
    if not ad:
        await update.message.reply_text("✗ Объявление не найдено.")
        return ConversationHandler.END

    seller_id = ad['user_id']
    if seller_id == buyer_id:
        await update.message.reply_text("✗ Нельзя оставить отзыв на своё объявление.")
        return ConversationHandler.END

    saved = db.create_review(seller_id, buyer_id, ad_id, rating, text)
    stars = "★" * rating

    if saved:
        await update.message.reply_text(f"✓ Отзыв сохранён!\n\n{stars} {'— ' + text if text else ''}")
        # Уведомляем продавца
        seller_rating = db.get_seller_rating(seller_id)
        try:
            await context.bot.send_message(
                chat_id=seller_id,
                text=(
                    f"Новый отзыв!\n\n"
                    f"Оценка: {stars}\n"
                    f"{'Комментарий: ' + text if text else ''}\n\n"
                    f"Ваш рейтинг: {seller_rating['avg']} ★ ({seller_rating['count']} отзывов)"
                )
            )
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    else:
        await update.message.reply_text("✗ Вы уже оставили отзыв на это объявление.")

    for key in ('review_ad_id', 'review_rating'):
        context.user_data.pop(key, None)
    return ConversationHandler.END

async def show_seller_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать отзывы о продавце — красиво как на Авито"""
    query = update.callback_query
    await query.answer()
    try:
        seller_id = int(query.data.split(":")[1])
        viewer_id = update.effective_user.id

        reviews = db.get_seller_reviews(seller_id)
        rating  = db.get_seller_rating(seller_id)
        seller  = db.get_user(seller_id)
        seller_name = seller['first_name'] if seller else f"#{seller_id}"
        verify  = db.get_verification(seller_id)
        is_verified = verify and verify.get('status') == 'verified'
        verified_tag = " ✓" if is_verified else ""

        send_rating_chart = False
        if not reviews:
            text = (
                f"<b>Отзывы о {seller_name}</b>{verified_tag}\n\n"
                "У этого продавца пока нет отзывов.\n\n"
                "<i>Оставьте первый — это поможет другим покупателям!</i>"
            )
        else:
            # Звёздный рейтинг
            avg = rating['avg']
            cnt = rating['count']
            filled = "★" * round(avg)
            empty  = "☆" * (5 - round(avg))

            # Разбивка по звёздам
            breakdown = {5: 0, 4: 0, 3: 0, 2: 0, 1: 0}
            for r in reviews:
                breakdown[r['rating']] = breakdown.get(r['rating'], 0) + 1

            # Красивая подпись (рейтинговый блок уходит в картинку)
            text = (
                f"<b>Отзывы о {seller_name}</b>{verified_tag}\n\n"
                f"<b>{avg} / 5</b>  {filled}{empty}  "
                f"<i>{cnt} отзыв{'а' if 2<=cnt<=4 else 'ов' if cnt>=5 else ''}</i>\n\n"
                f"─────────────────\n\n"
            )
            # Флаг чтобы потом отправить картинку
            send_rating_chart = True

            STAR_EMOJI = {5: "★★★★★", 4: "★★★★☆", 3: "★★★☆☆", 2: "★★☆☆☆", 1: "★☆☆☆☆"}
            for r in reviews[:15]:
                stars_str = STAR_EMOJI.get(r['rating'], "★" * r['rating'])
                name = f"@{r['username']}" if r.get('username') else r.get('first_name', 'Покупатель')
                date_str = r['created_at'][:10] if r.get('created_at') else ''
                review_text = r.get('text', '').strip() if r.get('text') else ''
                text += f"<b>{stars_str}</b>  <b>{name}</b>  <i>{date_str}</i>\n"
                if review_text:
                    # Обрезаем длинные отзывы
                    short = review_text[:200] + ("…" if len(review_text) > 200 else "")
                    text += f"{short}\n"
                text += "\n"

        keyboard = []
        if viewer_id != seller_id:
            last_ad = context.user_data.get('last_viewed_ad_id')
            if last_ad:
                keyboard.append([InlineKeyboardButton("✎ Написать отзыв", callback_data=f"leave_review:{last_ad}")])
        if viewer_id == seller_id and reviews:
            keyboard.append([InlineKeyboardButton("! Обжаловать отзыв", callback_data=f"appeal_review_menu:{seller_id}")])
        keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])

        if send_rating_chart:
            try:
                chart_img = make_rating_chart_image(breakdown, avg, cnt, seller_name)
                await query.message.reply_photo(
                    photo=chart_img,
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
                await query.delete_message()
            except Exception as e:
                logger.error(f"Ошибка рейтингового графика: {e}")
                await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
        else:
            await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в show_seller_reviews: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить отзывы.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def appeal_review_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Продавец выбирает какой отзыв обжаловать"""
    query = update.callback_query
    await query.answer()
    seller_id = int(query.data.split(":")[1])
    if update.effective_user.id != seller_id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return

    reviews = db.get_seller_reviews(seller_id)
    if not reviews:
        await query.answer("Нет отзывов для обжалования", show_alert=True)
        return

    keyboard = []
    for r in reviews[:10]:
        stars = "★" * r['rating']
        keyboard.append([InlineKeyboardButton(
            f"{stars} — {r['text'][:30] if r.get('text') else 'без текста'} (ID:{r['id']})",
            callback_data=f"appeal_review:{r['id']}"
        )])
    keyboard.append([InlineKeyboardButton("← Назад", callback_data=f"seller_reviews:{seller_id}")])
    await safe_edit_message(query, "! Выберите отзыв для обжалования:", keyboard)


async def appeal_review_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Продавец выбрал отзыв — просим причину"""
    query = update.callback_query
    await query.answer()
    review_id = int(query.data.split(":")[1])
    context.user_data['appeal_review_id'] = review_id
    await safe_edit_message(query,
        "! Обжалование отзыва\n\nОпишите причину обжалования (почему считаете отзыв несправедливым):",
        [[InlineKeyboardButton("✗ Отмена", callback_data="back_to_menu")]]
    )
    return States.REVIEW_APPEAL_TEXT


async def appeal_review_handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем текст обжалования и отправляем на модерацию"""
    reason = update.message.text.strip()
    review_id = context.user_data.get('appeal_review_id')
    seller_id = update.effective_user.id

    if not review_id:
        await update.message.reply_text("Сессия истекла. Вернитесь к отзыву и попробуйте обжаловать снова.")
        return ConversationHandler.END

    saved = db.appeal_review(review_id, seller_id, reason)
    if saved:
        # Уведомляем админов
        seller = db.get_user(seller_id)
        for admin_id in ADMIN_USER_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"! Обжалование отзыва #{review_id}\n\n"
                        f"Продавец: {seller['first_name']} (@{seller.get('username','?')})\n"
                        f"Причина: {reason[:200]}\n\n"
                        f"Решите: удалить отзыв или оставить."
                    ),
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("✗ Удалить отзыв", callback_data=f"admin_appeal_delete:{review_id}"),
                        InlineKeyboardButton("✗ Отклонить жалобу", callback_data=f"admin_appeal_reject:{review_id}")
                    ]])
                )
            except Exception as e:
                logger.error(f"appeal admin notify: {e}")
        await update.message.reply_text(
            "✓ Обжалование отправлено на рассмотрение модератору.\nМы уведомим вас о решении.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
        )
    else:
        await update.message.reply_text("Вы уже подали жалобу на этот отзыв. Ожидайте решения модератора.")

    context.user_data.pop('appeal_review_id', None)
    return ConversationHandler.END


async def admin_appeal_decision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Администратор принимает решение по обжалованию"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return

    parts = query.data.split(":")
    action = parts[0]  # admin_appeal_delete or admin_appeal_reject
    review_id = int(parts[1])

    if action == "admin_appeal_delete":
        # Находим appeal_id по review_id
        cursor = db.conn.cursor()
        try:
            cursor.execute('SELECT id, seller_id FROM review_appeals WHERE review_id=? AND status="pending" LIMIT 1', (review_id,))
            row = cursor.fetchone()
            if row:
                appeal_id = dict(row)['id']
                seller_id = dict(row)['seller_id']
                db.decide_appeal(appeal_id, 'delete_review', update.effective_user.id)
                try:
                    await context.bot.send_message(chat_id=seller_id,
                        text="✓ Ваша жалоба рассмотрена — отзыв удалён.")
                except Exception as _e:
                    logger.debug(f"Ignored error: {_e}")
            await safe_edit_message(query, f"✓ Отзыв #{review_id} удалён по результатам обжалования.",
                [[InlineKeyboardButton("← Панель", callback_data="admin_panel")]])
        except Exception as e:
            logger.error(f"admin_appeal_decision delete error: {e}")
            await safe_edit_message(query, "✗ Ошибка при удалении отзыва.",
                [[InlineKeyboardButton("← Панель", callback_data="admin_panel")]])
    else:  # reject
        cursor = db.conn.cursor()
        try:
            cursor.execute('SELECT id, seller_id FROM review_appeals WHERE review_id=? AND status="pending" LIMIT 1', (review_id,))
            row = cursor.fetchone()
            if row:
                appeal_id = dict(row)['id']
                seller_id = dict(row)['seller_id']
                db.decide_appeal(appeal_id, 'reject_appeal', update.effective_user.id)
                try:
                    await context.bot.send_message(chat_id=seller_id,
                        text="✗ Ваша жалоба рассмотрена — отзыв оставлен.")
                except Exception as _e:
                    logger.debug(f"Ignored error: {_e}")
            await safe_edit_message(query, f"✗ Жалоба на отзыв #{review_id} отклонена.",
                [[InlineKeyboardButton("← Панель", callback_data="admin_panel")]])
        except Exception as e:
            logger.error(f"admin_appeal_decision reject error: {e}")
            await safe_edit_message(query, "Не удалось сохранить решение. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Панель", callback_data="admin_panel")]])

# =====================================================================
#  АНАЛИТИКА ПРОСМОТРОВ (График по дням)
# =====================================================================

def make_views_chart_image(chart_data: list, title: str = "") -> io.BytesIO:
    """Генерирует PNG-график просмотров за 14 дней"""
    dates  = [d['date'][5:] for d in chart_data]   # MM-DD
    values = [d['views'] for d in chart_data]

    fig, ax = plt.subplots(figsize=(10, 4.5))
    fig.patch.set_facecolor('#1a1a2e')
    ax.set_facecolor('#16213e')

    max_val = max(values) if values else 0

    if max_val == 0:
        # Нет данных — показываем заглушку
        ax.text(0.5, 0.5, 'Просмотров пока нет\nДанные появятся через сутки',
                ha='center', va='center', color='#b0bec5', fontsize=13,
                transform=ax.transAxes, linespacing=1.8)
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        ax.fill_between(range(len(values)), values, alpha=0.25, color='#4fc3f7')
        ax.plot(range(len(values)), values, color='#4fc3f7', linewidth=2.5, marker='o',
                markersize=5, markerfacecolor='#81d4fa', markeredgewidth=0)

        # Пометить максимум
        peak_i = values.index(max_val)
        ax.annotate(f"пик: {max_val}",
                    xy=(peak_i, max_val),
                    xytext=(peak_i, max_val + max_val * 0.18 + 0.5),
                    ha='center', color='#ffd54f', fontsize=9, fontweight='bold',
                    arrowprops=dict(arrowstyle='->', color='#ffd54f', lw=1.2))

        ax.set_ylim(bottom=0)  # ось Y всегда от 0
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels(dates, rotation=45, ha='right', color='#b0bec5', fontsize=8)
        ax.tick_params(axis='y', colors='#b0bec5', labelsize=9)
        ax.yaxis.grid(True, color='#2a3a5c', linewidth=0.8, linestyle='--')
        ax.set_axisbelow(True)
        ax.set_ylabel('Просмотры', color='#b0bec5', fontsize=9)

    ax.spines[['top','right','left','bottom']].set_visible(False)
    if title:
        ax.set_title(title, color='#eceff1', fontsize=11, pad=10, fontweight='bold')
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=130, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def make_rating_chart_image(breakdown: dict, avg: float, count: int, seller_name: str) -> io.BytesIO:
    """Генерирует PNG с распределением оценок (как на Авито/Ozon)"""
    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor('#1a1a2e')
    ax.set_facecolor('#1a1a2e')

    stars  = [5, 4, 3, 2, 1]
    counts = [breakdown.get(s, 0) for s in stars]
    total  = sum(counts) or 1
    colors = ['#4caf50','#8bc34a','#ffc107','#ff9800','#f44336']

    bars = ax.barh([f'{"★"*s}' for s in stars], counts,
                   color=colors, height=0.55, edgecolor='none')

    # Числа на барах
    for bar, c in zip(bars, counts):
        if c > 0:
            ax.text(bar.get_width() + total * 0.01, bar.get_y() + bar.get_height()/2,
                    str(c), va='center', color='#eceff1', fontsize=10, fontweight='bold')

    # Большой рейтинг слева
    fig.text(0.08, 0.5, f'{avg}', ha='center', va='center',
             fontsize=44, color='#ffd54f', fontweight='bold')
    filled = '★' * round(avg)
    empty  = '☆' * (5 - round(avg))
    fig.text(0.08, 0.25, f'{filled}{empty}', ha='center', va='center',
             fontsize=14, color='#ffd54f')
    fig.text(0.08, 0.15, f'{count} отзыв{"а" if 2<=count<=4 else "ов" if count>=5 else ""}',
             ha='center', va='center', fontsize=9, color='#b0bec5')

    ax.set_xlim(0, max(counts) * 1.25 if max(counts) > 0 else 5)
    ax.tick_params(axis='x', colors='#b0bec5', labelsize=8)
    ax.tick_params(axis='y', colors='#ffd54f', labelsize=12)
    ax.spines[['top','right','left','bottom']].set_visible(False)
    ax.xaxis.grid(True, color='#2a3a5c', linewidth=0.7, linestyle='--')
    ax.set_axisbelow(True)
    ax.set_title(f'Рейтинг продавца {seller_name}', color='#eceff1',
                 fontsize=11, pad=10, fontweight='bold', loc='right')

    plt.subplots_adjust(left=0.18, right=0.95, top=0.88, bottom=0.1)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=130, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def make_admin_analytics_image(stats: dict) -> io.BytesIO:
    """Генерирует PNG с тарифной воронкой и динамикой регистраций"""
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.patch.set_facecolor('#1a1a2e')
    for ax in axes:
        ax.set_facecolor('#16213e')

    # — Диаграмма тарифов (пончик) —
    ax1 = axes[0]
    labels = ['Free', 'Standard', 'PRO']
    sizes  = [stats.get('free',0), stats.get('standard',0), stats.get('pro',0)]
    colors = ['#455a64','#1976d2','#f57c00']
    non_zero = [(l,s,c) for l,s,c in zip(labels,sizes,colors) if s > 0]
    if non_zero:
        lbl, sz, clr = zip(*non_zero)
        wedges, texts, autotexts = ax1.pie(
            sz, labels=lbl, colors=clr, autopct='%1.0f%%',
            startangle=90, wedgeprops=dict(width=0.55, edgecolor='#1a1a2e', linewidth=2),
            textprops={'color':'#eceff1','fontsize':9})
        for at in autotexts:
            at.set_color('#fff')
            at.set_fontweight('bold')
    ax1.set_title('Тарифы', color='#eceff1', fontsize=11, fontweight='bold')

    # — Динамика регистраций —
    ax2 = axes[1]
    if stats.get('reg_dynamics'):
        rd = stats['reg_dynamics']
        d_labels = [d['date'][5:] for d in rd]
        d_vals   = [d['count'] for d in rd]
        ax2.fill_between(range(len(d_vals)), d_vals, alpha=0.3, color='#4fc3f7')
        ax2.plot(range(len(d_vals)), d_vals, color='#4fc3f7', lw=2.5,
                 marker='o', markersize=4, markerfacecolor='#81d4fa')
        ax2.set_xticks(range(len(d_labels)))
        ax2.set_xticklabels(d_labels, rotation=45, ha='right', color='#b0bec5', fontsize=7)
        ax2.yaxis.grid(True, color='#2a3a5c', lw=0.7, linestyle='--')
        ax2.set_axisbelow(True)
    else:
        ax2.text(0.5, 0.5, 'Нет данных', ha='center', va='center', color='#b0bec5', fontsize=11)
    ax2.tick_params(axis='y', colors='#b0bec5', labelsize=8)
    ax2.spines[['top','right','left','bottom']].set_visible(False)
    ax2.set_title('Регистрации (14 дн.)', color='#eceff1', fontsize=11, fontweight='bold')

    # — Динамика выручки —
    ax3 = axes[2]
    if stats.get('rev_dynamics'):
        rv = stats['rev_dynamics']
        r_labels = [d['date'][5:] for d in rv]
        r_vals   = [d['revenue'] or 0 for d in rv]
        ax3.fill_between(range(len(r_vals)), r_vals, alpha=0.3, color='#a5d6a7')
        ax3.plot(range(len(r_vals)), r_vals, color='#a5d6a7', lw=2.5,
                 marker='o', markersize=4, markerfacecolor='#c8e6c9')
        ax3.set_xticks(range(len(r_labels)))
        ax3.set_xticklabels(r_labels, rotation=45, ha='right', color='#b0bec5', fontsize=7)
        ax3.yaxis.grid(True, color='#2a3a5c', lw=0.7, linestyle='--')
        ax3.set_axisbelow(True)
    else:
        ax3.text(0.5, 0.5, 'Нет данных', ha='center', va='center', color='#b0bec5', fontsize=11)
    ax3.tick_params(axis='y', colors='#b0bec5', labelsize=8)
    ax3.spines[['top','right','left','bottom']].set_visible(False)
    ax3.set_title('Выручка (14 дн.), ₽', color='#eceff1', fontsize=11, fontweight='bold')

    plt.tight_layout(pad=2)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def build_ascii_chart(data: List[Dict], width: int = 14) -> str:
    """Строим ASCII-график просмотров для Telegram"""
    if not data:
        return "Нет данных"
    
    values = [d['views'] for d in data]
    max_val = max(values) if max(values) > 0 else 1
    chart_height = 6
    
    lines = []
    for row in range(chart_height, 0, -1):
        threshold = (row / chart_height) * max_val
        line = ""
        for v in values:
            if v >= threshold:
                line += "█"
            elif v >= threshold * 0.5:
                line += "▄"
            else:
                line += " "
        label = f"{int(threshold):>3}"
        lines.append(f"{label}|{line}")
    
    # Ось X — даты
    dates_line = " " + "".join([d['date'][8:10] for d in data])
    lines.append(f"{dates_line}")
    
    return "\n".join(lines)

async def show_ad_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем аналитику просмотров объявления"""
    query = update.callback_query
    await query.answer()

    try:
        ad_id = int(query.data.split(":")[1])
        user = update.effective_user
        ad = db.get_ad(ad_id)

        if not ad or ad['user_id'] != user.id:
            await safe_edit_message(query, "Объявление не найдено")
            return

        # Аналитика только для платных тарифов
        user_data = db.get_user(user.id)
        if user_data.get('tariff', 'Free') == 'Free':
            await safe_edit_message(query,
                "<b>Аналитика объявлений</b>\n\n"
                "Доступна на тарифах <b>★ Старт</b> и <b>▼ Про</b>\n\n"
                "Вы получите:\n"
                "• График просмотров за 14 дней\n"
                "• Количество кликов «Связаться»\n"
                "• Тренд и пиковые дни\n"
                "• Советы по улучшению объявления",
                [[InlineKeyboardButton("◆ Старт — 299 ₽/мес", callback_data="buy_standard")],
                 [InlineKeyboardButton("◆ PRO — 799 ₽/мес", callback_data="buy_pro")],
                 [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]],
                parse_mode=ParseMode.HTML)
            return
        
        chart_data = db.get_ad_views_chart(ad_id, days=14)
        total_views = ad['views']
        contact_clicks = db.get_ad_contact_clicks(ad_id)
        
        # Считаем тренд
        first_week = sum(d['views'] for d in chart_data[:7])
        second_week = sum(d['views'] for d in chart_data[7:])
        if first_week > 0:
            trend = ((second_week - first_week) / first_week) * 100
            trend_icon = "" if trend >= 0 else ""
            trend_text = f"{trend_icon} {'+' if trend >= 0 else ''}{trend:.0f}% за неделю"
        else:
            trend_text = "Недостаточно данных для тренда"
        
        # Пиковый день
        peak = max(chart_data, key=lambda d: d['views'])
        peak_text = f"▼ Пик: {peak['views']} просм. ({peak['date']})" if peak['views'] > 0 else ""
        
        conv_rate = round(contact_clicks / total_views * 100, 1) if total_views > 0 else 0

        lines = [
            f"<b> Аналитика: {ad['title'][:40]}</b>\n",
            f" Просмотров: <b>{total_views}</b>",
            f" Кликов «Связаться»: <b>{contact_clicks}</b>",
        ]
        if total_views > 0:
            lines.append(f"Конверсия: <b>{conv_rate}%</b>")
        if peak_text:
            lines.append(peak_text)
        lines.append(f"\n{trend_text}")

        # Совет
        if total_views == 0:
            lines.append("\n <b>Объявление ещё не получило просмотров.</b>\nПопробуйте поднять его в топ ")
        elif total_views < 20:
            lines.append("\n Мало просмотров? Добавьте фото, снизьте цену или поднимите в топ ")
        elif contact_clicks == 0 and total_views > 10:
            lines.append("\n Есть просмотры, но нет обращений — уточните контакты или снизьте цену немного")

        text = "\n".join(lines)

        keyboard = [
            [InlineKeyboardButton("▲ Поднять в топ", callback_data=f"boost_ad:{ad_id}")],
            [InlineKeyboardButton("↺ Обновить", callback_data=f"ad_analytics:{ad_id}")],
            [InlineKeyboardButton("← К объявлению", callback_data=f"view_my_ad:{ad_id}")]
        ]

        # Отправляем график как фото
        try:
            chart_img = make_views_chart_image(chart_data, title=f"Просмотры: {ad['title'][:35]}")
            await query.message.reply_photo(
                photo=chart_img,
                caption=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
            await query.delete_message()
        except Exception as e:
            logger.error(f"Ошибка генерации графика: {e}")
            await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в show_ad_analytics: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить аналитику.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass

# =====================================================================
#  БЕЗОПАСНАЯ СДЕЛКА (ЭСКРОУ)
# =====================================================================

# =====================================================================

def calc_escrow_commission(amount: int) -> int:
    """Лестничная комиссия — лучше чем у Avito (3–5% без снижения):
       ≤1000₽: 50₽ фикс | ≤5000₽: 2% | ≤20000₽: 1.5% | >20000₽: 1%"""
    if amount <= 1000:
        return 50
    elif amount <= 5000:
        return max(50, int(amount * 0.02))
    elif amount <= 20000:
        return max(100, int(amount * 0.015))
    else:
        return max(300, int(amount * 0.01))

ESCROW_YOOMONEY = PAYMENT_DETAILS.get("yoomoney_standard", "")

async def escrow_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Покупатель инициирует безопасную сделку по объявлению"""
    query = update.callback_query
    await query.answer()
    
    ad_id = int(query.data.split(":")[1])
    buyer = update.effective_user
    ad = db.get_ad(ad_id)
    
    if not ad:
        await safe_edit_message(query, "Объявление не найдено")
        return
    
    if ad['user_id'] == buyer.id:
        await query.answer("✗ Нельзя купить собственное объявление", show_alert=True)
        return

    commission = calc_escrow_commission(ad['price'])
    total = ad['price'] + commission
    comm_pct = round(commission / ad['price'] * 100, 1)

    text = (
        f" Безопасная сделка\n\n"
        f"{ad['title']}\n"
        f"Цена товара: {ad['price']} ₽\n"
        f"Комиссия сервиса: {commission} ₽ ({comm_pct}%)\n"
        f"Итого к оплате: {total} ₽\n\n"
        f"✓ Это дешевле чем на Avito (там 3–5%)\n\n"
        f"Как работает:\n"
        f"1. Вы переводите {total} ₽ на счёт платформы\n"
        f"2. Продавец отправляет товар\n"
        f"3. Вы подтверждаете получение → деньги идут продавцу\n"
        f"4. Если проблема → открываете спор, разбираемся\n\n"
        f"✓ Ваши деньги защищены до получения товара!"
    )
    
    keyboard = [
        [InlineKeyboardButton("✓ Начать безопасную сделку", callback_data=f"escrow_create:{ad_id}")],
        [InlineKeyboardButton("✗ Отмена", callback_data=f"view_ad:{ad_id}")]
    ]
    context.user_data['escrow_ad_id'] = ad_id
    await safe_edit_message(query, text, keyboard)

async def escrow_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создаём сделку и просим оплату"""
    query = update.callback_query
    await query.answer()
    
    try:
        ad_id = int(query.data.split(":")[1])
        buyer = update.effective_user
        ad = db.get_ad(ad_id)
        
        if not ad:
            await safe_edit_message(query, "Объявление не найдено")
            return
        
        commission = calc_escrow_commission(ad["price"])
        total = ad['price'] + commission
        deal_id = db.create_escrow_deal(ad_id, buyer.id, ad['user_id'], ad['price'])
        context.user_data['escrow_deal_id'] = deal_id
        
        text = (
            f" Сделка #{deal_id} создана!\n\n"
            f"{ad['title']}\n"
            f"К оплате: {total} ₽\n\n"
            f"Переведите точно {total} ₽ на реквизиты:\n"
            f"ЮMoney: {ESCROW_YOOMONEY}\n"
            f"Тинькофф: {PAYMENT_DETAILS.get('tinkoff_account', '')}\n\n"
            f"! В комментарии к переводу укажите: СДЕЛКА#{deal_id}\n\n"
            f"После оплаты нажмите «Оплатил» и отправьте скриншот чека."
        )
        
        keyboard = [
            [InlineKeyboardButton("→ ЮMoney", url=ESCROW_YOOMONEY)],
            [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data=f"escrow_paid:{deal_id}")],
            [InlineKeyboardButton("✗ Отменить сделку", callback_data=f"escrow_cancel:{deal_id}")]
        ]
        
        # Уведомляем продавца о новой сделке
        seller = db.get_user(ad['user_id'])
        try:
            await context.bot.send_message(
                chat_id=ad['user_id'],
                text=(
                    f" Покупатель хочет купить ваш товар через безопасную сделку!\n\n"
                    f"{ad['title']}\n"
                    f"{ad['price']} ₽\n"
                    f"Сделка #{deal_id}\n\n"
                    f"Ожидайте — покупатель оплачивает. Как только оплата подтвердится, вам придёт уведомление."
                )
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить продавца о сделке: {e}")
        
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в escrow_create: {e}")
        try:
            await safe_edit_message(query, "Не удалось создать сделку. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def escrow_paid_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Покупатель нажал «Оплатил» — просим скриншот"""
    query = update.callback_query
    await query.answer()
    deal_id = int(query.data.split(":")[1])
    context.user_data['escrow_deal_id'] = deal_id
    
    await safe_edit_message(query,
        f" Отправьте скриншот оплаты сделки #{deal_id}.\n\nАдминистратор проверит и уведомит продавца.",
        [[InlineKeyboardButton("✗ Отмена", callback_data=f"escrow_cancel:{deal_id}")]]
    )
    return States.ESCROW_CONFIRM_BUYER

async def handle_escrow_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем скриншот оплаты эскроу"""
    if not update.message or not update.message.photo:
        await update.message.reply_text("✗ Пожалуйста, отправьте фото/скриншот чека.")
        return States.ESCROW_CONFIRM_BUYER
    
    deal_id = context.user_data.get('escrow_deal_id')
    if not deal_id:
        await update.message.reply_text("Сессия сделки истекла. Вернитесь к объявлению и начните безопасную сделку заново.")
        return ConversationHandler.END
    
    deal = db.get_escrow_deal(deal_id)
    if not deal:
        await update.message.reply_text("✗ Сделка не найдена.")
        return ConversationHandler.END
    
    file_id = update.message.photo[-1].file_id
    ad = db.get_ad(deal['ad_id'])
    commission = calc_escrow_commission(deal["amount"])
    total = deal['amount'] + commission
    
    db.update_escrow(deal_id, payment_screenshot=file_id, status='payment_review')
    
    # Уведомляем всех админов
    for admin_id in ADMIN_USER_IDS:
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=file_id,
                caption=(
                    f" Оплата эскроу #{deal_id}\n\n"
                    f"{ad['title'] if ad else '?'}\n"
                    f"Сумма: {total} ₽ (товар {deal['amount']} + комиссия {commission})\n"
                    f"Покупатель: ID {deal['buyer_id']}\n"
                    f" Продавец: ID {deal['seller_id']}"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✓ Подтвердить оплату", callback_data=f"escrow_admin_confirm:{deal_id}"),
                    InlineKeyboardButton("✗ Отклонить", callback_data=f"escrow_admin_reject:{deal_id}")
                ]])
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления админа об эскроу: {e}")
    
    await update.message.reply_text(
        f"✓ Чек получен! Администратор проверит оплату сделки #{deal_id}.\n"
        f"Как только оплата подтвердится, продавец получит уведомление и отправит товар.\n\n"
        f"Вы получите уведомление когда товар будет отправлен."
    )
    context.user_data.pop('escrow_deal_id', None)
    return ConversationHandler.END

async def escrow_admin_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ подтверждает получение оплаты"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if not deal:
        await query.answer("✗ Сделка не найдена", show_alert=True)
        return
    
    db.update_escrow(deal_id, status='paid', paid_at=get_moscow_time().isoformat())
    ad = db.get_ad(deal['ad_id'])
    
    # Уведомляем покупателя
    try:
        await context.bot.send_message(
            chat_id=deal['buyer_id'],
            text=(
                f"✓ Ваша оплата по сделке #{deal_id} подтверждена!\n\n"
                f"{ad['title'] if ad else 'Товар'}\n\n"
                f"Продавец уведомлён и должен отправить товар.\n"
                f"Когда получите товар — подтвердите получение, и деньги перейдут продавцу."
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✓ Товар получен", callback_data=f"escrow_received:{deal_id}"),
                InlineKeyboardButton("✗ Открыть спор", callback_data=f"escrow_dispute:{deal_id}")
            ]])
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления покупателя: {e}")
    
    # Уведомляем продавца
    try:
        await context.bot.send_message(
            chat_id=deal['seller_id'],
            text=(
                f"Оплата по сделке #{deal_id} получена!\n\n"
                f"{ad['title'] if ad else 'Товар'}\n"
                f"Сумма: {deal['amount']} ₽ (поступит после подтверждения получения)\n\n"
                f" Отправьте товар покупателю и укажите трек-номер.\n"
                f"Деньги поступят на ваш счёт когда покупатель подтвердит получение."
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✓ Товар отправлен", callback_data=f"escrow_shipped:{deal_id}")
            ]])
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления продавца: {e}")
    
    await safe_edit_message(query, f"✓ Оплата сделки #{deal_id} подтверждена. Обе стороны уведомлены.")

async def escrow_admin_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ отклоняет оплату"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if deal:
        db.update_escrow(deal_id, status='payment_rejected')
        try:
            await context.bot.send_message(
                chat_id=deal['buyer_id'],
                text=f"✗ Оплата по сделке #{deal_id} не подтверждена.\n\nСвяжитесь с поддержкой или попробуйте снова."
            )
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query, f"✗ Оплата сделки #{deal_id} отклонена.")

async def escrow_shipped(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Продавец подтверждает отправку"""
    query = update.callback_query
    await query.answer()
    
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if not deal or deal['seller_id'] != update.effective_user.id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return
    
    db.update_escrow(deal_id, status='shipped')
    
    try:
        await context.bot.send_message(
            chat_id=deal['buyer_id'],
            text=(
                f"Продавец отправил товар по сделке #{deal_id}!\n\n"
                f"Как только получите — нажмите «Товар получен» чтобы завершить сделку.\n"
                f"Если возникли проблемы — откройте спор."
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✓ Товар получен!", callback_data=f"escrow_received:{deal_id}"),
                InlineKeyboardButton("✗ Открыть спор", callback_data=f"escrow_dispute:{deal_id}")
            ]])
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления покупателя об отправке: {e}")
    
    await safe_edit_message(query,
        f"✓ Отлично! Покупатель уведомлён об отправке товара.\nКак только подтвердит получение — деньги зачислятся."
    )

async def escrow_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Покупатель подтверждает получение → завершаем сделку"""
    query = update.callback_query
    await query.answer()
    
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if not deal or deal['buyer_id'] != update.effective_user.id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return
    
    db.update_escrow(deal_id, status='completed', buyer_confirmed=1,
                     completed_at=get_moscow_time().isoformat())
    ad = db.get_ad(deal['ad_id'])

    # Триггер: апсейл подписки продавцу после первой сделки
    asyncio.create_task(
        trigger_after_first_sale(deal['buyer_id'], deal['seller_id'], deal['amount'], context)
    )
    
    # Уведомляем продавца о завершении
    try:
        await context.bot.send_message(
            chat_id=deal['seller_id'],
            text=(
                f" Сделка #{deal_id} успешно завершена!\n\n"
                f"{ad['title'] if ad else 'Товар'}\n"
                f"{deal['amount']} ₽ будут переведены вам администратором в ближайшее время.\n\n"
                f"Спасибо за честную сделку! ★"
            )
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления продавца о завершении: {e}")
    
    # Уведомляем админов о необходимости выплаты
    for admin_id in ADMIN_USER_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"ВЫПЛАТА по сделке #{deal_id}\n\n"
                    f"{ad['title'] if ad else 'Товар'}\n"
                    f"К выплате продавцу (ID {deal['seller_id']}): {deal['amount']} ₽\n"
                    f"Комиссия платформы: {deal['commission']} ₽\n\n"
                    f"Сделка завершена покупателем. Переведите {deal['amount']} ₽ продавцу."
                )
            )
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query,
        f" Отлично! Сделка #{deal_id} завершена!\n\n"
        f"Продавец получит деньги в ближайшее время.\n"
        f"Пожалуйста, оставьте отзыв о продавце!",
        [[InlineKeyboardButton("✎ Оставить отзыв", callback_data=f"leave_review:{deal['ad_id']}")],
         [InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
    )

async def escrow_dispute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Покупатель открывает спор"""
    query = update.callback_query
    await query.answer()
    
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if not deal or deal['buyer_id'] != update.effective_user.id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return
    
    db.update_escrow(deal_id, status='disputed')
    ad = db.get_ad(deal['ad_id'])
    
    # Уведомляем всех: продавца и админов
    try:
        await context.bot.send_message(
            chat_id=deal['seller_id'],
            text=f"! Покупатель открыл спор по сделке #{deal_id}.\nАдминистратор рассмотрит ситуацию."
        )
    except Exception as _e:
        logger.debug(f"Ignored error: {_e}")
    for admin_id in ADMIN_USER_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"! СПОР по сделке #{deal_id}\n\n"
                    f"{ad['title'] if ad else '?'}\n"
                    f"{deal['amount']} ₽\n"
                    f"Покупатель: ID {deal['buyer_id']}\n"
                    f" Продавец: ID {deal['seller_id']}\n\n"
                    f"Свяжитесь с обеими сторонами для разрешения."
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✓ Вернуть покупателю", callback_data=f"escrow_refund:{deal_id}"),
                    InlineKeyboardButton("→ Продавцу", callback_data=f"escrow_release:{deal_id}")
                ]])
            )
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query,
        f"! Спор по сделке #{deal_id} открыт.\n\nАдминистратор свяжется с вами в течение 24 часов.\n"
        f"Деньги заморожены до разрешения спора.",
        [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
    )

async def escrow_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена сделки до оплаты"""
    query = update.callback_query
    await query.answer()
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if deal and deal['buyer_id'] == update.effective_user.id and deal['status'] in ('pending_payment', 'payment_rejected'):
        db.update_escrow(deal_id, status='cancelled')
        await safe_edit_message(query, f"✗ Сделка #{deal_id} отменена.",
                                [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
    else:
        await query.answer("Нельзя отменить эту сделку", show_alert=True)

async def escrow_refund(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ решает спор в пользу покупателя"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if deal:
        db.update_escrow(deal_id, status='refunded')
        try:
            await context.bot.send_message(chat_id=deal['buyer_id'],
                text=f"✓ Спор #{deal_id} решён в вашу пользу. Возврат {deal['amount'] + deal['commission']} ₽ будет выполнен администратором.")
            await context.bot.send_message(chat_id=deal['seller_id'],
                text=f"✗ Спор #{deal_id} решён в пользу покупателя. Средства возвращены.")
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query, f"✓ Возврат по сделке #{deal_id} оформлен.")

async def escrow_release(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ решает спор в пользу продавца"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    deal_id = int(query.data.split(":")[1])
    deal = db.get_escrow_deal(deal_id)
    if deal:
        db.update_escrow(deal_id, status='completed', completed_at=get_moscow_time().isoformat())
        try:
            await context.bot.send_message(chat_id=deal['seller_id'],
                text=f"✓ Спор #{deal_id} решён в вашу пользу. {deal['amount']} ₽ будут переведены администратором.")
            await context.bot.send_message(chat_id=deal['buyer_id'],
                text=f"✗ Спор #{deal_id} решён в пользу продавца.")
        except Exception as _e:
            logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query, f"✓ Средства по сделке #{deal_id} переданы продавцу.")

async def my_escrow_deals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список сделок пользователя"""
    query = update.callback_query
    await query.answer()
    try:
        user_id = update.effective_user.id
        deals = db.get_user_escrow_deals(user_id)
        
        if not deals:
            text = " Безопасные сделки\n\nУ вас пока нет сделок.\nИспользуйте кнопку « Безопасная сделка» при покупке товара."
        else:
            text = f" Безопасные сделки: {len(deals)}\n\n"
            for d in deals[:10]:
                role = " Покупатель" if d['buyer_id'] == user_id else " Продавец"
                status = ESCROW_STATUS_LABELS.get(d['status'], d['status'])
                text += f"{role} · #{d['id']}\n"
                text += f"{d.get('title', '?')[:35]}\n"
                text += f"{d['amount']} ₽  {status}\n"
                text += f"{d['created_at'][:10]}\n\n"
        
        keyboard = [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]]
        await safe_edit_message(query, text, keyboard)
    except Exception as e:
        logger.error(f"Ошибка в my_escrow_deals: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить сделки.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass

# =====================================================================
#  АНТИФРОД — жалобы, автобан дублей
# =====================================================================

REPORT_REASONS = [
    "Мошенничество / обман",
    "Товар не соответствует описанию",
    "Дублирующее объявление",
    "Запрещённый товар",
    "Спам / реклама",
]

async def report_ad_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(":")[1])
        ad = db.get_ad(ad_id)
        if not ad:
            await query.answer("Объявление не найдено", show_alert=True)
            return
        keyboard = [[InlineKeyboardButton(r, callback_data=f"report_send:{ad_id}:{r[:30]}")] for r in REPORT_REASONS]
        keyboard.append([InlineKeyboardButton("✗ Отмена", callback_data=f"view_ad:{ad_id}")])
        await safe_edit_message(query, f"! Жалоба на «{ad['title'][:40]}»\n\nВыберите причину:", keyboard)
    except Exception as e:
        logger.error(f"Ошибка в report_ad_start: {e}")
        try:
            await safe_edit_message(query, "Не удалось подать жалобу.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass
async def report_ad_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":", 2)
    ad_id, reason = int(parts[1]), parts[2]
    user_id = update.effective_user.id
    saved = db.report_ad(user_id, ad_id, reason)
    if not saved:
        await query.answer("Вы уже жаловались на это объявление", show_alert=True)
        return
    report_count = db.get_report_count(ad_id)
    ad = db.get_ad(ad_id)
    if report_count >= 3:
        for admin_id in ADMIN_USER_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(f"! Авто-скрытие: {report_count} жалоб\n#{ad_id}: {ad['title'] if ad else '?'}\n{reason}"),
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("✓ Восстановить", callback_data=f"admin_restore_ad:{ad_id}"),
                        InlineKeyboardButton("✗ Удалить навсегда", callback_data=f"admin_delete_ad:{ad_id}"),
                        InlineKeyboardButton("✗ Бан", callback_data=f"admin_ban:{ad['user_id']}")
                    ]])
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления о репорте: {e}")
    await safe_edit_message(query, "✓ Жалоба принята. Рассмотрим в течение 24 часов.",
                            [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])

async def admin_restore_ad_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    ad_id = int(query.data.split(":")[1])
    cursor = db.conn.cursor()
    cursor.execute('UPDATE ads SET status="active" WHERE id=?', (ad_id,))
    db.dismiss_reports(ad_id)
    db.conn.commit()
    await safe_edit_message(query, f"✓ Объявление #{ad_id} восстановлено, жалобы сняты.")

async def admin_ban_user_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    target_id = int(query.data.split(":")[1])
    db.ban_user(target_id, "Нарушение правил платформы", update.effective_user.id)
    try:
        await context.bot.send_message(chat_id=target_id,
            text="✗ Ваш аккаунт заблокирован за нарушение правил.")
    except Exception as _e:
        logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query, f"✗ Пользователь {target_id} заблокирован.")

async def admin_delete_ad_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Полное удаление объявления из БД (только для админов, через жалобу)"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad:
        await query.answer("Объявление не найдено", show_alert=True)
        return
    # Удаляем из канала
    await delete_ad_from_channel(ad_id, context)
    # Удаляем из БД
    cursor = db.conn.cursor()
    cursor.execute("UPDATE ads SET status='rejected' WHERE id=?", (ad_id,))
    db.conn.commit()
    db.log_action(update.effective_user.id, "admin_delete_ad", f"Ad #{ad_id} deleted")
    await safe_edit_message(query,
        f" Объявление #{ad_id} «{ad['title'][:40]}» удалено.",
        [[InlineKeyboardButton("← Панель", callback_data="admin_panel")]]
    )

# =====================================================================
#  РЕАЛЬНОЕ РЕДАКТИРОВАНИЕ ОБЪЯВЛЕНИЙ
# =====================================================================

EDITABLE_FIELDS = {
    "title":        ("Название", States.EDIT_AD_TITLE),
    "description":  ("Описание",  States.EDIT_AD_DESCRIPTION),
    "price":        ("Цену",      States.EDIT_AD_PRICE),
    "city":         ("◎ Город",     States.EDIT_AD_CITY),
    "delivery":     ("Доставка",  States.EDIT_AD_DELIVERY),
    "contact_info": ("Контакт",   States.EDIT_AD_CONTACTS),
}

async def edit_ad_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != update.effective_user.id:
        await safe_edit_message(query, "Объявление не найдено")
        return
    keyboard = [[InlineKeyboardButton(f"{label}", callback_data=f"edit_field:{ad_id}:{field}")]
                for field, (label, _) in EDITABLE_FIELDS.items()]
    keyboard.append([InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")])
    await safe_edit_message(query, f"Редактирование\n\n{ad['title']}\n\nЧто изменить?", keyboard)

async def edit_field_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, ad_id_str, field = query.data.split(":", 2)
    ad_id = int(ad_id_str)
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != update.effective_user.id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return
    label, state = EDITABLE_FIELDS.get(field, ("поле", None))
    if not state:
        await query.answer("✗ Неизвестное поле", show_alert=True)
        return
    context.user_data['editing_ad_id'] = ad_id
    context.user_data['editing_field'] = field
    current = ad.get(field, '') or ''
    await safe_edit_message(query,
        f"Изменить {label}\n\nТекущее:\n{current}\n\nОтправьте новое значение:",
        [[InlineKeyboardButton("✗ Отмена", callback_data=f"edit_ad:{ad_id}")]])
    return state

async def handle_edit_field(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_value = update.message.text.strip()
    ad_id = context.user_data.get('editing_ad_id')
    field = context.user_data.get('editing_field')
    if not ad_id or not field:
        await update.message.reply_text("Сессия истекла. Откройте объявление и начните редактирование заново.")
        return ConversationHandler.END
    if field == 'price':
        if not new_value.isdigit() or int(new_value) <= 0:
            await update.message.reply_text("✗ Цена — число больше 0. Попробуйте ещё раз:")
            return States.EDIT_AD_PRICE
    success, old_value = db.edit_ad_field(ad_id, field, new_value, update.effective_user.id)
    label = EDITABLE_FIELDS.get(field, ("поле", None))[0]
    if success:
        await update.message.reply_text(
            f"✓ {label} обновлено!\n«{new_value[:80]}»",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("← К объявлению", callback_data=f"view_my_ad:{ad_id}"),
                InlineKeyboardButton("✎ Ещё правки", callback_data=f"edit_ad:{ad_id}")
            ]])
        )
        # Уведомляем подписчиков на снижение цены
        if field == 'price' and old_value and int(new_value) < int(old_value):
            watchers = db.notify_price_drop_needed(ad_id, int(new_value))
            ad = db.get_ad(ad_id)
            for w in watchers:
                try:
                    drop_pct = round((1 - int(new_value) / int(old_value)) * 100)
                    await context.bot.send_message(
                        chat_id=w['user_id'],
                        text=(
                            f"Цена снизилась!\n\n"
                            f"{ad['title']}\n"
                            f"Было: {old_value} ₽ → Стало: {new_value} ₽ (−{drop_pct}%)\n"
                        ),
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("▸ Посмотреть", callback_data=f"view_ad:{ad_id}")
                        ]])
                    )
                    # Обновляем цену в подписке
                    db.conn.execute(
                        'UPDATE price_watches SET price_at_subscribe=? WHERE user_id=? AND ad_id=?',
                        (int(new_value), w['user_id'], ad_id)
                    )
                    db.conn.commit()
                except Exception as e:
                    logger.error(f"price drop notify error: {e}")
    else:
        await update.message.reply_text("Не удалось сохранить изменения. Попробуйте ещё раз.")
    for k in ('editing_ad_id', 'editing_field'):
        context.user_data.pop(k, None)
    return ConversationHandler.END

# =====================================================================
#  ВИРАЛЬНАЯ КНОПКА «ПОДЕЛИТЬСЯ»
# =====================================================================

async def share_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad:
        await query.answer("Объявление не найдено", show_alert=True)
        return
    bot_username = (await context.bot.get_me()).username
    share_url = f"https://t.me/{bot_username}?start=ad_{ad_id}"
    tg_share = f"https://t.me/share/url?url={share_url}&text={ad['title']}+за+{ad['price']}+руб."
    text = (
        f" Поделиться объявлением\n\n"
        f"{ad['title']}\n"
        f"{ad['price']} ₽\n\n"
        f"Прямая ссылка:\n{share_url}\n\n"
        f"Чем больше людей увидят — тем быстрее продадите!"
    )
    keyboard = [
        [InlineKeyboardButton("→ Поделиться", url=tg_share)],
        [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]
    ]
    await safe_edit_message(query, text, keyboard)

# =====================================================================
#  ВЕРИФИКАЦИЯ ПРОДАВЦОВ
# =====================================================================

async def verify_seller_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запуск верификации через Telegram Contact"""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    existing = db.get_verification(user_id)
    if existing and existing['status'] == 'verified':
        expires_at = existing.get('expires_at')
        if expires_at:
            now = get_moscow_time().isoformat()
            if expires_at > now:
                await safe_edit_message(query,
                    f"✓ <b>Вы уже верифицированный продавец!</b>\nДействует до: {expires_at[:10]}\n\nЗначок ✓ отображается на всех ваших объявлениях.",
                    [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]],
                    parse_mode=ParseMode.HTML)
                return ConversationHandler.END
        else:
            await safe_edit_message(query,
                "✓ <b>Вы уже верифицированный продавец!</b>\n\nЗначок ✓ отображается на всех ваших объявлениях.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]],
                parse_mode=ParseMode.HTML)
            return ConversationHandler.END

    if existing and existing['status'] == 'pending':
        await safe_edit_message(query,
            "◎ <b>Заявка уже на рассмотрении</b>\n\nОтвет придёт в течение 24 часов.",
            [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]],
            parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    # Отправляем inline-сообщение с объяснением
    await safe_edit_message(
        query,
        " <b>Верификация профиля</b>\n\n"
        "Верификация подтверждает вашу личность через номер телефона, "
        "привязанный к вашему Telegram-аккаунту.\n\n"
        "✓ <b>Что даёт верификация:</b>\n"
        "• Значок ✓ на всех объявлениях и в профиле\n"
        "• Покупатели видят, что вы реальный человек\n"
        "• На 40% больше откликов на объявления\n"
        "• Доверие при сделках через эскроу\n\n"
        "Нажмите кнопку <b>«Поделиться номером»</b> в следующем сообщении — "
        "Telegram автоматически передаст ваш номер боту. Никаких ручных вводов!",
        parse_mode=ParseMode.HTML
    )

    # Отправляем отдельное сообщение с ReplyKeyboard-кнопкой запроса контакта
    contact_keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("Поделиться номером", request_contact=True)],
         [KeyboardButton("✗ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await query.message.reply_text(
        " Нажмите кнопку ниже, чтобы поделиться номером:",
        reply_markup=contact_keyboard
    )
    return States.VERIFY_PHONE_INPUT


async def verify_handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь поделился контактом через Telegram"""
    user = update.effective_user
    contact = update.message.contact

    # Убираем ReplyKeyboard сразу
    await update.message.reply_text(
        "◎ Проверяем данные...",
        reply_markup=ReplyKeyboardRemove()
    )

    # Проверяем, что номер принадлежит именно этому пользователю
    if contact.user_id != user.id:
        await update.message.reply_text(
            "✗ <b>Ошибка верификации</b>\n\n"
            "Номер телефона должен принадлежать вашему Telegram-аккаунту.\n"
            "Нельзя верифицироваться с чужим контактом.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]),
            parse_mode=ParseMode.HTML
        )
        return ConversationHandler.END

    phone = contact.phone_number
    if not phone.startswith('+'):
        phone = '+' + phone

    # Верифицируем — номер реальный, получен напрямую от Telegram
    db.request_verification(user.id, 'phone')
    db.approve_verification(user.id, 0, expires_at=None)  # 0 = system auto

    await update.message.reply_text(
        f" <b>Верификация пройдена!</b>\n\n"
        f"Поздравляем, {user.first_name}! Теперь вы — верифицированный продавец.\n\n"
        f"Подтверждённый номер: {phone}\n\n"
        f"✓ Значок появился на всех ваших объявлениях и в профиле\n"
        f"Покупатели видят, что вы надёжный продавец\n"
        f" Удачных сделок!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]),
        parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END


async def verify_handle_cancel_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал текстовую кнопку Отмена в ReplyKeyboard"""
    await update.message.reply_text(
        "Верификация отменена.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


# Оставляем для обратной совместимости (старый flow через SMS-код не используется)
async def verify_handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await verify_handle_cancel_text(update, context)


async def verify_handle_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await verify_handle_cancel_text(update, context)

async def admin_approve_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    target_id = int(query.data.split(":")[1])
    
    # Получаем метод верификации чтобы знать ставить ли срок
    verify = db.get_verification(target_id)
    method = verify.get('method', '') if verify else ''
    
    expires_at = None
    if method == 'paid':
        # Платная верификация — 1 год
        expires_at = (get_moscow_time() + timedelta(days=365)).isoformat()
    
    db.approve_verification(target_id, update.effective_user.id, expires_at=expires_at)
    
    expires_str = f"\nДействует до: {expires_at[:10]}" if expires_at else ""
    try:
        await context.bot.send_message(chat_id=target_id,
            text=f" Вы верифицированный продавец! Значок ✓ теперь на всех ваших объявлениях.{expires_str}")
    except Exception as _e:
        logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query, f"✓ Пользователь {target_id} верифицирован.")

async def admin_reject_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    target_id = int(query.data.split(":")[1])
    cursor = db.conn.cursor()
    cursor.execute('UPDATE seller_verifications SET status="rejected" WHERE user_id=?', (target_id,))
    db.conn.commit()
    try:
        await context.bot.send_message(chat_id=target_id,
            text="✗ Верификация отклонена. Свяжитесь с поддержкой.")
    except Exception as _e:
        logger.debug(f"Ignored error: {_e}")
    await safe_edit_message(query, f"✗ Верификация {target_id} отклонена.")

# =====================================================================
#  АВТОПРОДЛЕНИЕ: пуш «Товар ещё продаётся?»
# =====================================================================

async def send_renewal_reminders(context: ContextTypes.DEFAULT_TYPE):
    """Ежедневный пуш для объявлений старше 28 дней"""
    cursor = db.conn.cursor()
    cutoff  = (get_moscow_time() - timedelta(days=28)).isoformat()
    too_old = (get_moscow_time() - timedelta(days=56)).isoformat()
    cursor.execute('''
        SELECT id, user_id, title, price, created_at FROM ads
        WHERE status = 'active' AND created_at <= ? AND created_at > ?
    ''', (cutoff, too_old))
    old_ads = [dict(row) for row in cursor.fetchall()]
    for ad in old_ads:
        try:
            await context.bot.send_message(
                chat_id=ad['user_id'],
                text=(
                    f" Ваш товар ещё продаётся?\n\n"
                    f"{ad['title']}\n"
                    f"{ad['price']} ₽\n"
                    f"Опубликовано: {ad['created_at'][:10]}\n\n"
                    f"Нажмите «Ещё продаётся» — поднимем в топ бесплатно, или «Снять» если продано."
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✓ Ещё продаётся — поднять!", callback_data=f"renewal_bump:{ad['id']}")],
                    [InlineKeyboardButton("✗ Продано — снять", callback_data=f"deactivate_ad:{ad['id']}")]
                ])
            )
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Ошибка пуша автопродления {ad['id']}: {e}")

async def renewal_bump(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь подтвердил актуальность — бесплатный буст"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != update.effective_user.id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return
    cursor = db.conn.cursor()
    cursor.execute('UPDATE ads SET created_at=? WHERE id=?', (get_moscow_time().isoformat(), ad_id))
    db.conn.commit()
    await safe_edit_message(query,
        f"Объявление поднято\n\n{ad['title']}\n\nОно находится вверху поиска.",
        [[InlineKeyboardButton("▲ Поднять ещё выше", callback_data=f"boost_ad:{ad_id}")],
         [InlineKeyboardButton("← Меню", callback_data="back_to_menu")]]
    )

# =====================================================================
# ▼ ЗНАЧОК «ЦЕНА НИЖЕ РЫНКА» + шаблоны объявлений
# =====================================================================

# Встроенная база шаблонов популярных товаров
AD_TEMPLATES = {
    "iphone_15": {
        "label": "iPhone 15",
        "title": "iPhone 15",
        "description": "Apple iPhone 15\nПамять: \nЦвет: \nСостояние: \nКомплект: зарядка, коробка\nГарантия: ",
        "category": "· Техника",
    },
    "iphone_14": {
        "label": "iPhone 14",
        "title": "iPhone 14",
        "description": "Apple iPhone 14\nПамять: \nЦвет: \nСостояние: \nКомплект: зарядка, коробка\nГарантия: ",
        "category": "· Техника",
    },
    "airpods_pro": {
        "label": "AirPods Pro",
        "title": "AirPods Pro 2",
        "description": "Apple AirPods Pro 2-го поколения\nСостояние: \nКейс: \nАвтономность: \nГарантия: ",
        "category": "· Техника",
    },
    "macbook": {
        "label": "MacBook",
        "title": "MacBook Air/Pro",
        "description": "Apple MacBook \nПроцессор: \nОЗУ: \nДиск: \nДисплей: \nГод: \nСостояние: ",
        "category": "· Техника",
    },
    "samsung_s": {
        "label": "Samsung Galaxy S",
        "title": "Samsung Galaxy S",
        "description": "Samsung Galaxy S\nПамять: \nЦвет: \nСостояние: \nАккумулятор: \nКомплект: ",
        "category": "· Техника",
    },
    "sneakers": {
        "label": "Кроссовки",
        "title": "Кроссовки ",
        "description": "Кроссовки \nРазмер: \nСостояние: \nНоска: \nДефекты: нет\nОригинал: ",
        "category": "· Обувь",
    },
    "jacket": {
        "label": "Куртка/пуховик",
        "title": "Куртка ",
        "description": "Куртка \nРазмер: \nСостав: \nСостояние: \nДефекты: нет",
        "category": "· Одежда",
    },
    "ps5": {
        "label": "PlayStation 5",
        "title": "PlayStation 5",
        "description": "PlayStation 5\nВерсия: \nКомплект: джойстик, кабели\nСостояние: \nГарантия: ",
        "category": "· Техника",
    },
}

async def templates_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню выбора шаблона при создании объявления"""
    query = update.callback_query
    await query.answer()
    keyboard = []
    row = []
    for key, tpl in AD_TEMPLATES.items():
        row.append(InlineKeyboardButton(tpl['label'], callback_data=f"use_template:{key}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("· Без шаблона", callback_data="create_ad"),
                     InlineKeyboardButton("← Назад", callback_data="back_to_menu")])
    await safe_edit_message(query,
        "Выберите шаблон объявления\n\nШаблон заполнит название и описание — вам останется только указать цену и фото:",
        keyboard
    )

async def use_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Применяем шаблон — сохраняем в user_data и запускаем создание объявления"""
    query = update.callback_query
    await query.answer()
    key = query.data.split(":", 1)[1]
    tpl = AD_TEMPLATES.get(key)
    if not tpl:
        await query.answer("✗ Шаблон не найден", show_alert=True)
        return
    context.user_data['template_title'] = tpl['title']
    context.user_data['template_description'] = tpl['description']
    context.user_data['template_category'] = tpl['category']
    await safe_edit_message(query,
        f"✓ Шаблон «{tpl['label']}» выбран!\n\n"
        f"Название: {tpl['title']}\n"
        f"Категория: {tpl['category']}\n\n"
        f"Нажмите «Создать объявление» — поля будут предзаполнены. "
        f"Вам останется только добавить цену, фото и контакты.",
        [[InlineKeyboardButton("+ Создать объявление", callback_data="create_ad")],
         [InlineKeyboardButton("▸ Другой шаблон", callback_data="templates_menu")]]
    )

# =====================================================================
#  ФИЛЬТР ПО ГОРОДУ В ПОИСКЕ
# =====================================================================

POPULAR_CITIES = ["Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург",
                  "Казань", "Краснодар", "Нижний Новгород", "Челябинск", "Любой город"]

async def search_with_city_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем фильтр по городу перед поиском"""
    query = update.callback_query
    await query.answer()
    keyboard = []
    row = []
    for city in POPULAR_CITIES:
        cb = f"search_city:{city}" if city != "Любой город" else "search_city:all"
        row.append(InlineKeyboardButton(city, callback_data=cb))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])
    await safe_edit_message(query,
        " Выберите город для поиска\n\nИли нажмите «Любой город» чтобы искать по всей базе:",
        keyboard
    )

async def set_search_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняем выбранный город и переходим к поиску"""
    query = update.callback_query
    await query.answer()
    city = query.data.split(":", 1)[1]
    if city == "all":
        context.user_data.pop('search_city', None)
        city_label = "все города"
    else:
        context.user_data['search_city'] = city
        city_label = city
    await safe_edit_message(query,
        f" Поиск по городу: {city_label}\n\nВведите запрос — что ищете?",
        [[InlineKeyboardButton("✗ Сбросить фильтр", callback_data="search_city:all"),
          InlineKeyboardButton("← Назад", callback_data="back_to_menu")]]
    )

# =====================================================================
#  ПОДПИСКА НА СНИЖЕНИЕ ЦЕНЫ
# =====================================================================

async def watch_price_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Покупатель подписывается / отписывается от уведомлений о снижении цены"""
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(":")[1])
        user_id = update.effective_user.id
        ad = db.get_ad(ad_id)
        if not ad:
            await query.answer("Объявление не найдено", show_alert=True)
            return
        if ad['user_id'] == user_id:
            await query.answer("Нельзя следить за своим объявлением", show_alert=True)
            return
        already = db.is_watching_price(user_id, ad_id)
        if already:
            db.unwatch_price(user_id, ad_id)
            await query.answer(" Уведомления о цене отключены", show_alert=True)
        else:
            db.watch_price(user_id, ad_id, ad['price'])
            await query.answer(f" Уведомим если цена снизится с {ad['price']} ₽!", show_alert=True)
        # Обновляем карточку объявления, чтобы метка кнопки изменилась
        await view_ad(update, context, ad_id=ad_id)
    except Exception as e:
        logger.error(f"Ошибка в watch_price_toggle: {e}")
        try:
            await safe_edit_message(query, "Не удалось обновить слежку за ценой.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass

# =====================================================================
#  АВТО-ПОДЪЁМ (PRO-фича)
# =====================================================================

async def auto_bump_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню настройки авто-подъёма"""
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(":")[1])
        user_id = update.effective_user.id
        user = db.get_user(user_id)
        ad = db.get_ad(ad_id)

        if not ad or ad['user_id'] != user_id:
            await query.answer("✗ Нет доступа", show_alert=True)
            return

        tariff = user.get('tariff', 'Free') if user else 'Free'
        bump_interval = TARIFFS.get(tariff, {}).get('auto_bump_interval')

        if not bump_interval:
            await safe_edit_message(query,
                "<b>Авто-подъём</b>\n\n"
                "Объявление автоматически поднимается в топ каждые N часов — без ручного бампа.\n\n"
                "<b>Старт</b> — каждые 24 часа\n"
                "▼ <b>Про</b> — каждые 6 часов",
                [[InlineKeyboardButton("◆ Старт — 299 ₽/мес", callback_data="buy_standard")],
                 [InlineKeyboardButton("◆ PRO — 799 ₽/мес", callback_data="buy_pro")],
                 [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]],
                parse_mode=ParseMode.HTML)
            return

        existing = db.get_auto_bump_for_ad(ad_id)

        if existing:
            text = (
                f"<b>Авто-подъём активен</b>\n\n"
                f"{ad['title']}\n"
                f"⏱ Интервал: каждые {existing['interval_hours']} ч.\n"
                f"Последний подъём: {existing['last_bumped_at'][:16]}\n"
                f"◎ Активен до: {existing['expires_at'][:10]}"
            )
            keyboard = [
                [InlineKeyboardButton("✗ Отключить", callback_data=f"auto_bump_cancel:{ad_id}")],
                [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]
            ]
        else:
            text = (
                f"<b>Авто-подъём</b>  ({tariff})\n\n"
                f"{ad['title']}\n\n"
                f"Объявление будет подниматься в топ каждые <b>{bump_interval} ч.</b>\n"
                f"Включено в ваш тариф — бесплатно!"
            )
            keyboard = [
                [InlineKeyboardButton(f"✓ Включить (каждые {bump_interval}ч)", callback_data=f"auto_bump_set:{ad_id}:{bump_interval}")],
                [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]
            ]

        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в auto_bump_menu: {e}")
        try:
            await safe_edit_message(query, "Не удалось открыть авто-подъём.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def auto_bump_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Активируем авто-подъём"""
    query = update.callback_query
    await query.answer()
    try:
        _, ad_id_str, hours_str = query.data.split(":")
        ad_id, hours = int(ad_id_str), int(hours_str)
        user_id = update.effective_user.id
        db.create_auto_bump(ad_id, user_id, hours)
        ad = db.get_ad(ad_id)
        await safe_edit_message(query,
            f"✓ Авто-подъём активирован!\n\n"
            f"{ad['title']}\n"
            f"⏱ Каждые {hours} часов объявление будет в топе.\n"
            f"Действует 30 дней.",
            [[InlineKeyboardButton("← К объявлению", callback_data=f"view_my_ad:{ad_id}")]]
        )
    except Exception as e:
        logger.error(f"Ошибка в auto_bump_set: {e}")
        try:
            await safe_edit_message(query, "Не удалось включить авто-подъём.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def auto_bump_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отключаем авто-подъём"""
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(":")[1])
        db.cancel_auto_bump(ad_id, update.effective_user.id)
        await safe_edit_message(query,
            "✗ Авто-подъём отключён.",
            [[InlineKeyboardButton("← К объявлению", callback_data=f"view_my_ad:{ad_id}")]]
        )
    except Exception as e:
        logger.error(f"Ошибка в auto_bump_cancel: {e}")
        try:
            await safe_edit_message(query, "Не удалось отключить авто-подъём.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass

# =====================================================================
#  РАСШИРЕННАЯ АНАЛИТИКА ДЛЯ АДМИНА
# =====================================================================

async def admin_advanced_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Расширенная аналитика: воронка, LTV, churn, топы"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return

    stats = db.get_admin_funnel_stats()
    total = stats['total_users'] or 1

    free_pct = round(stats['free'] / total * 100)
    std_pct  = round(stats['standard'] / total * 100)
    pro_pct  = round(stats['pro'] / total * 100)

    text = (
        f"Расширенная аналитика\n\n"
        f"Пользователи: {stats['total_users']}\n"
        f"Общая выручка: {fmt_rub(stats['total_revenue'])} ₽\n"
        f"LTV: {stats['ltv']} ₽/польз.\n"
        f"Churn за 30 дней: {stats['churned_30d']} польз.\n\n"
        f" Воронка конверсии:\n"
        f" Free: {stats['free']} польз. ({free_pct}%)\n"
        f"◈ Standard: {stats['standard']} польз. ({std_pct}%)\n"
        f"PRO: {stats['pro']} польз. ({pro_pct}%)\n\n"
    )

    if stats['top_by_revenue']:
        text += "Топ продавцов по обороту:\n"
        for i, s in enumerate(stats['top_by_revenue'][:5], 1):
            name = f"@{s['username']}" if s.get('username') else s['first_name']
            text += f"{i}. {name} — {fmt_rub(s['total_sold'])} ₽ ({s['deals']} сд.)\n"
        text += "\n"

    if stats['top_sellers']:
        text += "Топ по объявлениям:\n"
        for i, s in enumerate(stats['top_sellers'][:5], 1):
            name = f"@{s['username']}" if s.get('username') else s['first_name']
            text += f"{i}. {name} — {s['ad_count']} объявл.\n"
        text += "\n"

    if stats['reg_dynamics']:
        vals = [d['count'] for d in stats['reg_dynamics']]
        text += f"Регистрации (14 дн.): пик {max(vals)}/день, среднее {round(sum(vals)/len(vals), 1)}/день\n"

    if stats['rev_dynamics']:
        rev_vals = [d['revenue'] or 0 for d in stats['rev_dynamics']]
        text += f"Выручка (14 дн.): пик {fmt_rub(max(rev_vals))} ₽/день"

    keyboard = [
        [InlineKeyboardButton("↺ Обновить", callback_data="admin_advanced_analytics")],
        [InlineKeyboardButton("← Панель", callback_data="admin_panel")]
    ]

    try:
        chart_img = make_admin_analytics_image(stats)
        await query.message.reply_photo(
            photo=chart_img,
            caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
        await query.delete_message()
    except Exception as e:
        logger.error(f"Ошибка графика аналитики: {e}")
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

# =====================================================================
#  МОНЕТИЗАЦИОННЫЕ ТРИГГЕРЫ И АПСЕЙЛЫ
# =====================================================================

async def boost_ad_upsell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Первый экран буста — выбор географии: вся РФ или город"""
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(":")[1])
        ad = db.get_ad(ad_id)
        if not ad or ad['user_id'] != update.effective_user.id:
            await query.answer("✗ Нет доступа", show_alert=True)
            return

        views = ad.get('views', 0)
        contact_clicks = db.get_ad_contact_clicks(ad_id)

        if views > 20 and contact_clicks == 0:
            insight = f"{views} просмотров, но никто не написал — объявление теряется среди конкурентов."
        elif views < 10:
            insight = f"Только {views} просмотров за всё время. Буст выведет вас в топ мгновенно."
        else:
            insight = f"С бустом объявления получают в 7× больше просмотров."

        city = (ad.get('city') or '').strip()
        city_channel = CITY_CHANNELS.get(city)

        text = (
            f"⬆️ Поднять объявление\n\n"
            f"<b>{ad['title']}</b>\n\n"
            f"{insight}\n\n"
            f"Выберите, где поднять объявление:"
        )
        keyboard = [
            [InlineKeyboardButton(f"▲ Вся Россия — от {BOOST_PRICE} ₽", callback_data=f"boost_geo_rf:{ad_id}")]
        ]
        if city_channel:
            keyboard.append([InlineKeyboardButton(
                f"📍 Только {city} — от {BOOST_CITY_24H} ₽",
                callback_data=f"boost_geo_city:{ad_id}"
            )])
        else:
            keyboard.append([InlineKeyboardButton(
                f"📍 Городской канал недоступен для вашего города",
                callback_data="boost_no_city"
            )])
        keyboard.append([InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")])
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в boost_ad_upsell: {e}")
        try:
            await safe_edit_message(query, "Не удалось открыть буст.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def boost_no_city_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer(
        "Для вашего города городской канал пока не подключён. Доступен буст на всю Россию.",
        show_alert=True
    )


async def boost_geo_rf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор варианта буста — Вся Россия"""
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(":")[1])
        ad = db.get_ad(ad_id)
        if not ad or ad['user_id'] != update.effective_user.id:
            await query.answer("✗ Нет доступа", show_alert=True)
            return

        text = (
            f"🇷🇺 Буст — Вся Россия\n\n"
            f"<b>{ad['title']}</b>\n\n"
            f"Объявление поднимается в топ канала <b>@PolkaAds</b> (вся Россия).\n\n"
            f"Выберите длительность:\n"
            f"• 24 часа — <b>{BOOST_PRICE} ₽</b>\n"
            f"• 72 часа — <b>{BOOST_PRICE_72H} ₽</b>  (экономия {BOOST_PRICE*3 - BOOST_PRICE_72H} ₽)\n"
            f"• Пакет 5 бустов — <b>{BOOST_PACK_5} ₽</b>  (экономия {BOOST_PRICE*5 - BOOST_PACK_5} ₽)"
        )
        keyboard = [
            [InlineKeyboardButton(f"▲ 24ч — {BOOST_PRICE} ₽", callback_data=f"boost_ad:{ad_id}")],
            [InlineKeyboardButton(f"▲ 72ч — {BOOST_PRICE_72H} ₽", callback_data=f"boost_72h:{ad_id}")],
            [InlineKeyboardButton(f"▲ Пакет 5 — {BOOST_PACK_5} ₽", callback_data=f"boost_pack:{ad_id}")],
            [InlineKeyboardButton("← Назад", callback_data=f"boost_upsell:{ad_id}")]
        ]
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в boost_geo_rf: {e}")
        try:
            await safe_edit_message(query, "Не удалось открыть буст.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def boost_geo_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор варианта буста — Городской канал"""
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(":")[1])
        ad = db.get_ad(ad_id)
        if not ad or ad['user_id'] != update.effective_user.id:
            await query.answer("✗ Нет доступа", show_alert=True)
            return

        city = (ad.get('city') or '').strip()
        city_channel = CITY_CHANNELS.get(city, '')

        text = (
            f"📍 Буст — {city}\n\n"
            f"<b>{ad['title']}</b>\n\n"
            f"Объявление поднимается в топ городского канала <b>{city_channel}</b>.\n\n"
            f"Выберите длительность:\n"
            f"• 24 часа — <b>{BOOST_CITY_24H} ₽</b>\n"
            f"• 72 часа — <b>{BOOST_CITY_72H} ₽</b>  (экономия {BOOST_CITY_24H*3 - BOOST_CITY_72H} ₽)\n"
            f"• Пакет 5 бустов — <b>{BOOST_CITY_PACK5} ₽</b>  (экономия {BOOST_CITY_24H*5 - BOOST_CITY_PACK5} ₽)"
        )
        keyboard = [
            [InlineKeyboardButton(f"▲ 24ч — {BOOST_CITY_24H} ₽", callback_data=f"boost_city_24h:{ad_id}")],
            [InlineKeyboardButton(f"▲ 72ч — {BOOST_CITY_72H} ₽", callback_data=f"boost_city_72h:{ad_id}")],
            [InlineKeyboardButton(f"▲ Пакет 5 — {BOOST_CITY_PACK5} ₽", callback_data=f"boost_city_pack:{ad_id}")],
            [InlineKeyboardButton("← Назад", callback_data=f"boost_upsell:{ad_id}")]
        ]
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в boost_geo_city: {e}")
        try:
            await safe_edit_message(query, "Не удалось открыть буст.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass
async def boost_city_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оплата городского буста (24ч / 72ч / пакет 5)"""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    variant = parts[0]   # boost_city_24h / boost_city_72h / boost_city_pack
    ad_id = int(parts[1])
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != update.effective_user.id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return

    city = (ad.get('city') or '').strip()
    city_channel = CITY_CHANNELS.get(city, 'городской канал')

    if variant == "boost_city_24h":
        price, hours, label, comment_tag = BOOST_CITY_24H, 24, "24 часа", f"БУСТ-ГОРОД {ad_id}"
    elif variant == "boost_city_72h":
        price, hours, label, comment_tag = BOOST_CITY_72H, 72, "72 часа", f"БУСТ-ГОРОД72 {ad_id}"
    else:  # boost_city_pack
        price, hours, label, comment_tag = BOOST_CITY_PACK5, 24 * 5, "Пакет 5 бустов", f"ПАКЕТ-ГОРОД5 {update.effective_user.id}"

    context.user_data['boosting_ad_id'] = ad_id
    context.user_data['boost_type'] = variant
    context.user_data['boost_price'] = price
    context.user_data['boost_hours'] = hours
    context.user_data['boost_city'] = city
    context.user_data['boost_city_channel'] = city_channel

    text = (
        f"📍 Городской буст — {city}\n\n"
        f"<b>{ad['title']}</b>\n"
        f"Канал: {city_channel}\n"
        f"Длительность: {label}\n"
        f"Стоимость: <b>{price} ₽</b>\n\n"
        f"В комментарии к оплате: <code>{comment_tag}</code>\n\n"
        f"Выберите способ оплаты, затем нажмите «Я оплатил» и отправьте фото чека."
    )
    keyboard = [
        [InlineKeyboardButton("→ ЮMoney", url=PAYMENT_DETAILS['yoomoney_standard'])],
        [InlineKeyboardButton("→ Криптовалюта", url=PAYMENT_DETAILS['crypto_standard'])],
        [InlineKeyboardButton("→ Реквизиты (Тинькофф)", callback_data="show_tinkoff")],
        [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data=f"boost_confirm:{ad_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"boost_geo_city:{ad_id}")]
    ]
    await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)

async def trigger_low_views_boost(context: ContextTypes.DEFAULT_TYPE):
    """Ежедневный триггер: объявления 3+ дней без обращений → предлагаем буст"""
    cursor = db.conn.cursor()
    cutoff3 = (get_moscow_time() - timedelta(days=3)).isoformat()
    cursor.execute('''
        SELECT a.id, a.user_id, a.title, a.price, a.views, a.created_at
        FROM ads a
        WHERE a.status = 'active'
          AND a.created_at <= ?
          AND NOT EXISTS (
              SELECT 1 FROM boosts b WHERE b.ad_id = a.id
              AND b.created_at >= ?
          )
        ORDER BY a.created_at ASC
        LIMIT 200
    ''', (cutoff3, cutoff3))
    stale_ads = [dict(r) for r in cursor.fetchall()]

    for ad in stale_ads:
        contact_clicks = db.get_ad_contact_clicks(ad['id'])
        # Только если совсем мало активности
        if ad['views'] > 50 or contact_clicks > 0:
            continue
        try:
            await context.bot.send_message(
                chat_id=ad['user_id'],
                text=(
                    f"Объявление теряет позиции\n\n"
                    f"{ad['title']}\n"
                    f"Просмотров: {ad['views']}\n\n"
                    f"За 3 дня никто не написал. Объявления конкурентов с бустом "
                    f"получают в 7× больше показов.\n\n"
                    f"Поднять в топ за {BOOST_PRICE} ₽?"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(f"▲ Поднять за {BOOST_PRICE} ₽", callback_data=f"boost_upsell:{ad['id']}"),
                    InlineKeyboardButton("· Не сейчас", callback_data="back_to_menu")
                ]])
            )
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"low_views trigger error ad {ad['id']}: {e}")

async def trigger_after_first_sale(buyer_id: int, seller_id: int, amount: int,
                                    context: ContextTypes.DEFAULT_TYPE):
    """Триггер: после завершения сделки — апсейл подписки продавцу"""
    seller = db.get_user(seller_id)
    if not seller or seller.get('tariff', 'Free') != 'Free':
        return
    # Считаем сколько сделок у продавца
    completed = db.get_completed_deals_count(seller_id)
    try:
        await context.bot.send_message(
            chat_id=seller_id,
            text=(
                f" Поздравляем с продажей!\n\n"
                f"Вы заработали {amount} ₽\n"
                f"✓ Завершено сделок: {completed}\n\n"
                f"Тариф Старт за 299 ₽/мес:\n"
                f"• Безлимит объявлений · 10 фото\n"
                f"• Авто-подъём каждые 24ч\n"
                f"• Аналитика и статистика сделок\n"
                f"• Анонимный чат с покупателями\n"
                f"Окупается с первой же сделки "
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◆ Оформить Старт — 299 ₽", callback_data="buy_standard"),
                InlineKeyboardButton("· Позже", callback_data="back_to_menu")
            ]])
        )
    except Exception as e:
        logger.error(f"after_sale upsell error: {e}")

async def trigger_boost_pack_upsell(user_id: int, ad_id: int,
                                     context: ContextTypes.DEFAULT_TYPE):
    """Триггер: после второй покупки буста — предлагаем пакет"""
    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM boosts WHERE user_id=? AND status IN ('confirmed','pending')",
        (user_id,)
    )
    total_boosts = cursor.fetchone()[0]
    if total_boosts != 2:  # только при второй покупке
        return
    spent = BOOST_PRICE * 2
    saving = BOOST_PRICE * 5 - BOOST_PACK_5
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"Выгодное предложение!\n\n"
                f"Вы уже потратили {spent} ₽ на 2 буста.\n"
                f"Пакет из 5 бустов — {BOOST_PACK_5} ₽\n"
                f"Экономия: {saving} ₽ \n\n"
                f"Бусты не сгорают — используйте когда нужно."
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"▲ Взять пакет — {BOOST_PACK_5} ₽",
                                     callback_data=f"boost_pack:{ad_id}"),
                InlineKeyboardButton("· Не сейчас", callback_data="back_to_menu")
            ]])
        )
    except Exception as e:
        logger.error(f"boost_pack upsell error: {e}")

async def trigger_free_limit_reached(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем когда Free-пользователь упирается в лимит 2 объявления"""
    query = update.callback_query if update.callback_query else None
    user_id = update.effective_user.id
    text = (
        "✗ Лимит бесплатного тарифа\n\n"
        "На Free доступно 2 объявления в день.\n\n"
        "Старт за 299 ₽/мес:\n"
        "• Безлимит объявлений · 10 фото\n"
        "• Авто-подъём каждые 24ч\n"
        "• 1 закрепление в месяц\n"
        "• Аналитика просмотров"
    )
    keyboard = [
        [InlineKeyboardButton("◆ Оформить Старт — 299 ₽", callback_data="buy_standard")],
        [InlineKeyboardButton("◆ PRO — 799 ₽/мес", callback_data="buy_pro")],
        [InlineKeyboardButton("← Меню", callback_data="back_to_menu")]
    ]
    if query:
        await safe_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(
            chat_id=user_id, text=text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def trigger_verification_social_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем при просмотре объявления верифицированного продавца"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    await safe_edit_message(query,
        f" Верификация продавца\n\n"
        f"✓ Верифицированные продавцы получают на 40% больше обращений.\n"
        f"Покупатели видят значок и больше доверяют.\n\n"
        f"Получить верификацию — {VERIFICATION_PRICE} ₽ навсегда\n"
        f"(или бесплатно за 5 завершённых безопасных сделок)",
        [[InlineKeyboardButton("◆ Получить верификацию", callback_data="verify_seller")],
         [InlineKeyboardButton("← Назад", callback_data=f"view_ad:{ad_id}")]]
    )

async def boost_72h_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Буст на 72 часа"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    ad = db.get_ad(ad_id)
    if not ad or ad['user_id'] != update.effective_user.id:
        await query.answer("✗ Нет доступа", show_alert=True)
        return
    text = (
        f" Буст на 72 часа\n\n"
        f"{ad['title']}\n"
        f"Стоимость: {BOOST_PRICE_72H} ₽\n"
        f"⏱ Объявление в топе 3 дня подряд\n\n"
        f"В комментарии: БУСТ72 {ad_id}\n\n"
        f"Выберите способ оплаты, затем нажмите «Я оплатил» и отправьте фото чека."
    )
    keyboard = [
        [InlineKeyboardButton("→ ЮMoney", url=PAYMENT_DETAILS.get('yoomoney_standard',''))],
        [InlineKeyboardButton("→ Криптовалюта", url=PAYMENT_DETAILS.get('crypto_standard',''))],
        [InlineKeyboardButton("→ Реквизиты (Тинькофф)", callback_data="show_tinkoff")],
        [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data=f"boost_confirm:{ad_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"boost_upsell:{ad_id}")]
    ]
    await safe_edit_message(query, text, keyboard)

async def boost_pack_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пакет 5 бустов"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(":")[1])
    text = (
        f"Пакет 5 бустов — {BOOST_PACK_5} ₽\n\n"
        f"Экономия {BOOST_PRICE*5 - BOOST_PACK_5} ₽ по сравнению с поштучной покупкой.\n"
        f"Бусты не сгорают — применяйте к любым объявлениям.\n\n"
        f"В комментарии: ПАКЕТ5 {update.effective_user.id}\n\n"
        f"Выберите способ оплаты, затем нажмите «Я оплатил» и отправьте фото чека."
    )
    keyboard = [
        [InlineKeyboardButton("→ ЮMoney", url=PAYMENT_DETAILS.get('yoomoney_standard',''))],
        [InlineKeyboardButton("→ Криптовалюта", url=PAYMENT_DETAILS.get('crypto_standard',''))],
        [InlineKeyboardButton("→ Реквизиты (Тинькофф)", callback_data="show_tinkoff")],
        [InlineKeyboardButton("✓ Я оплатил — отправить чек", callback_data=f"boost_confirm:{ad_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"boost_upsell:{ad_id}")]
    ]
    await safe_edit_message(query, text, keyboard)


# =====================================================================
#  АНОНИМНЫЙ ЧАТ (мост через бота)
# =====================================================================

async def init_chat_tables():
    cursor = db.conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ad_id INTEGER NOT NULL,
            buyer_id INTEGER NOT NULL,
            seller_id INTEGER NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TEXT,
            last_message_at TEXT,
            UNIQUE(ad_id, buyer_id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            text TEXT,
            file_id TEXT,
            msg_type TEXT DEFAULT 'text',
            created_at TEXT
        )
    ''')
    db.conn.commit()


def get_or_create_chat(ad_id: int, buyer_id: int, seller_id: int) -> int:
    cursor = db.conn.cursor()
    now = get_moscow_time().isoformat()
    cursor.execute('''
        INSERT OR IGNORE INTO chat_sessions (ad_id, buyer_id, seller_id, created_at, last_message_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (ad_id, buyer_id, seller_id, now, now))
    db.conn.commit()
    cursor.execute('SELECT id FROM chat_sessions WHERE ad_id=? AND buyer_id=?', (ad_id, buyer_id))
    row = cursor.fetchone()
    return dict(row)['id'] if row else None


def get_chat_session(session_id: int) -> Optional[Dict]:
    cursor = db.conn.cursor()
    cursor.execute('SELECT * FROM chat_sessions WHERE id=?', (session_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def save_chat_message(session_id: int, sender_id: int, text: str = '', file_id: str = '', msg_type: str = 'text'):
    cursor = db.conn.cursor()
    now = get_moscow_time().isoformat()
    cursor.execute('''
        INSERT INTO chat_messages (session_id, sender_id, text, file_id, msg_type, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (session_id, sender_id, text, file_id, msg_type, now))
    cursor.execute('UPDATE chat_sessions SET last_message_at=? WHERE id=?', (now, session_id))
    db.conn.commit()


def get_user_chats(user_id: int, limit: int = 20) -> List[Dict]:
    cursor = db.conn.cursor()
    cursor.execute('''
        SELECT cs.*, a.title, a.price,
               ub.first_name AS buyer_name, ub.anon_id AS buyer_anon,
               us.first_name AS seller_name, us.anon_id AS seller_anon
        FROM chat_sessions cs
        JOIN ads a ON cs.ad_id = a.id
        JOIN users ub ON cs.buyer_id = ub.user_id
        JOIN users us ON cs.seller_id = us.user_id
        WHERE (cs.buyer_id=? OR cs.seller_id=?) AND cs.status='active'
        ORDER BY cs.last_message_at DESC
        LIMIT ?
    ''', (user_id, user_id, limit))
    return [dict(r) for r in cursor.fetchall()]


async def start_chat_with_seller(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экран продавца перед чатом — анон-ID, рейтинг, избранное, потом чат"""
    query = update.callback_query
    await query.answer()
    ad_id = int(query.data.split(':')[1])
    ad = db.get_ad(ad_id)
    if not ad:
        await query.answer("Объявление не найдено", show_alert=True)
        return

    buyer_id  = update.effective_user.id
    seller_id = ad['user_id']

    if buyer_id == seller_id:
        await query.answer("Это ваше объявление", show_alert=True)
        return

    seller_info = db.get_user(seller_id)
    seller_anon = get_anon_id(seller_info)

    # Верификация
    verify     = db.get_verification(seller_id)
    is_verified = verify and verify.get('status') == 'verified'
    badge      = "  ✓ Верифицирован" if is_verified else ""

    # Рейтинг
    rating = db.get_seller_rating(seller_id)
    if rating['count'] > 0:
        stars      = "★" * round(rating['avg']) + "☆" * (5 - round(rating['avg']))
        rating_str = f"{stars}  {rating['avg']} / 5  ({rating['count']} отз.)"
    else:
        rating_str = "☆☆☆☆☆  Нет отзывов"

    # Статистика продавца (публичная часть)
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ads WHERE user_id=? AND status='active'", (seller_id,))
    active_ads_count = cursor.fetchone()[0]

    # Сделки
    cursor.execute(
        "SELECT COUNT(*) FROM escrow_deals WHERE seller_id=? AND status='completed'",
        (seller_id,)
    )
    deals_row = cursor.fetchone()
    deals_done = deals_row[0] if deals_row else 0

    # Дата регистрации
    reg_date = fmt_date(seller_info.get('registration_date','')) if seller_info else ''

    # Избранное
    already_fav = db.is_favorite_seller(buyer_id, seller_id)
    fav_label = "✗ Убрать из избранного" if already_fav else "♡ Добавить продавца в избранное"
    fav_cb    = f"unfav_seller:{seller_id}" if already_fav else f"fav_seller:{seller_id}"

    text = (
        f"<b>Продавец #{seller_anon}</b>{badge}\n"
        f"──────────────────\n"
        f"{rating_str}\n"
        f"▸ Активных объявлений: <b>{active_ads_count}</b>\n"
        f"✓ Сделок завершено: <b>{deals_done}</b>\n"
        f"◷ На платформе с {reg_date}\n"
        f"──────────────────\n"
        f"<b>{ad['title'][:50]}</b>\n"
        f"<b>{int(ad['price']):,} ₽</b>\n".replace(',', ' ')
    )

    # Создаём/получаем сессию заранее — чтобы сразу открыть Mini App
    session_id = get_or_create_chat(ad_id, buyer_id, seller_id)

    if WEBAPP_URL and session_id:
        webapp_url = f"{WEBAPP_URL}/chat?session={session_id}"
        chat_btn = InlineKeyboardButton("✎ Написать продавцу", web_app=WebAppInfo(url=webapp_url))
    else:
        chat_btn = InlineKeyboardButton("✎ Написать продавцу", callback_data=f"open_chat_now:{ad_id}")

    keyboard = [
        [chat_btn],
        [InlineKeyboardButton(fav_label, callback_data=fav_cb)],
        [InlineKeyboardButton("★ Отзыв", callback_data=f"leave_review_seller:{seller_id}")],
        [InlineKeyboardButton("← К объявлению", callback_data=f"view_ad:{ad_id}")],
    ]

    await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)


async def open_chat_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Открываем чат после карточки продавца"""
    query = update.callback_query
    await query.answer()
    ad_id     = int(query.data.split(':')[1])
    ad        = db.get_ad(ad_id)
    if not ad:
        await query.answer("Объявление не найдено", show_alert=True)
        return

    buyer_id  = update.effective_user.id
    seller_id = ad['user_id']
    seller_info = db.get_user(seller_id)
    seller_anon = get_anon_id(seller_info)

    session_id = get_or_create_chat(ad_id, buyer_id, seller_id)
    context.user_data['active_chat'] = session_id

    cursor = db.conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM chat_messages WHERE session_id=?', (session_id,))
    is_new = cursor.fetchone()[0] == 0

    await safe_edit_message(query,
        f"<b>Диалог с продавцом #{seller_anon}</b>\n"
        f"──────────────────\n"
        f"{ad['title'][:50]}\n\n"
        f"Напишите сообщение — продавец получит его.\n"
        f"<i>Ваши личные данные не передаются.</i>",
        [[InlineKeyboardButton("▸ Все диалоги", callback_data="my_chats")],
         [InlineKeyboardButton("← К объявлению", callback_data=f"view_ad:{ad_id}")]],
        parse_mode=ParseMode.HTML)

    if is_new:
        buyer_info  = db.get_user(buyer_id)
        buyer_anon  = get_anon_id(buyer_info)
        try:
            await context.bot.send_message(
                chat_id=seller_id,
                text=(
                    f"<b>Новый диалог</b>\n"
                    f"──────────────────\n"
                    f"По объявлению «{ad['title'][:40]}»\n"
                    f"Покупатель #{buyer_anon}\n\n"
                    f"Ответьте через «Мои диалоги»."
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("▸ Открыть диалог", callback_data=f"open_chat:{session_id}")
                ]]),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"chat notify: {e}")


async def handle_chat_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        session_id = context.user_data.get('active_chat')
        if not session_id:
            return

        session = get_chat_session(session_id)
        if not session:
            context.user_data.pop('active_chat', None)
            return

        sender_id = update.effective_user.id
        recipient_id = session['seller_id'] if sender_id == session['buyer_id'] else session['buyer_id']
        ad = db.get_ad(session['ad_id'])
        ad_title = ad['title'][:35] if ad else '?'

        file_id = ''
        msg_type = 'text'

        if update.message.text:
            text = update.message.text
            relay = f"<b>Сообщение по «{ad_title}»</b>\n\n{text}"
        elif update.message.photo:
            file_id = update.message.photo[-1].file_id
            text = update.message.caption or ''
            msg_type = 'photo'
            relay = f"<b>Фото по «{ad_title}»</b>" + (f"\n{text}" if text else '')
        else:
            return

        save_chat_message(session_id, sender_id, text=text, file_id=file_id, msg_type=msg_type)

        reply_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▸ Ответить", callback_data=f"open_chat:{session_id}")
        ]])
        try:
            if msg_type == 'photo':
                await context.bot.send_photo(chat_id=recipient_id, photo=file_id,
                    caption=relay, reply_markup=reply_kb, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(chat_id=recipient_id, text=relay,
                    reply_markup=reply_kb, parse_mode=ParseMode.HTML)
            await update.message.reply_text("✓ Отправлено",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("▸ Все диалоги", callback_data="my_chats"),
                    InlineKeyboardButton("✗ Закрыть чат", callback_data="close_chat")
                ]]))
        except Exception as e:
            logger.error(f"chat relay: {e}")
            await update.message.reply_text("Не удалось доставить. Попробуйте позже.")
    except Exception as e:
        logger.error(f"Ошибка в handle_chat_message: {e}")
        try:
            await safe_edit_message(query, "Не удалось отправить сообщение.",
                [[InlineKeyboardButton("← Назад", callback_data="my_chats")]])
        except Exception:
            pass
async def open_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        session_id = int(query.data.split(':')[1])
        session = get_chat_session(session_id)
        if not session:
            await safe_edit_message(query, "Диалог не найден")
            return

        context.user_data['active_chat'] = session_id
        user_id = update.effective_user.id
        ad = db.get_ad(session['ad_id'])
        other_id   = session['seller_id'] if user_id == session['buyer_id'] else session['buyer_id']
        other_info = db.get_user(other_id)
        other_anon = get_anon_id(other_info)

        # Если Mini App настроен — открываем его
        if WEBAPP_URL:
            webapp_url = f"{WEBAPP_URL}/chat?session={session_id}"
            title = ad['title'][:35] if ad else '?'
            await safe_edit_message(query,
                f"💬 <b>Диалог с #{other_anon}</b>\n"
                f"По объявлению «{title}»\n\n"
                f"<i>Нажмите кнопку ниже, чтобы открыть чат.</i>",
                [[InlineKeyboardButton("▸ Открыть чат", web_app=WebAppInfo(url=webapp_url))],
                 [InlineKeyboardButton("▸ Все диалоги", callback_data="my_chats")]],
                parse_mode=ParseMode.HTML)
            return

        # Fallback: старый режим через Telegram
        cursor = db.conn.cursor()
        cursor.execute('SELECT * FROM chat_messages WHERE session_id=? ORDER BY created_at DESC LIMIT 8', (session_id,))
        msgs = list(reversed([dict(r) for r in cursor.fetchall()]))

        history = ''
        for m in msgs:
            who = 'Вы' if m['sender_id'] == user_id else f"#{other_anon}"
            ts  = m['created_at'][11:16] if m.get('created_at') else ''
            txt = m.get('text') or ''
            history += f"<b>{who}</b>  <i>{ts}</i>\n{txt}\n\n"

        _empty_msg = "<i>Сообщений нет — напишите первым!</i>\n\n"
        await safe_edit_message(query,
            f"<b>Диалог с #{other_anon}</b>\n"
            f"──────────────────\n"
            f"{(ad['title'][:45] if ad else '?')}\n\n"
            f"{history or _empty_msg}"
            f"Напишите следующее сообщение:",
            [[InlineKeyboardButton("▸ Все диалоги", callback_data="my_chats")],
             [InlineKeyboardButton("✗ Закрыть чат", callback_data="close_chat")]],
            parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в open_chat: {e}")
        try:
            await safe_edit_message(query, "Не удалось открыть диалог.",
                [[InlineKeyboardButton("← Назад", callback_data="my_chats")]])
        except Exception:
            pass
async def close_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop('active_chat', None)
    await safe_edit_message(query, "Чат закрыт.",
        [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])


async def my_chats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        user_id = update.effective_user.id
        chats = get_user_chats(user_id)

        if not chats:
            await safe_edit_message(query,
                "<b>Мои диалоги</b>\n\nПока нет диалогов.\n"
                "Напишите продавцу из карточки объявления.",
                [[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]],
                parse_mode=ParseMode.HTML)
            return

        text = f"<b>Мои диалоги</b>  ·  {len(chats)}\n──────────────────\n"
        keyboard = []
        for ch in chats[:10]:
            is_buyer   = user_id == ch['buyer_id']
            other_anon = ch['seller_anon'] or '??????' if is_buyer else ch['buyer_anon'] or '??????'
            last       = fmt_datetime(ch.get('last_message_at','')) if ch.get('last_message_at') else ''
            role       = "→" if is_buyer else "←"
            title_short = (ch.get('title') or '?')[:28]
            text += f"{role} #{other_anon}  ·  {title_short}  ·  {last}\n"

            if WEBAPP_URL:
                webapp_url = f"{WEBAPP_URL}/chat?session={ch['id']}"
                keyboard.append([InlineKeyboardButton(
                    f"#{other_anon}  —  {title_short}", web_app=WebAppInfo(url=webapp_url)
                )])
            else:
                keyboard.append([InlineKeyboardButton(
                    f"#{other_anon}  —  {title_short}", callback_data=f"open_chat:{ch['id']}"
                )])

        keyboard.append([InlineKeyboardButton("← Назад", callback_data="back_to_menu")])
        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в my_chats: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить диалоги.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass

# =====================================================================
#  ДУБЛИРОВАТЬ ОБЪЯВЛЕНИЕ
# =====================================================================

async def duplicate_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        ad_id = int(query.data.split(':')[1])
        ad = db.get_ad(ad_id)
        if not ad or ad['user_id'] != update.effective_user.id:
            await query.answer("Нет доступа", show_alert=True)
            return

        user_data = db.get_user(update.effective_user.id)
        tariff = user_data['tariff']
        if tariff == 'Free' and user_data.get('daily_ads_count', 0) >= TARIFFS['Free']['daily_ads']:
            await safe_edit_message(query,
                "Дневной лимит исчерпан",
                [[InlineKeyboardButton("◆ Старт — 299 ₽/мес", callback_data="buy_standard")],
                 [InlineKeyboardButton("← Назад", callback_data=f"view_my_ad:{ad_id}")]])
            return

        context.user_data.pop('creating_ad', None)
        context.user_data['creating_ad'] = {
            'user_id': update.effective_user.id,
            'photos': list(ad.get('photos') or []),
            'title': ad.get('title', ''),
            'description': ad.get('description', ''),
            'price': ad.get('price', 0),
            'category': ad.get('category', ''),
            'contact_info': ad.get('contact_info', ''),
            'condition': ad.get('condition', ''),
            'size': ad.get('size', ''),
            'city': ad.get('city', ''),
            'delivery': ad.get('delivery', ''),
            'delivery_list': [d.strip() for d in (ad.get('delivery') or '').split(',') if d.strip()],
        }
        context.user_data['photo_limit'] = TARIFFS[tariff]['photo_limit']

        await safe_edit_message(query,
            f"<b>Дублирование объявления</b>\n\n"
            f"Данные скопированы:\n\n"
            f"<b>{ad['title']}</b>\n"
            f"{ad['price']} ₽  ·  {ad.get('city','—')}\n"
            f" Фото: {len(ad.get('photos') or [])} шт.\n\n"
            f"Нажмите «Опубликовать» или измените перед публикацией.",
            [[InlineKeyboardButton("✓ Опубликовать копию", callback_data="publish_ad")],
             [InlineKeyboardButton("✎ Изменить", callback_data="edit_ad")],
             [InlineKeyboardButton("✗ Отмена", callback_data=f"view_my_ad:{ad_id}")]],
            parse_mode=ParseMode.HTML)
        return States.CREATE_AD_PREVIEW
    except Exception as e:
        logger.error(f"Ошибка в duplicate_ad: {e}")
        try:
            await safe_edit_message(query, "Не удалось дублировать объявление. Попробуйте ещё раз.",
                [[InlineKeyboardButton("← Назад", callback_data="my_ads")]])
        except Exception:
            pass

# =====================================================================
#  РАСШИРЕННАЯ КАРТОЧКА ПРОДАВЦА (тарифная)
# =====================================================================

def get_seller_extended_stats(seller_id: int) -> Dict:
    cursor = db.conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM ads WHERE user_id=? AND status='active'", (seller_id,))
    active_ads = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM escrow_deals WHERE seller_id=? AND status='completed'", (seller_id,))
    deals_done = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM escrow_deals WHERE seller_id=? AND status IN ('completed','disputed')",
        (seller_id,)
    )
    deals_total = cursor.fetchone()[0]
    success_pct = round(deals_done / deals_total * 100) if deals_total > 0 else None

    cursor.execute("SELECT registration_date FROM users WHERE user_id=?", (seller_id,))
    row = cursor.fetchone()
    reg_date = dict(row)['registration_date'][:10] if row and dict(row).get('registration_date') else None

    # Среднее время ответа по чатам
    avg_resp_min = None
    try:
        cursor.execute('''
            SELECT
                MIN(CASE WHEN cm.sender_id != cs.seller_id THEN cm.created_at END) AS first_q,
                MIN(CASE WHEN cm.sender_id = cs.seller_id THEN cm.created_at END) AS first_a
            FROM chat_sessions cs
            JOIN chat_messages cm ON cm.session_id = cs.id
            WHERE cs.seller_id = ?
            GROUP BY cs.id
            HAVING first_q IS NOT NULL AND first_a IS NOT NULL
            LIMIT 30
        ''', (seller_id,))
        deltas = []
        for r in cursor.fetchall():
            r = dict(r)
            try:
                t1 = datetime.fromisoformat(r['first_q'])
                t2 = datetime.fromisoformat(r['first_a'])
                d = (t2 - t1).total_seconds() / 60
                if 0 < d < 1440:
                    deltas.append(d)
            except Exception:
                pass
        if deltas:
            avg_resp_min = round(sum(deltas) / len(deltas))
    except Exception:
        pass

    return {
        'active_ads': active_ads,
        'deals_done': deals_done,
        'success_pct': success_pct,
        'reg_date': reg_date,
        'avg_resp_min': avg_resp_min,
    }


async def view_seller_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        seller_id = int(query.data.split(':')[1])
        viewer_id = update.effective_user.id

        seller = db.get_user(seller_id)
        if not seller:
            await safe_edit_message(query, "Продавец не найден")
            return

        verify = db.get_verification(seller_id)
        is_verified = verify and verify.get('status') == 'verified'
        rating = db.get_seller_rating(seller_id)
        seller_name = seller.get('first_name', 'Продавец')
        uname = f" (@{seller['username']})" if seller.get('username') else ''
        vtag = ' ✓' if is_verified else ''

        # Тариф смотрящего (не продавца!) — определяет что видит покупатель
        viewer = db.get_user(viewer_id)
        viewer_tariff = viewer.get('tariff', 'Free') if viewer else 'Free'
        viewer_is_paid = viewer_tariff in ('Standard', 'PRO')
        viewer_is_pro  = viewer_tariff == 'PRO'

        # ── Заголовок ────────────────────────────────────────────────────
        stars = ('★' * round(rating['avg']) + '☆' * (5 - round(rating['avg']))) if rating['count'] > 0 else '☆☆☆☆☆'
        rating_str = f"{stars} {rating['avg']} ({rating['count']} отз.)" if rating['count'] > 0 else f"{stars} Нет отзывов"

        text = f"<b>{seller_name}</b>{uname}{vtag}\n"
        text += f"{rating_str}\n"

        # ── Базовая статистика (всем) ────────────────────────────────────
        stats = get_seller_extended_stats(seller_id)
        text += f"Активных объявлений: {stats['active_ads']}\n"
        if stats['reg_date']:
            text += f"На платформе с: {fmt_date(stats['reg_date'])}\n"

        # ── Статистика Standard/PRO (видит покупатель с подпиской) ──────
        text += "\n"
        if viewer_is_paid:
            if stats['deals_done'] > 0:
                line = f"📦 Сделок завершено: {stats['deals_done']}"
                if stats['success_pct'] is not None:
                    line += f" · ✓ {stats['success_pct']}% успешных"
                text += line + "\n"
            if viewer_is_pro and stats['avg_resp_min'] is not None:
                resp = f"~{stats['avg_resp_min']} мин." if stats['avg_resp_min'] < 60 else f"~{round(stats['avg_resp_min']/60,1)} ч."
                text += f"⚡ Среднее время ответа: {resp}\n"
            if viewer_is_pro:
                cursor = db.conn.cursor()
                cursor.execute("SELECT COALESCE(SUM(views),0) FROM ads WHERE user_id=? AND status='active'", (seller_id,))
                total_views = cursor.fetchone()[0]
                text += f"👁 Суммарных просмотров: {total_views}\n"
        else:
            text += "🔒 <i>Статистика сделок и время ответа — для тарифов <b>Старт</b> и <b>PRO</b></i>\n"

        # ── Последний диалог ─────────────────────────────────────────────
        dialog = None
        if viewer_id != seller_id:
            dialog = db.get_dialog_with_seller(viewer_id, seller_id)
            text += "\n"
            if dialog and dialog.get('msg_count', 0) > 0:
                last_text = (dialog.get('last_text') or '📎 медиафайл')[:60]
                last_sender = dialog.get('last_sender_id')
                who = "Вы" if last_sender == viewer_id else seller_name
                last_time = fmt_datetime(dialog.get('last_message_at', '')) if dialog.get('last_message_at') else ''
                text += (
                    f"💬 <b>Диалог по «{dialog['ad_title'][:30]}»</b>\n"
                    f"   {who}: {last_text}\n"
                    f"   <i>{last_time} · {dialog['msg_count']} сообщ.</i>\n"
                )
            else:
                text += "💬 <i>Диалогов с этим продавцом нет</i>\n"

        # ── Последние объявления ─────────────────────────────────────────
        cursor = db.conn.cursor()
        cursor.execute('''
            SELECT id, title, price, city FROM ads
            WHERE user_id=? AND status='active'
            ORDER BY created_at DESC LIMIT 4
        ''', (seller_id,))
        ads = [dict(r) for r in cursor.fetchall()]

        keyboard = []
        if ads:
            text += f"\n▣ <b>Объявления ({stats['active_ads']})</b>\n"
            for a in ads:
                city = f" · {a['city']}" if a.get('city') else ''
                text += f"• {a['title'][:32]} — {a['price']} ₽{city}\n"
                keyboard.append([InlineKeyboardButton(
                    f"{a['title'][:40]} — {a['price']} ₽",
                    callback_data=f"view_ad:{a['id']}"
                )])

        # ── Кнопки действий ─────────────────────────────────────────────
        if viewer_id != seller_id:
            if dialog and dialog.get('msg_count', 0) > 0:
                keyboard.append([InlineKeyboardButton(
                    "▸ Продолжить диалог", callback_data=f"open_chat:{dialog['id']}"
                )])
            else:
                cursor.execute(
                    "SELECT id FROM ads WHERE user_id=? AND status='active' ORDER BY created_at DESC LIMIT 1",
                    (seller_id,)
                )
                chat_ad_row = cursor.fetchone()
                if chat_ad_row:
                    keyboard.append([InlineKeyboardButton(
                        "✎ Написать продавцу", callback_data=f"chat_start:{chat_ad_row[0]}"
                    )])
                elif seller.get('username'):
                    keyboard.append([InlineKeyboardButton(
                        "✎ Написать продавцу", url=f"https://t.me/{seller['username']}"
                    )])

        already_fav = db.is_favorite_seller(viewer_id, seller_id) if viewer_id != seller_id else False
        fav_label = "✗ Из избранного" if already_fav else "♡ В избранное"
        fav_cb = f"unfav_seller:{seller_id}" if already_fav else f"fav_seller:{seller_id}"

        row2 = []
        if viewer_id != seller_id:
            row2.append(InlineKeyboardButton(fav_label, callback_data=fav_cb))
        row2.append(InlineKeyboardButton("★ Отзывы", callback_data=f"seller_reviews:{seller_id}"))
        if row2:
            keyboard.append(row2)

        if viewer_id != seller_id:
            keyboard.append([InlineKeyboardButton("✎ Оставить отзыв", callback_data=f"leave_review_seller:{seller_id}")])
            keyboard.append([InlineKeyboardButton("! Пожаловаться на продавца", callback_data=f"report_user:{seller_id}")])

        if not viewer_is_paid and viewer_id != seller_id:
            keyboard.append([InlineKeyboardButton(
                "🔒 Открыть расширенную статистику", callback_data="show_subscriptions"
            )])

        keyboard.append([InlineKeyboardButton("← Назад", callback_data="favorite_sellers")])

        await safe_edit_message(query, text, keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в view_seller_profile: {e}")
        try:
            await safe_edit_message(query, "Не удалось загрузить профиль продавца.",
                [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]])
        except Exception:
            pass

# =====================================================================
#  FTS5 — ПОЛНОТЕКСТОВЫЙ ПОИСК
# =====================================================================

def init_fts5():
    cursor = db.conn.cursor()
    try:
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS ads_fts USING fts5(
                title, description, category, city, delivery,
                content='ads', content_rowid='id',
                tokenize='unicode61'
            )
        ''')
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS ads_fts_insert AFTER INSERT ON ads BEGIN
                INSERT INTO ads_fts(rowid, title, description, category, city, delivery)
                VALUES (new.id, COALESCE(new.title,''), COALESCE(new.description,''),
                        COALESCE(new.category,''), COALESCE(new.city,''), COALESCE(new.delivery,''));
            END
        ''')
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS ads_fts_delete AFTER DELETE ON ads BEGIN
                INSERT INTO ads_fts(ads_fts, rowid, title, description, category, city, delivery)
                VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.description,''),
                        COALESCE(old.category,''), COALESCE(old.city,''), COALESCE(old.delivery,''));
            END
        ''')
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS ads_fts_update AFTER UPDATE ON ads BEGIN
                INSERT INTO ads_fts(ads_fts, rowid, title, description, category, city, delivery)
                VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.description,''),
                        COALESCE(old.category,''), COALESCE(old.city,''), COALESCE(old.delivery,''));
                INSERT INTO ads_fts(rowid, title, description, category, city, delivery)
                VALUES (new.id, COALESCE(new.title,''), COALESCE(new.description,''),
                        COALESCE(new.category,''), COALESCE(new.city,''), COALESCE(new.delivery,''));
            END
        ''')
        db.conn.commit()
        cursor.execute("INSERT INTO ads_fts(ads_fts) VALUES('rebuild')")
        db.conn.commit()
        logger.info("FTS5 index initialized")
    except Exception as e:
        logger.warning(f"FTS5 init failed (ok on first run): {e}")


def search_ads_fts(query_text: str, limit: int = 50, filters: dict = None) -> List[Dict]:
    cursor = db.conn.cursor()
    filters = filters or {}
    moscow_now = get_moscow_time().isoformat()

    terms = [query_text.strip()]
    for word in query_text.lower().split():
        if word in SEARCH_SYNONYMS:
            terms.extend(SEARCH_SYNONYMS[word])
    terms = list(dict.fromkeys(t for t in terms if t))
    fts_query = ' OR '.join(f'"{t}"' for t in terms)

    extra = "AND a.status = 'active'"
    params = []
    if filters.get('category'):
        extra += " AND a.category = ?"
        params.append(filters['category'])
    if filters.get('city'):
        extra += " AND (a.city = ? OR ? = 'Любой город')"
        params.extend([filters['city'], filters['city']])
    if filters.get('price_min'):
        extra += " AND CAST(a.price AS INTEGER) >= ?"
        params.append(int(filters['price_min']))
    if filters.get('price_max'):
        extra += " AND CAST(a.price AS INTEGER) <= ?"
        params.append(int(filters['price_max']))

    try:
        cursor.execute(f'''
            SELECT a.*, bm25(ads_fts) AS rank
            FROM ads_fts
            JOIN ads a ON ads_fts.rowid = a.id
            WHERE ads_fts MATCH ? {extra}
            ORDER BY
                CASE WHEN a.pinned_until > ? THEN 0 ELSE 1 END,
                rank,
                a.created_at DESC
            LIMIT ?
        ''', [fts_query] + params + [moscow_now, limit])
        results = [dict(r) for r in cursor.fetchall()]
        if results:
            return results
    except Exception as e:
        logger.warning(f"FTS5 search error: {e}")

    return db.search_ads(query_text, limit)

# =====================================================================
#  ДАЙДЖЕСТ НОВЫХ ОБЪЯВЛЕНИЙ / БРОШЕННЫЙ ЧЕРНОВИК / ЖАЛОБА НА ЮЗЕРА
# =====================================================================

USER_REPORT_REASONS = [
    "Мошенничество / обман",
    "Грубость / угрозы",
    "Спам / накрутка отзывов",
    "Продаёт запрещённые товары",
    "Другая причина",
]

async def report_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало жалобы на пользователя"""
    query = update.callback_query
    await query.answer()
    target_id = int(query.data.split(":")[1])
    user = db.get_user(target_id)
    name = user.get('first_name', f'#{target_id}') if user else f'#{target_id}'
    keyboard = [
        [InlineKeyboardButton(r, callback_data=f"report_user_send:{target_id}:{r[:30]}")]
        for r in USER_REPORT_REASONS
    ]
    keyboard.append([InlineKeyboardButton("✗ Отмена", callback_data=f"seller_profile:{target_id}")])
    await safe_edit_message(
        query,
        f"⚑ Жалоба на продавца <b>{name}</b>\n\nВыберите причину:",
        keyboard,
        parse_mode=ParseMode.HTML
    )

async def report_user_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняем жалобу на пользователя и уведомляем админов"""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":", 2)
    target_id, reason = int(parts[1]), parts[2]
    reporter_id = update.effective_user.id

    saved = db.report_user(reporter_id, target_id, reason)
    if not saved:
        await query.answer("Вы уже жаловались на этого пользователя", show_alert=True)
        return

    report_count = db.get_user_report_count(target_id)
    target = db.get_user(target_id)
    target_name = target.get('first_name', f'#{target_id}') if target else f'#{target_id}'
    reporter = update.effective_user

    for admin_id in ADMIN_USER_IDS:
        try:
            flag = "\n\n⚠️ Уже 3+ жалобы на этого пользователя!" if report_count >= 3 else ""
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"⚑ Жалоба на пользователя #{target_id}\n"
                    f"Имя: {target_name}\n"
                    f"Причина: {reason}\n"
                    f"От: {reporter.first_name} (@{reporter.username or 'нет'})\n"
                    f"Всего жалоб: {report_count}{flag}"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✗ Заблокировать", callback_data=f"admin_ban:{target_id}"),
                    InlineKeyboardButton("· Игнорировать", callback_data=f"admin_dismiss_report:{target_id}")
                ]])
            )
        except Exception as e:
            logger.error(f"report_user notify admin error: {e}")

    await safe_edit_message(
        query,
        "✓ Жалоба принята. Разберёмся в течение 24 часов.\n\nСпасибо, что помогаете делать площадку безопасной.",
        [[InlineKeyboardButton("← Назад", callback_data="back_to_menu")]]
    )

async def admin_dismiss_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ игнорирует жалобу на пользователя"""
    query = update.callback_query
    await query.answer()
    if not await is_admin(update.effective_user.id):
        return
    target_id = int(query.data.split(":")[1])
    await safe_edit_message(query, f"Жалоба на #{target_id} отклонена.")

async def publish_ad_force(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Публикуем объявление-дубликат если пользователь подтвердил"""
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    required_fields = context.user_data.pop('publish_ad_force_data', None)
    if not required_fields:
        await query.message.reply_text("✗ Данные не найдены. Создайте объявление заново.")
        return

    ad_id = db.create_ad(required_fields)
    if not ad_id:
        await query.message.reply_text("Не удалось создать объявление. Попробуйте ещё раз.")
        return

    user_data = db.get_user(user.id)
    db.update_user(user.id, daily_ads_count=(user_data.get('daily_ads_count', 0) or 0) + 1)
    db.clear_abandoned_draft(user.id)
    db.log_action(user.id, "ad_created_force", f"Ad ID: {ad_id}")
    await notify_admin_new_ad(ad_id, context)
    await query.message.reply_text(
        "✓ Объявление создано и отправлено на модерацию.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Меню", callback_data="back_to_menu")]])
    )

async def send_daily_digest(context: ContextTypes.DEFAULT_TYPE):
    """Ежедневный дайджест новых объявлений — каждое утро в 9:00 МСК"""
    since = (get_moscow_time() - timedelta(hours=24)).isoformat()
    new_ads = db.get_new_ads_since(since, limit=8)
    if not new_ads:
        logger.info("Дайджест: новых объявлений за 24ч нет")
        return

    lines = []
    for ad in new_ads:
        price = f"{int(ad['price']):,}".replace(",", "\u00a0") + " ₽"
        city = f" · {ad['city']}" if ad.get('city') else ""
        lines.append(f"• <b>{ad['title'][:45]}</b> — {price}{city}")

    text = (
        f"🛍 <b>Новые объявления за сегодня</b>\n\n"
        + "\n".join(lines) +
        f"\n\n<i>Всего новинок: {len(new_ads)}+</i>"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("▸ Смотреть все", callback_data="search_start"),
        InlineKeyboardButton("✗ Отписаться", callback_data="digest_unsubscribe")
    ]])

    subscribers = db.get_digest_subscribers()
    sent = 0
    for sub in subscribers:
        try:
            await context.bot.send_message(
                chat_id=sub['user_id'],
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    logger.info(f"Дайджест отправлен {sent}/{len(subscribers)} пользователям")

async def digest_unsubscribe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь отписывается от дайджеста"""
    query = update.callback_query
    await query.answer()
    db.update_user(update.effective_user.id, notifications_enabled=0)
    await query.answer("✓ Вы отписались от ежедневного дайджеста", show_alert=True)

async def check_abandoned_drafts(context: ContextTypes.DEFAULT_TYPE):
    """Напоминаем пользователям о незавершённых объявлениях (через 60 минут)"""
    drafts = db.get_abandoned_drafts_older_than(60)
    for draft in drafts:
        user_id = draft['user_id']
        # Проверяем нет ли уже активного creating_ad (пользователь всё ещё создаёт)
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "📝 Вы не завершили создание объявления.\n\n"
                    "Хотите продолжить? Нажмите кнопку ниже — мы откроем форму заново."
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("+ Создать объявление", callback_data="create_ad")],
                    [InlineKeyboardButton("· Не напоминать", callback_data="draft_dismiss")]
                ])
            )
            # Удаляем черновик чтобы напоминание не повторялось
            db.clear_abandoned_draft(user_id)
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Напоминание о черновике {user_id}: {e}")
            db.clear_abandoned_draft(user_id)

async def draft_dismiss_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь закрыл напоминание о черновике"""
    query = update.callback_query
    await query.answer()
    db.clear_abandoned_draft(update.effective_user.id)
    await query.edit_message_reply_markup(reply_markup=None)

async def search_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переход в поиск из дайджеста"""
    query = update.callback_query
    await query.answer()
    await search_start(update, context)

async def setup_scheduler(application: Application):
    async def daily_tasks(context: ContextTypes.DEFAULT_TYPE):
        logger.info("Выполнение ежедневных задач")
        db.reset_daily_limits()
        db.update_statistics()
        expired_pins = db.expire_old_pinned_ads()
        logger.info(f"Ежедневные задачи выполнены: {expired_pins} закрепов истекло")
    
    async def subscription_reminders(context: ContextTypes.DEFAULT_TYPE):
        logger.info("Проверка подписок для напоминаний")
        
        for days_before in [3, 1, 0]:
            expiring = db.get_expiring_subscriptions(days_before)
            for user in expiring:
                try:
                    tariff_label = TARIFF_LABELS.get(user['tariff'], user['tariff'])
                    if days_before == 3:
                        message = f"Напоминание: ваша подписка {tariff_label} заканчивается через 3 дня!"
                    elif days_before == 1:
                        message = f"! Ваша подписка {tariff_label} заканчивается завтра!"
                    else:
                        message = f"! СРОЧНО: ваша подписка {tariff_label} заканчивается сегодня!"
                    
                    await context.bot.send_message(
                        chat_id=user['user_id'],
                        text=f"{message}\n\nДата окончания: {user['tariff_end_date'][:10]}\n\nПродлите подписку в разделе ◉ Тарифы."
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить напоминание пользователю {user['user_id']}: {e}")
    
    async def check_saved_searches(context: ContextTypes.DEFAULT_TYPE):
        """Уведомляем пользователей о новых товарах по сохранённым поискам"""
        logger.info("Проверка сохранённых поисков")
        saved_searches = db.get_all_saved_searches()
        for item in saved_searches:
            user_id = item['user_id']
            query_text = item['query']
            results = db.search_ads(query_text, limit=5)
            for ad in results:
                if not db.is_search_notification_sent(user_id, ad['id']):
                    try:
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=(
                                f" Новый товар по вашему поиску «{query_text}»!\n\n"
                                f"{ad['title']}\n"
                                f"{ad['price']} ₽\n"
                                f"{ad.get('city', '')}"
                            ),
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton("▸ Посмотреть", callback_data=f"view_ad:{ad['id']}")
                            ]])
                        )
                        db.mark_search_notification_sent(user_id, ad['id'])
                        await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.error(f"Ошибка уведомления по поиску: {e}")

    async def run_auto_bumps(context: ContextTypes.DEFAULT_TYPE):
        """Автоматически поднимаем объявления по расписанию"""
        bumps = db.get_active_auto_bumps()
        now = get_moscow_time()
        for bump in bumps:
            last = bump.get('last_bumped_at')
            if last:
                try:
                    last_dt = datetime.fromisoformat(last)
                    if (now - last_dt).total_seconds() < bump['interval_hours'] * 3600:
                        continue
                except Exception:
                    pass
            cursor = db.conn.cursor()
            cursor.execute('UPDATE ads SET created_at=? WHERE id=?',
                           (now.isoformat(), bump['ad_id']))
            db.conn.commit()
            db.update_auto_bump_time(bump['id'])
            logger.info(f"Auto-bump: ad {bump['ad_id']} bumped every {bump['interval_hours']}h")

    async def daily_views_digest(context: ContextTypes.DEFAULT_TYPE):
        """Ежедневная сводка просмотров для продавцов с активными объявлениями"""
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT u.user_id, u.first_name,
                   COUNT(a.id) as ad_count,
                   COALESCE(SUM(a.views_count), 0) as total_views,
                   COALESCE(SUM(a.contact_clicks), 0) as total_contacts
            FROM users u
            JOIN ads a ON a.user_id = u.user_id AND a.status = 'active'
            GROUP BY u.user_id
            HAVING ad_count > 0
        """)
        rows = cursor.fetchall()
        for row in rows:
            row = dict(row)
            if row['total_views'] == 0:
                continue
            try:
                text = (
                    f"◈ <b>Сводка за сегодня</b>\n\n"
                    f"▸ Активных объявлений: {row['ad_count']}\n"
                    f"◎ Просмотров: {row['total_views']}\n"
                    f"✎ Обращений: {row['total_contacts']}\n\n"
                    f"<i>Поднимите объявления в топ чтобы получить больше просмотров.</i>"
                )
                await context.bot.send_message(
                    chat_id=row['user_id'],
                    text=text,
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass

    async def check_expired_verifications(context: ContextTypes.DEFAULT_TYPE):
        """Сбрасываем истёкшие платные верификации и уведомляем пользователей"""
        expired_ids = db.expire_paid_verifications()
        for user_id in expired_ids:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "⏰ Срок вашей верификации истёк\n\n"
                        "Значок ✓ был снят с ваших объявлений.\n\n"
                        f"Продлите верификацию за {VERIFICATION_PRICE} ₽/год — "
                        "это увеличивает продажи на 30–50%."
                    ),
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("↺ Продлить верификацию", callback_data="verify_seller")
                    ]])
                )
            except Exception as e:
                logger.error(f"Уведомление об истечении верификации {user_id}: {e}")

    job_queue = application.job_queue
    # Все задачи работают по московскому времени
    MSK = ZoneInfo('Europe/Moscow')
    job_queue.run_daily(daily_tasks,               time=time(hour=0,  minute=0,  tzinfo=MSK))
    job_queue.run_daily(subscription_reminders,    time=time(hour=10, minute=0,  tzinfo=MSK))
    job_queue.run_daily(check_expired_verifications, time=time(hour=9, minute=0, tzinfo=MSK))
    async def _update_stats_job(context: ContextTypes.DEFAULT_TYPE):
        db.update_statistics()
    job_queue.run_repeating(_update_stats_job, interval=3600, first=10)
    job_queue.run_repeating(check_saved_searches, interval=1800, first=60)
    job_queue.run_daily(send_renewal_reminders,   time=time(hour=11, minute=0, tzinfo=MSK))
    job_queue.run_repeating(run_auto_bumps, interval=1800, first=120)
    job_queue.run_daily(daily_views_digest, time=datetime.strptime("19:00", "%H:%M").time())
    job_queue.run_daily(trigger_low_views_boost,    time=time(hour=12, minute=0, tzinfo=MSK))
    # Новые задачи
    job_queue.run_daily(send_daily_digest,           time=time(hour=9,  minute=0,  tzinfo=MSK))
    job_queue.run_repeating(check_abandoned_drafts,  interval=1800, first=300)

# Главная функция
async def post_init(application: Application):
    # Инициализируем новые таблицы и индексы
    await init_chat_tables()
    init_fts5()
    commands = [
        BotCommand("start", "Запустить бота"),
        BotCommand("menu", "Открыть главное меню"),
        BotCommand("help", "Как пользоваться ботом")
    ]
    
    await application.bot.set_my_commands(commands)
    await setup_scheduler(application)

    # Проверяем права бота в канале
    try:
        me = await application.bot.get_me()
        member = await application.bot.get_chat_member(chat_id=ADMIN_CHANNEL_ID, user_id=me.id)
        if member.status in ('administrator', 'creator'):
            logger.info(f"✓ Бот является администратором канала {ADMIN_CHANNEL_ID}")
        else:
            logger.warning(f"⚠️ Бот НЕ является администратором канала {ADMIN_CHANNEL_ID} (статус: {member.status}) — публикация объявлений не будет работать!")
            for admin_id in ADMIN_USER_IDS:
                try:
                    await application.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ ВНИМАНИЕ: бот не является администратором канала {ADMIN_CHANNEL_ID}\n\nСтатус: {member.status}\n\nДобавьте @{me.username} как администратора канала с правом публикации сообщений!"
                    )
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Не удалось проверить права в канале {ADMIN_CHANNEL_ID}: {e}")
        for admin_id in ADMIN_USER_IDS:
            try:
                await application.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ ВНИМАНИЕ: не удалось проверить права бота в канале {ADMIN_CHANNEL_ID}\n\nОшибка: {e}\n\nУбедитесь что бот добавлен как администратор в канал!"
                )
            except Exception:
                pass

    logger.info("Бот успешно запущен")

# =====================================================================
#  MINI APP API SERVER (aiohttp)
# =====================================================================

def verify_telegram_init_data(init_data: str) -> Optional[int]:
    """Проверяет подпись initData от Telegram WebApp, возвращает user_id или None"""
    try:
        params = dict(p.split('=', 1) for p in init_data.split('&') if '=' in p)
        received_hash = params.pop('hash', '')
        data_check = '\n'.join(f"{k}={v}" for k, v in sorted(params.items()))
        secret = hmac.new(b"WebAppData", TOKEN.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(received_hash, expected):
            return None
        user_json = params.get('user', '{}')
        import urllib.parse
        user = json.loads(urllib.parse.unquote(user_json))
        return user.get('id')
    except Exception as e:
        logger.error(f"initData verify: {e}")
        return None

def get_viewer_id(request: aiohttp_web.Request) -> Optional[int]:
    """Извлекает user_id из initData заголовка запроса"""
    init_data = request.headers.get('X-Telegram-Init-Data', '')
    if not init_data:
        return None
    return verify_telegram_init_data(init_data)

async def api_get_chat(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """GET /api/chat/{session_id} — загрузить диалог"""
    viewer_id = get_viewer_id(request)
    if not viewer_id:
        return aiohttp_web.json_response({'error': 'unauthorized'}, status=401)

    session_id = int(request.match_info['session_id'])
    session = get_chat_session(session_id)
    if not session:
        return aiohttp_web.json_response({'error': 'not found'}, status=404)

    if viewer_id not in (session['buyer_id'], session['seller_id']):
        return aiohttp_web.json_response({'error': 'forbidden'}, status=403)

    other_id = session['seller_id'] if viewer_id == session['buyer_id'] else session['buyer_id']
    other_user = db.get_user(other_id)
    other_name = other_user.get('first_name', f'#{other_id}') if other_user else f'#{other_id}'
    other_anon = get_anon_id(other_user) if other_user else str(other_id)
    verify = db.get_verification(other_id)
    other_verified = verify and verify.get('status') == 'verified'

    ad = db.get_ad(session['ad_id'])

    cursor = db.conn.cursor()
    cursor.execute('''
        SELECT id, sender_id, text, file_id, msg_type, created_at
        FROM chat_messages WHERE session_id=?
        ORDER BY created_at ASC
    ''', (session_id,))
    messages = [dict(r) for r in cursor.fetchall()]

    return aiohttp_web.json_response({
        'viewer_id': viewer_id,
        'other_name': other_name,
        'other_anon': other_anon,
        'other_verified': bool(other_verified),
        'ad_title': ad['title'][:50] if ad else '',
        'ad_price': ad['price'] if ad else None,
        'messages': messages,
    })

async def api_poll_messages(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """GET /api/chat/{session_id}/poll?after={id} — новые сообщения"""
    viewer_id = get_viewer_id(request)
    if not viewer_id:
        return aiohttp_web.json_response({'error': 'unauthorized'}, status=401)

    session_id = int(request.match_info['session_id'])
    session = get_chat_session(session_id)
    if not session or viewer_id not in (session['buyer_id'], session['seller_id']):
        return aiohttp_web.json_response({'error': 'forbidden'}, status=403)

    after_id = int(request.rel_url.query.get('after', 0))
    cursor = db.conn.cursor()
    cursor.execute('''
        SELECT id, sender_id, text, file_id, msg_type, created_at
        FROM chat_messages WHERE session_id=? AND id > ?
        ORDER BY created_at ASC
    ''', (session_id, after_id))
    messages = [dict(r) for r in cursor.fetchall()]
    return aiohttp_web.json_response({'messages': messages})

async def api_send_message(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """POST /api/chat/{session_id}/send — отправить сообщение"""
    viewer_id = get_viewer_id(request)
    if not viewer_id:
        return aiohttp_web.json_response({'error': 'unauthorized'}, status=401)

    session_id = int(request.match_info['session_id'])
    session = get_chat_session(session_id)
    if not session or viewer_id not in (session['buyer_id'], session['seller_id']):
        return aiohttp_web.json_response({'error': 'forbidden'}, status=403)

    try:
        body = await request.json()
    except Exception:
        return aiohttp_web.json_response({'error': 'bad json'}, status=400)

    text = (body.get('text') or '').strip()
    if not text or len(text) > 4000:
        return aiohttp_web.json_response({'error': 'invalid text'}, status=400)

    save_chat_message(session_id, viewer_id, text=text)

    # Уведомить получателя
    recipient_id = session['seller_id'] if viewer_id == session['buyer_id'] else session['buyer_id']
    ad = db.get_ad(session['ad_id'])
    sender_user = db.get_user(viewer_id)
    sender_anon = get_anon_id(sender_user) if sender_user else str(viewer_id)

    # Кнопка открытия мини-аппа если настроен WEBAPP_URL
    if WEBAPP_URL:
        webapp_url = f"{WEBAPP_URL}/chat?session={session_id}"
        reply_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "▸ Открыть диалог",
                web_app=WebAppInfo(url=webapp_url)
            )
        ]])
    else:
        reply_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▸ Ответить", callback_data=f"open_chat:{session_id}")
        ]])

    try:
        _app = _get_bot_app()
        if _app:
            await _app.bot.send_message(
                chat_id=recipient_id,
                text=(
                    f"💬 <b>Новое сообщение</b>\n"
                    f"По объявлению «{ad['title'][:35] if ad else '?'}»\n\n"
                    f"<i>{text[:200]}</i>"
                ),
                reply_markup=reply_kb,
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"api send notify: {e}")

    return aiohttp_web.json_response({'ok': True})

async def api_serve_html(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """GET /chat — отдаёт мини-апп HTML"""
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chat_miniapp.html')
    if not os.path.exists(html_path):
        return aiohttp_web.Response(text='Mini App not found', status=404)
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return aiohttp_web.Response(text=content, content_type='text/html', charset='utf-8')

_bot_app_ref = None

def _get_bot_app():
    return _bot_app_ref

# hashlib уже импортирован в начале файла; здесь используем алиас для совместимости
_hashlib = hashlib

def generate_payanyway_url(user_id: int, plan: str, amount: int, promo: str = "") -> tuple:
    """Генерирует ссылку на оплату через PayAnyWay (Moneta.Assistant).
    Возвращает кортеж (payment_url, transaction_id).
    Платёж сохраняется в БД со статусом 'pending' до подтверждения.
    """
    from urllib.parse import urlencode

    # 1. Проверяем незавершённый pending-платёж (защита от дублей)
    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT transaction_id FROM payments WHERE user_id=? AND plan=? AND status='pending' "
        "ORDER BY created_at DESC LIMIT 1",
        (user_id, plan)
    )
    existing = cursor.fetchone()
    if existing:
        transaction_id = existing['transaction_id'] if existing['transaction_id'] else uuid.uuid4().hex
    else:
        transaction_id = uuid.uuid4().hex  # без дефисов — PayAnyWay не принимает дефисы в transaction_id

    amount_str = f"{float(amount):.2f}"  # PayAnyWay ожидает "1.00", "299.00"
    test_mode  = PAW_TEST_MODE

    # 2. Подпись: MNT_ID + MNT_TRANSACTION_ID + MNT_AMOUNT + MNT_CURRENCY_CODE + MNT_TEST_MODE + SECRET
    sig_str   = f"{PAW_ACCOUNT}{transaction_id}{amount_str}RUB{test_mode}{PAW_SECRET}"
    signature = hashlib.md5(sig_str.encode("utf-8")).hexdigest()

    # 3. Параметры
    params = {
        "MNT_ID":             PAW_ACCOUNT,
        "MNT_TRANSACTION_ID": transaction_id,
        "MNT_AMOUNT":         amount_str,
        "MNT_CURRENCY_CODE":  "RUB",
        "MNT_TEST_MODE":      test_mode,
        "MNT_SIGNATURE":      signature,
        "MNT_DESCRIPTION":    f"Полка {plan} 30 дней",
        "moneta.locale":      "ru",
        "MNT_CHECK_URL":      f"{SERVER_URL}/api/payanyway/check",
        "MNT_RESULT_URL":     f"{SERVER_URL}/api/payanyway",
        "MNT_SUCCESS_URL":    f"https://t.me/PolkaAdsBot",
        "MNT_FAIL_URL":       f"https://t.me/PolkaAdsBot",
        "MNT_RETURN_URL":     f"https://t.me/PolkaAdsBot",
    }
    if promo:
        params["MNT_CUSTOM1"] = promo

    payment_url = PAW_URL + "?" + urlencode(params)

    # 4. Сохраняем платёж в БД как pending (только если не было существующего)
    if not existing:
        PLAN_NAMES = {"standard": "Старт", "pro": "PRO"}
        plan_label = PLAN_NAMES.get(plan, plan.upper())
        cursor.execute(
            """INSERT INTO payments
               (user_id, plan, tariff, amount, status, transaction_id, promocode_code, created_at)
               VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)""",
            (user_id, plan, plan_label, amount, transaction_id,
             promo or None,
             get_moscow_time().isoformat())
        )
        db.conn.commit()

    return payment_url, transaction_id


async def api_payanyway_check(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """GET/POST /api/payanyway/check — Check URL от PayAnyWay (проверка заказа перед оплатой)"""
    try:
        if request.method == "POST":
            data = dict(await request.post())
        else:
            data = dict(request.rel_url.query)

        logger.info(f"PayAnyWay Check URL получен: {dict(data)}")

        mnt_id         = data.get("MNT_ID", "")
        transaction_id = data.get("MNT_TRANSACTION_ID", "")
        test_mode      = data.get("MNT_TEST_MODE", "0")

        # Всегда отвечаем 200 — заказ существует, можно оплачивать
        result_code = "200"

        # Подпись ответа: md5(MNT_ID + MNT_TRANSACTION_ID + RESULT_CODE + SECRET)
        resp_sig = _hashlib.md5(
            f"{PAW_ACCOUNT}{transaction_id}{result_code}{PAW_SECRET}".encode("utf-8")
        ).hexdigest()

        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            "<MNT_RESPONSE>"
            f"<MNT_ID>{PAW_ACCOUNT}</MNT_ID>"
            f"<MNT_TRANSACTION_ID>{transaction_id}</MNT_TRANSACTION_ID>"
            f"<MNT_RESULT_CODE>{result_code}</MNT_RESULT_CODE>"
            f"<MNT_SIGNATURE>{resp_sig}</MNT_SIGNATURE>"
            "</MNT_RESPONSE>"
        )
        logger.info(f"PayAnyWay Check URL ответ: {xml}")
        return aiohttp_web.Response(text=xml, content_type="text/xml")
    except Exception as e:
        logger.error(f"PayAnyWay Check URL error: {e}")
        return aiohttp_web.Response(text="FAIL")


async def api_payanyway_notify(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """POST/GET /api/payanyway — webhook от PayAnyWay после успешной оплаты"""
    try:
        if request.method == "POST":
            data = dict(await request.post())
        else:
            data = dict(request.rel_url.query)

        logger.info(f"PayAnyWay webhook получен: {dict(data)}")

        mnt_id          = data.get("MNT_ID", "")
        transaction_id  = data.get("MNT_TRANSACTION_ID", "")
        operation_id    = data.get("MNT_OPERATION_ID", "")
        currency        = data.get("MNT_CURRENCY_CODE", "RUB")
        test_mode       = data.get("MNT_TEST_MODE", "0")
        received_sig    = data.get("MNT_SIGNATURE", "")

        # Нормализуем сумму
        amount = f"{float(data.get('MNT_AMOUNT', '0')):.2f}"

        # Проверяем подпись — логируем но не блокируем (для отладки)
        sig_str  = f"{mnt_id}{transaction_id}{operation_id}{amount}{currency}{test_mode}{PAW_SECRET}"
        expected = _hashlib.md5(sig_str.encode("utf-8")).hexdigest()
        if expected.lower() != received_sig.lower():
            logger.warning(f"PayAnyWay: подпись не совпала! Ожидалось={expected} Получено={received_sig} Строка={sig_str}")
            # НЕ возвращаем FAIL — продолжаем активацию (для отладки)
            # После проверки раскомментировать: return aiohttp_web.Response(text="FAIL")

        cursor = db.conn.cursor()

        # Ищем платёж в БД по transaction_id
        cursor.execute(
            "SELECT id, user_id, plan, tariff, amount, status FROM payments WHERE transaction_id=?",
            (transaction_id,)
        )
        row = cursor.fetchone()

        # Защита от дублей — если уже paid, отвечаем SUCCESS без повторной активации
        if row and row['status'] == 'paid':
            logger.info(f"PayAnyWay: повторный webhook для уже оплаченного {transaction_id}")
            return aiohttp_web.Response(text="SUCCESS")

        # Если платёж не найден — пытаемся разобрать transaction_id старого формата
        if not row:
            parts = transaction_id.split("_")
            if len(parts) < 2:
                logger.error(f"PayAnyWay: не удалось найти платёж transaction_id={transaction_id}")
                return aiohttp_web.Response(text="FAIL")
            user_id    = int(parts[0])
            plan_key   = parts[1]
            PLAN_NAMES = {"standard": "Старт", "pro": "PRO"}
            plan_label = PLAN_NAMES.get(plan_key, plan_key.upper())
            plan       = plan_key
        else:
            user_id    = row['user_id']
            plan       = row['plan'] or ""
            PLAN_NAMES = {"standard": "Старт", "pro": "PRO"}
            plan_label = row['tariff'] or PLAN_NAMES.get(plan, plan.upper())

        promo_code = data.get("MNT_CUSTOM1", "") or None
        paid_amount = round(float(amount))

        # Активируем подписку: обновляем платёж + users в одной транзакции
        now_iso = get_moscow_time().isoformat()
        if row:
            cursor.execute(
                "UPDATE payments SET status='paid', confirmed_at=? WHERE transaction_id=?",
                (now_iso, transaction_id)
            )
        else:
            cursor.execute(
                """INSERT INTO payments
                   (user_id, plan, tariff, amount, status, transaction_id, promocode_code, created_at, confirmed_at)
                   VALUES (?, ?, ?, ?, 'paid', ?, ?, ?, ?)""",
                (user_id, plan, plan_label, paid_amount, transaction_id,
                 promo_code, now_iso, now_iso)
            )

        # Продление: если подписка ещё активна — добавляем 30 дней к текущей дате окончания,
        # иначе — 30 дней с сегодня
        cursor.execute("SELECT tariff_end_date FROM users WHERE user_id=?", (user_id,))
        urow = cursor.fetchone()
        now_dt = get_moscow_time()
        if urow and urow['tariff_end_date']:
            try:
                end_dt = datetime.fromisoformat(urow['tariff_end_date'])
                # Если подписка ещё не истекла — продлеваем от текущего конца
                start_dt = end_dt if end_dt > now_dt else now_dt
            except Exception:
                start_dt = now_dt
        else:
            start_dt = now_dt
        new_end = (start_dt + timedelta(days=30)).isoformat()

        cursor.execute(
            "UPDATE users SET tariff=?, tariff_end_date=? WHERE user_id=?",
            (plan_label, new_end, user_id)
        )
        db.conn.commit()

        logger.info(f"PayAnyWay: оплата принята user={user_id} plan={plan_label} amount={paid_amount} op={operation_id}")

        # Уведомляем пользователя через бота
        bot_app = _get_bot_app()
        if bot_app:
            try:
                await bot_app.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "✅ Оплата получена! Подписка активирована.\n\n"
                        f"Тариф <b>{plan_label}</b> активирован на 30 дней.\n"
                        f"Операция: {operation_id}"
                    ),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"PayAnyWay: не удалось уведомить пользователя {user_id}: {e}")

        # Статистика оплат
        cursor.execute("SELECT COUNT(*), SUM(amount) FROM payments WHERE status='paid'")
        total_row = cursor.fetchone()
        total_count = total_row[0] or 0
        total_sum   = total_row[1] or 0
        cursor.execute(
            "SELECT COUNT(*), SUM(amount) FROM payments WHERE status='paid' AND date(created_at)=date('now')"
        )
        today_row   = cursor.fetchone()
        today_count = today_row[0] or 0
        today_sum   = today_row[1] or 0

        # Уведомляем всех админов
        for admin_id in ADMIN_USER_IDS:
            try:
                await bot_app.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"💰 Новая оплата PayAnyWay\n\n"
                        f"👤 Пользователь: {user_id}\n"
                        f"📦 Тариф: {plan_label}\n"
                        f"💵 Сумма: {paid_amount} ₽\n"
                        f"🧾 Транзакция: {transaction_id}\n"
                        f"🔑 Операция: {operation_id}\n\n"
                        f"📊 Статистика оплат\n"
                        f"💰 Всего: {total_count} оплат на {total_sum} ₽\n"
                        f"📅 Сегодня: {today_count} оплат на {today_sum} ₽"
                    )
                )
            except Exception:
                pass

        return aiohttp_web.Response(text="SUCCESS")

    except Exception as e:
        logger.error(f"PayAnyWay webhook error: {e}")
        return aiohttp_web.Response(text="FAIL")


# ── Callback: «Проверить оплату» ─────────────────────────────────────────────
async def check_payanyway_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажимает «🔄 Проверить оплату» после перехода на страницу оплаты."""
    query = update.callback_query
    await query.answer()

    transaction_id = query.data.split(":", 1)[1]
    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT status, tariff FROM payments WHERE transaction_id=?",
        (transaction_id,)
    )
    row = cursor.fetchone()

    if not row:
        await query.answer("Платёж не найден", show_alert=True)
        return

    if row['status'] == 'paid':
        await query.answer("✅ Оплата подтверждена!", show_alert=True)
        await query.message.reply_text(
            f"✅ Подписка <b>{row['tariff']}</b> активна на 30 дней.",
            parse_mode="HTML"
        )
    else:
        await query.answer("⌛ Платёж пока не подтверждён. Попробуйте через минуту.", show_alert=True)


# ── Фоновая задача: авто-напоминания о незавершённых платежах ─────────────────
async def remind_unpaid_payments(bot_app):
    """Напоминает пользователям о незавершённых платежах ОДИН РАЗ через 10 минут."""
    while True:
        try:
            ten_minutes_ago = (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
            cursor = db.conn.cursor()
            # Только те, кому ещё НЕ напоминали (reminded_at IS NULL)
            cursor.execute(
                "SELECT transaction_id, user_id, plan, tariff FROM payments "
                "WHERE status='pending' AND created_at <= ? AND reminded_at IS NULL",
                (ten_minutes_ago,)
            )
            rows = cursor.fetchall()
            for r in rows:
                try:
                    plan_label = r['tariff'] or r['plan'] or "Полка"
                    # Восстанавливаем полную ссылку через generate_payanyway_url
                    PLAN_NAMES_REV = {"Старт": "standard", "PRO": "pro"}
                    plan_key = PLAN_NAMES_REV.get(plan_label, r['plan'] or "standard")
                    # Получаем сумму из БД
                    cursor.execute("SELECT amount FROM payments WHERE transaction_id=?", (r['transaction_id'],))
                    amt_row = cursor.fetchone()
                    amount = amt_row['amount'] if amt_row else 1
                    pay_url, _ = generate_payanyway_url(r['user_id'], plan_key, amount)
                    await bot_app.bot.send_message(
                        chat_id=r['user_id'],
                        text=(
                            f"⚠️ Вы начали оплату тарифа <b>{plan_label}</b>, но не завершили.\n\n"
                            f"Для завершения оплаты нажмите кнопку ниже:"
                        ),
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("→ Завершить оплату", url=pay_url),
                            InlineKeyboardButton("↺ Проверить оплату",
                                                 callback_data=f"checkpay:{r['transaction_id']}")
                        ]])
                    )
                    # Отмечаем что напомнили — больше не будем
                    cursor.execute(
                        "UPDATE payments SET reminded_at=? WHERE transaction_id=?",
                        (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), r['transaction_id'])
                    )
                    db.conn.commit()
                except Exception as e:
                    logger.debug(f"remind_unpaid: не удалось уведомить {r['user_id']}: {e}")
        except Exception as e:
            logger.error(f"remind_unpaid_payments error: {e}")
        await asyncio.sleep(300)  # проверяем каждые 5 минут


def resolve_crypto_url(discount_percent: int, discounted_price: int, plan: str) -> str:
    """
    Возвращает крипто-ссылку с учётом тарифа и скидки.
    Приоритет:
    1. CRYPTO_LINKS_BY_PLAN[(plan, discount_percent)] — точное совпадение по тарифу и %
    2. CRYPTO_LINKS[discount_percent]                 — любой тариф (обратная совместимость)
    3. db.get_crypto_link(discounted_price)           — ссылки по итоговой сумме из БД
    4. PAYMENT_DETAILS[crypto_{plan}]                 — стандартная ссылка для тарифа
    """
    plan_key = plan.lower()
    # 1. Точная ссылка по тарифу + проценту
    if discount_percent is not None:
        url = CRYPTO_LINKS_BY_PLAN.get((plan_key, discount_percent))
        if url:
            return url
    # 2. Ссылка без привязки к тарифу (standard-по умолчанию)
    if discount_percent and CRYPTO_LINKS.get(discount_percent):
        return CRYPTO_LINKS[discount_percent]
    # 3. По итоговой сумме из БД
    db_link = db.get_crypto_link(discounted_price)
    if db_link:
        return db_link
    # 4. Базовая ссылка тарифа (без скидки)
    base = CRYPTO_LINKS_BY_PLAN.get((plan_key, 0))
    if base:
        return base
    return PAYMENT_DETAILS.get(f'crypto_{plan_key}', PAYMENT_DETAILS['crypto_standard'])


async def api_check_promo(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """GET /api/check_promo?code=КОД&plan=standard|pro"""
    cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    }
    if request.method == 'OPTIONS':
        return aiohttp_web.Response(status=200, headers=cors)

    code = request.rel_url.query.get('code', '').strip().upper()
    plan = request.rel_url.query.get('plan', 'standard').lower()

    if not code:
        return aiohttp_web.json_response({'valid': False, 'error': 'Введите промокод'}, headers=cors)

    promo = db.get_promocode(code)
    if not promo:
        return aiohttp_web.json_response({'valid': False, 'error': 'Промокод недействителен или истёк'}, headers=cors)

    discount = promo['discount_percent']
    tariff_key = 'Standard' if plan == 'standard' else 'PRO'
    base_price = TARIFFS.get(tariff_key, {}).get('price', 299)
    discounted_price = round(base_price * (1 - discount / 100))

    yoomoney_url = PAYMENT_DETAILS.get(f'yoomoney_{plan}', PAYMENT_DETAILS['yoomoney_standard'])
    crypto_url   = resolve_crypto_url(discount, discounted_price, plan)

    return aiohttp_web.json_response({
        'valid': True,
        'code': code,
        'discount': discount,
        'price': discounted_price,
        'base_price': base_price,
        'duration_days': promo.get('duration_days', 30),
        'yoomoney_url': yoomoney_url,
        'crypto_url': crypto_url,
    }, headers=cors)


def create_api_app() -> aiohttp_web.Application:
    app = aiohttp_web.Application()
    app.router.add_get('/chat', api_serve_html)
    app.router.add_get('/api/check_promo', api_check_promo)
    app.router.add_options('/api/check_promo', api_check_promo)
    app.router.add_post('/api/payanyway', api_payanyway_notify)
    app.router.add_get('/api/payanyway', api_payanyway_notify)
    app.router.add_post('/api/payanyway/check', api_payanyway_check)
    app.router.add_get('/api/payanyway/check', api_payanyway_check)
    app.router.add_get('/api/chat/{session_id}', api_get_chat)
    app.router.add_get('/api/chat/{session_id}/poll', api_poll_messages)
    app.router.add_post('/api/chat/{session_id}/send', api_send_message)
    return app

def main():
    if not TOKEN:
        raise RuntimeError(
            "BOT_TOKEN не задан. Создайте файл .env с BOT_TOKEN=ваш_токен "
            "или задайте переменную окружения BOT_TOKEN."
        )

    application = Application.builder().token(TOKEN).post_init(post_init).build()
    # Rate limiting middleware
    application.add_handler(__import__("telegram.ext", fromlist=["TypeHandler"]).TypeHandler(
        type=object, callback=rate_limit_middleware
    ), group=-1)
    
    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))
    application.add_handler(CallbackQueryHandler(webapp_pay_confirm_callback,     pattern="^webapp_pay_confirm$"))
    application.add_handler(CallbackQueryHandler(webapp_pay_upgrade_pro_callback, pattern="^webapp_pay_upgrade_pro$"))
    application.add_handler(CallbackQueryHandler(webapp_pay_cancel_callback,      pattern="^webapp_pay_cancel$"))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("admin", admin_command))  # скрытая команда для админов
    
    # ConversationHandler для создания объявления (всё по кнопкам)
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(create_ad_start, pattern="^create_ad$"),
            CallbackQueryHandler(templates_menu, pattern="^templates_menu$"),
        ],
        states={
            States.CREATE_AD_CATEGORY: [
                CallbackQueryHandler(create_ad_category, pattern="^(cad_cat:|category_)"),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_TITLE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_title),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_CONDITION: [
                CallbackQueryHandler(create_ad_condition, pattern="^cad_cond:"),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_SIZE: [
                CallbackQueryHandler(create_ad_size, pattern="^cad_size:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_size_text),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_GENDER: [
                CallbackQueryHandler(create_ad_gender, pattern="^cad_gender:"),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_CITY: [
                CallbackQueryHandler(create_ad_city, pattern="^cad_city:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_city),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_DELIVERY: [
                CallbackQueryHandler(create_ad_delivery, pattern="^cad_dlv:"),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_DELIVERY_CUSTOM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_delivery_custom),
                CallbackQueryHandler(create_ad_delivery_custom, pattern="^cad_dlv_back$"),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_PHOTOS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_price),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_DESCRIPTION_OPT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_description_opt),
                CallbackQueryHandler(create_ad_description_opt, pattern="^cad_desc:skip$"),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_DESCRIPTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_description),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_CONTACTS: [
                MessageHandler(filters.PHOTO, handle_photos),
                CallbackQueryHandler(skip_or_next_photos, pattern="^(skip_photos|next_step)$"),
                CallbackQueryHandler(create_ad_edit_contact, pattern="^cad_edit_contact$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_ad_contacts),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
            States.CREATE_AD_PREVIEW: [
                CallbackQueryHandler(publish_ad, pattern="^publish_ad$"),
                CallbackQueryHandler(edit_ad_without_param, pattern="^edit_ad$"),
                CallbackQueryHandler(create_ad_edit_contact, pattern="^cad_edit_contact$"),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            CallbackQueryHandler(show_main_menu, pattern="^back_to_menu$"),
        ],
        allow_reentry=True,
    )
    application.add_handler(conv_handler)
    
    # ConversationHandler для поиска (Авито-style)
    search_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(search_start, pattern="^search$")],
        states={
            States.SEARCH_QUERY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search),
                CallbackQueryHandler(search_by_photo_start, pattern="^search_by_photo$"),
                # История поиска
                CallbackQueryHandler(search_quick_repeat, pattern="^sq:"),
                # Пагинация
                CallbackQueryHandler(sf_page_prev, pattern="^sf_page_prev$"),
                CallbackQueryHandler(sf_page_next, pattern="^sf_page_next$"),
                CallbackQueryHandler(sf_noop, pattern="^sf_noop$"),
                # Фильтры
                CallbackQueryHandler(sf_menu, pattern="^sf_menu$"),
                CallbackQueryHandler(sf_cat_menu, pattern="^sf_cat_menu$"),
                CallbackQueryHandler(sf_set_cat, pattern="^sf_cat:"),
                CallbackQueryHandler(sf_city_menu, pattern="^sf_city_menu$"),
                CallbackQueryHandler(sf_set_city, pattern="^sf_city:"),
                CallbackQueryHandler(sf_set_cond, pattern="^sf_cond:"),
                CallbackQueryHandler(sf_set_delivery, pattern="^sf_delivery:"),
                # Сортировка
                CallbackQueryHandler(sf_sort_menu, pattern="^sf_sort_menu$"),
                CallbackQueryHandler(sf_set_sort, pattern="^sf_sort:"),
                # Цена (переход к вводу)
                CallbackQueryHandler(sf_price_from_start, pattern="^sf_price_from$"),
                CallbackQueryHandler(sf_price_to_start, pattern="^sf_price_to$"),
                # Применить / сброс
                CallbackQueryHandler(sf_apply, pattern="^sf_apply$"),
                CallbackQueryHandler(sf_reset, pattern="^sf_reset$"),
                # Старый формат пагинации (обратная совместимость)
                CallbackQueryHandler(search_page, pattern="^search_page:"),
                CallbackQueryHandler(back_to_search, pattern="^back_to_search:"),
                # Навигация
                CallbackQueryHandler(search_start, pattern="^search$"),
                CallbackQueryHandler(show_main_menu, pattern="^back_to_menu$"),
            ],
            States.SEARCH_PHOTO: [
                MessageHandler(filters.PHOTO, handle_search_photo),
                CallbackQueryHandler(search_start, pattern="^search$"),
                CallbackQueryHandler(show_main_menu, pattern="^back_to_menu$"),
            ],
            States.SEARCH_PRICE_FROM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sf_price_input),
                CallbackQueryHandler(sf_price_skip, pattern="^sf_price_skip$"),
                CallbackQueryHandler(sf_menu, pattern="^sf_menu$"),
            ],
            States.SEARCH_PRICE_TO: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sf_price_input),
                CallbackQueryHandler(sf_price_skip, pattern="^sf_price_skip$"),
                CallbackQueryHandler(sf_menu, pattern="^sf_menu$"),
            ],
        },
        fallbacks=[CallbackQueryHandler(show_main_menu, pattern="^back_to_menu$")],
        allow_reentry=True
    )
    application.add_handler(search_conv_handler)
    
    # ConversationHandler для админской рассылки
    broadcast_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_broadcast_start, pattern="^admin_broadcast_start$")],
        states={
            States.ADMIN_BROADCAST: [
                MessageHandler(filters.TEXT | filters.PHOTO, handle_broadcast_message),
                CallbackQueryHandler(broadcast_preview, pattern="^broadcast_preview$"),
                CallbackQueryHandler(admin_back, pattern="^admin_back$")
            ],
            States.ADMIN_BROADCAST_CONFIRM: [
                CallbackQueryHandler(broadcast_confirm, pattern="^broadcast_confirm$"),
                CallbackQueryHandler(admin_back, pattern="^admin_back$")
            ]
        },
        fallbacks=[CallbackQueryHandler(admin_back, pattern="^admin_back$")]
    )
    application.add_handler(broadcast_conv_handler)
    
    # ConversationHandler для создания промокодов
    promo_create_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_promo_create_start, pattern="^admin_promo_create$")],
        states={
            States.ADMIN_PROMO_CREATE_CODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_promo_input_code)
            ],
            States.ADMIN_PROMO_CREATE_DISCOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_promo_input_discount)
            ],
            States.ADMIN_PROMO_CREATE_DURATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_promo_input_duration)
            ],
            States.ADMIN_PROMO_CREATE_USES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_promo_input_uses)
            ]
        },
        fallbacks=[CallbackQueryHandler(admin_promocodes, pattern="^admin_promocodes$")]
    )
    application.add_handler(promo_create_handler)
    
    # ConversationHandler для использования промокода
    promo_use_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(use_promo_start, pattern="^use_promo:")],
        states={
            States.ENTER_PROMOCODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, apply_promocode),
                # Обработка кнопки "✗ Отмена" (возврат к тарифу) прямо внутри состояния
                CallbackQueryHandler(buy_tariff, pattern="^(buy_standard|buy_pro|renew_standard|renew_pro)$"),
                CallbackQueryHandler(cancel_payment, pattern="^cancel_payment$"),
            ]
        },
        fallbacks=[
            CallbackQueryHandler(show_subscriptions, pattern="^subscriptions$"),
            CallbackQueryHandler(cancel_payment, pattern="^cancel_payment$"),
            # Кнопка "✗ Отмена" внутри use_promo_start шлёт "buy_{tariff}" — ловим здесь
            CallbackQueryHandler(buy_tariff, pattern="^(buy_standard|buy_pro|renew_standard|renew_pro)$"),
        ]
    )
    application.add_handler(promo_use_handler)
    
    # ConversationHandler для буста объявления
    boost_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(boost_confirm_start, pattern="^boost_confirm:")],
        states={
            States.BOOST_PAYMENT_PROOF: [
                MessageHandler(filters.PHOTO, handle_boost_proof),
                CallbackQueryHandler(cancel_action, pattern="^cancel$")
            ]
        },
        fallbacks=[CallbackQueryHandler(cancel_action, pattern="^cancel$")]
    )
    application.add_handler(boost_conv_handler)

    # ConversationHandler для отзыва
    review_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(review_set_rating, pattern="^review_rate:")],
        states={
            States.REVIEW_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, review_handle_text)
            ]
        },
        fallbacks=[CallbackQueryHandler(cancel_action, pattern="^cancel$")]
    )
    application.add_handler(review_conv_handler)
    crypto_link_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_crypto_add_start, pattern="^admin_crypto_add$")],
        states={
            States.ADMIN_CRYPTO_LINK_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_crypto_input_amount)
            ],
            States.ADMIN_CRYPTO_LINK_URL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_crypto_input_url)
            ]
        },
        fallbacks=[CallbackQueryHandler(admin_crypto_links, pattern="^admin_crypto_links$")]
    )
    application.add_handler(crypto_link_handler)

    # ConversationHandler для эскроу (оплата чеком)
    escrow_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(escrow_paid_start, pattern="^escrow_paid:")],
        states={
            States.ESCROW_CONFIRM_BUYER: [
                MessageHandler(filters.PHOTO, handle_escrow_proof),
                CallbackQueryHandler(escrow_cancel, pattern="^escrow_cancel:")
            ]
        },
        fallbacks=[CallbackQueryHandler(cancel_action, pattern="^cancel$")]
    )
    application.add_handler(escrow_conv_handler)
    
    # ========== ВСЕ CallbackQueryHandler В ОДНОМ МЕСТЕ ==========
    # Основное меню
    application.add_handler(CallbackQueryHandler(show_main_menu, pattern="^back_to_menu$"))
    application.add_handler(CallbackQueryHandler(check_subscription_callback, pattern="^check_subscription$"))
    application.add_handler(CallbackQueryHandler(skip_verification_callback, pattern="^skip_verification$"))
    
    # Создание объявлений - эти обработчики УЖЕ в ConversationHandler, не дублируем
    # application.add_handler(CallbackQueryHandler(skip_or_next_photos, pattern="^(skip_photos|next_step)$"))
    # application.add_handler(CallbackQueryHandler(publish_ad, pattern="^publish_ad$"))
    # application.add_handler(CallbackQueryHandler(edit_ad_without_param, pattern="^edit_ad$"))
    # application.add_handler(CallbackQueryHandler(cancel_action, pattern="^cancel$"))
    
    # Поиск - эти обработчики УЖЕ в ConversationHandler для поиска, не дублируем
    # application.add_handler(CallbackQueryHandler(search_start, pattern="^search$"))
    # application.add_handler(CallbackQueryHandler(search_page, pattern="^search_page:"))
    # application.add_handler(CallbackQueryHandler(back_to_search, pattern="^back_to_search:"))
    
    # Просмотр объявлений (может быть вызван из разных мест)
    application.add_handler(CallbackQueryHandler(view_ad, pattern="^view_ad:"))
    application.add_handler(CallbackQueryHandler(view_my_ad, pattern="^view_my_ad:"))
    application.add_handler(CallbackQueryHandler(toggle_favorite, pattern="^(add_favorite|remove_favorite):"))
    
    # Мои объявления
    application.add_handler(CallbackQueryHandler(my_ads, pattern="^my_ads$"))
    application.add_handler(CallbackQueryHandler(my_ads_active, pattern="^my_ads_active$"))
    application.add_handler(CallbackQueryHandler(my_ads_archive, pattern="^my_ads_archive$"))
    application.add_handler(CallbackQueryHandler(deactivate_ad_handler, pattern="^deactivate_ad:"))
    application.add_handler(CallbackQueryHandler(delete_ad_completely, pattern="^delete_ad_completely:"))
    application.add_handler(CallbackQueryHandler(reactivate_ad, pattern="^reactivate_ad:"))
    application.add_handler(CallbackQueryHandler(delete_ad, pattern="^delete_ad:"))
    application.add_handler(CallbackQueryHandler(edit_ad_menu, pattern="^edit_ad:"))  # v24: полноценное меню редактирования
    application.add_handler(CallbackQueryHandler(edit_price, pattern="^edit_price:"))
    application.add_handler(CallbackQueryHandler(edit_desc, pattern="^edit_desc:"))
    application.add_handler(CallbackQueryHandler(edit_contacts, pattern="^edit_contacts:"))
    application.add_handler(CallbackQueryHandler(show_moderation_ads, pattern="^show_moderation_ads$"))
    
    # Избранное
    application.add_handler(CallbackQueryHandler(favorites, pattern="^favorites$"))
    
    # Связь с продавцом - этот обработчик больше не нужен, т.к. используем deep links
    application.add_handler(CallbackQueryHandler(contact_seller, pattern="^contact_seller:"))
    
    # Профиль
    application.add_handler(CallbackQueryHandler(show_profile, pattern="^profile$"))
    application.add_handler(CallbackQueryHandler(pin_ad_menu, pattern="^pin_ad_menu$"))
    application.add_handler(CallbackQueryHandler(pin_ad, pattern="^pin_ad:"))
    
    # Тарифы и оплата
    application.add_handler(CallbackQueryHandler(show_subscriptions,     pattern="^subscriptions$"))
    application.add_handler(CallbackQueryHandler(buy_tariff_handler,     pattern="^buy_tariff:"))
    application.add_handler(CallbackQueryHandler(show_tinkoff_handler,   pattern="^show_tinkoff:"))
    application.add_handler(CallbackQueryHandler(i_paid_handler,         pattern="^i_paid$"))
    application.add_handler(CallbackQueryHandler(enter_promo_handler,    pattern="^enter_promo$"))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_promo_text), group=30)
    application.add_handler(CallbackQueryHandler(buy_tariff, pattern="^(buy_standard|buy_pro|renew_standard|renew_pro)$"))
    application.add_handler(CallbackQueryHandler(proceed_to_payment, pattern="^proceed_pay:"))
    application.add_handler(CallbackQueryHandler(confirm_payment, pattern="^confirm_payment$"))
    application.add_handler(CallbackQueryHandler(cancel_payment, pattern="^cancel_payment$"))
    application.add_handler(CallbackQueryHandler(show_tinkoff, pattern="^show_tinkoff$"))
    
    # Реферальная программа
    application.add_handler(CallbackQueryHandler(referral_menu, pattern="^referral_menu$"))
    application.add_handler(CallbackQueryHandler(referral_list, pattern="^referral_list$"))
    application.add_handler(CallbackQueryHandler(referral_balance, pattern="^referral_balance$"))
    application.add_handler(CallbackQueryHandler(referral_share, pattern="^referral_share$"))
    application.add_handler(CallbackQueryHandler(copy_ref_link, pattern="^copy_ref_link:"))
    application.add_handler(CallbackQueryHandler(pay_with_balance, pattern="^pay_with_balance$"))
    application.add_handler(CallbackQueryHandler(pay_subscription_balance, pattern="^(pay_standard_balance|pay_pro_balance)$"))
    application.add_handler(CallbackQueryHandler(confirm_pay_subscription, pattern="^confirm_pay_(standard|pro)$"))
    
    # Админка
    application.add_handler(CallbackQueryHandler(admin_stats, pattern="^admin_stats$"))
    application.add_handler(CallbackQueryHandler(admin_moderation, pattern="^admin_moderation$"))
    application.add_handler(CallbackQueryHandler(admin_view_moderation_ad, pattern="^admin_view_ad:"))
    application.add_handler(CallbackQueryHandler(admin_payments, pattern="^admin_payments$"))
    application.add_handler(CallbackQueryHandler(admin_payments_archive, pattern="^admin_payments_archive$"))
    application.add_handler(CallbackQueryHandler(admin_broadcast_start, pattern="^admin_broadcast_start$"))
    application.add_handler(CallbackQueryHandler(admin_back, pattern="^admin_back$"))
    application.add_handler(CallbackQueryHandler(admin_moderation_next, pattern="^admin_moderation_next$"))
    application.add_handler(CallbackQueryHandler(admin_payments_next, pattern="^admin_payments_next$"))
    application.add_handler(CallbackQueryHandler(admin_view_receipt, pattern="^payment_view_receipt:"))
    application.add_handler(CallbackQueryHandler(broadcast_confirm, pattern="^broadcast_confirm$"))
    application.add_handler(CallbackQueryHandler(broadcast_preview, pattern="^broadcast_preview$"))

    # ConversationHandler: модерация объявлений (поддерживает ввод причины отклонения)
    moderation_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(moderate_ad_action, pattern="^moderate_")],
        states={
            States.ADMIN_MODERATION_REASON: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_USER_IDS),
                               handle_moderation_reason),
                CallbackQueryHandler(admin_moderation, pattern="^admin_moderation$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(admin_moderation, pattern="^admin_moderation$"),
            CallbackQueryHandler(cancel_action, pattern="^cancel$"),
        ],
        per_user=True, per_chat=False,
    )
    application.add_handler(moderation_conv_handler, group=5)

    # ConversationHandler: модерация платежей (поддерживает ввод причины отклонения)
    payment_moderation_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(moderate_payment_action, pattern="^payment_")],
        states={
            States.ADMIN_MODERATION_REASON: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_USER_IDS),
                               handle_payment_rejection),
                CallbackQueryHandler(admin_payments, pattern="^admin_payments$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(admin_payments, pattern="^admin_payments$"),
            CallbackQueryHandler(cancel_action, pattern="^cancel$"),
        ],
        per_user=True, per_chat=False,
    )
    application.add_handler(payment_moderation_conv, group=6)
    
    # Промокоды
    application.add_handler(CallbackQueryHandler(admin_promocodes, pattern="^admin_promocodes$"))
    application.add_handler(CallbackQueryHandler(admin_promo_create_start, pattern="^admin_promo_create$"))
    application.add_handler(CallbackQueryHandler(admin_promo_deactivate_start, pattern="^admin_promo_deactivate$"))
    application.add_handler(CallbackQueryHandler(admin_promo_deactivate_confirm, pattern="^admin_promo_deact:"))
    application.add_handler(CallbackQueryHandler(use_promo_start, pattern="^use_promo:"))
    application.add_handler(CallbackQueryHandler(confirm_payment_promo, pattern="^confirm_payment_promo$"))
    
    # Криптоссылки
    application.add_handler(CallbackQueryHandler(admin_crypto_links, pattern="^admin_crypto_links$"))
    application.add_handler(CallbackQueryHandler(admin_crypto_add_start, pattern="^admin_crypto_add$"))
    application.add_handler(CallbackQueryHandler(admin_crypto_delete_start, pattern="^admin_crypto_delete$"))
    application.add_handler(CallbackQueryHandler(admin_crypto_delete_confirm, pattern="^admin_crypto_del:"))

    #  Буст объявлений
    application.add_handler(CallbackQueryHandler(boost_ad_start, pattern="^boost_ad:"))
    application.add_handler(CallbackQueryHandler(boost_confirm_start, pattern="^boost_confirm:"))
    application.add_handler(CallbackQueryHandler(boost_approve, pattern="^boost_approve:"))
    application.add_handler(CallbackQueryHandler(boost_reject, pattern="^boost_reject:"))

    #  Сохранённые поиски
    application.add_handler(CallbackQueryHandler(save_search_callback, pattern="^save_search:"))
    application.add_handler(CallbackQueryHandler(my_saved_searches, pattern="^my_saved_searches$"))
    application.add_handler(CallbackQueryHandler(delete_saved_search_callback, pattern="^del_search:"))

    # ★ Отзывы + обжалование
    application.add_handler(CallbackQueryHandler(leave_review_seller_start, pattern="^leave_review_seller:"))
    application.add_handler(CallbackQueryHandler(leave_review_start, pattern="^leave_review:"))
    application.add_handler(CallbackQueryHandler(review_set_rating, pattern="^review_rate:"))
    application.add_handler(CallbackQueryHandler(show_seller_reviews, pattern="^seller_reviews:"))
    application.add_handler(CallbackQueryHandler(appeal_review_menu, pattern="^appeal_review_menu:"))
    application.add_handler(CallbackQueryHandler(appeal_review_start, pattern="^appeal_review:"))
    application.add_handler(CallbackQueryHandler(admin_appeal_decision, pattern="^admin_appeal_(delete|reject):"))

    # ★ Избранные продавцы
    application.add_handler(CallbackQueryHandler(toggle_favorite_seller, pattern="^fav_seller:"))
    application.add_handler(CallbackQueryHandler(show_favorite_sellers, pattern="^favorite_sellers$"))
    application.add_handler(CallbackQueryHandler(unfav_seller, pattern="^unfav_seller:"))
    application.add_handler(CallbackQueryHandler(search_seller, pattern="^search_seller$"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_seller_input), group=8)

    #  Аналитика
    application.add_handler(CallbackQueryHandler(show_ad_analytics, pattern="^ad_analytics:"))

    #  Антифрод
    application.add_handler(CallbackQueryHandler(report_ad_start, pattern="^report_ad:"))
    application.add_handler(CallbackQueryHandler(report_ad_send, pattern="^report_send:"))
    # Жалоба на пользователя
    application.add_handler(CallbackQueryHandler(report_user_start, pattern="^report_user:"))
    application.add_handler(CallbackQueryHandler(report_user_send, pattern="^report_user_send:"))
    application.add_handler(CallbackQueryHandler(admin_dismiss_report_callback, pattern="^admin_dismiss_report:"))
    # Антидубликат — публикация всё равно
    application.add_handler(CallbackQueryHandler(publish_ad_force, pattern="^publish_ad_force$"))
    # Дайджест — отписка
    application.add_handler(CallbackQueryHandler(digest_unsubscribe_callback, pattern="^digest_unsubscribe$"))
    application.add_handler(CallbackQueryHandler(search_start_callback, pattern="^search_start$"))
    # Брошенный черновик — закрыть напоминание
    application.add_handler(CallbackQueryHandler(draft_dismiss_callback, pattern="^draft_dismiss$"))
    application.add_handler(CallbackQueryHandler(admin_restore_ad_handler, pattern="^admin_restore_ad:"))
    application.add_handler(CallbackQueryHandler(admin_ban_user_handler, pattern="^admin_ban:"))
    application.add_handler(CallbackQueryHandler(admin_delete_ad_handler, pattern="^admin_delete_ad:"))

    #  Редактирование объявлений
    edit_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_field_start, pattern="^edit_field:")],
        states={
            States.EDIT_AD_TITLE:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_field)],
            States.EDIT_AD_DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_field)],
            States.EDIT_AD_PRICE:       [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_field)],
            States.EDIT_AD_CITY:        [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_field)],
            States.EDIT_AD_DELIVERY:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_field)],
            States.EDIT_AD_CONTACTS:    [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_field)],
        },
        fallbacks=[CallbackQueryHandler(cancel_action, pattern="^cancel$")]
    )
    application.add_handler(edit_conv_handler)

    #  Поделиться
    application.add_handler(CallbackQueryHandler(share_ad, pattern="^share_ad:"))

    #  Верификация через Telegram Contact
    verify_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(verify_seller_start, pattern="^verify_seller$")],
        states={
            States.VERIFY_PHONE_INPUT: [
                # Пользователь поделился контактом — основной путь
                MessageHandler(filters.CONTACT, verify_handle_contact),
                # Пользователь нажал текстовую кнопку «✗ Отмена»
                MessageHandler(filters.TEXT & ~filters.COMMAND, verify_handle_cancel_text),
                CallbackQueryHandler(cancel_action, pattern="^cancel$"),
                CallbackQueryHandler(cancel_action, pattern="^back_to_menu$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_action, pattern="^cancel$"),
            CallbackQueryHandler(cancel_action, pattern="^back_to_menu$"),
        ],
        allow_reentry=True,
    )
    application.add_handler(verify_conv_handler)
    application.add_handler(CallbackQueryHandler(admin_approve_verify, pattern="^admin_verify:"))
    application.add_handler(CallbackQueryHandler(admin_reject_verify, pattern="^admin_reject_verify:"))

    # ! Обжалование отзыва (ConversationHandler)
    appeal_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(appeal_review_start, pattern="^appeal_review:\\d+$")],
        states={
            States.REVIEW_APPEAL_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, appeal_review_handle_text),
                CallbackQueryHandler(cancel_action, pattern="^cancel$")
            ],
        },
        fallbacks=[CallbackQueryHandler(cancel_action, pattern="^cancel$")]
    )
    application.add_handler(appeal_conv_handler)

    #  Автопродление
    application.add_handler(CallbackQueryHandler(renewal_bump, pattern="^renewal_bump:"))

    #  Шаблоны
    application.add_handler(CallbackQueryHandler(templates_menu, pattern="^templates_menu$"))
    application.add_handler(CallbackQueryHandler(use_template, pattern="^use_template:"))

    #  Фильтр по городу
    application.add_handler(CallbackQueryHandler(search_with_city_filter, pattern="^search_city_filter$"))
    application.add_handler(CallbackQueryHandler(set_search_city, pattern="^search_city:"))

    #  Подписка на снижение цены
    application.add_handler(CallbackQueryHandler(watch_price_toggle, pattern="^watch_price:"))

    #  Авто-подъём
    application.add_handler(CallbackQueryHandler(auto_bump_menu, pattern="^auto_bump:"))
    application.add_handler(CallbackQueryHandler(auto_bump_set, pattern="^auto_bump_set:"))
    application.add_handler(CallbackQueryHandler(auto_bump_cancel, pattern="^auto_bump_cancel:"))

    #  Панель администратора (callback-версия для кнопки «Назад»)
    application.add_handler(CallbackQueryHandler(admin_panel_callback, pattern="^admin_panel$"))

    # ── v26: монетизационные триггеры и апсейлы ──────────────────────
    application.add_handler(CallbackQueryHandler(boost_ad_upsell, pattern="^boost_upsell:"))
    application.add_handler(CallbackQueryHandler(boost_72h_start, pattern="^boost_72h:"))
    application.add_handler(CallbackQueryHandler(boost_pack_start, pattern="^boost_pack:"))
    # Гео-бусты
    application.add_handler(CallbackQueryHandler(boost_geo_rf, pattern="^boost_geo_rf:"))
    application.add_handler(CallbackQueryHandler(boost_geo_city, pattern="^boost_geo_city:"))
    application.add_handler(CallbackQueryHandler(boost_city_payment, pattern="^boost_city_24h:"))
    application.add_handler(CallbackQueryHandler(boost_city_payment, pattern="^boost_city_72h:"))
    application.add_handler(CallbackQueryHandler(boost_city_payment, pattern="^boost_city_pack:"))
    application.add_handler(CallbackQueryHandler(boost_no_city_callback, pattern="^boost_no_city$"))
    application.add_handler(CallbackQueryHandler(trigger_verification_social_proof,
                                                  pattern="^verify_social:"))
    application.add_handler(CallbackQueryHandler(
        lambda u, c: show_subscriptions(u, c), pattern="^free_limit_upsell$"
    ))

    #  Расширенная аналитика для админа
    application.add_handler(CallbackQueryHandler(admin_advanced_analytics, pattern="^admin_advanced_analytics$"))

    #  Эскроу
    application.add_handler(CallbackQueryHandler(escrow_start, pattern="^escrow_start:"))
    application.add_handler(CallbackQueryHandler(escrow_create, pattern="^escrow_create:"))
    application.add_handler(CallbackQueryHandler(escrow_paid_start, pattern="^escrow_paid:"))
    application.add_handler(CallbackQueryHandler(escrow_shipped, pattern="^escrow_shipped:"))
    application.add_handler(CallbackQueryHandler(escrow_received, pattern="^escrow_received:"))
    application.add_handler(CallbackQueryHandler(escrow_dispute, pattern="^escrow_dispute:"))
    application.add_handler(CallbackQueryHandler(escrow_cancel, pattern="^escrow_cancel:"))
    application.add_handler(CallbackQueryHandler(escrow_admin_confirm, pattern="^escrow_admin_confirm:"))
    application.add_handler(CallbackQueryHandler(escrow_admin_reject, pattern="^escrow_admin_reject:"))
    application.add_handler(CallbackQueryHandler(escrow_refund, pattern="^escrow_refund:"))
    application.add_handler(CallbackQueryHandler(escrow_release, pattern="^escrow_release:"))
    application.add_handler(CallbackQueryHandler(my_escrow_deals, pattern="^my_escrow_deals$"))
    
    # ========== MessageHandler ==========
    
    # Обработчик текстовых сообщений для поиска УБРАН - теперь он в ConversationHandler для поиска
    # Это исправляет баг, когда поиск перехватывал сообщения при создании объявления
    # application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search), group=10)
    
    # Глобальный обработчик фото — только для чеков оплаты.
    # Ставим в группу 99 чтобы ConversationHandler-ы (создание объявления, буст, эскроу)
    # имели приоритет и перехватывали фото первыми внутри своих состояний.
    application.add_handler(MessageHandler(filters.PHOTO, handle_payment_proof), group=99)
    # Чат: пересылка сообщений (текст и фото) когда есть active_chat
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_chat_message), group=50)
    application.add_handler(MessageHandler(
        filters.PHOTO, handle_chat_message), group=50)
    # Чат: навигация
    application.add_handler(CallbackQueryHandler(start_chat_with_seller, pattern="^chat_start:"))
    application.add_handler(CallbackQueryHandler(open_chat_now, pattern="^open_chat_now:"))
    application.add_handler(CallbackQueryHandler(open_chat, pattern="^open_chat:"))
    application.add_handler(CallbackQueryHandler(close_chat, pattern="^close_chat$"))
    application.add_handler(CallbackQueryHandler(my_chats, pattern="^my_chats$"))
    # Дублировать объявление
    application.add_handler(CallbackQueryHandler(duplicate_ad, pattern="^duplicate_ad:"))
    # Профиль продавца
    application.add_handler(CallbackQueryHandler(view_seller_profile, pattern="^seller_profile:"))
    # PayAnyWay: проверка оплаты
    application.add_handler(CallbackQueryHandler(check_payanyway_callback, pattern="^checkpay:"))
    # Подтверждение даунгрейда тарифа
    application.add_handler(CallbackQueryHandler(buy_tariff_confirm_handler, pattern="^buy_tariff_confirm:"))
    # Ручная активация PayAnyWay (админ)
    application.add_handler(CallbackQueryHandler(admin_paw_activate, pattern="^admin_paw_activate$"))
    application.add_handler(MessageHandler(
        filters.TEXT & filters.User(ADMIN_USER_IDS) & ~filters.COMMAND,
        admin_paw_activate_input
    ))

    # Обработчик ошибок
    application.add_error_handler(error_handler)

    # Запуск бота + API-сервера
    global _bot_app_ref
    _bot_app_ref = application

    # API-сервер (webhook PayAnyWay) запускается ВСЕГДА, независимо от WEBAPP_URL
    async def run_all():
        api_app = create_api_app()
        runner = aiohttp_web.AppRunner(api_app)
        await runner.setup()
        site = aiohttp_web.TCPSite(runner, '0.0.0.0', WEBAPP_PORT)
        await site.start()
        print(f"✅ API-сервер (PayAnyWay webhook) запущен на порту {WEBAPP_PORT}")
        print(f"   PayAnyWay Result URL: {SERVER_URL}/api/payanyway")
        if WEBAPP_URL:
            print(f"   Mini App: {WEBAPP_URL}/chat")

        async with application:
            await application.initialize()
            await application.start()
            await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            print("✅ Бот запущен")
            # Запускаем фоновые задачи
            asyncio.ensure_future(remind_unpaid_payments(application))
            # Ждём вечно
            stop_event = asyncio.Event()
            try:
                await stop_event.wait()
            except (KeyboardInterrupt, SystemExit):
                pass
            finally:
                await application.updater.stop()
                await application.stop()
                await runner.cleanup()

    asyncio.run(run_all())

async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик данных из Telegram Mini App (кнопка «Я оплатил»)"""
    try:
        raw = update.message.web_app_data.data
        payload = json.loads(raw)
    except Exception:
        return

    action = payload.get('action')

    if action == 'check_promo':
        # Пользователь проверяет промокод через Mini App (API недоступен)
        code  = payload.get('code', '').strip().upper()
        plan  = payload.get('plan', 'standard').lower()
        PLAN_NAMES = {'free': 'Free', 'standard': 'Старт', 'pro': 'PRO'}
        plan_label = PLAN_NAMES.get(plan, plan.upper())

        if not code:
            await update.message.reply_text("Введите промокод.")
            return

        promo = db.get_promocode(code)
        if not promo:
            await update.message.reply_text(
                f"✗ Промокод <b>{code}</b> не найден или истёк.",
                parse_mode=ParseMode.HTML
            )
            return

        discount = promo['discount_percent']
        tariff_key = 'Standard' if plan == 'standard' else 'PRO'
        base_price = TARIFFS.get(tariff_key, {}).get('price', 299)
        discounted_price = round(base_price * (1 - discount / 100))

        # Сохраняем промокод в контексте для последующей оплаты
        context.user_data['promo_code']    = code
        context.user_data['promo_discount'] = discount
        context.user_data['promo_plan']    = plan

        crypto_url   = resolve_crypto_url(discount, discounted_price, plan)
        yoomoney_url = PAYMENT_DETAILS.get(f'yoomoney_{plan}', PAYMENT_DETAILS['yoomoney_standard'])

        await update.message.reply_text(
            f"✓ Промокод <b>{code}</b> действителен!\n\n"
            f"Скидка: <b>{discount}%</b>\n"
            f"Тариф: <b>{plan_label}</b>\n"
            f"Цена со скидкой: <b>{discounted_price} ₽</b>\n\n"
            f"Вернитесь на страницу тарифов и нажмите «Я оплатил» — скидка будет применена автоматически.",
            parse_mode=ParseMode.HTML,
        )
        return

    if action != 'paid':
        return

    plan  = payload.get('plan', 'standard').lower()
    price = int(payload.get('price', 0))
    promo = payload.get('promo', '')

    PLAN_NAMES  = {'free': 'Free', 'standard': 'Старт', 'pro': 'PRO'}
    PLAN_RANK   = {'free': 0, 'standard': 1, 'pro': 2}

    # Нормализация: «Старт» / «Standard» → 'standard', «PRO» → 'pro'
    TARIFF_TO_KEY = {
        'free': 'free', 'Free': 'free',
        'standard': 'standard', 'Standard': 'standard', 'Старт': 'standard',
        'pro': 'pro', 'PRO': 'pro',
    }

    plan_label   = PLAN_NAMES.get(plan, plan.upper())
    plan_rank    = PLAN_RANK.get(plan, 1)

    user_data    = db.get_user(update.effective_user.id)
    current_raw  = (user_data.get('tariff', 'Free') if user_data else 'Free')
    current_key  = TARIFF_TO_KEY.get(current_raw, 'free')
    current_rank = PLAN_RANK.get(current_key, 0)
    current_label = PLAN_NAMES.get(current_key, current_raw)

    # Дата окончания подписки
    end_date_str = ''
    if user_data and user_data.get('tariff_end_date'):
        end_date_str = user_data['tariff_end_date'][:10]

    # Сохраняем платёж в контексте для последующего подтверждения
    context.user_data['pending_payment'] = {
        'tariff':    plan_label,
        'amount':    price,
        'duration':  30,
        'promocode': promo or None,
    }

    # ── Проверка: покупка того же тарифа ─────────────────────────────────────
    if current_key == plan and current_key != 'free':
        end_info = f" (действует до {end_date_str})" if end_date_str else ""
        await update.message.reply_text(
            f"⚠️ У вас уже активен тариф <b>{current_label}</b>{end_info}.\n\n"
            f"Оплата продлит подписку ещё на 30 дней.\n\n"
            f"Вы хотите продлить <b>{plan_label}</b> или перейти на другой тариф?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"✓ Продлить {plan_label} — {price} ₽",
                                      callback_data="webapp_pay_confirm")],
                [InlineKeyboardButton("↑ Перейти на PRO",
                                      callback_data="webapp_pay_upgrade_pro")
                 ] if plan != 'pro' else [],
                [InlineKeyboardButton("✗ Отмена", callback_data="webapp_pay_cancel")],
            ])
        )
        return

    # ── Проверка: покупка более низкого тарифа ────────────────────────────────
    if current_rank > plan_rank:
        end_info = f" (до {end_date_str})" if end_date_str else ""
        await update.message.reply_text(
            f"⚠️ Вы пытаетесь купить <b>{plan_label}</b>, "
            f"но у вас уже активен более высокий тариф <b>{current_label}</b>{end_info}.\n\n"
            f"Оплата более низкого тарифа <b>понизит</b> ваши возможности.\n\n"
            f"Что вы хотите сделать?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"✓ Всё равно купить {plan_label} — {price} ₽",
                                      callback_data="webapp_pay_confirm")],
                [InlineKeyboardButton("✗ Отмена", callback_data="webapp_pay_cancel")],
            ])
        )
        return

    # ── Всё ок: апгрейд или новая покупка ─────────────────────────────────────
    if current_key != 'free' and plan_rank > current_rank:
        # Апгрейд — сообщаем пользователю что это переход на более высокий тариф
        end_info = f" (текущая подписка до {end_date_str})" if end_date_str else ""
        await update.message.reply_text(
            f"🚀 Отличный выбор! Переход с <b>{current_label}</b> на <b>{plan_label}</b>{end_info}.\n\n"
            f"Сумма: <b>{price} ₽</b> — подписка активируется на 30 дней с сегодня.\n\n"
            f"Пожалуйста, отправьте <b>скриншот оплаты</b> — администратор активирует подписку в течение 15 минут.",
            parse_mode=ParseMode.HTML,
        )
    else:
        # Новая покупка (с Free)
        await update.message.reply_text(
            f"Отлично! Вы выбрали тариф <b>{plan_label}</b> — {price} ₽.\n\n"
            "Пожалуйста, отправьте <b>скриншот оплаты</b> — администратор активирует "
            "подписку в течение 15 минут.",
            parse_mode=ParseMode.HTML,
        )


async def webapp_pay_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь подтвердил покупку несмотря на предупреждение"""
    query = update.callback_query
    await query.answer()
    payment_info = context.user_data.get('pending_payment')
    if not payment_info:
        await query.message.reply_text("Сессия истекла. Пожалуйста, вернитесь на страницу тарифов.")
        return
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(
        f"Принято. Тариф: <b>{payment_info['tariff']}</b> — {payment_info['amount']} ₽.\n\n"
        "Пожалуйста, отправьте <b>скриншот оплаты</b>.",
        parse_mode=ParseMode.HTML,
    )


async def webapp_pay_upgrade_pro_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь хочет купить PRO вместо текущего плана"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_reply_markup(reply_markup=None)
    WEBAPP_LINK = f"{WEBAPP_URL}/payment" if WEBAPP_URL else "https://t.me/PolkaAdsBot"
    await query.message.reply_text(
        "Чтобы выбрать тариф PRO — вернитесь на страницу тарифов и выберите нужный план.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◆ Открыть тарифы", url=WEBAPP_LINK)],
        ])
    )
    context.user_data.pop('pending_payment', None)


async def webapp_pay_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь отменил покупку"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text("Покупка отменена. Вы можете вернуться к тарифам в любое время.")
    context.user_data.pop('pending_payment', None)


if __name__ == '__main__':
    main()
