"""
ماژول فالگیر (از پروژهٔ «فالگیر زند») — بدون هیچ API خارجی.
دیتابیس: data/hafez.json (۴۹۵ غزل + تعبیر) و data/tarot.json (۷۸ کارت).
خروجی هر فال: لیستی از پیام‌های HTML (هر فال یک پیام) (هرکدام زیر ۴۰۹۶ کاراکتر).
"""
import html
import json
import logging
import random
import re
from pathlib import Path

log = logging.getLogger("fal")
DATA = Path(__file__).parent / "data"


def _load(name):
    try:
        return json.loads((DATA / name).read_text(encoding="utf-8"))
    except Exception as e:
        log.error("cannot load %s: %s", name, e)
        return []


HAFEZ = _load("hafez.json")
TAROT = _load("tarot.json")
READY = bool(HAFEZ) and bool(TAROT)

SEP = "━━━━━━━━━"

INTRO = (
    "👋 ربات <b>فالگیر</b> فعال است.\n"
    "در گروه کافیست بنویسید:\n"
    "• <b>فال</b> → فال حافظ\n"
    "• <b>فال تاروت</b> → فال تاروت"
)

# «فال» فقط به‌صورت کلمهٔ مستقل («فالگوش» اشتباهی فعال نمی‌شود)
FAL_WORD = re.compile(r"(?<![\u0600-\u06FF])فال(?![\u0600-\u06FF])")
TAROT_KW = "تاروت"
HAFEZ_KW = "حافظ"


def normalize(text):
    return text.replace("ي", "ی").replace("ك", "ک").replace("\u0640", "")


def classify(text):
    """'tarot' | 'hafez' | None"""
    text = normalize(text)
    if not FAL_WORD.search(text):
        i = text.find(TAROT_KW)  # شکل چسبیدهٔ «فالتاروت»
        if i >= 3 and text[i - 3:i] == "فال":
            return "tarot"
        return None
    if TAROT_KW in text:
        return "tarot"
    return "hafez"  # «فال» ساده و «فال حافظ» هر دو -> حافظ


def _e(s):
    return html.escape(s.strip(), quote=False)


def hafez_parts():
    g = random.choice(HAFEZ)
    verses = [v.strip() for v in g["poem"].split("\n") if v.strip()]
    body = "\n".join(f"{'🕊' if i % 2 == 0 else '🍃'} {_e(v)}" for i, v in enumerate(verses))
    return [
        f"🪶 ❪ <b>فـال حـافـظ شـیـرازي</b> ❫ 🪶\n{SEP}\n{body}\n{SEP}\n"
        f"📜 ❪ <b>تـعـبـیـر فـال</b> ❫\n\n✨ {_e(g['meaning'])}",
    ]


def tarot_parts():
    card = random.choice(TAROT)
    rev = random.random() < 0.5
    orient = "🔴 <b>معکوس</b>" if rev else "🟢 <b>صاف (مستقیم)</b>"
    word = "معکوس" if rev else "صاف"
    text = card["reversed"] if rev else card["upright"]
    return [
        f"🃏 ❪ <b>فـال تـاروت</b> ❫ 🃏\n{SEP}\n"
        f"{card['emoji']} ❪ <b>{_e(card['name_fa'])}</b> ❫\n"
        f"🃏 {_e(card['name_en'])}\n📍 جایگاه: حال\n{orient}\n{SEP}\n\n"
        f"📝 <b>توضیح کارت:</b>\n{_e(card['desc'])}\n\n"
        f"🔮 <b>تعبیر ({word}):</b>\n{_e(text)}\n\n"
        f"💖 <b>تعبیر عاشقانه:</b>\n{_e(card['love'])}",
    ]


def make(kind):
    return tarot_parts() if kind == "tarot" else hafez_parts()
