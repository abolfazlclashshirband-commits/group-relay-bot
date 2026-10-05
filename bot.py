"""
Group Relay Bot  —  ربات رابط بین PV تو و یک گروه (تک‌کاربره)

- پیام‌های تو در PV ربات  ->  داخل گروه (با هویت ربات، یعنی هویت تو مخفی)
- پیام‌های گروه           ->  داخل PV ربات (با نام فرستنده)
- ریپلای‌ها در هر دو طرف حفظ می‌شود
- ارسال تدریجی (sendMessageDraft) برای نمایش پیام‌های گروه در PV
- ارسال تدریجی (ارسال + ویرایش پیاپی) برای پیام‌های تو در گروه
- ویرایش پیام‌ها در هر دو طرف همگام می‌شود

فقط به httpx نیاز دارد و مستقیم با Bot API حرف می‌زند
(تا به لایبرری‌های عقب‌مانده از API جدید وابسته نباشد).
"""
import asyncio
import json
import logging
import os
import random
import sqlite3
import sys
import time
from pathlib import Path

import httpx

import fal

# ----------------------------------------------------------------- config
BASE_DIR = Path(__file__).parent


def load_env():
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)
# روی Railway حتماً Volume وصل کن؛ مسیرش خودکار در RAILWAY_VOLUME_MOUNT_PATH می‌آید
DATA_DIR = Path(os.environ.get("DATA_DIR") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or BASE_DIR)
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"
DB_FILE = DATA_DIR / "relay.db"
FAL_MAX_LEN = int(os.environ.get("FAL_MAX_LEN", "80"))         # پیام‌های بلندتر فال حساب نمی‌شوند
ENV_GROUP_ID = os.environ.get("GROUP_ID", "").strip()  # اختیاری؛ پشتیبان اگر state پاک شد

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],  # Railway لاگ stdout را نشان می‌دهد
)
# مهم: httpx هر درخواست را با URL کامل (شامل توکن!) در سطح INFO لاگ می‌کند
for noisy in ("httpx", "httpcore"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger("relay")

# ----------------------------------------------------------------- state
DEFAULT_STATE = {
    "group_id": None,      # گروه لینک‌شده
    "stream": True,        # ارسال تدریجی فعال؟
    "mirror": True,        # نمایش پیام‌های گروه در PV فعال؟
    "draft_ok": True,      # اگر sendMessageDraft خطا داد خودکار False می‌شود
    "fal": True,           # فالگیر برای اعضای گروه فعال؟
}


def load_state():
    st = dict(DEFAULT_STATE)
    if STATE_FILE.exists():
        try:
            st.update(json.loads(STATE_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return st


def save_state():
    STATE_FILE.write_text(json.dumps(STATE, ensure_ascii=False, indent=2), encoding="utf-8")


STATE = load_state()
if not STATE["group_id"] and ENV_GROUP_ID.lstrip("-").isdigit():
    STATE["group_id"] = int(ENV_GROUP_ID)

# ----------------------------------------------------------------- db (map pv <-> group)
db = sqlite3.connect(DB_FILE)
db.execute(
    "CREATE TABLE IF NOT EXISTS map ("
    "pv_id INTEGER, grp_id INTEGER, origin TEXT)"  # origin: 'owner' | 'group'
)
db.execute("CREATE TABLE IF NOT EXISTS senders (grp_id INTEGER, user_id INTEGER, name TEXT)")
db.execute("CREATE INDEX IF NOT EXISTS i_sender ON senders(grp_id)")
db.execute("CREATE TABLE IF NOT EXISTS whispers (pv_id INTEGER, receiver INTEGER, eph_id INTEGER)")
db.execute("CREATE TABLE IF NOT EXISTS fal_msgs (grp_id INTEGER)")
db.execute("CREATE INDEX IF NOT EXISTS i_fal ON fal_msgs(grp_id)")
db.execute("CREATE INDEX IF NOT EXISTS i_pv ON map(pv_id)")
db.execute("CREATE INDEX IF NOT EXISTS i_grp ON map(grp_id)")
db.commit()


def map_add(pv_id, grp_id, origin):
    db.execute("INSERT INTO map VALUES (?,?,?)", (pv_id, grp_id, origin))
    db.commit()


def fal_msg_add(grp_id):
    db.execute("INSERT INTO fal_msgs VALUES (?)", (grp_id,))
    db.commit()


def is_fal_msg(grp_id):
    return db.execute("SELECT 1 FROM fal_msgs WHERE grp_id=?", (grp_id,)).fetchone() is not None


def sender_add(grp_id, user_id, name):
    db.execute("INSERT INTO senders VALUES (?,?,?)", (grp_id, user_id, name))
    db.commit()


def sender_of(grp_id):
    r = db.execute("SELECT user_id, name FROM senders WHERE grp_id=? ORDER BY rowid DESC", (grp_id,)).fetchone()
    return r if r else None


def whisper_add(pv_id, receiver, eph_id):
    db.execute("INSERT INTO whispers VALUES (?,?,?)", (pv_id, receiver, eph_id))
    db.commit()


def whisper_get(pv_id):
    return db.execute(
        "SELECT receiver, eph_id FROM whispers WHERE pv_id=? ORDER BY rowid DESC", (pv_id,)
    ).fetchone()


def pv_to_grp(pv_id):
    r = db.execute("SELECT grp_id FROM map WHERE pv_id=? ORDER BY rowid DESC", (pv_id,)).fetchone()
    return r[0] if r else None


def grp_to_pv(grp_id):
    r = db.execute("SELECT pv_id FROM map WHERE grp_id=? ORDER BY rowid DESC", (grp_id,)).fetchone()
    return r[0] if r else None


def owner_msg_to_grp(pv_id):
    r = db.execute(
        "SELECT grp_id FROM map WHERE pv_id=? AND origin='owner' ORDER BY rowid DESC", (pv_id,)
    ).fetchone()
    return r[0] if r else None


def group_msg_to_pv(grp_id):
    r = db.execute(
        "SELECT pv_id FROM map WHERE grp_id=? AND origin='group' ORDER BY rowid DESC", (grp_id,)
    ).fetchone()
    return r[0] if r else None


# ----------------------------------------------------------------- API
class ApiError(Exception):
    def __init__(self, code, desc, params=None):
        super().__init__(f"{code}: {desc}")
        self.code, self.desc, self.params = code, desc, params or {}


_client: httpx.AsyncClient = None
BOT_ID = None
BOT_USERNAME = ""


async def api(method, **params):
    """صدا زدن Bot API؛ خودکار روی 429 صبر و تکرار می‌کند."""
    params = {k: v for k, v in params.items() if v is not None}
    for attempt in range(4):
        r = await _client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=params, timeout=70
        )
        data = r.json()
        if data.get("ok"):
            return data["result"]
        err = ApiError(data.get("error_code"), data.get("description"), data.get("parameters"))
        if err.code == 429:
            wait = int(err.params.get("retry_after", 3)) + 1
            log.warning("429 on %s, sleeping %ss", method, wait)
            await asyncio.sleep(wait)
            continue
        raise err
    raise ApiError(429, "too many retries")


# ----------------------------------------------------------------- helpers
def u16(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def shift_entities(entities, delta):
    out = []
    for e in entities or []:
        e = dict(e)
        e["offset"] += delta
        out.append(e)
    return out


def sender_name(user_or_chat):
    if not user_or_chat:
        return "ناشناس"
    if "title" in user_or_chat:  # sender_chat (کانال/ادمین ناشناس)
        return user_or_chat["title"]
    name = (user_or_chat.get("first_name", "") + " " + user_or_chat.get("last_name", "")).strip()
    return name or "ناشناس"


def sender_label(msg):
    u = msg.get("sender_chat") or msg.get("from")
    name = sender_name(u)
    uname = (u or {}).get("username")
    return name, (f" (@{uname})" if uname else "")


def build_prefix(msg, edited=False, quote=None):
    """
    خروجی: (prefix_text, entities)  —  سرنخ ریپلای + نام فرستنده
    """
    lines = ""
    rt = msg.get("reply_to_message")
    if rt:
        rfrom = rt.get("from") or {}
        if rfrom.get("id") == BOT_ID and is_fal_msg(rt["message_id"]):
            lines += "↩️ پاسخ به ربات فال\n"
        elif rfrom.get("id") == BOT_ID:
            lines += "↩️ پاسخ به شما\n"
        else:
            lines += f"↩️ پاسخ به {sender_name(rt.get('sender_chat') or rfrom)}\n"
        if quote:
            lines += f"« {quote} »\n"
    name, uname = sender_label(msg)
    head = "👤 "
    entities = [{"type": "bold", "offset": u16(lines + head), "length": u16(name)}]
    tail = uname + (" ✏️ ویرایش‌شده" if edited else "") + "\n"
    return lines + head + name + tail, entities


def reply_snippet(msg):
    rt = msg.get("reply_to_message")
    if not rt:
        return ""
    t = rt.get("text") or rt.get("caption") or ""
    return (t[:50] + "…") if len(t) > 50 else t


def media_kind(msg):
    for k in ("photo", "video", "animation", "audio", "document", "voice"):
        if k in msg:
            return "caption"
    for k in ("text",):
        if k in msg:
            return "text"
    return "other"


def is_owner(msg_or_user_id):
    return msg_or_user_id == OWNER_ID


def split_progressive(text, steps):
    """متن را به prefix‌های رو به رشد (کلمه‌به‌کلمه) تقسیم می‌کند."""
    words = text.split(" ")
    if len(words) < 3:
        return [text]
    steps = min(steps, len(words))
    cuts = sorted({max(1, round(len(words) * (i + 1) / steps)) for i in range(steps)})
    return [" ".join(words[:c]) for c in cuts]



# ----------------------------------------------------------------- typing indicator
class Typing:
    """نشانگر «در حال تایپ…» پیوسته (هر ۴ ثانیه تازه می‌شود) تا پایان ارسال تدریجی."""

    def __init__(self, chat_id, action="typing"):
        self.chat_id, self.action, self.task = chat_id, action, None

    async def _loop(self):
        try:
            while True:
                try:
                    await api("sendChatAction", chat_id=self.chat_id, action=self.action)
                except ApiError:
                    pass
                await asyncio.sleep(4)
        except asyncio.CancelledError:
            pass

    async def __aenter__(self):
        self.task = asyncio.create_task(self._loop())
        return self

    async def __aexit__(self, *exc):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


# ----------------------------------------------------------------- albums
ALBUM_WAIT = 1.2  # ثانیه صبر برای جمع‌شدن همهٔ آیتم‌های آلبوم
ALBUMS = {}       # (source, media_group_id) -> list[msg]


def buffer_album(source, msg, flush):
    key = (source, msg["media_group_id"])
    if key not in ALBUMS:
        ALBUMS[key] = []

        async def later():
            await asyncio.sleep(ALBUM_WAIT)
            items = sorted(ALBUMS.pop(key), key=lambda m: m["message_id"])
            try:
                await flush(items)
            except Exception:
                log.exception("album flush failed")

        asyncio.create_task(later())
    ALBUMS[key].append(msg)


def input_media(msg, caption=None, entities=None):
    """تبدیل پیام گروه به InputMedia برای sendMediaGroup؛ None اگر پشتیبانی نشود."""
    if "photo" in msg:
        t, fid = "photo", msg["photo"][-1]["file_id"]
    elif "video" in msg:
        t, fid = "video", msg["video"]["file_id"]
    elif "document" in msg:
        t, fid = "document", msg["document"]["file_id"]
    elif "audio" in msg:
        t, fid = "audio", msg["audio"]["file_id"]
    else:
        return None
    m = {"type": t, "media": fid}
    if caption:
        m["caption"], m["caption_entities"] = caption, entities
    return m


def album_compatible(items):
    """عکس+ویدیو با هم مجازند؛ فایل و صوت فقط با هم‌نوع خودشان."""
    cls = set()
    for m in items:
        if "photo" in m or "video" in m:
            cls.add("visual")
        elif "document" in m:
            cls.add("document")
        elif "audio" in m:
            cls.add("audio")
        else:
            return False
    return len(cls) == 1


# ----------------------------------------------------------------- gradual sending
async def draft_stream_to_owner(prefix, body):
    """
    نمایش تدریجی در PV با sendMessageDraft (Bot API 9.5+ برای همهٔ رباتها).
    اگر خطا داد، غیرفعال می‌شود و پیام معمولی ارسال می‌شود.
    """
    if not (STATE["stream"] and STATE["draft_ok"]) or len(body) < 60:
        return
    draft_id = random.randint(1, 2**31 - 1)
    try:
        for part in split_progressive(body, 7)[:-1]:
            await api("sendMessageDraft", chat_id=OWNER_ID, draft_id=draft_id, text=prefix + part)
            await asyncio.sleep(0.3)
    except ApiError as e:
        log.warning("sendMessageDraft failed (%s) -> disabling drafts", e)
        STATE["draft_ok"] = False
        save_state()


async def send_text_to_group(msg):
    """ارسال متن مالک به گروه؛ در صورت فعال‌بودن، به‌صورت تدریجی (ارسال + ادیت)."""
    gid = STATE["group_id"]
    text, ents = msg["text"], msg.get("entities")
    rp = reply_params_for_group(msg)

    if STATE["stream"] and len(text) >= 80:
        parts = split_progressive(text, 5)
        if len(parts) > 1:
            async with Typing(gid):
                # قبل از اولین ارسال کمی «تایپ» می‌کند تا طبیعی‌تر دیده شود
                await asyncio.sleep(1.2)
                sent = await api("sendMessage", chat_id=gid, text=parts[0], reply_parameters=rp)
                for part in parts[1:-1]:
                    await asyncio.sleep(0.9)
                    try:
                        await api("editMessageText", chat_id=gid, message_id=sent["message_id"], text=part)
                    except ApiError as e:
                        if "not modified" not in str(e):
                            raise
                await asyncio.sleep(0.9)
                await api(
                    "editMessageText", chat_id=gid, message_id=sent["message_id"], text=text, entities=ents
                )
            return sent["message_id"]
    sent = await api("sendMessage", chat_id=gid, text=text, entities=ents, reply_parameters=rp)
    return sent["message_id"]


def reply_params_for_group(msg):
    rt = msg.get("reply_to_message")
    if not rt:
        return None
    target = pv_to_grp(rt["message_id"])
    if target:
        return {"message_id": target, "allow_sending_without_reply": True}
    return None


# ----------------------------------------------------------------- owner (PV) -> group
async def react(chat_id, message_id, emoji):
    try:
        await api(
            "setMessageReaction",
            chat_id=chat_id,
            message_id=message_id,
            reaction=[{"type": "emoji", "emoji": emoji}],
        )
    except ApiError:
        pass


async def owner_album_flush(items):
    first = items[0]
    try:
        res = await api(
            "copyMessages",
            chat_id=STATE["group_id"],
            from_chat_id=OWNER_ID,
            message_ids=[m["message_id"] for m in items],
            reply_parameters=reply_params_for_group(first),
        )
        for m, r in zip(items, res):
            map_add(m["message_id"], r["message_id"], "owner")
        await react(OWNER_ID, first["message_id"], "👌")
    except ApiError as e:
        log.error("owner album failed: %s", e)
        await api("sendMessage", chat_id=OWNER_ID, text=f"❌ ارسال آلبوم ناموفق بود:\n{e.desc}")


async def owner_message(msg):
    if msg.get("media_group_id") and STATE["group_id"]:
        buffer_album("owner", msg, owner_album_flush)
        return
    if not STATE["group_id"]:
        await api(
            "sendMessage",
            chat_id=OWNER_ID,
            text="⚠️ هنوز گروهی لینک نشده. ربات را به گروه اضافه کن (یا داخل گروه /link بزن).",
        )
        return
    try:
        if "text" in msg:
            gid = await send_text_to_group(msg)
        else:
            res = await api(
                "copyMessage",
                chat_id=STATE["group_id"],
                from_chat_id=OWNER_ID,
                message_id=msg["message_id"],
                reply_parameters=reply_params_for_group(msg),
            )
            gid = res["message_id"]
        map_add(msg["message_id"], gid, "owner")
        await react(OWNER_ID, msg["message_id"], "👌")
    except ApiError as e:
        log.error("owner->group failed: %s", e)
        await api(
            "sendMessage",
            chat_id=OWNER_ID,
            text=f"❌ ارسال به گروه ناموفق بود:\n{e.desc}",
            reply_parameters={"message_id": msg["message_id"]},
        )


async def owner_edited(msg):
    gid = owner_msg_to_grp(msg["message_id"])
    if not gid or not STATE["group_id"]:
        return
    try:
        if "text" in msg:
            await api(
                "editMessageText",
                chat_id=STATE["group_id"],
                message_id=gid,
                text=msg["text"],
                entities=msg.get("entities"),
            )
        elif "caption" in msg:
            await api(
                "editMessageCaption",
                chat_id=STATE["group_id"],
                message_id=gid,
                caption=msg["caption"],
                caption_entities=msg.get("caption_entities"),
            )
        await react(OWNER_ID, msg["message_id"], "✍")
    except ApiError as e:
        if "not modified" not in str(e):
            log.error("edit sync failed: %s", e)


# ----------------------------------------------------------------- group -> owner (PV)
async def group_message(msg, edited=False):
    if not STATE["mirror"]:
        return
    frm = msg.get("from") or {}
    if frm.get("id") == BOT_ID:
        return  # پیام‌های خود ربات (کپی پیام‌های مالک) دوباره نمایش داده نمی‌شود

    # --- ادیت
    if edited:
        pv_id = group_msg_to_pv(msg["message_id"])
        if not pv_id:
            return
        prefix, ents = build_prefix(msg, edited=True)
        try:
            if "text" in msg:
                await api(
                    "editMessageText",
                    chat_id=OWNER_ID,
                    message_id=pv_id,
                    text=prefix + msg["text"],
                    entities=ents + shift_entities(msg.get("entities"), u16(prefix)),
                )
            elif "caption" in msg:
                await api(
                    "editMessageCaption",
                    chat_id=OWNER_ID,
                    message_id=pv_id,
                    caption=(prefix + msg["caption"])[:1024],
                    caption_entities=ents + shift_entities(msg.get("caption_entities"), u16(prefix)),
                )
        except ApiError as e:
            if "not modified" not in str(e):
                log.error("group edit mirror failed: %s", e)
        return

    # --- آلبوم: جمع می‌کنیم و یک‌جا می‌فرستیم
    if msg.get("media_group_id"):
        buffer_album("group", msg, group_album_flush)
        return

    # --- پیام جدید
    rt = msg.get("reply_to_message")
    reply_to_pv = grp_to_pv(rt["message_id"]) if rt else None
    rp = {"message_id": reply_to_pv, "allow_sending_without_reply": True} if reply_to_pv else None
    quote = reply_snippet(msg) if (rt and not reply_to_pv) else None
    prefix, ents = build_prefix(msg, quote=quote)
    if frm.get("id"):
        sender_add(msg["message_id"], frm["id"], sender_name(frm))

    try:
        kind = media_kind(msg)
        if kind == "text":
            await draft_stream_to_owner(prefix, msg["text"])
            sent = await api(
                "sendMessage",
                chat_id=OWNER_ID,
                text=prefix + msg["text"],
                entities=ents + shift_entities(msg.get("entities"), u16(prefix)),
                reply_parameters=rp,
            )
            map_add(sent["message_id"], msg["message_id"], "group")
        elif kind == "caption" and len(prefix) + len(msg.get("caption", "")) <= 1024:
            cap = prefix + msg.get("caption", "")
            sent = await api(
                "copyMessage",
                chat_id=OWNER_ID,
                from_chat_id=msg["chat"]["id"],
                message_id=msg["message_id"],
                caption=cap,
                caption_entities=ents + shift_entities(msg.get("caption_entities"), u16(prefix)),
                reply_parameters=rp,
            )
            map_add(sent["message_id"], msg["message_id"], "group")
        else:
            # استیکر / ویدیونوت / کپشن بلند / ... : اول هدر، بعد خود پیام
            head = await api(
                "sendMessage", chat_id=OWNER_ID, text=prefix.rstrip("\n"), entities=ents, reply_parameters=rp
            )
            map_add(head["message_id"], msg["message_id"], "group")
            sent = await api(
                "copyMessage",
                chat_id=OWNER_ID,
                from_chat_id=msg["chat"]["id"],
                message_id=msg["message_id"],
                reply_parameters={"message_id": head["message_id"]},
            )
            map_add(sent["message_id"], msg["message_id"], "group")
    except ApiError as e:
        log.error("group->owner failed: %s", e)


async def group_album_flush(items):
    first = items[0]
    for m in items:
        f = m.get("from") or {}
        if f.get("id"):
            sender_add(m["message_id"], f["id"], sender_name(f))
    rt = first.get("reply_to_message")
    reply_to_pv = grp_to_pv(rt["message_id"]) if rt else None
    rp = {"message_id": reply_to_pv, "allow_sending_without_reply": True} if reply_to_pv else None
    quote = reply_snippet(first) if (rt and not reply_to_pv) else None
    prefix, ents = build_prefix(first, quote=quote)
    cap_src = next((m for m in items if m.get("caption")), None)
    caption = prefix + (cap_src["caption"] if cap_src else "")
    cap_ents = ents + (shift_entities(cap_src.get("caption_entities"), u16(prefix)) if cap_src else [])

    if album_compatible(items) and len(caption) <= 1024:
        media = [input_media(m) for m in items]
        media[0]["caption"], media[0]["caption_entities"] = caption.rstrip("\n"), cap_ents
        sent = await api("sendMediaGroup", chat_id=OWNER_ID, media=media, reply_parameters=rp)
        for m, r in zip(items, sent):
            map_add(r["message_id"], m["message_id"], "group")
    else:
        head = await api(
            "sendMessage", chat_id=OWNER_ID, text=prefix.rstrip("\n"), entities=ents, reply_parameters=rp
        )
        map_add(head["message_id"], first["message_id"], "group")
        sent = await api(
            "copyMessages",
            chat_id=OWNER_ID,
            from_chat_id=first["chat"]["id"],
            message_ids=[m["message_id"] for m in items],
            reply_parameters={"message_id": head["message_id"]},
        )
        for m, r in zip(items, sent):
            map_add(r["message_id"], m["message_id"], "group")


async def group_service(msg):
    """اعلان ساده برای ورود/خروج اعضا."""
    if not STATE["mirror"]:
        return
    note = None
    if msg.get("new_chat_members"):
        names = "، ".join(sender_name(u) for u in msg["new_chat_members"])
        note = f"➕ {names} به گروه پیوست"
    elif msg.get("left_chat_member"):
        note = f"➖ {sender_name(msg['left_chat_member'])} گروه را ترک کرد"
    if note:
        try:
            await api("sendMessage", chat_id=OWNER_ID, text=note, disable_notification=True)
        except ApiError:
            pass


# ----------------------------------------------------------------- commands
HELP = (
    "🤖 ربات رابط گروه\n\n"
    "هر پیامی که اینجا بفرستی داخل گروه ارسال می‌شود (با هویت ربات).\n"
    "پیام‌های گروه هم اینجا نمایش داده می‌شود.\n\n"
    "دستورها:\n"
    "/status — وضعیت\n"
    "/fal on|off — فالگیر گروه (ریپلای روی پیام عضو + /fal [تاروت]: فال برای او)\n"
    "/stream — روشن/خاموش ارسال تدریجی\n"
    "/mirror — روشن/خاموش نمایش پیام‌های گروه (و ری‌اکشن‌ها)\n"
    "/w متن — (ریپلای روی پیام یک عضو) نجوا: فقط همان عضو در گروه می‌بیند\n"
    "/del — روی یک پیام خودت (یا نجوا) ریپلای کن تا از گروه حذف شود\n"
    "/unlink — قطع ارتباط و خروج ربات از گروه\n"
    "/link — (داخل گروه) لینک‌کردن همان گروه"
)


def onoff(v):
    return "روشن ✅" if v else "خاموش ⛔"



async def whisper_command(msg):
    """/w متن  (ریپلای روی پیام یک عضو)  ->  پیام نجوا: فقط همان عضو در گروه می‌بیند."""
    cid = OWNER_ID
    t = msg["text"]
    parts = t.split(None, 1)
    rt = msg.get("reply_to_message")
    if not STATE["group_id"]:
        await api("sendMessage", chat_id=cid, text="⚠️ گروهی لینک نشده.")
        return
    if len(parts) < 2 or not rt:
        await api("sendMessage", chat_id=cid, text="🤫 روی پیام یک عضو ریپلای کن و بنویس:\n/w متن نجوا")
        return
    gid = pv_to_grp(rt["message_id"])
    who = sender_of(gid) if gid else None
    if not who:
        await api("sendMessage", chat_id=cid, text="❌ فرستندهٔ این پیام مشخص نیست. روی پیام یکی از اعضا ریپلای کن.")
        return
    user_id, name = who
    after = parts[1]
    start = t.index(after, len(parts[0]))
    off = u16(t[:start])
    ents = [dict(e, offset=e["offset"] - off) for e in (msg.get("entities") or []) if e["offset"] >= off]
    params = dict(
        chat_id=STATE["group_id"],
        text=after,
        entities=ents or None,
        ephemeral_message_parameters={"receiver_user_id": user_id},
    )
    try:
        try:
            res = await api("sendMessage", reply_parameters={"message_id": gid, "allow_sending_without_reply": True}, **params)
        except ApiError as e:
            if "reply" not in (e.desc or "").lower():
                raise
            res = await api("sendMessage", **params)
        eph = res.get("ephemeral_message_id") if isinstance(res, dict) else None
        if eph:
            whisper_add(msg["message_id"], user_id, eph)
        await react(cid, msg["message_id"], "🙊")
        await api(
            "sendMessage",
            chat_id=cid,
            text=f"🤫 نجوا برای {name} ارسال شد (فقط خودش می‌بیند).\nنکته: اگر آفلاین باشد ممکن است نرسد.",
            reply_parameters={"message_id": msg["message_id"]},
            disable_notification=True,
        )
    except ApiError as e:
        log.error("whisper failed: %s", e)
        await api("sendMessage", chat_id=cid, text=f"❌ نجوا ارسال نشد:\n{e.desc}")


async def owner_command(msg):
    cmd = msg["text"].split()[0].split("@")[0].lower()
    cid = OWNER_ID
    if cmd in ("/start", "/help"):
        await api("sendMessage", chat_id=cid, text=HELP)
    elif cmd == "/w":
        await whisper_command(msg)
    elif cmd == "/fal":
        args = msg["text"].split()[1:]
        sub = args[0].lower() if args else ""
        rt = msg.get("reply_to_message")
        if sub in ("on", "off"):
            STATE["fal"] = sub == "on"
            save_state()
            await api("sendMessage", chat_id=cid, text=f"فالگیر گروه: {onoff(STATE['fal'])}")
        elif not rt or sub == "status":
            await api(
                "sendMessage",
                chat_id=cid,
                text=f"🔮 فالگیر گروه: {onoff(STATE['fal'])}\n"
                "/fal on | /fal off\n"
                "برای گرفتن فال برای یک عضو: روی پیامش ریپلای کن و بنویس /fal یا /fal تاروت",
            )
        else:
            gid = pv_to_grp(rt["message_id"])
            if not gid or not STATE["group_id"]:
                await api("sendMessage", chat_id=cid, text="❌ این پیام در گروه نیست.")
                return
            who = sender_of(gid)
            kind = fal.classify("فال " + " ".join(args)) or "hafez"
            asyncio.create_task(send_fortune(kind, gid, None, who[1] if who else None, gid))
            await react(cid, msg["message_id"], "🔥")
    elif cmd == "/status":
        g = STATE["group_id"]
        title = "—"
        if g:
            try:
                title = (await api("getChat", chat_id=g)).get("title", str(g))
            except ApiError:
                title = str(g)
        await api(
            "sendMessage",
            chat_id=cid,
            text=(
                f"گروه: {title}\n"
                f"ارسال تدریجی: {onoff(STATE['stream'])}\n"
                f"نمایش پیام‌های گروه: {onoff(STATE['mirror'])}\n"
                f"فالگیر گروه: {onoff(STATE['fal'])}\n"
                f"sendMessageDraft: {'فعال' if STATE['draft_ok'] else 'غیرفعال (خطا داد)'}"
            ),
        )
    elif cmd == "/stream":
        STATE["stream"] = not STATE["stream"]
        STATE["draft_ok"] = True
        save_state()
        await api("sendMessage", chat_id=cid, text=f"ارسال تدریجی: {onoff(STATE['stream'])}")
    elif cmd == "/mirror":
        STATE["mirror"] = not STATE["mirror"]
        save_state()
        await api("sendMessage", chat_id=cid, text=f"نمایش پیام‌های گروه: {onoff(STATE['mirror'])}")
    elif cmd == "/unlink":
        old = STATE["group_id"]
        STATE["group_id"] = None
        save_state()
        if old:
            await leave(old)
        await api("sendMessage", chat_id=cid, text="🔌 قطع شد و ربات از گروه خارج شد.")
    elif cmd == "/del":
        rt = msg.get("reply_to_message")
        wh = whisper_get(rt["message_id"]) if rt else None
        if wh:  # حذف پیام نجوا
            try:
                await api(
                    "deleteEphemeralMessage",
                    chat_id=STATE["group_id"],
                    receiver_user_id=wh[0],
                    ephemeral_message_id=wh[1],
                )
                await react(cid, rt["message_id"], "🕊")
            except ApiError as e:
                await api("sendMessage", chat_id=cid, text=f"❌ حذف نشد: {e.desc}")
            return
        gid = owner_msg_to_grp(rt["message_id"]) if rt else None
        if not gid:
            await api("sendMessage", chat_id=cid, text="روی یکی از پیام‌های خودت ریپلای کن و /del بزن.")
            return
        try:
            await api("deleteMessage", chat_id=STATE["group_id"], message_id=gid)
            await react(cid, rt["message_id"], "🕊")
        except ApiError as e:
            await api("sendMessage", chat_id=cid, text=f"❌ حذف نشد: {e.desc}")
    else:
        await api("sendMessage", chat_id=cid, text="دستور ناشناخته. /help")



# ----------------------------------------------------------------- reactions
def fmt_reaction(r):
    t = r.get("type")
    if t == "emoji":
        return r["emoji"]
    if t == "paid":
        return "⭐"
    return "✨"  # custom_emoji


async def reaction_update(ev):
    chat = ev["chat"]

    # ---- ری‌اکشن مالک در PV  ->  ری‌اکشن ربات روی پیام متناظر در گروه
    if chat["type"] == "private":
        if chat["id"] != OWNER_ID or not STATE["group_id"]:
            return
        gid = pv_to_grp(ev["message_id"])
        if not gid:
            return
        emojis = [r["emoji"] for r in ev.get("new_reaction", []) if r.get("type") == "emoji"]
        try:
            await api(
                "setMessageReaction",
                chat_id=STATE["group_id"],
                message_id=gid,
                reaction=[{"type": "emoji", "emoji": emojis[0]}] if emojis else [],
            )
        except ApiError as e:
            log.warning("reaction -> group failed: %s", e)
        return

    # ---- ری‌اکشن اعضای گروه  ->  اعلان در PV
    if chat["id"] != STATE["group_id"] or not STATE["mirror"]:
        return
    who = ev.get("user") or ev.get("actor_chat") or {}
    if who.get("id") == BOT_ID:
        return
    old = [fmt_reaction(r) for r in ev.get("old_reaction", [])]
    new = [fmt_reaction(r) for r in ev.get("new_reaction", [])]
    added = [x for x in new if x not in old]
    removed = [x for x in old if x not in new]
    if not added and not removed:
        return
    name = sender_name(who)
    text = name + " " + "".join(added)
    if removed:
        text += (" " if added else "") + "➖" + "".join(removed)
    pv_id = grp_to_pv(ev["message_id"])
    try:
        await api(
            "sendMessage",
            chat_id=OWNER_ID,
            text=text,
            entities=[{"type": "bold", "offset": 0, "length": u16(name)}],
            reply_parameters={"message_id": pv_id, "allow_sending_without_reply": True} if pv_id else None,
            disable_notification=True,
        )
    except ApiError as e:
        log.error("reaction mirror failed: %s", e)


# ----------------------------------------------------------------- fortune (فالگیر)
async def send_fortune(kind, reply_to, thread_id=None, who=None, notify_pv_for=None):
    """
    فال را در گروه لینک‌شده می‌فرستد: «در حال تایپ…» + چند پیام پشت‌سرهم (مثل نمایش تدریجی).
    reply_to: شناسهٔ پیام گروه که فال روی آن ریپلای می‌شود.
    """
    gid = STATE["group_id"]
    if not gid:
        return
    rp = {"message_id": reply_to, "allow_sending_without_reply": True} if reply_to else None
    try:
        parts = fal.make(kind)
        async with Typing(gid):
            for i, part in enumerate(parts):
                await asyncio.sleep(1.2)
                res = await api(
                    "sendMessage",
                    chat_id=gid,
                    text=part,
                    parse_mode="HTML",
                    reply_parameters=rp,
                    message_thread_id=thread_id,
                )
                fal_msg_add(res["message_id"])
    except ApiError as e:
        log.error("fortune failed: %s", e)
        return
    if STATE["mirror"] and who:
        label = "تاروت" if kind == "tarot" else "حافظ"
        pv_id = grp_to_pv(notify_pv_for) if notify_pv_for else None
        try:
            await api(
                "sendMessage",
                chat_id=OWNER_ID,
                text=f"🔮 فال {label} برای {who} گرفته شد",
                reply_parameters={"message_id": pv_id, "allow_sending_without_reply": True} if pv_id else None,
                disable_notification=True,
            )
        except ApiError:
            pass


async def maybe_fortune(msg):
    """اگر عضوی در گروه «فال» گفت، برایش فال می‌گیرد."""
    if not STATE["fal"] or not fal.READY:
        return
    frm = msg.get("from") or {}
    text = (msg.get("text") or "").strip()
    if not text or frm.get("is_bot") or not frm.get("id"):
        return
    kind = None
    if text.startswith("/"):
        first, _, rest = text.partition(" ")
        cmd, _, target = first.partition("@")
        if target and target.lower() != BOT_USERNAME:
            return  # دستور برای ربات دیگری است
        cmd = cmd.lower()
        if cmd == "/fal":
            kind = fal.classify("فال " + rest)
        elif cmd == "/start":
            try:
                await api("sendMessage", chat_id=msg["chat"]["id"], text=fal.INTRO, parse_mode="HTML")
            except ApiError:
                pass
            return
    elif len(text) <= FAL_MAX_LEN:
        kind = fal.classify(text)
    if not kind:
        return
    thread = msg.get("message_thread_id") if msg.get("is_topic_message") else None
    asyncio.create_task(
        send_fortune(kind, msg["message_id"], thread, sender_name(frm), msg["message_id"])
    )

# ----------------------------------------------------------------- update routing
async def leave(chat_id):
    try:
        await api("leaveChat", chat_id=chat_id)
    except ApiError as e:
        log.warning("leaveChat %s failed: %s", chat_id, e)


async def handle_my_chat_member(upd):
    chat, new, who = upd["chat"], upd["new_chat_member"], upd["from"]
    if chat["type"] == "private":
        return
    if new["status"] in ("member", "administrator"):
        if who["id"] == OWNER_ID and chat["type"] in ("group", "supergroup"):
            old = STATE["group_id"]
            STATE["group_id"] = chat["id"]
            save_state()
            if old and old != chat["id"]:
                await leave(old)  # فقط یک گروه فعال؛ از گروه قبلی خارج می‌شود
            await api(
                "sendMessage",
                chat_id=OWNER_ID,
                text=f"✅ به گروه «{chat.get('title')}» لینک شد."
                + ("\n(از گروه قبلی خارج شد)" if old and old != chat["id"] else "")
                + "\nبرای دیدن همهٔ پیام‌ها و ری‌اکشن‌ها، ربات را ادمین گروه کن.",
            )
        else:
            # هر کس غیر از مالک (یا هر کانال) -> خروج فوری + اطلاع به مالک
            log.warning("unauthorized add by %s to chat %s -> leaving", who.get("id"), chat["id"])
            await leave(chat["id"])
            uname = f" (@{who['username']})" if who.get("username") else ""
            try:
                await api(
                    "sendMessage",
                    chat_id=OWNER_ID,
                    text=f"🚨 {sender_name(who)}{uname} [{who.get('id')}] ربات را به «{chat.get('title')}» اضافه کرد؛ ربات خارج شد.",
                )
            except ApiError:
                pass
    elif new["status"] in ("left", "kicked") and STATE["group_id"] == chat["id"]:
        STATE["group_id"] = None
        save_state()
        await api("sendMessage", chat_id=OWNER_ID, text="⚠️ ربات از گروه حذف شد و لینک قطع شد.")


async def handle_update(upd):
    if "my_chat_member" in upd:
        await handle_my_chat_member(upd["my_chat_member"])
        return
    if "message_reaction" in upd:
        await reaction_update(upd["message_reaction"])
        return

    for key, edited in (("message", False), ("edited_message", True)):
        msg = upd.get(key)
        if not msg:
            continue
        chat = msg["chat"]
        # ---- PV با مالک
        if chat["type"] == "private":
            if chat["id"] != OWNER_ID:
                return  # تک‌کاربره: بقیه نادیده گرفته می‌شوند
            if edited:
                await owner_edited(msg)
            elif msg.get("text", "").startswith("/"):
                await owner_command(msg)
            else:
                await owner_message(msg)
            return
        # ---- گروه
        if chat["type"] in ("group", "supergroup"):
            sender = (msg.get("from") or {}).get("id")
            # تبدیل گروه به سوپرگروه: شناسهٔ چت عوض می‌شود
            if "migrate_to_chat_id" in msg and chat["id"] == STATE["group_id"]:
                STATE["group_id"] = msg["migrate_to_chat_id"]
                save_state()
                return
            if "migrate_from_chat_id" in msg and msg["migrate_from_chat_id"] == STATE["group_id"]:
                STATE["group_id"] = chat["id"]
                save_state()
                return
            if msg.get("text", "").startswith("/link") and (msg.get("from") or {}).get("id") == OWNER_ID:
                STATE["group_id"] = chat["id"]
                save_state()
                await api("sendMessage", chat_id=OWNER_ID, text=f"✅ لینک شد به «{chat.get('title')}»")
                try:
                    await api("deleteMessage", chat_id=chat["id"], message_id=msg["message_id"])
                except ApiError:
                    pass
                return
            if chat["id"] != STATE["group_id"]:
                # گروه لینک‌نشده: اگر کسی غیر از مالک پیام بدهد، ربات خارج می‌شود
                if sender != OWNER_ID:
                    log.warning("message in unlinked chat %s -> leaving", chat["id"])
                    await leave(chat["id"])
                return
            if any(k in msg for k in ("new_chat_members", "left_chat_member")):
                await group_service(msg)
            else:
                if not edited:
                    await maybe_fortune(msg)
                await group_message(msg, edited=edited)
        return


async def main():
    global _client, BOT_ID, BOT_USERNAME
    if not BOT_TOKEN or OWNER_ID <= 0:
        sys.exit("BOT_TOKEN و OWNER_ID (عدد مثبت) را تنظیم کن؛ بدون آنها ربات اجرا نمی‌شود.")
    _client = httpx.AsyncClient()
    me = await api("getMe")
    BOT_ID = me["id"]
    BOT_USERNAME = (me.get("username") or "").lower()
    await api("deleteWebhook")
    log.info("Started as @%s (owner=%s, group=%s)", me.get("username"), OWNER_ID, STATE["group_id"])
    try:
        await api(
            "sendMessage", chat_id=OWNER_ID, text="🟢 ربات آنلاین شد. /help", disable_notification=True
        )
    except ApiError:
        log.warning("Owner has not started the bot yet.")

    offset = None
    allowed = ["message", "edited_message", "my_chat_member", "message_reaction"]
    while True:
        try:
            updates = await api("getUpdates", offset=offset, timeout=50, allowed_updates=allowed)
        except (httpx.HTTPError, ApiError) as e:
            log.warning("getUpdates error: %s", e)
            await asyncio.sleep(3)
            continue
        for upd in updates:
            offset = upd["update_id"] + 1
            try:
                await handle_update(upd)
            except Exception:
                log.exception("update handling failed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
