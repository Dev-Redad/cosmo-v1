# bot.py — Mongo-backed (PTB 13.15) + Channel-selling add-on
# - Media selling unchanged
# - Channel selling now sends a **request-to-join** link (creates_join_request=True)
# - Auto-approves join requests only for users who paid (c_orders)
# - PhonePe Business parsing; unique amount locks; configurable unpaid-QR cleanup
# - Safety patch: delivery messages wrapped to avoid crashes if user hasn’t opened DM

import os, logging, time, random, re, unicodedata
from datetime import datetime, timedelta
from urllib.parse import quote

from telegram import Update, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, Filters, CallbackContext,
    ConversationHandler, CallbackQueryHandler, ChatJoinRequestHandler
)
from telegram.error import BadRequest, Unauthorized

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

logging.basicConfig(format="%(asctime)s %(levelname)s:%(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("upi-mongo-bot")

TOKEN = "8384100649:AAGz4Hof9roaVzFMEe5eoftuylIfXPsgb6Y"
ADMIN_IDS = [7861718777, 6053105336, 7381642564]

OWNER_ID = 8054729538  # Owner can add/remove admins
DEFAULT_ADMIN_IDS = ADMIN_IDS[:]  # seed for first run

STORAGE_CHANNEL_ID = -1002724249292
PAYMENT_NOTIF_CHANNEL_ID = -1002865174188

UPI_ID = "q57609025@ybl"
UPI_PAYEE_NAME = "Seller"

PAY_WINDOW_MINUTES = 5
GRACE_SECONDS = 10
DELETE_AFTER_MINUTES = 10

PROTECT_CONTENT_ENABLED = False
FORCE_SUBSCRIBE_ENABLED = True
FORCE_SUBSCRIBE_CHANNEL_IDS = []

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://Hui:Hui@cluster0.3lpdrgm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
mdb = MongoClient(MONGO_URI)["upi_bot"]

c_users    = mdb["users"]
c_products = mdb["products"]
c_config   = mdb["config"]
c_sessions = mdb["sessions"]
c_locks    = mdb["locks"]
c_paylog   = mdb["payments"]
c_orders   = mdb["orders"]
c_sales    = mdb["sales"]

c_users.create_index([("user_id", ASCENDING)], unique=True)
c_products.create_index([("item_id", ASCENDING)], unique=True)
c_config.create_index([("key", ASCENDING)], unique=True)
c_sessions.create_index([("key", ASCENDING)], unique=True)
c_sessions.create_index([("amount_key", ASCENDING)])
c_sessions.create_index([("hard_expire_at", ASCENDING)], expireAfterSeconds=0)
c_paylog.create_index([("ts", ASCENDING)])
c_orders.create_index([("user_id", ASCENDING), ("channel_id", ASCENDING)], unique=True)
c_sales.create_index([("admin_id", ASCENDING), ("ts", ASCENDING)])

def cfg(key, default=None):
    doc = c_config.find_one({"key": key})
    return doc["value"] if doc and "value" in doc else default

def set_cfg(key, value):
    c_config.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)

def get_admin_ids():
    ids = cfg("admin_ids", None)
    if ids is None:
        set_cfg("admin_ids", DEFAULT_ADMIN_IDS)
        ids = DEFAULT_ADMIN_IDS
    out = set(int(x) for x in (ids or []))
    out.add(int(OWNER_ID))
    return sorted(out)

def set_admin_ids(ids):
    set_cfg("admin_ids", [int(x) for x in ids])

def is_owner(uid: int) -> bool:
    return int(uid) == int(OWNER_ID)

def is_admin(uid: int) -> bool:
    return int(uid) in get_admin_ids()

def fmt_amt(x: float) -> str:
    return f"{int(x)}" if abs(x - int(x)) < 1e-9 else f"{x:.2f}"

def ist_today_bounds_utc():
    from datetime import datetime, timedelta
    ist_offset = timedelta(hours=5, minutes=30)
    now_utc = datetime.utcnow()
    now_ist = now_utc + ist_offset
    start_ist = datetime(now_ist.year, now_ist.month, now_ist.day)
    start_utc = start_ist - ist_offset
    end_utc = start_utc + timedelta(days=1)
    return start_utc, end_utc

def parse_start_payload(payload: str):
    try:
        if "__admin_" in payload:
            item_id, aid = payload.split("__admin_", 1)
            return item_id, int(aid)
    except Exception:
        pass
    return payload, None

def amount_key(x: float) -> str:
    return f"{x:.2f}" if abs(x - int(x)) > 1e-9 else str(int(x))

def build_upi_uri(amount: float, note: str):
    amt = f"{int(amount)}" if abs(amount-int(amount))<1e-9 else f"{amount:.2f}"
    pa = quote(UPI_ID, safe=''); pn = quote(UPI_PAYEE_NAME, safe=''); tn = quote(note, safe='')
    return f"upi://pay?pa={pa}&pn={pn}&am={amt}&cu=INR&tn={tn}"

def qr_url(data: str):
    return f"https://api.qrserver.com/v1/create-qr-code/?data={quote(data, safe='')}&size=512x512&qzone=2"

def add_user(uid, uname): c_users.update_one({"user_id": uid},{"$set":{"username":uname or ""}},upsert=True)
def get_all_user_ids(): return list(c_users.distinct("user_id"))

def reserve_amount_key(k: str, hard_expire_at: datetime) -> bool:
    try:
        c_locks.insert_one({"amount_key": k,"hard_expire_at": hard_expire_at,"created_at": datetime.utcnow()})
        return True
    except DuplicateKeyError:
        return False
def release_amount_key(k: str): c_locks.delete_one({"amount_key": k})

def pick_unique_amount(lo: float, hi: float, hard_expire_at: datetime) -> float:
    lo, hi = int(lo), int(hi); ints = list(range(lo, hi+1)); random.shuffle(ints)
    for v in ints:
        if reserve_amount_key(str(v), hard_expire_at): return float(v)
    for base in ints:
        for p in range(1,100):
            key = f"{base}.{p:02d}"
            if reserve_amount_key(key, hard_expire_at): return float(f"{base}.{p:02d}")
    return float(ints[-1])

def _normalize_digits(s: str) -> str:
    out=[]
    for ch in s:
        if unicodedata.category(ch).startswith('M'):
            continue
        if ch.isdigit():
            try: out.append(str(unicodedata.digit(ch))); continue
            except Exception: pass
        out.append(ch)
    return "".join(out)

PHONEPE_RE = re.compile(
    r"(?:Money received|Received Rs\.?)\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)",
    re.I | re.S
)
def parse_phonepe_amount(text: str):
    norm = _normalize_digits(text or "")
    m = PHONEPE_RE.search(norm)
    if not m: return None
    try: return float(m.group(1).replace(",",""))
    except: return None

def force_subscribe(fn):
    def wrapper(update: Update, context: CallbackContext, *a, **k):
        if (not FORCE_SUBSCRIBE_ENABLED) or (not FORCE_SUBSCRIBE_CHANNEL_IDS) or is_admin(update.effective_user.id):
            return fn(update, context, *a, **k)
        uid = update.effective_user.id
        need=[]
        for ch in FORCE_SUBSCRIBE_CHANNEL_IDS:
            try:
                st = context.bot.get_chat_member(ch, uid).status
                if st not in ("member","administrator","creator"): need.append(ch)
            except: need.append(ch)
        if not need: return fn(update, context, *a, **k)
        context.user_data['pending_command']={'fn':fn,'update':update}
        btns=[]
        for ch in need:
            try:
                chat=context.bot.get_chat(ch)
                link=chat.invite_link or context.bot.export_chat_invite_link(ch)
                btns.append([InlineKeyboardButton(f"Join {chat.title}", url=link)])
            except Exception as e: log.warning(f"Invite link fail {ch}: {e}")
        btns.append([InlineKeyboardButton("✅ I have joined", callback_data="check_join")])
        msg = cfg("force_sub_text","Join required channels to continue.")
        photo = cfg("force_sub_photo_id")
        if photo: update.effective_message.reply_photo(photo=photo, caption=msg, reply_markup=InlineKeyboardMarkup(btns))
        else: update.effective_message.reply_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    return wrapper

def check_join(update: Update, context: CallbackContext):
    data = context.user_data.get('pending_command') or {}
    fn = data.get('fn'); upd=data.get('update')
    if not fn or not upd: return
    uid = upd.effective_user.id
    need=[]
    for ch in FORCE_SUBSCRIBE_CHANNEL_IDS:
        try:
            st = context.bot.get_chat_member(ch, uid).status
            if st not in ("member","administrator","creator"): need.append(ch)
        except: need.append(ch)
    if not need:
        fn(upd, context)
        context.user_data.pop('pending_command', None)
    else:
        upd.effective_message.reply_text("Still missing subscriptions. Please join and press the button again.")

def _auto_delete_messages(context: CallbackContext):
    data = context.job.context
    ids = data.get("message_ids") or []
    for mid in ids:
        try: context.bot.delete_message(chat_id=data["chat_id"], message_id=mid)
        except Exception: pass

def _delete_unpaid_qr(context: CallbackContext):
    data = context.job.context
    if c_sessions.find_one({"key": data["sess_key"]}):
        try: context.bot.delete_message(chat_id=data["chat_id"], message_id=data["qr_message_id"])
        except Exception: pass

def start_purchase(ctx: CallbackContext, chat_id: int, uid: int, item_id: str, ref_admin_id: int = None):
    prod = c_products.find_one({"item_id": item_id})
    if not prod: return ctx.bot.send_message(chat_id, "❌ Item not found.")
    mn, mx = prod.get("min_price"), prod.get("max_price")
    if mn is None or mx is None:
        v=float(prod.get("price",0))
        # --- FREE PATH: single price == 0 ---
        if v == 0:
            # deliver and schedule auto-delete of delivered messages
            deliver_ids = deliver(ctx, uid, item_id, return_ids=True) or []
            if deliver_ids:
                ctx.job_queue.run_once(
                    _auto_delete_messages,
                    timedelta(minutes=DELETE_AFTER_MINUTES),
                    context={"chat_id": chat_id, "message_ids": deliver_ids},
                    name=f"free_del_{uid}_{int(time.time())}"
                )
            # mark order for channels so join-requests auto-approve
            if "channel_id" in prod:
                try:
                    c_orders.update_one(
                        {"user_id": uid, "channel_id": int(prod["channel_id"])},
                        {"$set": {"item_id": item_id, "paid_at": datetime.utcnow(), "status": "free"}},
                        upsert=True
                    )
                except Exception:
                    pass
            return
        if v<=0: return ctx.bot.send_message(chat_id,"❌ Price not set.")

        created = datetime.utcnow()
        hard_expire_at = created + timedelta(minutes=PAY_WINDOW_MINUTES, seconds=GRACE_SECONDS)
        amt = v; akey = amount_key(amt)

        uri = build_upi_uri(amt, f"order_uid_{uid}")
        img = qr_url(uri)
        display_amt = int(amt) if abs(amt-int(amt))<1e-9 else f"{amt:.2f}"
        caption = (
             f"Pay ₹{display_amt} for the item\n\n"
             f"Upi id - `{UPI_ID}`.\n\n"
             "Instructions:\n"
             "• Scan this QR or copy the UPI ID\n"
             f"• Pay exactly ₹{display_amt} within {PAY_WINDOW_MINUTES} minutes\n"
             "Verification is automatic. Delivery right after payment."
        )
        sent = ctx.bot.send_photo(chat_id=chat_id, photo=img, caption=caption, parse_mode=ParseMode.MARKDOWN)

        sess_key = f"{uid}:{item_id}:{int(time.time())}"
        c_sessions.insert_one({
            "key": sess_key,
            "user_id": uid,
            "chat_id": chat_id,
            "item_id": item_id,
            "amount": float(amt),
            "amount_key": akey,
            "created_at": datetime.utcnow(),
            "hard_expire_at": datetime.utcnow() + timedelta(minutes=PAY_WINDOW_MINUTES, seconds=GRACE_SECONDS),
            "qr_message_id": sent.message_id,
            "ref_admin_id": int(ref_admin_id) if ref_admin_id else None,
        })

        qr_timeout_mins = int(cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES))
        ctx.job_queue.run_once(
            _delete_unpaid_qr,
            timedelta(minutes=qr_timeout_mins, seconds=1),
            context={"sess_key": sess_key, "chat_id": chat_id, "qr_message_id": sent.message_id},
            name=f"qr_expire_{uid}_{int(time.time())}"
        )
        return

    # RANGE PRICING path
    created = datetime.utcnow()
    hard_expire_at = created + timedelta(minutes=PAY_WINDOW_MINUTES, seconds=GRACE_SECONDS)
    amt = pick_unique_amount(mn, mx, datetime.utcnow() + timedelta(minutes=PAY_WINDOW_MINUTES)); akey = amount_key(amt)

    uri = build_upi_uri(amt, f"order_uid_{uid}")
    img = qr_url(uri)
    display_amt = int(amt) if abs(amt-int(amt))<1e-9 else f"{amt:.2f}"
    caption = (
         f"Pay ₹{display_amt} for the item\n\n"
         f"Upi id - `{UPI_ID}`.\n\n"
         "Instructions:\n"
         "• Scan this QR or copy the UPI ID\n"
         f"• Pay exactly ₹{display_amt} within {PAY_WINDOW_MINUTES} minutes\n"
         "Verification is automatic. Delivery right after payment."
    )
    sent = ctx.bot.send_photo(chat_id=chat_id, photo=img, caption=caption, parse_mode=ParseMode.MARKDOWN)

    sess_key = f"{uid}:{item_id}:{int(time.time())}"
    c_sessions.insert_one({
        "key": sess_key,
        "user_id": uid,
        "chat_id": chat_id,
        "item_id": item_id,
        "amount": float(amt),
        "amount_key": akey,
        "created_at": datetime.utcnow(),
        "hard_expire_at": datetime.utcnow() + timedelta(minutes=PAY_WINDOW_MINUTES, seconds=GRACE_SECONDS),
        "qr_message_id": sent.message_id,
        "ref_admin_id": int(ref_admin_id) if ref_admin_id else None,
    })

    qr_timeout_mins = int(cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES))
    ctx.job_queue.run_once(
        _delete_unpaid_qr,
        timedelta(minutes=qr_timeout_mins, seconds=1),
        context={"sess_key": sess_key, "chat_id": chat_id, "qr_message_id": sent.message_id},
        name=f"qr_expire_{uid}_{int(time.time())}"
    )

def deliver(ctx: CallbackContext, uid: int, item_id: str, return_ids: bool = False):
    """
    Deliver product:
      - Files: copy messages + warning (warning not deleted)
      - Channel: send request-to-join link
    """
    prod = c_products.find_one({"item_id": item_id}) or {}
    file_msg_ids=[]
    # Files
    files = prod.get("files") or []
    for f in files:
        try:
            m = ctx.bot.copy_message(chat_id=uid, from_chat_id=f["channel_id"], message_id=f["message_id"],
                                     protect_content=PROTECT_CONTENT_ENABLED)
            file_msg_ids.append(m.message_id)
        except Exception as e:
            log.warning(f"Copy failed for file {f}: {e}")

    # Channel product
    if "channel_id" in prod:
        try:
            ch_id = int(prod["channel_id"])
            inv = ctx.bot.create_chat_invite_link(ch_id, name=f"Order_{uid}_{int(time.time())}",
                                                  creates_join_request=True)
            btn = InlineKeyboardButton("Join Channel", url=inv.invite_link)
            ctx.bot.send_message(uid, f"Your access to the channel is ready.\nTap the button to request to join.",
                                 reply_markup=InlineKeyboardMarkup([[btn]]))
        except Exception as e:
            log.error(f"Invite link create/send failed: {e}")

    # Add a warning message (not deleted)
    try:
        ctx.bot.send_message(uid, "⚠️ Files auto-delete here in 10 minutes. Save now.")
    except Exception as e:
        log.warning(f"Warn send failed (uid={uid}): {e}")

    return file_msg_ids if return_ids else None

GET_PRODUCT_FILES, PRICE = range(2)
GET_BROADCAST_FILES, GET_BROADCAST_TEXT = range(2)

CHANNEL_REF_RE = re.compile(r"^(?:https?://t\.me/|@)([A-Za-z0-9_]{4,})$")

@force_subscribe
def start(update: Update, context: CallbackContext):
    update.message.reply_text("Send any product file to add it.\nOr send a channel link/username to sell access.")

def add_product_start(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id): return
    context.user_data['new_files']=[]
    if update.message.effective_attachment:
        try:
            fwd=context.bot.forward_message(STORAGE_CHANNEL_ID, update.message.chat_id, update.message.message_id)
            context.user_data['new_files'].append({"channel_id": fwd.chat_id,"message_id": fwd.message_id})
            update.message.reply_text("✅ First file added. Send more or /done.")
        except Exception as e:
            log.error(f"Store fail on first file: {e}")
            update.message.reply_text("Failed to store first file.")
    else:
        update.message.reply_text("Send product files now. Use /done when finished.")
    return GET_PRODUCT_FILES

def get_product_files(update: Update, context: CallbackContext):
    if not update.message.effective_attachment:
        update.message.reply_text("That wasn’t a file. Send files or /done.")
        return GET_PRODUCT_FILES
    try:
        fwd=context.bot.forward_message(STORAGE_CHANNEL_ID, update.message.chat_id, update.message.message_id)
        context.user_data.setdefault('new_files',[]).append({"channel_id": fwd.chat_id,"message_id": fwd.message_id})
        update.message.reply_text("✅ Added. Send more or /done.")
    except Exception as e:
        log.error(f"Store fail: {e}")
        update.message.reply_text("Failed to store the file.")
    return GET_PRODUCT_FILES

def finish_adding_files(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id): return
    if not context.user_data.get('new_files'):
        update.message.reply_text("No files were added. Send a file first.")
        return ConversationHandler.END
    update.message.reply_text("Send a price like `10` or a range like `10-30`.", parse_mode=ParseMode.MARKDOWN)
    return PRICE

def _resolve_channel(context: CallbackContext, text: str) -> int:
    if text.startswith("@"):
        chat = context.bot.get_chat(text)
        return chat.id
    # t.me link
    username = text.rsplit("/",1)[-1]
    chat = context.bot.get_chat(f"@{username}")
    return chat.id

def add_channel_start(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id): return
    text = (update.message.text or "").strip()
    if not CHANNEL_REF_RE.match(text): return
    try:
        ch_id = _resolve_channel(context, text)
    except (BadRequest, Unauthorized) as e:
        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
        return
    context.user_data['channel_id'] = int(ch_id)
    update.message.reply_text("Send a price like `10` or a range like `10-30`.", parse_mode=ParseMode.MARKDOWN)
    return PRICE

def get_price(update: Update, context: CallbackContext):
    t = update.message.text.strip()
    try:
        if "-" in t:
            a, b = t.split("-", 1)
            a, b = a.strip(), b.strip()
            mn, mx = float(a), float(b)
            # --- ALLOW ZERO PRICE RANGE ---
            assert mx >= mn and mn >= 0
        else:
            v = float(t)
            # --- ALLOW ZERO SINGLE PRICE ---
            assert v >= 0
            mn = mx = v
    except:
        update.message.reply_text("Invalid. Send like 10 or 10-30.")
        return PRICE

    # Channel product?
    ch_id = context.user_data.get('channel_id')
    if ch_id:
        item_id = f"chan_{abs(ch_id)}_{int(time.time())}"
        doc = {"item_id": item_id, "min_price": mn, "max_price": mx, "channel_id": int(ch_id)}
        if mn == mx:
            doc["price"] = mn
        c_products.insert_one(doc)
        link = f"https://t.me/{context.bot.username}?start={item_id}"
        link_mine = f"{link}__admin_{update.effective_user.id}"
        update.message.reply_text(f"✅ Channel product added.\nLink:\n`{link}`\n\nYour trackable link:\n`{link_mine}`", parse_mode=ParseMode.MARKDOWN)
        context.user_data.clear()
        return ConversationHandler.END

    if not context.user_data.get('new_files'):
        update.message.reply_text("No files yet. Send a file or /cancel.")
        return PRICE
    item_id = f"files_{int(time.time())}"
    doc = {"item_id": item_id, "min_price": mn, "max_price": mx, "files": context.user_data['new_files']}
    if mn == mx:
        doc["price"] = mn
    c_products.insert_one(doc)
    link = f"https://t.me/{context.bot.username}?start={item_id}"
    link_mine = f"{link}__admin_{update.effective_user.id}"
    update.message.reply_text(f"✅ Product added.\nLink:\n`{link}`\n\nYour trackable link:\n`{link_mine}`", parse_mode=ParseMode.MARKDOWN)
    context.user_data.clear()
    return ConversationHandler.END

def cancel_conv(update: Update, context: CallbackContext):
    context.user_data.clear()
    update.message.reply_text("Canceled.")
    return ConversationHandler.END

def bc_start(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        return
    context.user_data['b_files'] = []
    context.user_data['b_text'] = None
    update.message.reply_text("Send files for broadcast. /done when finished.")
    return GET_BROADCAST_FILES

def bc_files(update: Update, context: CallbackContext):
    if not update.message.effective_attachment:
        update.message.reply_text("That wasn’t a file. Send files or /done.")
        return GET_BROADCAST_FILES
    try:
        fwd=context.bot.forward_message(STORAGE_CHANNEL_ID, update.message.chat_id, update.message.message_id)
        context.user_data.setdefault('b_files',[]).append({"channel_id": fwd.chat_id,"message_id": fwd.message_id})
        update.message.reply_text("✅ Added. Send more or /done.")
    except Exception as e:
        log.error(f"Store fail: {e}")
        update.message.reply_text("Failed to store the file.")
    return GET_BROADCAST_FILES

def bc_done(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    if not context.user_data.get('b_files'):
        update.message.reply_text("No files. Broadcast canceled.")
        return ConversationHandler.END
    update.message.reply_text("Send the broadcast text (or /cancel to abort).")
    return GET_BROADCAST_TEXT

def bc_text(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    context.user_data['b_text'] = update.message.text or ""
    ids = get_all_user_ids()
    ok=0; fail=0
    for uid in ids:
        try:
            if context.user_data['b_text']:
                context.bot.send_message(uid, context.user_data['b_text'])
            for f in context.user_data.get('b_files', []):
                context.bot.copy_message(chat_id=uid, from_chat_id=f["channel_id"], message_id=f["message_id"],
                                         protect_content=PROTECT_CONTENT_ENABLED)
            ok += 1
        except Exception as e:
            fail += 1
    update.message.reply_text(f"Broadcast sent to {ok} users ({fail} failed).")
    context.user_data.clear()
    return ConversationHandler.END

def qr_timeout_show(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id): return
    mins = cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES)
    update.message.reply_text(f"QR auto-delete if unpaid: {mins} minutes.")

def set_qr_timeout(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id): return
    if not context.args:
        return update.message.reply_text("Usage: /set_qr_timeout <minutes>")
    try:
        mins = int(float(context.args[0]))
        if mins < 1 or mins > 180:
            return update.message.reply_text("Choose 1–180 minutes.")
    except Exception:
        return update.message.reply_text("Send a number (1–180).")
    set_cfg("qr_unpaid_delete_minutes", mins)
    update.message.reply_text(f"Set to {mins} minutes.")

def on_channel_post(update: Update, context: CallbackContext):
    msg = update.channel_post
    if not msg or msg.chat_id != PAYMENT_NOTIF_CHANNEL_ID:
        return
    text = msg.text or msg.caption or ""
    low = text.lower()
    if ("phonepe business" not in low) or (("received rs" not in low and "money received" not in low)):
        return
    amt = parse_phonepe_amount(text)
    if amt is None:
        return

    ts = (msg.date or datetime.utcnow()).replace(tzinfo=None)
    akey = amount_key(amt)
    try:
        c_paylog.insert_one({"key": akey, "ts": ts, "raw": text[:500]})
    except:
        pass

    matches = list(c_sessions.find({"amount_key": akey, "created_at": {"$lte": ts}, "hard_expire_at": {"$gte": ts}}))
    for s in matches:
        qr_mid = s.get("qr_message_id")
        if qr_mid:
            try:
                context.bot.delete_message(chat_id=s["chat_id"], message_id=qr_mid)
            except Exception as e:
                log.debug(f"Delete QR failed: {e}")

        try:
            confirm_msg = context.bot.send_message(s["chat_id"], "✅ Payment received. Delivering your item…")
            confirm_msg_id = confirm_msg.message_id
        except Exception as e:
            log.warning(f"Notify user fail: {e}")
            confirm_msg_id = None

        ids_to_delete = []
        if confirm_msg_id:
            ids_to_delete.append(confirm_msg_id)

        deliver_ids = deliver(context, s["user_id"], s["item_id"], return_ids=True)
        # Record attributed sale if session had a referring admin
        try:
            ref_admin = s.get("ref_admin_id")
            if ref_admin:
                c_sales.insert_one({
                    "admin_id": int(ref_admin),
                    "user_id": s["user_id"],
                    "item_id": s["item_id"],
                    "amount": float(s.get("amount", 0.0)),
                    "ts": ts,
                })
        except Exception as e:
            log.error(f"Sales insert failed: {e}")
        ids_to_delete.extend(deliver_ids or [])

        prod = c_products.find_one({"item_id": s["item_id"]}) or {}
        if "channel_id" in prod:
            try:
                c_orders.update_one(
                    {"user_id": s["user_id"], "channel_id": int(prod["channel_id"])},
                    {"$set": {"item_id": s["item_id"], "paid_at": ts, "status": "paid"}},
                    upsert=True
                )
            except Exception as e:
                log.error(f"Order upsert failed: {e}")

        if ids_to_delete:
            context.job_queue.run_once(
                _auto_delete_messages,
                timedelta(minutes=DELETE_AFTER_MINUTES),
                context={"chat_id": s["chat_id"], "message_ids": ids_to_delete},
                name=f"del_{s['user_id']}_{int(time.time())}"
            )

        c_sessions.delete_one({"_id": s["_id"]})
        release_amount_key(akey)

# ---- Auto-approve join-requests for paid buyers ----
def on_join_request(update: Update, context: CallbackContext):
    req = update.chat_join_request
    if not req:
        return
    uid = req.from_user.id
    ch_id = req.chat.id
    has_access = c_orders.find_one({"user_id": uid, "channel_id": ch_id})
    if has_access:
        try:
            context.bot.approve_chat_join_request(ch_id, uid)
        except Exception as e:
            log.error(f"Approve join failed: {e}")

def stats(update, context):
    if not is_admin(update.effective_user.id): return
    users = c_users.count_documents({})
    sessions = c_sessions.count_documents({})
    update.message.reply_text(f"Users: {users}\nPending sessions: {sessions}")

def protect_on(update, context):
    if not is_admin(update.effective_user.id): return
    global PROTECT_CONTENT_ENABLED
    PROTECT_CONTENT_ENABLED = True
    update.message.reply_text("Content protection ON.")
def protect_off(update, context):
    if not is_admin(update.effective_user.id): return
    global PROTECT_CONTENT_ENABLED
    PROTECT_CONTENT_ENABLED = False
    update.message.reply_text("Content protection OFF.")

def cmd_start(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    add_user(uid, update.effective_user.username)
    msg = update.message or (update.callback_query and update.callback_query.message)
    chat_id = msg.chat_id
    if context.args:
        item_id, ref_admin = parse_start_payload(context.args[0])
        return start_purchase(context, chat_id, uid, item_id, ref_admin)
    photo = cfg("welcome_photo_id")
    text = cfg("welcome_text", "Welcome!")
    (msg.reply_photo(photo=photo, caption=text) if photo else msg.reply_text(text))

def earning(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    start_utc, end_utc = ist_today_bounds_utc()

    agg_today = list(c_sales.aggregate([
        {"$match": {"admin_id": uid, "ts": {"$gte": start_utc, "$lt": end_utc}}},
        {"$group": {"_id": None, "sum": {"$sum": "$amount"}}}
    ]))
    today_sum = float(agg_today[0]["sum"]) if agg_today else 0.0

    agg_total = list(c_sales.aggregate([
        {"$match": {"admin_id": uid}},
        {"$group": {"_id": None, "sum": {"$sum": "$amount"}}}
    ]))
    total_sum = float(agg_total[0]["sum"]) if agg_total else 0.0

    update.message.reply_text(
        f"💰 Your earnings\nToday (IST): ₹{fmt_amt(today_sum)}\nTotal: ₹{fmt_amt(total_sum)}"
    )

def addadmin(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    if not context.args:
        return update.message.reply_text("Usage: /addadmin <user_id>")
    try:
        new_id = int(context.args[0])
    except Exception:
        return update.message.reply_text("Invalid user_id.")
    ids = set(get_admin_ids())
    if new_id in ids:
        return update.message.reply_text(f"{new_id} is already an admin.")
    ids.add(new_id)
    # store without duplicating owner
    ids_no_owner = sorted(x for x in ids if x != OWNER_ID)
    set_admin_ids(ids_no_owner)
    update.message.reply_text(f"✅ Added admin {new_id}.")

def rmadmin(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    if not context.args:
        return update.message.reply_text("Usage: /rmadmin <user_id>")
    try:
        rem_id = int(context.args[0])
    except Exception:
        return update.message.reply_text("Invalid user_id.")
    if rem_id == OWNER_ID:
        return update.message.reply_text("Owner cannot be removed.")
    ids = set(get_admin_ids())
    if rem_id not in ids:
        return update.message.reply_text(f"{rem_id} is not an admin.")
    ids.discard(rem_id)
    ids_no_owner = sorted(x for x in ids if x != OWNER_ID)
    set_admin_ids(ids_no_owner)
    update.message.reply_text(f"✅ Removed admin {rem_id}.")

def admins(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    ids = get_admin_ids()
    update.message.reply_text("Admins:\n" + "\n".join(str(x) for x in ids))

def main():
    set_cfg("welcome_text", cfg("welcome_text", "Welcome!"))
    set_cfg("force_sub_text", cfg("force_sub_text", "Join required channels to continue."))
    if cfg("qr_unpaid_delete_minutes") is None:
        set_cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES)

    os.system(f'curl -s "https://api.telegram.org/bot{TOKEN}/deleteWebhook" >/dev/null')

    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    # Files product flow
    add_conv = ConversationHandler(
        entry_points=[MessageHandler((Filters.document | Filters.video | Filters.photo), add_product_start)],
        states={
            GET_PRODUCT_FILES: [MessageHandler((Filters.document | Filters.video | Filters.photo) & ~Filters.command, get_product_files),
                               CommandHandler('done', finish_adding_files)],
            PRICE: [MessageHandler(Filters.text & ~Filters.command, get_price)]
        },
        fallbacks=[CommandHandler('cancel', cancel_conv)]
    )

    # Channel product flow — trigger only on channel reference
    add_channel_conv = ConversationHandler(
        entry_points=[MessageHandler(Filters.regex(CHANNEL_REF_RE) & ~Filters.command, add_channel_start)],
        states={PRICE: [MessageHandler(Filters.text & ~Filters.command, get_price)]},
        fallbacks=[CommandHandler('cancel', cancel_conv)],
        name="add_channel_conv",
        persistent=False
    )

    dp.add_handler(add_conv, group=0)
    dp.add_handler(add_channel_conv, group=0)

    # Broadcast & misc (self-guarded)
    dp.add_handler(CommandHandler("broadcast", bc_start))
    dp.add_handler(CommandHandler("start", cmd_start))
    dp.add_handler(CommandHandler("stats", stats))
    dp.add_handler(CommandHandler("qr_timeout", qr_timeout_show))
    dp.add_handler(CommandHandler("set_qr_timeout", set_qr_timeout))
    dp.add_handler(CommandHandler("protect_on", protect_on))
    dp.add_handler(CommandHandler("protect_off", protect_off))
    dp.add_handler(CommandHandler("earning", earning))
    dp.add_handler(CommandHandler("addadmin", addadmin))
    dp.add_handler(CommandHandler("rmadmin", rmadmin))
    dp.add_handler(CommandHandler("admins", admins))
    dp.add_handler(CallbackQueryHandler(on_cb, pattern="^(check_join)$"))

    # Payments + join requests
    dp.add_handler(MessageHandler(Filters.update.channel_post & Filters.chat(PAYMENT_NOTIF_CHANNEL_ID) & Filters.text, on_channel_post))
    dp.add_handler(ChatJoinRequestHandler(on_join_request))

    logging.info("Bot running…"); updater.start_polling(); updater.idle()

def on_cb(update: Update, context: CallbackContext):
    if update.callback_query and update.callback_query.data=="check_join":
        check_join(update, context)

if __name__ == "__main__": main()
