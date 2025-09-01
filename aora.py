# bot.py — Mongo-backed (PTB 13.15) + Multi-UPI engine
# - Channel selling uses request-to-join links (creates_join_request=True)
# - Auto-approves join requests only for users who paid (c_orders)
# - Unique amount locks, configurable unpaid-QR cleanup
# - Multi-UPI: ranges, limits (fixed or randomized), least-used selection, MAIN fallback
# - Timezone-aware datetimes (UTC/IST) — no utcnow() warnings
# - Added earlier: Force-UPI with respect flags, UPI names, per-UPI amount totals (today/yesterday/all-time) in /settings
# - Updated now: Parser also matches "Received 800.00 Rupees From …" (amount before currency)

import os, logging, time, random, re, unicodedata
from datetime import datetime, timedelta, timezone
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

# === Your bot token ===
TOKEN = "8352423948:AAEP_WHdxNGziUabzMwO9_YiEp24_d0XYVk"

# Admin user IDs
ADMIN_IDS = [7223414109, 6053105336, 7381642564, 7748361879]

# Channels
STORAGE_CHANNEL_ID = -1002724249292
PAYMENT_NOTIF_CHANNEL_ID = -1002865174188

# Legacy single-UPI fields (kept for initial pool seed)
UPI_ID = "dexar@slc"
UPI_PAYEE_NAME = "Seller"

# Payments & housekeeping
PAY_WINDOW_MINUTES = 5
GRACE_SECONDS = 10
DELETE_AFTER_MINUTES = 10

# Options
PROTECT_CONTENT_ENABLED = False
FORCE_SUBSCRIBE_ENABLED = True
FORCE_SUBSCRIBE_CHANNEL_IDS = []

# Mongo
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
c_upi_state = mdb["upi_state"]

c_users.create_index([("user_id", ASCENDING)], unique=True)
c_products.create_index([("item_id", ASCENDING)], unique=True)
c_config.create_index([("key", ASCENDING)], unique=True)
c_locks.create_index([("amount_key", ASCENDING)], unique=True)
c_locks.create_index([("hard_expire_at", ASCENDING)], expireAfterSeconds=0)
c_sessions.create_index([("key", ASCENDING)], unique=True)
c_sessions.create_index([("amount_key", ASCENDING)])
c_sessions.create_index([("hard_expire_at", ASCENDING)], expireAfterSeconds=0)
c_paylog.create_index([("ts", ASCENDING)])
c_orders.create_index([("user_id", ASCENDING), ("channel_id", ASCENDING)], unique=True)
c_upi_state.create_index([("upi", ASCENDING)], unique=True)

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

def cfg(key, default=None):
    doc = c_config.find_one({"key": key})
    return doc["value"] if doc and "value" in doc else default

def set_cfg(key, value):
    c_config.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)

def amount_key(x: float) -> str:
    return f"{x:.2f}" if abs(x - int(x)) > 1e-9 else str(int(x))

# === IST helpers (tz-aware) ===
def now_ist():
    return datetime.now(IST)

def today_ist_str():
    return datetime.now(IST).strftime("%Y-%m-%d")

# === Multi-UPI config helpers ===
def get_upi_pool():
    return cfg("upi_pool", [])

def set_upi_pool(pool):
    main_seen = False
    for u in pool:
        if u.get("main", False):
            if not main_seen:
                main_seen = True
            else:
                u["main"] = False
    if not main_seen and pool:
        pool[0]["main"] = True
    set_cfg("upi_pool", pool)

def _refresh_state_for_today(upi_entry):
    """
    Ensure c_upi_state[upi] has today's date, count, daily_max, and amount tallies.
    On day rollover (IST):
      - amt_yday <- previous amt_today
      - amt_today <- 0
      - count <- 0
      - daily_max (fixed if max_txn; randomized if rand_min/rand_max)
    Accumulators kept in doc:
      amt_today, amt_yday, amt_all
    """
    upi = upi_entry["upi"]
    today = today_ist_str()
    st = c_upi_state.find_one({"upi": upi})
    need_reset = (not st) or (st.get("date") != today)

    prev_amt_today = (st or {}).get("amt_today", 0.0)
    prev_amt_all   = (st or {}).get("amt_all", 0.0)

    if need_reset:
        rmin = upi_entry.get("rand_min")
        rmax = upi_entry.get("rand_max")
        mx   = upi_entry.get("max_txn")
        if rmin is not None and rmax is not None:
            try:
                rmin_i, rmax_i = int(rmin), int(rmax)
                if rmax_i < rmin_i: rmin_i, rmax_i = rmax_i, rmin_i
                todays_max = random.randint(rmin_i, rmax_i)
            except:
                todays_max = int(mx) if mx is not None else None
        else:
            todays_max = int(mx) if mx is not None else None

        c_upi_state.update_one(
            {"upi": upi},
            {"$set": {
                "date": today,
                "count": 0,
                "daily_max": todays_max,
                "amt_yday": prev_amt_today if st else 0.0,
                "amt_today": 0.0,
                "amt_all": prev_amt_all if st else 0.0
            }},
            upsert=True
        )
        st = c_upi_state.find_one({"upi": upi})
    return st

def _get_main_upi(pool):
    for u in pool:
        if u.get("main"):
            return u
    return pool[0] if pool else None

def _within_amount(upi_entry, amount):
    amin = upi_entry.get("min_amt")
    amax = upi_entry.get("max_amt")
    if amin is not None and amount < amin: return False
    if amax is not None and amount > amax: return False
    return True

def _forced_choice(amount):
    """
    If a force UPI is configured, decide whether to use it based on respect flags.
    Returns upi string or None if not forcing / not eligible under respect rules.
    """
    f = cfg("force_upi")
    if not f or not isinstance(f, dict) or not f.get("upi"):
        return None
    pool = get_upi_pool()
    entry = next((x for x in pool if x.get("upi") == f["upi"]), None)
    if not entry:
        return None

    respect_txn = bool(f.get("respect_txn", False))
    respect_amt = bool(f.get("respect_amount", False))

    # Respect amount limits?
    if respect_amt and not _within_amount(entry, amount):
        return None

    # Respect transaction cap?
    if respect_txn:
        st = _refresh_state_for_today(entry)
        dmax = st.get("daily_max")
        used = int(st.get("count", 0))
        if (dmax is not None) and (used >= dmax):
            return None

    # Otherwise forced regardless of caps/range
    return entry["upi"]

def select_upi_for_amount(amount):
    """
    Choose UPI considering Force mode first, else least-used among range-eligible & under cap.
    If none eligible:
      - if some match range but are capped → MAIN
      - if none match range → MAIN if it matches; otherwise MAIN anyway (safety)
    """
    # 1) Force-UPI path
    forced = _forced_choice(amount)
    if forced:
        return forced

    # 2) Normal selection
    pool = get_upi_pool()
    if not pool:
        return None
    main_entry = _get_main_upi(pool)
    eligible_by_range = []
    eligible_final = []

    for u in pool:
        st = _refresh_state_for_today(u)
        if _within_amount(u, amount):
            eligible_by_range.append((u, st))
            dmax = st.get("daily_max")
            used = int(st.get("count", 0))
            if (dmax is None) or (used < dmax):
                eligible_final.append((u, used))

    if eligible_final:
        min_used = min(u for (_, u) in eligible_final)
        candidates = [u for (u, used) in eligible_final if used == min_used]
        return random.choice(candidates)["upi"]

    if eligible_by_range:
        return (main_entry or eligible_by_range[0][0])["upi"]

    if main_entry and _within_amount(main_entry, amount):
        return main_entry["upi"]

    return (main_entry or pool[0])["upi"]

def _bump_usage(upi):
    pool = get_upi_pool()
    entry = next((x for x in pool if x["upi"] == upi), None)
    if not entry:
        return
    _refresh_state_for_today(entry)
    c_upi_state.update_one({"upi": upi}, {"$inc": {"count": 1}})

def _bump_amount(upi, amt: float):
    pool = get_upi_pool()
    entry = next((x for x in pool if x["upi"] == upi), None)
    if not entry:
        return
    _refresh_state_for_today(entry)
    c_upi_state.update_one({"upi": upi}, {"$inc": {"amt_today": float(amt), "amt_all": float(amt)}})

def build_upi_uri(amount: float, note: str, upi_id: str):
    amt = f"{int(amount)}" if abs(amount-int(amount))<1e-9 else f"{amount:.2f}"
    pa = quote(upi_id, safe=''); pn = quote(UPI_PAYEE_NAME, safe=''); tn = quote(note, safe='')
    return f"upi://pay?pa={pa}&pn={pn}&am={amt}&cu=INR&tn={tn}"

def qr_url(data: str):
    return f"https://api.qrserver.com/v1/create-qr-code/?data={quote(data, safe='')}&size=512x512&qzone=2"

def add_user(uid, uname): c_users.update_one({"user_id": uid},{"$set":{"username":uname or ""}},upsert=True)
def get_all_user_ids(): return list(c_users.distinct("user_id"))

def reserve_amount_key(k: str, hard_expire_at: datetime) -> bool:
    try:
        c_locks.insert_one({"amount_key": k,"hard_expire_at": hard_expire_at,"created_at": datetime.now(UTC)})
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
        # drop combining marks to avoid stylized digits confusing the regex window
        if unicodedata.category(ch).startswith('M'):
            continue
        if ch.isdigit():
            try:
                out.append(str(unicodedata.digit(ch))); continue
            except Exception:
                pass
        out.append(ch)
    return "".join(out)

# === Payment parser patterns ===
# 1) Currency before amount (original styles)
PHONEPE_RE = re.compile(
    r"(?:you['’]ve\s*received\s*(?:rs\.?|rupees|₹)|money\s*received|payment\s*received|upi\s*payment\s*received|credited(?:\s*by)?\s*(?:rs\.?|rupees|₹)?|received\s*(?:rs\.?|rupees|₹)|paid\s*you\s*₹)\s*[.:₹\s]*([0-9][0-9,]*(?:\.[0-9]{1,2})?)",
    re.I | re.S
)
# 2) Amount before currency (BharatPe style: "Received 800.00 Rupees From …")
AMOUNT_BEFORE_CURRENCY_RE = re.compile(
    r"(?:received|credited)\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:rupees|rs\.?|₹)\b",
    re.I | re.S
)
# 3) GPay explicit "paid you ₹500.00" safety (some clients insert extra spaces)
GPAY_PAID_YOU_RE = re.compile(
    r"paid\s*you\s*[₹\s]*([0-9][0-9,]*(?:\.[0-9]{1,2})?)",
    re.I | re.S
)

def parse_phonepe_amount(text: str):
    norm = _normalize_digits(text or "")
    for pat in (PHONEPE_RE, AMOUNT_BEFORE_CURRENCY_RE, GPAY_PAID_YOU_RE):
        m = pat.search(norm)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except:
                pass
    return None

def force_subscribe(fn):
    def wrapper(update: Update, context: CallbackContext, *a, **k):
        if (not FORCE_SUBSCRIBE_ENABLED) or (not FORCE_SUBSCRIBE_CHANNEL_IDS) or (update.effective_user.id in ADMIN_IDS):
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

def check_join_cb(update: Update, context: CallbackContext):
    q=update.callback_query; uid=q.from_user.id; need=[]
    for ch in FORCE_SUBSCRIBE_CHANNEL_IDS:
        try:
            st=context.bot.get_chat_member(ch, uid).status
            if st not in ("member","administrator","creator"): need.append(ch)
        except: need.append(ch)
    if not need:
        try: q.message.delete()
        except: pass
        q.answer("Thank you!", show_alert=True)
        pend = context.user_data.pop('pending_command', None)
        if pend: return pend['fn'](pend['update'], context)
    else: q.answer("Still not joined all.", show_alert=True)

def _auto_delete_messages(context: CallbackContext):
    data = context.job.context
    chat_id = data["chat_id"]
    ids = data["message_ids"]
    for mid in ids:
        try: context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception: pass

def _delete_unpaid_qr(context: CallbackContext):
    data = context.job.context
    if c_sessions.find_one({"key": data["sess_key"]}):
        try: context.bot.delete_message(chat_id=data["chat_id"], message_id=data["qr_message_id"])
        except Exception: pass

def start_purchase(ctx: CallbackContext, chat_id: int, uid: int, item_id: str):
    prod = c_products.find_one({"item_id": item_id})
    if not prod: return ctx.bot.send_message(chat_id, "❌ Item not found.")
    mn, mx = prod.get("min_price"), prod.get("max_price")
    if mn is None or mx is None:
        v=float(prod.get("price",0))
        # FREE PATH: single price == 0
        if v == 0:
            deliver_ids = deliver(ctx, uid, item_id, return_ids=True) or []
            if deliver_ids:
                ctx.job_queue.run_once(
                    _auto_delete_messages,
                    timedelta(minutes=DELETE_AFTER_MINUTES),
                    context={"chat_id": chat_id, "message_ids": deliver_ids},
                    name=f"free_del_{uid}_{int(time.time())}"
                )
            if "channel_id" in prod:
                try:
                    c_orders.update_one(
                        {"user_id": uid, "channel_id": int(prod["channel_id"])},
                        {"$set": {"item_id": item_id, "paid_at": datetime.now(UTC), "status": "free"}},
                        upsert=True
                    )
                except Exception:
                    pass
            return
        if v<=0: return ctx.bot.send_message(chat_id,"❌ Price not set.")
        mn=mx=v
    else:
        # FREE PATH: range 0-0
        try:
            if float(mn) == 0 and float(mx) == 0:
                deliver_ids = deliver(ctx, uid, item_id, return_ids=True) or []
                if deliver_ids:
                    ctx.job_queue.run_once(
                        _auto_delete_messages,
                        timedelta(minutes=DELETE_AFTER_MINUTES),
                        context={"chat_id": chat_id, "message_ids": deliver_ids},
                        name=f"free_del_{uid}_{int(time.time())}"
                    )
                if "channel_id" in prod:
                    try:
                        c_orders.update_one(
                            {"user_id": uid, "channel_id": int(prod["channel_id"])},
                            {"$set": {"item_id": item_id, "paid_at": datetime.now(UTC), "status": "free"}},
                            upsert=True
                        )
                    except Exception:
                        pass
                return
        except Exception:
            pass

    created = datetime.now(UTC)
    hard_expire_at = created + timedelta(minutes=PAY_WINDOW_MINUTES)
    amt = pick_unique_amount(mn, mx, datetime.now(UTC) + timedelta(minutes=PAY_WINDOW_MINUTES))
    akey = amount_key(amt)

    # === Multi-UPI selection ===
    chosen_upi = select_upi_for_amount(float(amt)) or UPI_ID
    uri = build_upi_uri(amt, f"order_uid_{uid}", chosen_upi)
    img = qr_url(uri)
    display_amt = int(amt) if abs(amt-int(amt))<1e-9 else f"{amt:.2f}"
    caption = (
         f"Pay ₹{display_amt} for the item\n\n"
         f"UPI ID — `{chosen_upi}`\n\n"
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
        "upi_id": chosen_upi,
        "created_at": datetime.now(UTC),
        "hard_expire_at": datetime.now(UTC) + timedelta(minutes=PAY_WINDOW_MINUTES, seconds=GRACE_SECONDS),
        "qr_message_id": sent.message_id,
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
      - Channel: create a request-to-join invite link and DM it
    """
    prod = c_products.find_one({"item_id": item_id})
    if not prod:
        try: ctx.bot.send_message(uid, "❌ Item missing.")
        except Exception as e: log.error(f"Notify missing item failed (to {uid}): {e}")
        return [] if return_ids else None

    # Channel product
    if "channel_id" in prod:
        ch_id = prod["channel_id"]
        link = None
        try:
            cil = ctx.bot.create_chat_invite_link(ch_id, creates_join_request=True)
            link = cil.invite_link
        except Exception as e:
            log.warning(f"Create join-request link failed for {ch_id}: {e}")
            try:
                ctx.bot.send_message(uid, "⚠️ Channel link is temporarily unavailable. Please try again in a moment.")
            except Exception as ee:
                log.error(f"Notify link-missing failed (to {uid}): {ee}")
            return [] if return_ids else None

        try:
            m = ctx.bot.send_message(
                uid,
                f"🔗 Request-to-join link:\n{link}\n\nTap *Request*, and I'll auto-approve you for this account.",
                parse_mode=ParseMode.MARKDOWN
            )
            return [m.message_id] if return_ids else None
        except Exception as e:
            log.error(f"Send channel link failed (to {uid}): {e}")
            return [] if return_ids else None

    # Files product
    file_msg_ids = []
    for f in prod.get("files", []):
        try:
            m = ctx.bot.copy_message(
                chat_id=uid,
                from_chat_id=f["channel_id"],
                message_id=f["message_id"],
                protect_content=PROTECT_CONTENT_ENABLED
            )
            file_msg_ids.append(m.message_id)
            time.sleep(0.35)
        except Exception as e:
            log.error(f"Deliver fail (to {uid} from {f.get('channel_id')}): {e}")

    try:
        ctx.bot.send_message(uid, "⚠️ Files auto-delete here in 10 minutes. Save now.")
    except Exception as e:
        log.error(f"Warn send fail (to {uid}): {e}")

    return file_msg_ids if return_ids else None

# ---- Payment sniffer (PhonePe/SBI/GPay/Slice/BharatPe) ----
def on_channel_post(update: Update, context: CallbackContext):
    msg = update.channel_post
    if not msg or msg.chat_id != PAYMENT_NOTIF_CHANNEL_ID:
        return
    text = msg.text or msg.caption or ""
    low = text.lower()

    # Broadened gate: accept common payers + phrases (GPay, Slice, BharatPe, PhonePe) and receipt keywords
    if not any(k in low for k in (
        "phonepe business","phonepe","gpay","google pay","slice","bharatpe",
        "money received","payment received","upi payment received",
        "received rs","received ₹","rupees","paid you ₹","credited"
    )):
        return

    amt = parse_phonepe_amount(text)
    if amt is None:
        return

    ts = (msg.date or datetime.now(UTC))
    ts = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    ts = ts.astimezone(UTC)

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

        used_upi = s.get("upi_id")
        if used_upi:
            try:
                _bump_usage(used_upi)
                _bump_amount(used_upi, s.get("amount", 0.0))
            except Exception as e:
                log.warning(f"UPI usage/amount bump failed for {used_upi}: {e}")

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
    users = c_users.count_documents({})
    sessions = c_sessions.count_documents({})
    update.message.reply_text(f"Users: {users}\nPending sessions: {sessions}")

def protect_on(update, context):
    global PROTECT_CONTENT_ENABLED
    PROTECT_CONTENT_ENABLED = True
    update.message.reply_text("Content protection ON.")
def protect_off(update, context):
    global PROTECT_CONTENT_ENABLED
    PROTECT_CONTENT_ENABLED = False
    update.message.reply_text("Content protection OFF.")

# ---- Admin: QR timeout config ----
def qr_timeout_show(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
    mins = cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES)
    update.message.reply_text(f"QR auto-delete if unpaid: {mins} minutes.")

def set_qr_timeout(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
    if not context.args:
        return update.message.reply_text("Usage: /set_qr_timeout <minutes>")
    try:
        mins = int(float(context.args[0]))
        if mins < 1 or mins > 180:
            return update.message.reply_text("Choose 1–180 minutes.")
    except Exception:
        return update.message.reply_text("Invalid number. Example: /set_qr_timeout 5")
    set_cfg("qr_unpaid_delete_minutes", mins)
    update.message.reply_text(f"QR auto-delete timeout set to {mins} minutes.")

# ---- Product add (files) ----
GET_PRODUCT_FILES, PRICE, GET_BROADCAST_FILES, GET_BROADCAST_TEXT, BROADCAST_CONFIRM = range(5)

def add_product_start(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
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
        update.message.reply_text("Not a file. Send again or /done.")
        return GET_PRODUCT_FILES
    try:
        fwd=context.bot.forward_message(STORAGE_CHANNEL_ID, update.message.chat_id, update.message.message_id)
        context.user_data['new_files'].append({"channel_id": fwd.chat_id,"message_id": fwd.message_id})
        update.message.reply_text("✅ Added. Send more or /done.")
        return GET_PRODUCT_FILES
    except Exception as e:
        log.error(str(e))
        update.message.reply_text("Store failed.")
        return ConversationHandler.END

def finish_adding_files(update: Update, context: CallbackContext):
    if not context.user_data.get('new_files'):
        update.message.reply_text("No files yet. Send one or /cancel.")
        return GET_PRODUCT_FILES
    update.message.reply_text("Now send price or range (10 or 10-30).")
    return PRICE

# ---- Product add (channel) ----
CHANNEL_REF_RE = re.compile(r"^\s*(?:-100\d{5,}|@[\w\d_]{5,}|https?://t\.me/[\w\d_+/]+)\s*$")

def _get_bot_id(context: CallbackContext) -> int:
    bid = context.bot_data.get("__bot_id__")
    if bid: return bid
    me = context.bot.get_me()
    context.bot_data["__bot_id__"] = me.id
    return me.id

def _resolve_channel(context: CallbackContext, ref: str):
    ref = ref.strip()
    if ref.startswith("-100") and ref[4:].isdigit():
        chat = context.bot.get_chat(int(ref))
    else:
        key = re.search(r"t\.me/([^/?\s]+)", ref).group(1) if ref.startswith("http") else ref
        chat = context.bot.get_chat(key)
    return chat.id

def _bot_is_admin(context: CallbackContext, chat_id: int) -> bool:
    try:
        bot_id = _get_bot_id(context)
        st = context.bot.get_chat_member(chat_id, bot_id).status
        return st in ("administrator","creator")
    except Exception as e:
        log.info(f"Admin check failed for {chat_id}: {e}")
        return False

def add_channel_start(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
    text = (update.message.text or "").strip()
    if not CHANNEL_REF_RE.match(text): return
    try:
        ch_id = _resolve_channel(context, text)
    except (BadRequest, Unauthorized) as e:
        update.message.reply_text(f"❌ I couldn't access that channel: {e}")
        return
    if not _bot_is_admin(context, ch_id):
        update.message.reply_text("❌ I'm not an admin there. Add me and try again.")
        return
    context.user_data.clear()
    context.user_data["channel_id"] = ch_id
    update.message.reply_text("Channel recognized. Now send price or range (10 or 10-30).")
    return PRICE

def get_price(update: Update, context: CallbackContext):
    t = update.message.text.strip()
    try:
        if "-" in t:
            a, b = t.split("-", 1)
            mn, mx = float(a), float(b)
            assert mx >= mn and mn >= 0
        else:
            v = float(t); assert v >= 0
            mn = mx = v
    except:
        update.message.reply_text("Invalid. Send like 10 or 10-30.")
        return PRICE

    item_id = f"item_{int(time.time())}"
    if "channel_id" in context.user_data:
        doc = {"item_id": item_id, "min_price": mn, "max_price": mx, "channel_id": int(context.user_data["channel_id"])}
        if mn == mx: doc["price"] = mn
        c_products.insert_one(doc)
        link = f"https://t.me/{context.bot.username}?start={item_id}"
        update.message.reply_text(f"✅ Channel product added.\nLink:\n`{link}`", parse_mode=ParseMode.MARKDOWN)
        context.user_data.clear()
        return ConversationHandler.END

    if not context.user_data.get('new_files'):
        update.message.reply_text("No files yet. Send a file or /cancel.")
        return PRICE
    doc = {"item_id": item_id, "min_price": mn, "max_price": mx, "files": context.user_data['new_files']}
    if mn == mx: doc["price"] = mn
    c_products.insert_one(doc)
    link = f"https://t.me/{context.bot.username}?start={item_id}"
    update.message.reply_text(f"✅ Product added.\nLink:\n`{link}`", parse_mode=ParseMode.MARKDOWN)
    context.user_data.clear()
    return ConversationHandler.END

def cancel_conv(update: Update, context: CallbackContext):
    context.user_data.clear()
    update.message.reply_text("Canceled.")
    return ConversationHandler.END

# ---- Broadcast (optional) ----
def bc_start(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
    context.user_data['b_files'] = []; context.user_data['b_text'] = None
    update.message.reply_text("Send files for broadcast. /done when finished.")
    return GET_BROADCAST_FILES

def bc_files(update, context):
    if update.message.effective_attachment:
        context.user_data['b_files'].append(update.message)
        update.message.reply_text("File added. /done when finished.")
    else:
        update.message.reply_text("Send a file or /done.")
    return GET_BROADCAST_FILES

def bc_done_files(update, context):
    update.message.reply_text("Now send the text (or /skip).")
    return GET_BROADCAST_TEXT

def bc_text(update, context):
    context.user_data['b_text'] = update.message.text
    return bc_confirm(update, context)

def bc_skip(update, context):
    return bc_confirm(update, context)

def bc_confirm(update, context):
    total = c_users.count_documents({})
    buttons = [[InlineKeyboardButton("✅ Send", callback_data="send_bc")], [InlineKeyboardButton("❌ Cancel", callback_data="cancel_bc")]]
    update.message.reply_text(f"Broadcast to {total} users. Proceed?", reply_markup=InlineKeyboardMarkup(buttons))
    return BROADCAST_CONFIRM

def bc_send(update, context):
    q = update.callback_query; q.answer(); q.edit_message_text("Broadcasting…")
    files = context.user_data.get('b_files', []); text = context.user_data.get('b_text')
    ok = fail = 0
    for uid in get_all_user_ids():
        try:
            for m in files:
                context.bot.copy_message(uid, m.chat_id, m.message_id)
                time.sleep(0.1)
            if text: context.bot.send_message(uid, text)
            ok += 1
        except Exception as e:
            log.error(e); fail += 1
    q.message.reply_text(f"Done. Sent:{ok} Fail:{fail}")
    context.user_data.clear()
    return ConversationHandler.END

def on_cb(update: Update, context: CallbackContext):
    q = update.callback_query; q.answer()
    if q.data == "check_join":
        return check_join_cb(update, context)

# === Multi-UPI conversation states ===
(UPI_ADD_UPI, UPI_ADD_MIN, UPI_ADD_MAX, UPI_ADD_LIMIT, UPI_ADD_MAIN,
 UPI_EDIT_NAME, UPI_EDIT_MIN, UPI_EDIT_MAX, UPI_EDIT_LIMIT) = range(100, 109)

# === Force-UPI states are handled via callback-data only (no text states) ===

# === /settings UI ===
def _force_status_text():
    f = cfg("force_upi")
    if f and isinstance(f, dict) and f.get("upi"):
        rt = "yes" if f.get("respect_txn") else "no"
        ra = "yes" if f.get("respect_amount") else "no"
        # include optional name and when it was set (IST)
        nm = None
        for u in get_upi_pool():
            if u.get("upi") == f["upi"]:
                nm = u.get("name")
                break
        label = f"`{f['upi']}`" + (f" ({nm})" if nm else "")
        when = ""
        set_at = f.get("set_at")
        if set_at:
            try:
                dt = datetime.fromisoformat(set_at)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=UTC)
                when = " • set: " + dt.astimezone(IST).strftime("%Y-%m-%d %I:%M %p IST")
            except Exception:
                pass
        return f"*Forced UPI:* {label}  •  respect max-txns: *{rt}*  •  respect amount: *{ra}*{when}"
    return "*Forced UPI:* none"

def _render_settings_text():
    pool = get_upi_pool()
    if not pool:
        return "No UPI IDs configured yet. Tap ➕ Add UPI."

    lines = ["*Current UPI Configuration* (resets daily at 12:00 AM IST)\n"]
    # Top-level forced status (with time)
    lines.append(_force_status_text())
    lines.append("")  # spacer

    f = cfg("force_upi")  # cache for per-row markers
    forced_upi = f.get("upi") if isinstance(f, dict) else None
    fr_txn = "yes" if (isinstance(f, dict) and f.get("respect_txn")) else "no"
    fr_amt = "yes" if (isinstance(f, dict) and f.get("respect_amount")) else "no"

    for i, u in enumerate(pool, 1):
        st = _refresh_state_for_today(u)
        used = st.get("count", 0)
        dmax = st.get("daily_max")
        rng  = f"{u.get('min_amt', 'none')} – {u.get('max_amt', 'none')}"
        lim_label = "none"
        if u.get("rand_min") is not None and u.get("rand_max") is not None:
            lim_label = f"{int(u['rand_min'])}-{int(u['rand_max'])} (today: {dmax if dmax is not None else '∞'})"
        elif u.get("max_txn") is not None:
            lim_label = f"{int(u['max_txn'])}"
        nm = u.get("name") or "—"
        amt_today = st.get("amt_today", 0.0)
        amt_yday  = st.get("amt_yday", 0.0)
        amt_all   = st.get("amt_all", 0.0)

        is_forced = (forced_upi == u['upi'])
        header = f"{i}. `{u['upi']}` {'(MAIN)' if u.get('main') else ''}{' (FORCED)' if is_forced else ''}\n"
        forced_line = (f"\n   • FORCED NOW — respect max-txns: {fr_txn}; respect amount: {fr_amt}") if is_forced else ""

        lines.append(
            header +
            f"   • name: {nm}\n"
            f"   • amount range: {rng}\n"
            f"   • daily limit: {lim_label} | used today: {used}/{dmax if dmax is not None else '∞'}\n"
            f"   • collected: today ₹{amt_today:.2f} | yesterday ₹{amt_yday:.2f} | all-time ₹{amt_all:.2f}"
            + forced_line
        )
    return "\n".join(lines)

def _settings_keyboard():
    pool = get_upi_pool()
    rows = [
        [InlineKeyboardButton("➕ Add UPI", callback_data="upi:add")],
        [InlineKeyboardButton("⚡ Force UPI", callback_data="upi:force"),
         InlineKeyboardButton("🧹 Clear Force", callback_data="upi:force_clear")],
        [InlineKeyboardButton("🔄 Reset Today Counts", callback_data="upi:reset")]
    ]
    for idx, u in enumerate(pool):
        rows.append([
            InlineKeyboardButton("⭐ Main",  callback_data=f"upi:main:{idx}"),
            InlineKeyboardButton("✏️ Edit",  callback_data=f"upi:edit:{idx}"),
            InlineKeyboardButton("🗑️ Delete",callback_data=f"upi:del:{idx}")
        ])
    return InlineKeyboardMarkup(rows)

def settings_cmd(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
    update.message.reply_text(_render_settings_text(), parse_mode=ParseMode.MARKDOWN, reply_markup=_settings_keyboard())

def _settings_refresh(chat_id, context):
    try:
        context.bot.send_message(chat_id, _render_settings_text(), parse_mode=ParseMode.MARKDOWN, reply_markup=_settings_keyboard())
    except Exception as e:
        log.error(f"settings refresh failed: {e}")

def upi_cb(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
    q = update.callback_query; q.answer()
    data = q.data or ""
    pool = get_upi_pool()

    if data == "upi:add":
        # handled by conversation entry; ignore here
        return ConversationHandler.END

    if data == "upi:reset":
        for u in pool:
            _refresh_state_for_today(u)
            c_upi_state.update_one({"upi": u["upi"]}, {"$set": {"count": 0, "amt_today": 0.0}})
        q.edit_message_text("Counts & today's amounts reset (IST).")
        _settings_refresh(q.message.chat_id, context)
        return ConversationHandler.END

    if data == "upi:force":
        # present list of UPIs to choose
        btns = []
        for i,u in enumerate(pool):
            nm = u.get("name") or ""
            label = f"{u['upi']} {f'({nm})' if nm else ''}"
            btns.append([InlineKeyboardButton(label, callback_data=f"upi:forcepick:{i}")])
        q.message.reply_text("Choose a UPI to force:", reply_markup=InlineKeyboardMarkup(btns))
        return ConversationHandler.END

    if data == "upi:force_clear":
        set_cfg("force_upi", None)
        q.message.reply_text("Force cleared.")
        _settings_refresh(q.message.chat_id, context)
        return ConversationHandler.END

    if data.startswith("upi:forcepick:"):
        try:
            idx = int(data.split(":")[2])
        except:
            return ConversationHandler.END
        if idx < 0 or idx >= len(pool):
            q.message.reply_text("Invalid selection.")
            return ConversationHandler.END
        context.user_data["__force_idx__"] = idx
        q.message.reply_text(
            "Respect *max number of transactions* for this forced UPI?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes", callback_data="upi:force:txn:1"),
                 InlineKeyboardButton("No",  callback_data="upi:force:txn:0")]
            ])
        )
        return ConversationHandler.END

    if data.startswith("upi:force:txn:"):
        v = data.split(":")[-1]
        context.user_data["__force_txn__"] = (v == "1")
        q.message.reply_text(
            "Respect *amount limits* (min/max) for this forced UPI?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes", callback_data="upi:force:amt:1"),
                 InlineKeyboardButton("No",  callback_data="upi:force:amt:0")]
            ])
        )
        return ConversationHandler.END

    if data.startswith("upi:force:amt:"):
        v = data.split(":")[-1]
        respect_amount = (v == "1")
        idx = context.user_data.get("__force_idx__")
        if idx is None or idx < 0 or idx >= len(pool):
            q.message.reply_text("Force session expired. Try again.")
            return ConversationHandler.END
        respect_txn = bool(context.user_data.get("__force_txn__", False))
        upi = pool[idx]["upi"]
        set_cfg("force_upi", {"upi": upi, "respect_txn": respect_txn, "respect_amount": respect_amount, "set_at": now_ist().isoformat()})
        context.user_data.pop("__force_idx__", None)
        context.user_data.pop("__force_txn__", None)
        q.message.reply_text(f"Forced to `{upi}` (respect max-txns: {'yes' if respect_txn else 'no'}, respect amount: {'yes' if respect_amount else 'no'}).", parse_mode=ParseMode.MARKDOWN)
        _settings_refresh(q.message.chat_id, context)
        return ConversationHandler.END

    parts = data.split(":")
    if len(parts) == 3 and parts[1] in ("main","edit","del"):
        _, action, idx_s = parts
        idx = int(idx_s)
        if idx < 0 or idx >= len(pool):
            q.message.reply_text("Invalid selection.")
            return ConversationHandler.END

        if action == "main":
            for i,u in enumerate(pool):
                u["main"] = (i == idx)
            set_upi_pool(pool)
            q.message.reply_text(f"Set MAIN to `{pool[idx]['upi']}`.", parse_mode=ParseMode.MARKDOWN)
            _settings_refresh(q.message.chat_id, context)
            return ConversationHandler.END

        if action == "del":
            up = pool.pop(idx)
            set_upi_pool(pool)
            q.message.reply_text(f"Removed `{up['upi']}`.", parse_mode=ParseMode.MARKDOWN)
            _settings_refresh(q.message.chat_id, context)
            return ConversationHandler.END

        if action == "edit":
            # handled by edit conversation entry; ignore here
            return ConversationHandler.END

    return ConversationHandler.END

# === Inline-button entry points for conversations ===
def addupi_cb_entry(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS:
        return ConversationHandler.END
    q = update.callback_query
    q.answer()
    context.user_data.clear()
    context.user_data["__mode__"] = "add"
    q.message.reply_text("Send the UPI ID to add (e.g., dexar@slc).")
    return UPI_ADD_UPI

def edit_cb_entry(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS:
        return ConversationHandler.END
    q = update.callback_query
    q.answer()
    parts = (q.data or "").split(":")
    try:
        idx = int(parts[2])
    except Exception:
        q.message.reply_text("Invalid selection.")
        return ConversationHandler.END

    pool = get_upi_pool()
    if idx < 0 or idx >= len(pool):
        q.message.reply_text("Invalid selection.")
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data["__mode__"] = "edit"
    context.user_data["edit_idx"] = idx
    u = pool[idx]
    q.message.reply_text(
        f"Editing `{u['upi']}`.\nSend *display name* or type `skip` to leave unchanged (current: {(u.get('name') or '—')}).",
        parse_mode=ParseMode.MARKDOWN
    )
    return UPI_EDIT_NAME

# --- One-by-one flows (/addupi and Edit) ---
def addupi_cmd(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_IDS: return
    context.user_data.clear()
    context.user_data["__mode__"] = "add"
    update.message.reply_text("Send the UPI ID to add (e.g., dexar@slc).")
    return UPI_ADD_UPI

def upi_add__upi(update: Update, context: CallbackContext):
    upi = (update.message.text or "").strip()
    if not upi or "@" not in upi:
        update.message.reply_text("Send a valid UPI ID (looks like name@bank).")
        return UPI_ADD_UPI
    context.user_data["new_upi"] = upi
    update.message.reply_text("Send *minimum amount* or `none`.", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_MIN

def upi_add__min(update: Update, context: CallbackContext):
    t = (update.message.text or "").strip().lower()
    val = None if t in ("none","-","na","n/a") else t
    try:
        context.user_data["min_amt"] = (None if val is None else float(val))
    except:
        update.message.reply_text("Invalid. Send a number or `none`.")
        return UPI_ADD_MIN
    update.message.reply_text("Send *maximum amount* or `none`.", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_MAX

def upi_add__max(update: Update, context: CallbackContext):
    t = (update.message.text or "").strip().lower()
    val = None if t in ("none","-","na","n/a") else t
    try:
        max_amt = (None if val is None else float(val))
    except:
        update.message.reply_text("Invalid. Send a number or `none`.")
        return UPI_ADD_MAX
    context.user_data["max_amt"] = max_amt
    update.message.reply_text("Send *daily transaction limit*:\n• `none` (no cap)\n• `7` (fixed)\n• `5-10` (random daily pick)", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_LIMIT

def upi_add__limit(update: Update, context: CallbackContext):
    t = (update.message.text or "").strip().lower().replace(" ", "")
    mx = None; rmin = None; rmax = None
    try:
        if t in ("none","-","na","n/a"):
            pass
        elif "-" in t:
            a,b = t.split("-",1)
            rmin = int(float(a)); rmax = int(float(b))
            if rmax < rmin: rmin, rmax = rmax, rmin
        else:
            mx = int(float(t))
            if mx < 0: mx = 0
    except:
        update.message.reply_text("Invalid. Send `none`, a number like `5`, or a range like `5-10`.")
        return UPI_ADD_LIMIT

    context.user_data["max_txn"] = mx
    context.user_data["rand_min"] = rmin
    context.user_data["rand_max"] = rmax
    update.message.reply_text("Make this the *MAIN* UPI? Reply `yes` or `no`.", parse_mode=ParseMode.MARKDOWN)
    return UPI_ADD_MAIN

def upi_add__main(update: Update, context: CallbackContext):
    ans = (update.message.text or "").strip().lower()
    make_main = ans in ("y","yes","true","1")
    # ---- FIX: guard against missing 'new_upi' to avoid KeyError ----
    new_upi = context.user_data.get("new_upi")
    if not new_upi:
        update.message.reply_text("Session expired. Please run /addupi again.")
        return ConversationHandler.END
    pool = get_upi_pool()
    entry = {
        "upi": new_upi,
        "name": None,  # name can be set later via Edit
        "min_amt": context.user_data.get("min_amt"),
        "max_amt": context.user_data.get("max_amt"),
        "max_txn": context.user_data.get("max_txn"),
        "rand_min": context.user_data.get("rand_min"),
        "rand_max": context.user_data.get("rand_max"),
        "main": make_main
    }
    pool.append(entry)
    set_upi_pool(pool)
    context.user_data.clear()
    # (also fixed param name here)
    update.message.reply_text(f"Added `{entry['upi']}`.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# --- Edit flow (Name → Min → Max → Limit) ---
def upi_edit__name(update: Update, context: CallbackContext):
    pool = get_upi_pool()
    idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again.")
        return ConversationHandler.END
    t = (update.message.text or "").strip()
    if t.lower() != "skip":
        pool[idx]["name"] = t if t else None
        set_upi_pool(pool)
    update.message.reply_text(
        f"Send *minimum amount* or `none` (current: {pool[idx].get('min_amt','none')}).",
        parse_mode=ParseMode.MARKDOWN
    )
    return UPI_EDIT_MIN

def upi_edit__min(update: Update, context: CallbackContext):
    pool = get_upi_pool()
    idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again.")
        return ConversationHandler.END
    t = (update.message.text or "").strip().lower()
    try:
        pool[idx]["min_amt"] = None if t in ("none","-","na","n/a") else float(t)
    except:
        update.message.reply_text("Invalid. Send a number or `none`.")
        return UPI_EDIT_MIN
    set_upi_pool(pool)
    update.message.reply_text(f"Send *maximum amount* or `none` (current: {pool[idx].get('max_amt','none')}).", parse_mode=ParseMode.MARKDOWN)
    return UPI_EDIT_MAX

def upi_edit__max(update: Update, context: CallbackContext):
    pool = get_upi_pool()
    idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again.")
        return ConversationHandler.END
    t = (update.message.text or "").strip().lower()
    try:
        pool[idx]["max_amt"] = None if t in ("none","-","na","n/a") else float(t)
    except:
        update.message.reply_text("Invalid. Send a number or `none`.")
        return UPI_EDIT_MAX
    set_upi_pool(pool)
    update.message.reply_text("Send *daily transaction limit*:\n• `none`\n• `7`\n• `5-10`", parse_mode=ParseMode.MARKDOWN)
    return UPI_EDIT_LIMIT

def upi_edit__limit(update: Update, context: CallbackContext):
    pool = get_upi_pool()
    idx = context.user_data.get("edit_idx", -1)
    if idx < 0 or idx >= len(pool):
        update.message.reply_text("Edit session expired. Try /settings again.")
        return ConversationHandler.END

    t = (update.message.text or "").strip().lower().replace(" ", "")
    mx = None; rmin = None; rmax = None
    try:
        if t in ("none","-","na","n/a"):
            pass
        elif "-" in t:
            a,b = t.split("-",1)
            rmin = int(float(a)); rmax = int(float(b))
            if rmax < rmin: rmin, rmax = rmax, rmin
        else:
            mx = int(float(t))
            if mx < 0: mx = 0
    except:
        update.message.reply_text("Invalid. Send `none`, `5`, or `5-10`.")
        return UPI_EDIT_LIMIT

    pool[idx]["max_txn"] = mx
    pool[idx]["rand_min"] = rmin
    pool[idx]["rand_max"] = rmax
    set_upi_pool(pool)
    context.user_data.clear()
    update.message.reply_text("Updated.")
    return ConversationHandler.END

# ---- Start & main wiring ----
def cmd_start(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    add_user(uid, update.effective_user.username)
    msg = update.message or (update.callback_query and update.callback_query.message)
    chat_id = msg.chat_id
    if context.args:
        return start_purchase(context, chat_id, uid, context.args[0])
    photo = cfg("welcome_photo_id")
    text = cfg("welcome_text", "Welcome!")
    (msg.reply_photo(photo=photo, caption=text) if photo else msg.reply_text(text))

def main():
    # default config seeds
    set_cfg("welcome_text", cfg("welcome_text", "Welcome!"))
    set_cfg("force_sub_text", cfg("force_sub_text", "Join required channels to continue."))
    if cfg("qr_unpaid_delete_minutes") is None:
        set_cfg("qr_unpaid_delete_minutes", PAY_WINDOW_MINUTES)
    if cfg("upi_pool") is None:
        set_upi_pool([{"upi": UPI_ID, "name": None, "min_amt": None, "max_amt": None, "max_txn": None, "rand_min": None, "rand_max": None, "main": True}])
    if cfg("force_upi") is None:
        set_cfg("force_upi", None)

    # clear webhook (if any)
    os.system(f'curl -s "https://api.telegram.org/bot{TOKEN}/deleteWebhook" >/dev/null')

    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    admin = Filters.user(ADMIN_IDS)

    # Files product flow
    add_conv = ConversationHandler(
        entry_points=[MessageHandler((Filters.document | Filters.video | Filters.photo) & admin, add_product_start)],
        states={
            GET_PRODUCT_FILES: [MessageHandler((Filters.document | Filters.video | Filters.photo) & ~Filters.command, get_product_files),
                               CommandHandler('done', finish_adding_files, filters=admin)],
            PRICE: [MessageHandler(Filters.text & ~Filters.command, get_price)]
        },
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=admin)]
    )

    # Channel product flow
    add_channel_conv = ConversationHandler(
        entry_points=[MessageHandler(Filters.regex(CHANNEL_REF_RE) & ~Filters.command & admin, add_channel_start)],
        states={PRICE: [MessageHandler(Filters.text & ~Filters.command, get_price)]},
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=admin)],
        name="add_channel_conv",
        persistent=False
    )

    dp.add_handler(add_conv, group=0)
    dp.add_handler(add_channel_conv, group=0)

    # Broadcast & misc
    dp.add_handler(CommandHandler("broadcast", bc_start, filters=admin))
    dp.add_handler(CommandHandler("start", cmd_start))
    dp.add_handler(CommandHandler("stats", stats, filters=admin))
    dp.add_handler(CommandHandler("qr_timeout", qr_timeout_show, filters=admin))
    dp.add_handler(CommandHandler("set_qr_timeout", set_qr_timeout, filters=admin))
    dp.add_handler(CommandHandler("protect_on", protect_on, filters=admin))
    dp.add_handler(CommandHandler("protect_off", protect_off, filters=admin))
    dp.add_handler(CallbackQueryHandler(on_cb, pattern="^(check_join)$"))

    # Payments + join requests
    dp.add_handler(MessageHandler(Filters.update.channel_post & Filters.chat(PAYMENT_NOTIF_CHANNEL_ID) & Filters.text, on_channel_post))
    dp.add_handler(ChatJoinRequestHandler(on_join_request))

    # === Multi-UPI: admin commands & flows ===
    dp.add_handler(CommandHandler("settings", settings_cmd, filters=admin))
    dp.add_handler(CommandHandler("addupi", addupi_cmd, filters=admin))

    # Conversations own add/edit entry points:
    add_upi_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(addupi_cb_entry, pattern=r"^upi:add$"),
            CommandHandler("addupi", addupi_cmd, filters=admin)
        ],
        states={
            UPI_ADD_UPI:   [MessageHandler(Filters.text & ~Filters.command, upi_add__upi)],
            UPI_ADD_MIN:   [MessageHandler(Filters.text & ~Filters.command, upi_add__min)],
            UPI_ADD_MAX:   [MessageHandler(Filters.text & ~Filters.command, upi_add__max)],
            UPI_ADD_LIMIT: [MessageHandler(Filters.text & ~Filters.command, upi_add__limit)],
            UPI_ADD_MAIN:  [MessageHandler(Filters.text & ~Filters.command, upi_add__main)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=admin)],
        name="add_upi_conv",
        persistent=False
    )
    dp.add_handler(add_upi_conv, group=0)

    edit_upi_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_cb_entry, pattern=r"^upi:edit:\d+$")],
        states={
            UPI_EDIT_NAME: [MessageHandler(Filters.text & ~Filters.command, upi_edit__name)],
            UPI_EDIT_MIN:  [MessageHandler(Filters.text & ~Filters.command, upi_edit__min)],
            UPI_EDIT_MAX:  [MessageHandler(Filters.text & ~Filters.command, upi_edit__max)],
            UPI_EDIT_LIMIT:[MessageHandler(Filters.text & ~Filters.command, upi_edit__limit)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conv, filters=admin)],
        name="edit_upi_conv",
        persistent=False
    )
    dp.add_handler(edit_upi_conv, group=0)

    # Generic UPI actions (NOT add/edit) handled here; group=1 so convs run first
    dp.add_handler(CallbackQueryHandler(upi_cb, pattern=r"^upi:(reset|force|force_clear|forcepick:\d+|force:txn:(?:0|1)|force:amt:(?:0|1)|main:\d+|del:\d+)$"), group=1)

    logging.info("Bot running…")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
