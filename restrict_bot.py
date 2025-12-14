# -*- coding: utf-8 -*-
import os
import psutil
import time
import asyncio
import re
import shutil
import subprocess
import gc
import datetime
import uuid
from pathlib import Path
from collections import defaultdict
import motor.motor_asyncio
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated, UserAlreadyParticipant,
    InviteHashExpired, UsernameNotOccupied, FileReferenceExpired, UserNotParticipant,
    ApiIdInvalid, PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    SessionPasswordNeeded, PasswordHashInvalid, PeerIdInvalid, AuthKeyUnregistered, UserDeactivated
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

API_ID = int(os.environ.get("API_ID", "") or 0)
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DB_URI = os.environ.get("DB_URI", "")
DB_NAME = os.environ.get("DB_NAME", "")
STRING_SESSION = os.environ.get("STRING_SESSION", None)

LOGIN_SYSTEM = os.environ.get("LOGIN_SYSTEM", "True").lower() == "true"
ERROR_MESSAGE = os.environ.get("ERROR_MESSAGE", "True").lower() == "true"
WAITING_TIME = int(os.environ.get("WAITING_TIME", 3))

admin_str = os.environ.get("ADMINS", "")
ADMINS = [int(x) for x in admin_str.split(",") if x.strip().isdigit()]

sudo_str = os.environ.get("SUDOS", "486433780")
SUDOS = [int(x) for x in sudo_str.split(",") if x.strip().isdigit()]

HELP_TXT = """**📚 BOT'S HELP MENU**

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬✘▬

**🟢 1. PRIVATE CHATS**

• First, send the **Invite Link** of the chat.
  *(Not needed if you are already a member via the session account)*
• Then, send the **Post Link** you want to download or forward.

**🤖 2. BOT CHATS**

• Send the link with `/b/`, the bot's username, and message ID.
• You usually need an unofficial client (like Plus Messenger or Nekogram) to get these links.
• **Format:** `https://t.me/b/botusername/4321`

**📦 3. BATCH / MULTI-POSTS**

• Send links in the "From - To" format to download or forward multiple files at once.
• Works for both Public and Private links.
• **Examples:**
  ├ `https://t.me/xxxx/1001-1010`
  └ `https://t.me/c/xxxx/101 - 120`

**💡 Note:** Spaces between the numbers do not matter!

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬✘▬"""

# ==============================================================================
# --- DATABASE ---
# ==============================================================================

class Database:
    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.col = self.db.users

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            session = None,
            api_id = None,
            api_hash = None,
        )

    async def add_user(self, id, name):
        user = self.new_user(id, name)
        if not await self.is_user_exist(id):
            await self.col.insert_one(user)

    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        return bool(user)

    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count

    async def get_all_users(self):
        cursor = self.col.find({})
        return cursor

    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})

    async def set_session(self, id, session):
        await self.col.update_one({'id': int(id)}, {'$set': {'session': session}})

    async def get_session(self, id):
        user = await self.col.find_one({'id': int(id)})
        if user:
            return user.get('session')
        return None

    async def set_api_id(self, id, api_id):
        await self.col.update_one({'id': int(id)}, {'$set': {'api_id': api_id}})

    async def get_api_id(self, id):
        user = await self.col.find_one({'id': int(id)})
        return user.get('api_id')

    async def set_api_hash(self, id, api_hash):
        await self.col.update_one({'id': int(id)}, {'$set': {'api_hash': api_hash}})

    async def get_api_hash(self, id):
        user = await self.col.find_one({'id': int(id)})
        return user.get('api_hash')

    async def total_session_users_count(self):
        count = await self.col.count_documents({"session": {"$ne": None}})
        return count

db = Database(DB_URI, DB_NAME)

# ==============================================================================
# --- CLIENT & GLOBAL STATE ---
# ==============================================================================

app = Client(
    "RestrictedBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=50,
    sleep_threshold=5
)

BOT_START_TIME = time.time()

ACTIVE_PROCESSES = defaultdict(dict)  # user_id -> { task_uuid: info_dict, ... }
CANCEL_FLAGS = {}  # task_uuid -> True when cancelled

batch_temp = type("BT", (), {})()
batch_temp.ACTIVE_TASKS = defaultdict(int)
batch_temp.IS_BATCH = defaultdict(bool)

UPLOAD_SEMAPHORE = asyncio.Semaphore(3)
USER_UPLOAD_LOCKS = defaultdict(asyncio.Lock)
PENDING_TASKS = {}
PROGRESS = {}
SESSION_STRING_SIZE = 351

MAX_CONCURRENT_TASKS_PER_USER = int(os.environ.get("MAX_TASKS_PER_USER", "3"))

GlobalUserSession = None
if STRING_SESSION and not LOGIN_SYSTEM:
    try:
        GlobalUserSession = Client("GlobalUser", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)
        GlobalUserSession.start()
    except Exception as e:
        print(f"Failed to start Global User Session: {e}")

# ==============================================================================
# --- HELPERS ---
# ==============================================================================

def _pretty_bytes(n: float) -> str:
    try:
        n = float(n)
    except Exception:
        return "0 B"
    if n == 0: return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    unit = units[i]
    if unit == "B": return f"{int(n)} {unit}"
    else: return f"{n:.1f} {unit}"

def get_readable_time(seconds: int) -> str:
    try:
        seconds = int(seconds)
    except Exception:
        seconds = 0
    if seconds <= 0: return "0s"
    time_parts = []
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: time_parts.append(f"{h}h")
    if m > 0: time_parts.append(f"{m}m")
    if not time_parts or s > 0: time_parts.append(f"{s}s")
    return " ".join(time_parts)

def generate_bar(percent: float, length: int = 10) -> str:
    filled_length = int(length * percent / 100)
    bar = '■' * filled_length + '□' * (length - filled_length)
    return bar

def sanitize_filename(filename: str) -> str:
    if not filename: return "unnamed_file"
    filename = re.sub(r'[:]', "-", filename)
    filename = re.sub(r'[\\/*?"<>|\[\]]', "", filename)
    name, ext = os.path.splitext(filename)
    if len(name) > 60:
        name = name[:60]
    if not ext:
        ext = ".dat"
    return f"{name}{ext}"

async def split_file_python(file_path, chunk_size=1900*1024*1024):
    file_path = Path(file_path)
    if not file_path.exists():
        return []
    part_num = 0
    parts = []
    buffer_size = 10 * 1024 * 1024
    file_size = os.path.getsize(file_path)
    if file_size <= chunk_size:
        return [file_path]
    with open(file_path, 'rb') as source:
        while True:
            part_name = file_path.parent / f"{file_path.name}.part{part_num:03d}"
            current_chunk_size = 0
            with open(part_name, 'wb') as dest:
                while current_chunk_size < chunk_size:
                    read_size = min(buffer_size, chunk_size - current_chunk_size)
                    data = source.read(read_size)
                    if not data:
                        break
                    dest.write(data)
                    current_chunk_size += len(data)
            if current_chunk_size == 0:
                if os.path.exists(part_name):
                    os.remove(part_name)
                break
            parts.append(part_name)
            part_num += 1
    return parts

def progress(current, total, message, typ, task_uuid=None):
    if task_uuid and CANCEL_FLAGS.get(task_uuid):
        raise Exception("CANCELLED_BY_USER")

    try:
        msg_id = int(message.id)
    except:
        try:
            msg_id = int(message)
        except:
            return
    key = f"{msg_id}:{typ}"
    now = time.time()
    if key not in PROGRESS:
        PROGRESS[key] = {
            "current": 0, "total": int(total), "percent": 0.0,
            "last_time": now, "last_current": 0, "speed": 0.0, "eta": None
        }
    rec = PROGRESS[key]
    rec["current"] = int(current)
    rec["total"] = int(total)
    if total > 0:
        rec["percent"] = (current / total) * 100.0
    dt = now - rec["last_time"]
    if dt >= 1 or current == total:
        delta_bytes = current - rec["last_current"]
        if dt <= 0: dt = 0.1
        speed = delta_bytes / dt
        rec["speed"] = speed
        rec["last_time"] = now
        rec["last_current"] = current
        if speed > 0 and total > current:
            rec["eta"] = (total - current) / speed
            
async def downstatus(client: Client, status_message: Message, chat, index: int, total_count: int):
    msg_id = status_message.id
    key = f"{msg_id}:down"
    last_text = ""
    while True:
        rec = PROGRESS.get(key)
        if not rec:
            await asyncio.sleep(1)
            continue
        if rec["current"] == rec["total"] and rec["total"] > 0:
            break
        percent = rec.get("percent", 0.0)
        cur = rec.get("current", 0)
        tot = rec.get("total", 0)
        speed = rec.get("speed", 0.0)
        eta = rec.get("eta")
        bar = generate_bar(percent, length=12) # Slightly longer bar
        
        # --- NEW STYLE ---
        status = (
            f"📥 **Downloading File ({index}/{total_count})**\n"
            f"└ 📂 `{max(0, total_count-index)}` remaining\n\n"
            f"**{percent:.1f}%** │ `{bar}`\n\n"
            f"🚀 **Speed:** `{_pretty_bytes(speed)}/s`\n"
            f"💾 **Size:** `{_pretty_bytes(cur)} / {_pretty_bytes(tot)}`\n"
            f"⏳ **ETA:** `{get_readable_time(int(eta) if eta else 0)}`"
        )
        # -----------------

        if status != last_text:
            try:
                await client.edit_message_text(chat, msg_id, status)
                last_text = status
            except:
                pass
        await asyncio.sleep(10) # 10s is smoother than 15s
    try:
        await client.edit_message_text(chat, msg_id, f"✅ **Download Complete** ({index}/{total_count})\n⚡ **Processing file...**")
    except:
        pass

async def upstatus(client: Client, status_message: Message, chat, index: int, total_count: int):
    msg_id = status_message.id
    key = f"{msg_id}:up"
    last_text = ""
    while True:
        rec = PROGRESS.get(key)
        if not rec:
            await asyncio.sleep(1)
            continue
        if rec["current"] == rec["total"] and rec["total"] > 0:
            break
        percent = rec.get("percent", 0.0)
        cur = rec.get("current", 0)
        tot = rec.get("total", 0)
        speed = rec.get("speed", 0.0)
        eta = rec.get("eta")
        bar = generate_bar(percent, length=12)

        # --- NEW STYLE ---
        status = (
            f"☁️ **Uploading File ({index}/{total_count})**\n"
            f"└ 📤 `{max(0, total_count-index)}` remaining\n\n"
            f"**{percent:.1f}%** │ `{bar}`\n\n"
            f"🚀 **Speed:** `{_pretty_bytes(speed)}/s`\n"
            f"💾 **Size:** `{_pretty_bytes(cur)} / {_pretty_bytes(tot)}`\n"
            f"⏳ **ETA:** `{get_readable_time(int(eta) if eta else 0)}`"
        )
        # -----------------

        if status != last_text:
            try:
                await client.edit_message_text(chat, msg_id, status)
                last_text = status
            except:
                pass
        await asyncio.sleep(10)
    try:
        await client.edit_message_text(chat, msg_id, f"✅ **Upload Complete** ({index}/{total_count})")
    except:
        pass

def get_message_type(msg: Message):
    if msg.document: return "Document"
    if msg.video: return "Video"
    if msg.animation: return "Animation"
    if msg.sticker: return "Sticker"
    if msg.voice: return "Voice"
    if msg.audio: return "Audio"
    if msg.photo: return "Photo"
    if msg.text: return "Text"
    return None

# ==============================================================================
# --- HANDLERS (START/HELP/STATUS/CANCEL/etc.) ---
# ==============================================================================

@app.on_message(filters.command(["start"]) & (filters.private | filters.group))
async def send_start(client: Client, message: Message):
    # --- 1. Log and Save User (Database) ---
    user_id = message.from_user.id
    user_name = message.from_user.first_name
    
    try:
        if not await db.is_user_exist(user_id):
            await db.add_user(user_id, user_name)
            print(f"New user {user_id} saved to database.") # Simple logging
    except Exception as e:
        print(f"Failed to save user {user_id}: {e}")

    # --- 2. Send Welcome Video & Text ---
    welcome_video_url = "https://files.catbox.moe/o9azww.mp4"
    welcome_text = (
        f"<b>👋 Hi {message.from_user.mention}, I am Save Restricted Content Bot.</b>\n\n"
        "<b>For downloading restricted content /login first.</b>\n\n"
        "<b>Know how to use bot by - /help</b>"
    )
    
    buttons = [
        [InlineKeyboardButton("❣️ Developer", url = "https://t.me/thanuj66")],
        [InlineKeyboardButton('🔍 sᴜᴘᴘᴏʀᴛ ɢʀᴏᴜᴘ', url='https://t.me/telegram'), InlineKeyboardButton('🤖 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ', url='https://t.me/telegram')]
    ]

    # Try sending video, fall back to message if video fails/is invalid
    try:
        await client.send_video(
            chat_id=message.chat.id, 
            video=welcome_video_url, 
            caption=welcome_text, 
            reply_markup=InlineKeyboardMarkup(buttons),
            reply_to_message_id=message.id
        )
    except Exception as e:
        # Fallback if video link dies or fails
        await client.send_message(
            chat_id=message.chat.id,
            text=welcome_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            reply_to_message_id=message.id
        )

@app.on_message(filters.command(["help"]) & (filters.private | filters.group))
async def send_help(client: Client, message: Message):
    await client.send_message(message.chat.id, f"{HELP_TXT}")

@app.on_message(filters.command(["cancel"]) & (filters.private | filters.group))
async def send_cancel(client: Client, message: Message):
    user_id = message.from_user.id

    # 1. Check if user is stuck in "Setup Mode" (waiting for ID or Delay)
    if user_id in PENDING_TASKS:
        del PENDING_TASKS[user_id]
        await message.reply("✅ **Setup process cancelled.** You can send a new link now.")
        return

    # 2. Check if user has active downloads running
    user_tasks = ACTIVE_PROCESSES.get(user_id, {})
    if not user_tasks:
        await message.reply("✅ **No active tasks to cancel.**")
        return

    # 3. Show menu to cancel active downloads
    buttons = []
    for tid, info in list(user_tasks.items()):
        label = info.get("item", "Task")
        label_short = (label[:26] + "...") if len(label) > 29 else label
        buttons.append([InlineKeyboardButton(f"🛑 {label_short}", callback_data=f"cancel_task:{tid}")])
    buttons.append([InlineKeyboardButton("🛑 Cancel ALL My Tasks", callback_data="cancel_all")])
    buttons.append([InlineKeyboardButton("❌ Close Menu", callback_data="close_menu")])

    await message.reply(
        "**🚫 Cancel Tasks**\n\nSelect the task you want to cancel:",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )
    
@app.on_callback_query(filters.regex(r"^cancel_") | filters.regex(r"^cancel_task:"))
async def cancel_callback(client: Client, query):
    user_id = query.from_user.id
    data = query.data

    # --- FIX: Handle "cancel_setup" here because the regex ^cancel_ catches it ---
    if data == "cancel_setup":
        if user_id in PENDING_TASKS:
            del PENDING_TASKS[user_id]
        await query.message.edit("❌ **Task Setup Cancelled.**")
        return
    # --------------------------------------------------------------------------

    if data == "cancel_all":
        user_tasks = list(ACTIVE_PROCESSES.get(user_id, {}).keys())
        if not user_tasks:
            await query.answer("No active tasks to cancel.", show_alert=True)
            try: await query.message.delete()
            except: pass
            return
        for tid in user_tasks:
            CANCEL_FLAGS[tid] = True
        batch_temp.IS_BATCH[user_id] = True
        await query.message.edit("**🛑 Cancelling ALL your tasks...**\n(This may take a moment to stop current downloads)")
        return

    if data.startswith("cancel_task:"):
        task_uuid = data.split(":",1)[1]
        user_tasks = ACTIVE_PROCESSES.get(user_id, {})
        if task_uuid not in user_tasks:
            await query.answer("Task not found or already finished.", show_alert=True)
            try: await query.message.delete()
            except: pass
            return
        CANCEL_FLAGS[task_uuid] = True
        await query.message.edit(f"🛑 **Task cancelled:** `{user_tasks[task_uuid].get('item','Task')}`\nIt will stop shortly.")
        return
        
@app.on_callback_query(filters.regex("^close_menu"))
async def close_menu(client, query):
    try:
        await query.message.delete()
    except:
        await query.answer("Menu closed.")

@app.on_message(filters.command(["status"]) & (filters.user(ADMINS) | filters.user(SUDOS)))
async def status_style_handler(client, message):
    uptime_seconds = int(time.time() - BOT_START_TIME)
    uptime_str = get_readable_time(uptime_seconds)

    cpu = psutil.cpu_percent()
    mem = psutil.virtual_memory().percent
    disk = psutil.disk_usage('.')
    disk_total = _pretty_bytes(disk.total)
    disk_free = _pretty_bytes(disk.free)

    # Queue Logic
    active_count = 0
    queue_list = []
    if ACTIVE_PROCESSES:
        for uid, tasks in ACTIVE_PROCESSES.items():
            for t_id, info in tasks.items():
                active_count += 1
                queue_list.append(f"• {info.get('user')} → `{info.get('item')[:20]}...`")
    
    queue_text = "\n".join(queue_list) if queue_list else "😴 No active tasks."

    msg = (
        f"🔰 **SYSTEM DASHBOARD**\n\n"
        f"⏱ **Uptime:** `{uptime_str}`\n"
        f"🧠 **RAM:** `{mem}%`  │  ⚙️ **CPU:** `{cpu}%`\n"
        f"💿 **Disk:** `{disk_free}` free / `{disk_total}` total\n\n"
        f"📉 **Active Tasks ({active_count})**\n"
        f"{queue_text}"
    )

    await message.reply(msg, quote=True)
    
@app.on_message(filters.command(["botstats"]) & filters.user(ADMINS))
async def bot_stats_handler(client: Client, message: Message):
    total_users = await db.total_users_count()
    session_users = await db.total_session_users_count()
    await message.reply(
        "**Bot Stats**\n\n"
        f"**Total Users:** `{total_users}` (All users who ever hit /start)\n"
        f"**Logged-in Users:** `{session_users}` (Users with a saved session)"
    )

# ==============================================================================
# --- LOGIN / LOGOUT (async login handler inserted) ---
# ==============================================================================

@app.on_message(filters.private & ~filters.forwarded & filters.command(["logout"]))
async def logout(client, message):
    user_id = message.from_user.id
    if not await db.is_user_exist(user_id):
        return await message.reply_text("You are not logged in.")

    status_msg = await message.reply("📡 **Connecting to Telegram to terminate session...**")

    # 1. Get session details needed to connect
    session_string = await db.get_session(user_id)
    api_id = await db.get_api_id(user_id)
    api_hash = await db.get_api_hash(user_id)

    # 2. Perform Remote Logout (Remove from Devices)
    if session_string:
        user_client = None
        try:
            # Use stored keys or fallback to global env
            use_api_id = int(api_id) if api_id else API_ID
            use_api_hash = api_hash if api_hash else API_HASH
            
            user_client = Client(
                ":memory:", 
                session_string=session_string, 
                api_id=use_api_id, 
                api_hash=use_api_hash,
                no_updates=True
            )
            
            await user_client.connect()
            
            # Try to logout, ignoring "Already Terminated" errors
            try:
                await user_client.log_out()
                await status_msg.edit("✅ **Session successfully removed from Telegram Devices.**")
            except Exception as e:
                # If the session dies instantly, Pyrogram might complain. We consider this a success.
                if "terminated" in str(e) or "Connection" in str(e):
                    await status_msg.edit("✅ **Session terminated successfully.**")
                else:
                    raise e
            
        except AuthKeyUnregistered:
            # This happens if the user already manually removed it from devices
            await status_msg.edit("⚠️ **Session was already invalid.** Cleaning local database...")
        except Exception as e:
            # For any other real error, we just log it but still clean local DB
            print(f"Remote logout warning: {e}")
            await status_msg.edit("✅ **Local session cleared.** (Remote session might already be gone)")
        finally:
            try:
                if user_client and user_client.is_connected:
                    await user_client.disconnect()
            except: pass

    # 3. Clean up Local Database
    await db.set_session(user_id, session=None)
    await db.set_api_id(user_id, api_id=None)
    await db.set_api_hash(user_id, api_hash=None)
    
    await message.reply("**Logout Complete** ♦\n(You are now disconnected)")

@app.on_message(filters.private & ~filters.forwarded & filters.command(["login"]))
async def login_handler(bot: Client, message: Message):
    
    if not await db.is_user_exist(message.from_user.id):
        await db.add_user(message.from_user.id, message.from_user.first_name)
        
    user_data = await db.get_session(message.from_user.id)
    if user_data is not None:
        await message.reply("**You Are Already Logged In. First /logout Your Old Session. Then Do Login.**")
        return  
    user_id = int(message.from_user.id)

    # --- Check Env Variables First ---
    if API_ID != 0 and API_HASH:
        await message.reply("**🔑 Specific API ID and HASH found in variables. Using them automatically...**")
        api_id = API_ID
        api_hash = API_HASH
    else:
        # YouTube Link Removed Here
        api_id_msg = await bot.ask(user_id, "<b>Send Your API ID.</b>", filters=filters.text)
        if api_id_msg.text == '/cancel':
            return await api_id_msg.reply('<b>process cancelled !</b>')
        try:
            api_id = int(api_id_msg.text)
            if api_id < 1000000 or api_id > 99999999:
                 await api_id_msg.reply("**❌ Invalid API ID**\n\nPlease start again with /login.", quote=True)
                 return
        except ValueError:
            await api_id_msg.reply("**Api id must be an integer, start your process again by /login**", quote=True)
            return
        
        api_hash_msg = await bot.ask(user_id, "**Now Send Me Your API HASH**", filters=filters.text)
        if api_hash_msg.text == '/cancel':
            return await api_hash_msg.reply('<b>process cancelled !</b>')
        api_hash = api_hash_msg.text

        if len(api_hash) != 32:
             await api_hash_msg.reply("**❌ Invalid API HASH**\n\nPlease start again with /login.", quote=True)
             return

    # --- NEW STYLED TEXT ---
    login_text = (
        "🔐 **Login Process Initiated**\n\n"
        "Please send your **Phone Number** in international format.\n"
        "Example: `+1234567890`\n\n"
        "🛡️ *Your session is stored securely locally.*"
    )
    # -----------------------

    phone_number_msg = await bot.ask(chat_id=user_id, text=login_text, filters=filters.text)
    if phone_number_msg.text=='/cancel':
        return await phone_number_msg.reply('<b>process cancelled !</b>')
    phone_number = phone_number_msg.text
    
    # Connect for auth
    client_auth = Client(":memory:", api_id=api_id, api_hash=api_hash)
    await client_auth.connect()
    
    await phone_number_msg.reply("Sending OTP...")
    try:
        code = await client_auth.send_code(phone_number)
        phone_code_msg = await bot.ask(user_id, "Please check for an OTP in official telegram account. If you got it, send OTP here after reading the below format. \n\nIf OTP is `12345`, **please send it as** `1 2 3 4 5`.\n\n**Enter /cancel to cancel The Procces**", filters=filters.text, timeout=600)
    except PhoneNumberInvalid:
        await phone_number_msg.reply('`PHONE_NUMBER` **is invalid.**')
        await client_auth.disconnect()
        return
        
    if phone_code_msg.text=='/cancel':
        await client_auth.disconnect()
        return await phone_code_msg.reply('<b>process cancelled !</b>')
        
    try:
        phone_code = phone_code_msg.text.replace(" ", "")
        await client_auth.sign_in(phone_number, code.phone_code_hash, phone_code)
    except PhoneCodeInvalid:
        await phone_code_msg.reply('**OTP is invalid.**')
        await client_auth.disconnect()
        return
    except PhoneCodeExpired:
        await phone_code_msg.reply('**OTP is expired.**')
        await client_auth.disconnect()
        return
    except SessionPasswordNeeded:
        two_step_msg = await bot.ask(user_id, '**Your account has enabled two-step verification. Please provide the password.\n\nEnter /cancel to cancel The Procces**', filters=filters.text, timeout=300)
        if two_step_msg.text=='/cancel':
            await client_auth.disconnect()
            return await two_step_msg.reply('<b>process cancelled !</b>')
        try:
            password = two_step_msg.text
            await client_auth.check_password(password=password)
        except PasswordHashInvalid:
            await two_step_msg.reply('**Invalid Password Provided**')
            await client_auth.disconnect()
            return
            
    string_session = await client_auth.export_session_string()
    await client_auth.disconnect()
    
    if len(string_session) < SESSION_STRING_SIZE:
        return await message.reply('<b>invalid session sring</b>')
    try:
        user_data = await db.get_session(message.from_user.id)
        if user_data is None:
            # Verification check
            uclient = Client(":memory:", session_string=string_session, api_id=api_id, api_hash=api_hash)
            await uclient.connect()
            
            await db.set_session(message.from_user.id, session=string_session)
            await db.set_api_id(message.from_user.id, api_id=api_id)
            await db.set_api_hash(message.from_user.id, api_hash=api_hash)
            
            try:
                await uclient.disconnect()
            except:
                pass
    except Exception as e:
        return await message.reply_text(f"<b>ERROR IN LOGIN:</b> `{e}`")
    await bot.send_message(message.from_user.id, "<b>Account Login Successfully.\n\nIf You Get Any Error Related To AUTH KEY Then /logout first and /login again</b>")

# ==============================================================================
# --- BROADCAST ---
# ==============================================================================

async def broadcast_messages(user_id, message):
    try:
        await message.copy(chat_id=user_id)
        return True, "Success"
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await broadcast_messages(user_id, message)
    except InputUserDeactivated:
        await db.delete_user(int(user_id))
        return False, "Deleted"
    except UserIsBlocked:
        await db.delete_user(int(user_id))
        return False, "Blocked"
    except PeerIdInvalid:
        await db.delete_user(int(user_id))
        return False, "Error"
    except Exception as e:
        return False, "Error"

@app.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot, message):
    users = await db.get_all_users()
    b_msg = message.reply_to_message
    if not b_msg:
        return await message.reply_text("**Reply This Command To Your Broadcast Message**")
    sts = await message.reply_text(text='Broadcasting your messages...')
    start_time = time.time()
    total_users = await db.total_users_count()
    done = 0
    blocked = 0
    deleted = 0
    failed = 0
    success = 0
    async for user in users:
        if 'id' in user:
            pti, sh = await broadcast_messages(int(user['id']), b_msg)
            if pti:
                success += 1
            elif pti == False:
                if sh == "Blocked":
                    blocked += 1
                elif sh == "Deleted":
                    deleted += 1
                elif sh == "Error":
                    failed += 1
            done += 1
            if not done % 20:
                await sts.edit(f"Broadcast in progress:\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")
        else:
            done += 1
            failed += 1
            if not done % 20:
                await sts.edit(f"Broadcast in progress:\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")

    time_taken = str(datetime.timedelta(seconds=int(time.time()-start_time)))
    await sts.edit(f"Broadcast Completed:\nCompleted in {time_taken} seconds.\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")

# ==============================================================================
# --- CORE: receive links / start tasks / processing / cancel checks ---
# ==============================================================================

@app.on_message((filters.text | filters.caption) & filters.private & ~filters.command(["dl", "start", "help", "cancel", "botstats", "login", "logout", "broadcast", "status"]))
async def save(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in PENDING_TASKS:
        if PENDING_TASKS[user_id].get("status") == "waiting_id":
            await process_custom_destination(client, message)
            return
        if PENDING_TASKS[user_id].get("status") == "waiting_speed":
            await process_speed_input(client, message)
            return

    link_text = message.text or message.caption
    if not link_text or "https://t.me/" not in link_text:
        return

    PENDING_TASKS[user_id] = {"link": link_text, "status": "waiting_choice"}
    buttons = [
        [InlineKeyboardButton("📂 Send to DM (Here)", callback_data="dest_dm")],
        [InlineKeyboardButton("📢 Send to Channel/Group", callback_data="dest_custom")],
        [InlineKeyboardButton("❌ Cancel Setup", callback_data="cancel_setup")]
    ]
    await message.reply(
        "✨ **Link Detected!**\n\n"
        "I am ready to process this content. Please tell me where you want the files sent:",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )
    
@app.on_message(filters.command(["dl"]) & (filters.private | filters.group))
async def dl_handler(client: Client, message: Message):
    user_id = message.from_user.id
    link_text = ""
    reply = message.reply_to_message
    if reply and (reply.text or reply.caption):
        link_text = reply.text or reply.caption
    elif len(message.command) > 1:
        link_text = message.text.split(None, 1)[1]
    if not link_text or "https://t.me/" not in link_text:
        await message.reply_text("**Usage:**\n• Reply to a link with /dl\n• Or send `/dl https://t.me/...`")
        return
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        PENDING_TASKS[user_id] = {
            "link": link_text,
            "dest_chat_id": message.chat.id,
            "dest_thread_id": message.message_thread_id,
            "dest_title": message.chat.title or "This Group",
            "status": "waiting_speed"
        }
        await ask_for_speed(message)
        return
    PENDING_TASKS[user_id] = {"link": link_text, "status": "waiting_choice"}
    buttons = [
        [InlineKeyboardButton("📂 Send to DM (Here)", callback_data="dest_dm")],
        [InlineKeyboardButton("📢 Send to Channel/Group", callback_data="dest_custom")]
    ]
    await message.reply(
        "✨ **Link Detected!**\n\n"
        "I am ready to process this content. Please tell me where you want the files sent:",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )
    
@app.on_callback_query(filters.regex("^dest_"))
async def destination_callback(client: Client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS:
        return await query.answer("❌ Task expired. Send link again.", show_alert=True)
    choice = query.data
    if choice == "dest_dm":
        PENDING_TASKS[user_id]["dest_chat_id"] = user_id
        PENDING_TASKS[user_id]["dest_thread_id"] = None
        await ask_for_speed(query.message)
    elif choice == "dest_custom":
        PENDING_TASKS[user_id]["status"] = "waiting_id"
        buttons = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_setup")]]
        await query.message.edit(
            "📝 **Send the Target Chat ID**\n\n"
            "Examples:\n"
            "• Channel/Group: `-100123456789`\n"
            "• Specific Topic: `-100123456789/5`\n\n"
            "⚠️ __Make sure I am an admin in that chat!__",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

async def process_custom_destination(client: Client, message: Message):
    user_id = message.from_user.id
    text = message.text.strip()
    try:
        if message.reply_to_message and message.reply_to_message.from_user.is_self:
            await message.reply_to_message.delete()
    except:
        pass
    try:
        dest_chat_id = None
        dest_thread_id = None
        if "/" in text:
            parts = text.split("/")
            dest_chat_id = int(parts[0])
            dest_thread_id = int(parts[1])
        else:
            dest_chat_id = int(text)
        try:
            chat = await client.get_chat(dest_chat_id)
            title = chat.title or "Target Chat"
        except Exception as e:
            await message.reply(f"❌ **Invalid Chat ID** or I am not an admin there.\nError: `{e}`")
            return
        PENDING_TASKS[user_id]["dest_chat_id"] = dest_chat_id
        PENDING_TASKS[user_id]["dest_thread_id"] = dest_thread_id
        PENDING_TASKS[user_id]["dest_title"] = title
        PENDING_TASKS[user_id]["status"] = "waiting_speed"
        await ask_for_speed(message)
    except ValueError:
        await message.reply("❌ Invalid ID format. Please send a number like `-100...`")

async def ask_for_speed(message: Message):
    buttons = [
        [InlineKeyboardButton("⚡ Default (3s)", callback_data="speed_default")],
        [InlineKeyboardButton("⚙️ Manual Speed", callback_data="speed_manual")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_setup")]
    ]
    text = "**🚀 Select Forwarding Speed**\n\nHow fast should I process messages?"
    if isinstance(message, Message) and message.from_user.is_bot:
        await message.edit(text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons), quote=True)

@app.on_callback_query(filters.regex("^speed_"))
async def speed_callback(client: Client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS:
        await query.answer("❌ Task expired. Please start over.", show_alert=True)
        try: await query.message.delete()
        except: pass
        return
    choice = query.data
    task_data = PENDING_TASKS[user_id]
    if choice == "speed_default":
        try: await query.message.delete()
        except: pass
        if user_id in PENDING_TASKS: del PENDING_TASKS[user_id]
        await start_task_final(client, query.message, task_data, delay=3, user_id=user_id)
    elif choice == "speed_manual":
        PENDING_TASKS[user_id]["status"] = "waiting_speed"
        await query.message.edit(
            "⏱ **Enter Delay in Seconds**\n\n"
            "Send a number (e.g., `0`, `5`, `10`).\n"
            "0 = Max Speed (Risk of FloodWait)\n"
            "3 = Safe Default",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_setup")]])
        )

async def process_speed_input(client: Client, message: Message):
    user_id = message.from_user.id
    text = message.text.strip()
    try:
        if message.reply_to_message and message.reply_to_message.from_user.is_self:
            await message.reply_to_message.delete()
    except:
        pass
    if not text.isdigit():
        return await message.reply("❌ Please send a valid number (0, 1, 2...).")
    delay = int(text)
    if user_id in PENDING_TASKS:
        task_data = PENDING_TASKS[user_id]
        del PENDING_TASKS[user_id]
        await start_task_final(client, message, task_data, delay, user_id=user_id)
    else:
        await message.reply("❌ Task expired.")

async def start_task_final(client: Client, message_context: Message, task_data: dict, delay: int, user_id: int):
    # FIX 1: Check Limit Here
    if user_id not in ADMINS and batch_temp.ACTIVE_TASKS[user_id] >= MAX_CONCURRENT_TASKS_PER_USER:
        try:
            msg = f"⚠️ **Limit Reached:** You have {batch_temp.ACTIVE_TASKS[user_id]} active tasks. Please wait."
            if isinstance(message_context, Message):
                if message_context.from_user.is_bot:
                    await message_context.edit(msg)
                else:
                    await message_context.reply(msg)
        except:
            pass
        return

    task_uuid = uuid.uuid4().hex
    dest = task_data.get("dest_title", "Direct Message")
    
    # FIX 2: Increment Counter Here (Only Once)
    batch_temp.ACTIVE_TASKS[user_id] += 1
    batch_temp.IS_BATCH[user_id] = False

    try:
        if isinstance(message_context, Message):
            if message_context.from_user.is_bot:
                await message_context.edit(f"✅ **Task Started!**\nDestination: `{dest}`\nSpeed: `{delay}s` delay\nTask ID: `{task_uuid[:8]}`")
            else:
                await message_context.reply(f"✅ **Task Started!**\nDestination: `{dest}`\nSpeed: `{delay}s` delay\nTask ID: `{task_uuid[:8]}`")
    except:
        pass

    if user_id not in ACTIVE_PROCESSES:
        ACTIVE_PROCESSES[user_id] = {}
    ACTIVE_PROCESSES[user_id][task_uuid] = {
        "user": task_data.get("dest_title", f"User({user_id})"),
        "item": task_data.get("link", "Unknown"),
        "started": time.time()
    }

    asyncio.create_task(
        process_links_logic(
            client,
            message_context,
            task_data["link"],
            dest_chat_id=task_data.get("dest_chat_id"),
            dest_thread_id=task_data.get("dest_thread_id"),
            delay=delay,
            acc_user_id=user_id,
            task_uuid=task_uuid
        )
    )
    
async def process_links_logic(client: Client, message: Message, text: str, dest_chat_id=None, dest_thread_id=None, delay=3, acc_user_id=None, task_uuid=None):
    if acc_user_id:
        user_id = acc_user_id
        try:
            user_obj = await client.get_users(user_id)
        except:
            user_obj = None
        user_mention = user_obj.mention if user_obj else f"User({user_id})"
    elif message.from_user:
        user_id = message.from_user.id
        user_mention = message.from_user.mention
    else:
        user_id = ADMINS[0] if ADMINS else 0
        user_mention = "Channel"

    if user_id not in ACTIVE_PROCESSES:
        ACTIVE_PROCESSES[user_id] = {}
    if not task_uuid:
        task_uuid = uuid.uuid4().hex
        ACTIVE_PROCESSES[user_id][task_uuid] = {"user": user_mention, "item": text[:50]+"...", "started": time.time()}
    else:
        if task_uuid not in ACTIVE_PROCESSES[user_id]:
            ACTIVE_PROCESSES[user_id][task_uuid] = {"user": user_mention, "item": text[:50]+"...", "started": time.time()}

    if dest_chat_id is None:
        dest_chat_id = message.chat.id
    if dest_thread_id is None:
        dest_thread_id = message.message_thread_id

    # JOIN link handling & subsequent processing (kept mostly same)
    if ("https://t.me/+" in text or "https://t.me/joinchat/" in text):
        join_client = None
        if LOGIN_SYSTEM == True:
            user_data = await db.get_session(user_id)
            if user_data is None:
                await message.reply("**You must /login first to join chats.**")
                if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                    del ACTIVE_PROCESSES[user_id][task_uuid]
                batch_temp.ACTIVE_TASKS[user_id] -= 1
                if batch_temp.ACTIVE_TASKS[user_id] <= 0:
                    batch_temp.ACTIVE_TASKS[user_id] = 0
                    batch_temp.IS_BATCH[user_id] = False
                return
            api_id_from_db = await db.get_api_id(user_id)
            api_hash_from_db = await db.get_api_hash(user_id)
            try:
                join_client = Client(":memory:", session_string=user_data, api_hash=str(api_hash_from_db), api_id=int(api_id_from_db), no_updates=True)
                await join_client.connect()
            except Exception as e:
                await message.reply(f"**Login invalid.**\n`{e}`")
                if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                    del ACTIVE_PROCESSES[user_id][task_uuid]
                batch_temp.ACTIVE_TASKS[user_id] -= 1
                return
        else:
            if GlobalUserSession is None:
                await client.send_message(message.chat.id, "String Session is not Set")
                if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                    del ACTIVE_PROCESSES[user_id][task_uuid]
                batch_temp.ACTIVE_TASKS[user_id] -= 1
                return
            join_client = GlobalUserSession

        chat_info = None
        try:
            try:
                chat_info = await join_client.join_chat(text)
                await message.reply(f"**✅ Joined Chat:** `{chat_info.title}`\n**Analyzing messages...**")
            except UserAlreadyParticipant:
                try:
                    invite_check = await join_client.check_chat_invite_link(text)
                    chat_info = invite_check.chat
                    await message.reply(f"**⚠️ Already in chat:** `{chat_info.title}`\n**Proceeding to scan...**")
                except Exception:
                    await message.reply("**❌ Already in chat, but couldn't resolve Link.**\nPlease send a post link from inside the chat instead.")
                    if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                        del ACTIVE_PROCESSES[user_id][task_uuid]
                    batch_temp.ACTIVE_TASKS[user_id] -= 1
                    return
            except InviteHashExpired:
                await client.send_message(message.chat.id, "❌ **Invalid / Expired Link**")
                if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                    del ACTIVE_PROCESSES[user_id][task_uuid]
                batch_temp.ACTIVE_TASKS[user_id] -= 1
                return
            except Exception as e:
                await client.send_message(message.chat.id, f"**Join Error:** {e}")
                if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                    del ACTIVE_PROCESSES[user_id][task_uuid]
                batch_temp.ACTIVE_TASKS[user_id] -= 1
                return

            if chat_info:
                target_chat_id = chat_info.id
                last_msg_id = 0
                async for msg in join_client.get_chat_history(target_chat_id, limit=1):
                    last_msg_id = msg.id
                if last_msg_id > 0:
                    chat_id_clean = str(target_chat_id).replace("-100", "")
                    virtual_link = f"https://t.me/c/{chat_id_clean}/1-{last_msg_id}"
                    await message.reply(f"**🔄 Auto-Processing:**\nFound {last_msg_id} messages.\nStarting Batch Task...")
                    await process_links_logic(client, message, virtual_link, dest_chat_id=dest_chat_id, dest_thread_id=dest_thread_id, delay=delay, acc_user_id=acc_user_id, task_uuid=task_uuid)
                else:
                    await message.reply("**❌ Chat seems empty.**")
        except Exception as e:
            await client.send_message(message.chat.id, f"**Error:** {e}")
        finally:
            if LOGIN_SYSTEM == True and join_client and join_client.is_connected:
                try: await join_client.stop()
                except: pass
        return

    # Now handle t.me links (batch of message ids)
    if "https://t.me/" in text:
        acc = None
        success_count = 0
        failed_count = 0
        total_count = 0
        status_message = None
        filter_thread_id = None

        try:
            was_cancelled = False

            clean_text = text.replace("https://", "").replace("http://", "").replace("t.me/", "").replace("c/", "")
            parts = clean_text.split("/")

            if len(parts) >= 3 and parts[1].isdigit():
                filter_thread_id = int(parts[1])

            try:
                last_segment = parts[-1].strip()
                if "-" in text:
                    range_match = re.search(r"(\d+)\s*-\s*(\d+)", text)
                    if range_match:
                        fromID = int(range_match.group(1))
                        toID = int(range_match.group(2))
                    else:
                        fromID = int(last_segment)
                        toID = fromID
                else:
                    fromID = int(last_segment)
                    toID = fromID
            except Exception as e:
                await message.reply_text(f"**Link Error:** `{e}`")
                raise ValueError("Link parse error")

            if filter_thread_id and fromID < filter_thread_id:
                await client.send_message(
                    message.chat.id,
                    f"**⚡ Auto-Optimization Triggered**\n\n"
                    f"Requested Start: `{fromID}`\n"
                    f"Topic Starts at: `{filter_thread_id}`\n"
                    f"**Action:** Skipping `{fromID}-{filter_thread_id}` to avoid FloodWait."
                )
                fromID = filter_thread_id

            datas = text.split("/")
            total_count = max(1, toID - fromID + 1)

            status_text_header = f"**Batch Task Started!** 🚀\n"
            if filter_thread_id:
                status_text_header += f"**Filter:** `Topic {filter_thread_id} Only` 🎯\n"

            start_time = time.time()
            last_update_time = start_time
            status_message = await client.send_message(
                message.chat.id,
                f"{status_text_header}\n"
                f"**Total:** `{total_count}`\n"
                f"**Processed:** `0`\n"
                f"**Success:** `0`\n"
                f"**Failed/Skipped:** `0`\n"
                f"**ETA:** `...`",
                reply_to_message_id=message.id
            )

            if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                ACTIVE_PROCESSES[user_id][task_uuid]["item"] = f"Batch Processing ({total_count} msgs)"

            if LOGIN_SYSTEM == True:
                user_data = await db.get_session(user_id)
                if user_data is None:
                    await message.reply("**/login First.**")
                    raise ValueError("User not logged in, stopping task.")
                api_id = await db.get_api_id(user_id)
                api_hash = await db.get_api_hash(user_id)
                try:
                    acc = Client(":memory:", session_string=user_data, api_hash=api_hash, api_id=api_id, no_updates=True)
                    await acc.connect()
                    async for _ in acc.get_dialogs(limit=1): pass
                except (AuthKeyUnregistered, UserDeactivated):
                    await message.reply("**❌ Your Session is Invalid.**\n\nI have logged you out. Please /login again.")
                    await db.delete_user(user_id)
                    raise ValueError("Session invalid, logged out.")
                except Exception as e:
                    await message.reply(f"**Login Failed:** `{e}`")
                    raise ValueError("Session connection failed.")
            else:
                if GlobalUserSession is None:
                    await client.send_message(message.chat.id, f"**String Session is not Set**")
                    raise ValueError("String session not set, stopping task.")
                if batch_temp.ACTIVE_TASKS[user_id] > 1:
                    await client.send_message(message.chat.id, f"**Concurrent tasks are not supported when using the bot's global session.**")
                    raise ValueError("Global session cannot be used concurrently.")
                acc = GlobalUserSession

            for index, msgid in enumerate(range(fromID, toID+1), start=1):
                # cancellation checks
                if batch_temp.IS_BATCH.get(user_id) or CANCEL_FLAGS.get(task_uuid):
                    was_cancelled = True
                    break

                needs_retry = True
                while needs_retry:
                    is_success = False
                    try:
                        if "https://t.me/c/" in text:
                            chatid = int("-100" + parts[0])
                            if not parts[0].isdigit():
                                chatid = int("-100" + datas[4])
                        else:
                            chatid = parts[0]

                        try:
                            msg = await acc.get_messages(chatid, msgid)
                        except:
                            msg = None

                        if msg and not msg.empty:
                            if filter_thread_id:
                                current_topic = msg.message_thread_id
                                if current_topic != filter_thread_id:
                                    needs_retry = False
                                    await asyncio.sleep(6)
                                    break
                            if "https://t.me/c/" in text:
                                is_success = await handle_private(client, acc, message, chatid, msgid, index, total_count, status_message, dest_chat_id, dest_thread_id, delay, user_id, task_uuid)
                            else:
                                try:
                                    await client.copy_message(dest_chat_id, msg.chat.id, msg.id, message_thread_id=dest_thread_id)
                                    is_success = True
                                    await asyncio.sleep(delay)
                                except:
                                    is_success = await handle_private(client, acc, message, chatid, msgid, index, total_count, status_message, dest_chat_id, dest_thread_id, delay, user_id, task_uuid)
                        else:
                            await asyncio.sleep(6)

                        needs_retry = False
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 6)
                    except Exception as loop_e:
                        print(f"Loop Error: {loop_e}")
                        is_success = False
                        needs_retry = False

                if is_success:
                    success_count += 1
                else:
                    failed_count += 1

                current_time = time.time()
                if (current_time - last_update_time) > 60:
                    last_update_time = current_time
                    elapsed_time = current_time - start_time
                    if elapsed_time > 0:
                        items_per_second = index / elapsed_time
                        items_remaining = total_count - index
                        eta_seconds = items_remaining / items_per_second if items_per_second > 0 else 0
                        eta_str = get_readable_time(int(eta_seconds))
                    else:
                        eta_str = "..."
                    try:
                        await status_message.edit_text(
                            f"{status_text_header}\n"
                            f"**Total:** `{total_count}`\n"
                            f"**Processed:** `{index}`\n"
                            f"**Success:** `{success_count}`\n"
                            f"**Failed/Skipped:** `{failed_count}`\n"
                            f"**ETA:** `{eta_str}`"
                        )
                    except Exception as e:
                        print(f"Error updating status: {e}")

        except Exception as e:
            print(f"Error in task setup: {e}")
        finally:
            if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                try:
                    del ACTIVE_PROCESSES[user_id][task_uuid]
                except:
                    pass
            if user_id in ACTIVE_PROCESSES and not ACTIVE_PROCESSES[user_id]:
                try: del ACTIVE_PROCESSES[user_id]
                except: pass

            batch_temp.ACTIVE_TASKS[user_id] -= 1
            if batch_temp.ACTIVE_TASKS[user_id] < 0:
                batch_temp.ACTIVE_TASKS[user_id] = 0
            if batch_temp.ACTIVE_TASKS[user_id] == 0:
                batch_temp.IS_BATCH[user_id] = False

            if LOGIN_SYSTEM == True and acc:
                try:
                    if acc.is_connected: await acc.stop()
                except:
                    pass

            if 'was_cancelled' in locals() and was_cancelled:
                try:
                    await client.send_message(
                        chat_id=message.chat.id,
                        text=f"**Batch was Cancelled!** 🛑 {user_mention}\n\n"
                             f"**Total Requested:** `{total_count}`\n"
                             f"**Successfully Saved:** `{success_count}`\n"
                             f"**Failed/Skipped:** `{failed_count}`",
                        reply_to_message_id=message.id
                    )
                except:
                    pass
            else:
                if 'total_count' in locals() and total_count > 0:
                    try:
                        await client.send_message(
                            chat_id=message.chat.id,
                            text=f"**Batch Completed!** ✨ {user_mention}\n\n"
                                 f"**Total Requested:** `{total_count}`\n"
                                 f"**Successfully Saved:** `{success_count}`\n"
                                 f"**Failed/Skipped:** `{failed_count}`",
                            reply_to_message_id=message.id
                        )
                    except:
                        pass

            if 'status_message' in locals() and status_message:
                try: await status_message.delete()
                except: pass

# ==============================================================================
# --- handle_private: downloads & uploads with per-task cancel checks ---
# ==============================================================================

async def handle_private(client: Client, acc, message: Message, chatid, msgid: int, index: int, total_count: int, status_message: Message, dest_chat_id, dest_thread_id, delay, user_id, task_uuid=None):
    msg = None
    try:
        msg = await acc.get_messages(chatid, msgid)
    except UserNotParticipant:
        return False
    except Exception as e:
        print(f"Failed to get message {msgid}. It might be deleted.")
        return False

    if msg.empty:
        return False
    msg_type = get_message_type(msg)
    if not msg_type:
        return False

    if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)):
        return False

    # FAST FORWARD (Copy Message)
    if not msg.chat.has_protected_content:
        try:
            # Note: Bots can't copy files > 2GB usually. If this fails for large files, it falls back to download/upload.
            await acc.copy_message(chat_id=dest_chat_id, from_chat_id=chatid, message_id=msgid, message_thread_id=dest_thread_id)
            await asyncio.sleep(delay)
            return True
        except FileReferenceExpired:
            pass
        except Exception:
            pass

    if "Text" == msg_type:
        try: await client.send_message(dest_chat_id, msg.text, entities=msg.entities, message_thread_id=dest_thread_id)
        except: pass
        return True

    # SETUP DOWNLOAD
    asyncio.create_task(downstatus(client, status_message, message.chat.id, index, total_count))
    
    task_id = status_message.id
    task_folder_path = Path(f"./downloads/{user_id}/{task_id}/")
    task_folder_path.mkdir(parents=True, exist_ok=True)

    original_filename = "unknown_file.dat"
    if msg.document and msg.document.file_name: original_filename = msg.document.file_name
    elif msg.video and msg.video.file_name: original_filename = msg.video.file_name
    elif msg.audio and msg.audio.file_name: original_filename = msg.audio.file_name
    elif msg_type == "Photo": original_filename = f"{msgid}.jpg"
    elif msg_type == "Voice": original_filename = f"{msgid}.ogg"

    safe_filename = sanitize_filename(original_filename)
    if not safe_filename.strip(): safe_filename = f"{msgid}.dat"
    file_path_to_save = task_folder_path / safe_filename

    file_path = None
    ph_path = None
    download_success = False
    upload_success = False

    # --- AUTO-DETECT PREMIUM LIMIT ---
    # Default: 2000MB (2GB)
    split_limit = 2000 * 1024 * 1024 
    chunk_size_split = 1900 * 1024 * 1024
    is_premium = False
    
    try:
        # Check if the User Session (acc) is Premium
        me = acc.me if acc.me else await acc.get_me()
        if me.is_premium:
            is_premium = True
            split_limit = 4000 * 1024 * 1024 # ~4GB for Premium
            chunk_size_split = 3900 * 1024 * 1024
    except Exception as e:
        print(f"Error checking premium status: {e}")
        pass
    # ---------------------------------

    try: # MAIN TRY BLOCK FOR CLEANUP
        for attempt in range(3):
            if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)):
                return False
            try:
                msg_fresh = await acc.get_messages(chatid, msgid)
                if msg_fresh.empty: return False

                file_size = 0
                if msg_fresh.document: file_size = msg_fresh.document.file_size
                elif msg_fresh.video: file_size = msg_fresh.video.file_size
                elif msg_fresh.audio: file_size = msg_fresh.audio.file_size
                
                # --- LOGIC: SPLIT OR DOWNLOAD ---
                if file_size > split_limit:
                    # File is too big (e.g. >2GB for Free, or >4GB for Premium) -> MUST SPLIT
                    
                    # 1. Download
                    file_path = await acc.download_media(
                        msg_fresh, 
                        file_name=str(file_path_to_save), 
                        progress=progress, 
                        progress_args=[status_message, "down", task_uuid]
                    )
                    
                    # 2. Split
                    await status_message.edit_text(f"Processing large file ({_pretty_bytes(file_size)})... Splitting 🔪")
                    # Use 2GB chunks for splitting to ensure the BOT can upload them safely
                    parts = await split_file_python(file_path, chunk_size=1900*1024*1024)
                    
                    # 3. Upload Parts
                    caption = msg.caption[:1024] if msg.caption else ""
                    async with USER_UPLOAD_LOCKS[user_id]:
                        async with UPLOAD_SEMAPHORE:
                            for part in parts:
                                if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)):
                                    raise Exception("CANCELLED_BY_USER")
                                
                                while True:
                                    try:
                                        # Use Bot (client) to upload split parts (they are <2GB)
                                        await client.send_document(dest_chat_id, str(part), caption=f"{caption}", message_thread_id=dest_thread_id)
                                        break
                                    except FloodWait as e:
                                        await asyncio.sleep(e.value)
                                    except Exception as e:
                                        print(f"Error uploading part: {e}")
                                        break
                                try: os.remove(part)
                                except: pass
                    
                    try:
                        if file_path and os.path.exists(file_path): os.remove(file_path)
                    except: pass
                    return True # Finished handling split file

                else:
                    # File is within limit (<= 2GB OR <= 4GB if Premium) -> NORMAL DOWNLOAD
                    file_path = await acc.download_media(
                        msg_fresh, 
                        file_name=str(file_path_to_save), 
                        progress=progress, 
                        progress_args=[status_message, "down", task_uuid]
                    )
                
                # THUMBNAIL
                try:
                    if "Document" == msg_type and msg_fresh.document.thumbs:
                        ph_path = await acc.download_media(msg_fresh.document.thumbs[0].file_id, file_name=str(task_folder_path / "thumb.jpg"))
                    elif "Video" == msg_type and msg_fresh.video.thumbs:
                        ph_path = await acc.download_media(msg_fresh.video.thumbs[0].file_id, file_name=str(task_folder_path / "thumb.jpg"))
                    elif "Audio" == msg_type and msg_fresh.audio.thumbs:
                        ph_path = await acc.download_media(msg_fresh.audio.thumbs[0].file_id, file_name=str(task_folder_path / "thumb.jpg"))
                except:
                    ph_path = None

                download_success = True
                break

            except Exception as e:
                if "CANCELLED_BY_USER" in str(e): return False
                if isinstance(e, FileReferenceExpired): await asyncio.sleep(5)
                else: await asyncio.sleep(5)

        if not download_success:
            return False

        if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)):
            return False

        asyncio.create_task(upstatus(client, status_message, message.chat.id, index, total_count))
        
        caption = msg.caption[:1024] if msg.caption else None

        if file_path and not os.path.exists(file_path):
             return True

        # --- UPLOAD STRATEGY ---
        # If file > 2GB, Bot (client) cannot upload it. We must use User Session (acc).
        uploader = client
        if os.path.getsize(file_path) > 2000 * 1024 * 1024:
            uploader = acc 
            # Note: If uploader is 'acc', it sends to dest_chat_id as the User.
            # If dest_chat_id is the Bot's DM, the file will appear in "Saved Messages" of the User.

        async with USER_UPLOAD_LOCKS[user_id]:
            async with UPLOAD_SEMAPHORE:
                while True:
                    if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)):
                        break
                    try:
                        if "Document" == msg_type:
                            await uploader.send_document(dest_chat_id, file_path, thumb=ph_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Video" == msg_type:
                            await uploader.send_video(dest_chat_id, file_path, duration=msg.video.duration, width=msg.video.width, height=msg.video.height, thumb=ph_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Audio" == msg_type:
                            await uploader.send_audio(dest_chat_id, file_path, thumb=ph_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Photo" == msg_type:
                            await uploader.send_photo(dest_chat_id, file_path, caption=caption, message_thread_id=dest_thread_id)
                        elif "Voice" == msg_type:
                            await uploader.send_voice(dest_chat_id, file_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Animation" == msg_type:
                            await uploader.send_animation(dest_chat_id, file_path, caption=caption, message_thread_id=dest_thread_id)
                        elif "Sticker" == msg_type:
                            await uploader.send_sticker(dest_chat_id, file_path, message_thread_id=dest_thread_id)
                        upload_success = True
                        break
                    except Exception as e:
                        if "CANCELLED_BY_USER" in str(e):
                            upload_success = False
                            break
                        if isinstance(e, FloodWait):
                            await asyncio.sleep(e.value)
                        else:
                            print(f"Upload failed: {e}")
                            upload_success = False
                            break
        
        return upload_success

    finally:
        # CLEANUP
        try:
            if f"{task_id}:down" in PROGRESS: del PROGRESS[f"{task_id}:down"]
            if f"{task_id}:up" in PROGRESS: del PROGRESS[f"{task_id}:up"]
        except: pass
        try:
            if task_folder_path.exists(): shutil.rmtree(task_folder_path)
        except Exception as e: pass
        gc.collect()

# ==============================================================================
# --- Koyeb health check (optional) ---
# ==============================================================================
try:
    from aiohttp import web
except ImportError:
    web = None

async def _koyeb_health_handler(request):
    return web.Response(text="OK", status=200)

async def start_koyeb_health_check(host: str = "0.0.0.0", port: int | str = 8080):
    if web is None:
        print("aiohttp not installed; Koyeb health check not started.")
        return
    try:
        port = int(os.environ.get("PORT", str(port)))
    except Exception:
        port = 8080
    app_web = web.Application()
    app_web.router.add_get("/", _koyeb_health_handler)
    app_web.router.add_get("/health", _koyeb_health_handler)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"Starting Koyeb health check server on port {port}...")

# ==============================================================================
# --- MAIN ENTRY POINT ---
# ==============================================================================

async def main():
    if os.path.exists("./downloads"):
        try:
            shutil.rmtree("./downloads")
            print("✅ Cleanup: Deleted old downloads folder.")
        except Exception as e:
            print(f"⚠️ Cleanup Error: {e}")
    await app.start()
    print("Bot Started")
    asyncio.create_task(start_koyeb_health_check())
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
