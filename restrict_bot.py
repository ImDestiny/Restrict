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
from concurrent.futures import ThreadPoolExecutor
    
# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

API_ID = int(os.environ.get("API_ID", "") or 0)
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DB_URI = os.environ.get("DB_URI", "")
DB_NAME = os.environ.get("DB_NAME", "")
STRING_SESSION = os.environ.get("STRING_SESSION", None)

# Error Log Channel (Optional)
# Usage: "-100xxxx" for channel, or "-100xxxx/5" for Group Topic
LOG_CHANNEL = os.environ.get("LOG_CHANNEL", "") 

# Queue System
TASK_QUEUE = defaultdict(list) # Stores pending tasks: user_id -> [task_data, ...]

# Create a thread pool for blocking tasks
io_executor = ThreadPoolExecutor(max_workers=4)

LOGIN_SYSTEM = os.environ.get("LOGIN_SYSTEM", "True").lower() == "true"
ERROR_MESSAGE = os.environ.get("ERROR_MESSAGE", "True").lower() == "true"
WAITING_TIME = int(os.environ.get("WAITING_TIME", 3))

admin_str = os.environ.get("ADMINS", "")
ADMINS = [int(x) for x in admin_str.split(",") if x.strip().isdigit()]

sudo_str = os.environ.get("SUDOS", "")
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
    sleep_threshold=20,
    # max_concurrent_transmissions=10, 
    ipv6=False                    
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

def generate_bar(percent: float, length: int = 12) -> str:
    """Generates a status bar in the style: 〘⬤⬤⬤⬤◔○○○○○○○〙 34.3%"""
    filled_length = int(length * percent / 100)
    # Determine if we need a half-filled circle (◔)
    fraction = (percent / 100 * length) - filled_length
    has_half = fraction >= 0.5
    
    bar = '⬤' * filled_length
    if has_half and filled_length < length:
        bar += '◔'
        bar += '○' * (length - filled_length - 1)
    else:
        bar += '○' * (length - filled_length)
        
    return f"〘{bar}〙 {percent:.1f}%"
    
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

async def check_link_restriction(user_id, link_text):
    """
    Analyzes the link to determine if the source content is restricted.
    Supports Topics/Threads correctly.
    """
    # 1. Standardize the link
    clean_text = link_text.replace("https://", "").replace("http://", "").replace("t.me/", "").replace("c/", "")
    
    # Remove any range "100-200" to get just the starting ID
    if "-" in clean_text:
        clean_text = clean_text.split("-")[0].strip()
        
    parts = clean_text.split("/")
    
    is_private = False
    chat_id = None
    msg_id = None

    try:
        if "t.me/b/" in link_text:
            return False, "🤖 **Bot Link:** Content availability depends on the bot."
            
        msg_id = int(parts[-1])

        if "t.me/c/" in link_text:
            is_private = True
            chat_id = int("-100" + parts[0])
        else:
            chat_id = parts[0]
            
    except Exception:
        return None, "⚠️ **Could not analyze link.** (Format not recognized)"

    # 2. Select the Client (User vs Bot)
    is_temp_client = False # New flag to track if we need to disconnect later
    check_client = app 
    
    if is_private:
        user_session = await db.get_session(user_id)
        if not user_session:
            return None, "🔒 **Private Link:** Please /login to verify restrictions."
        
        api_id = await db.get_api_id(user_id)
        api_hash = await db.get_api_hash(user_id)
        check_client = Client(":memory:", session_string=user_session, api_id=api_id, api_hash=api_hash, no_updates=True)
        is_temp_client = True # Mark as temporary

    # 3. Check the Message
    is_restricted = False
    status_msg = ""
    
    try:
        if is_temp_client:
            await check_client.connect()
            
        msg = await check_client.get_messages(chat_id, msg_id)
        
        if getattr(msg.chat, "has_protected_content", False) or getattr(msg, "has_protected_content", False):
            is_restricted = True
            status_msg = "🔒 **Source is RESTRICTED** (Will use Download Mode)"
        else:
            is_restricted = False
            status_msg = "🔓 **Source is PUBLIC/UNRESTRICTED** (Will use Fast Forward)"
            
    except Exception as e:
        if "CHANNEL_PRIVATE" in str(e) or "USER_NOT_PARTICIPANT" in str(e):
            status_msg = "⚠️ **Private Chat:** I can't check yet (You need to join first)."
        else:
            status_msg = f"⚠️ **Check Failed:** `{str(e)[:30]}...`"
    finally:
        # ONLY disconnect if it's NOT the main app client
        if is_temp_client:
            try: await check_client.disconnect()
            except: pass
        
    return is_restricted, status_msg
        
async def split_file_python(file_path, chunk_size=1900*1024*1024):
    loop = asyncio.get_running_loop()
    # Run the blocking logic in a separate thread
    return await loop.run_in_executor(io_executor, _split_file_sync, file_path, chunk_size)

# Move the actual logic to a sync helper function
def _split_file_sync(file_path, chunk_size):
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
            
        status = (
            f"📥 **Downloading File ({index}/{total_count})**\n"
            f"└ 📂 `{max(0, total_count-index)}` remaining\n\n"
            f"**{rec.get('percent', 0):.1f}%** │ `{generate_bar(rec.get('percent', 0), length=12)}`\n\n"
            f"🚀 **Speed:** `{_pretty_bytes(rec.get('speed', 0))}/s`\n"
            f"💾 **Size:** `{_pretty_bytes(rec.get('current', 0))} / {_pretty_bytes(rec.get('total', 0))}`\n"
            f"⏳ **ETA:** `{get_readable_time(int(rec.get('eta', 0)) if rec.get('eta') else 0)}`"
        )

        if status != last_text:
            try:
                await client.edit_message_text(chat, msg_id, status)
                last_text = status
            except Exception:
                pass
        
        # --- DYNAMIC SLEEP LOGIC ---
        total_size = rec.get("total", 0)
        # If file is smaller than 50MB (52428800 bytes), update every 5s
        if total_size > 0 and total_size < 50 * 1024 * 1024:
            await asyncio.sleep(9) 
        else:
            # For larger files, update every 20s to prevent Rate Limits
            await asyncio.sleep(20)
            
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
            
        status = (
            f"☁️ **Uploading File ({index}/{total_count})**\n"
            f"└ 📤 `{max(0, total_count-index)}` remaining\n\n"
            f"**{rec.get('percent', 0):.1f}%** │ `{generate_bar(rec.get('percent', 0), length=12)}`\n\n"
            f"🚀 **Speed:** `{_pretty_bytes(rec.get('speed', 0))}/s`\n"
            f"💾 **Size:** `{_pretty_bytes(rec.get('current', 0))} / {_pretty_bytes(rec.get('total', 0))}`\n"
            f"⏳ **ETA:** `{get_readable_time(int(rec.get('eta', 0)) if rec.get('eta') else 0)}`"
        )

        if status != last_text:
            try:
                await client.edit_message_text(chat, msg_id, status)
                last_text = status
            except Exception:
                pass
        
        # --- DYNAMIC SLEEP LOGIC ---
        total_size = rec.get("total", 0)
        if total_size > 0 and total_size < 50 * 1024 * 1024:
            await asyncio.sleep(9) 
        else:
            await asyncio.sleep(20)

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
    mem = psutil.virtual_memory().percent
    cpu = psutil.cpu_percent()
    
    # Get Disk Usage
    total, used, free = shutil.disk_usage(".")
    disk_free = free / (1024**3)
    disk_total = total / (1024**3)
    
    active_count = 0
    queue_list = []
    
    for uid, tasks in ACTIVE_PROCESSES.items():
        for t_id, info in tasks.items():
            active_count += 1
            src = info.get("source_title", "Source")
            dst = info.get("dest_title_name", "Destination") # Use the Chat Title name
            queue_list.append(f"• {src} → {dst}")
    
    queue_text = "\n".join(queue_list) if queue_list else "😴 No active tasks."

    msg = (
        f"🔰 **SYSTEM DASHBOARD**\n\n"
        f"⏱ **Uptime:** `{uptime_str}`\n"
        f"🧠 **RAM:** `{mem}%`  │  ⚙️ **CPU:** `{cpu}%` \n"
        f"💿 **Disk:** `{disk_free:.1f} GB free / {disk_total:.1f} GB total` \n\n"
        f"📉 **Active Tasks ({active_count})**\n"
        f"{queue_text}"
    )
    await message.reply(msg, quote=True)
    
@app.on_message(filters.command(["botstats"]) & filters.user(ADMINS))
async def bot_stats_handler(client: Client, message: Message):
    wait = await message.reply("📊 **Generating detailed stats...**")
    total_users = await db.total_users_count()
    all_users_cursor = await db.get_all_users()
    
    logged_in_list = []
    async for user in all_users_cursor:
        if user.get("session"):
            user_id = user['id']
            name = user.get("name") or f"User:{user_id}"
            user_tasks = ACTIVE_PROCESSES.get(user_id, {})
            
            if user_tasks:
                task_details = []
                for t_id, info in user_tasks.items():
                    src = info.get("source_title", "Source")
                    dst = info.get("dest_title", "Dest")
                    tot = info.get("total", 0)
                    curr = info.get("current", 0)
                    start_t = info.get("started", time.time())
                    
                    percent = (curr / tot * 100) if tot > 0 else 0
                    
                    # ETA Math
                    elapsed = time.time() - start_t
                    eta_str = "Calculating..."
                    if curr > 0 and elapsed > 0:
                        eta_str = get_readable_time(int(((tot - curr) / (curr / elapsed))))

                    task_details.append(
                        f"      └ 🏃 {src} → {info.get('dest_title_name', 'Destination')}"
                    )
                    
                tasks_str = "\n" + "\n".join(task_details)
                logged_in_list.append(f"• **{name}** [`{user_id}`]{tasks_str}")
            else:
                logged_in_list.append(f"• **{name}** [`{user_id}`] (IDLE 😴)")

    logged_in_text = "\n\n".join(logged_in_list) if logged_in_list else "No users logged in."
    stats_msg = (
        "📊 **DETAILED BOT STATISTICS**\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"👥 **Total Users:** `{total_users}`\n"
        f"🔑 **Logged-in Users:** `{len(logged_in_list)}`\n\n"
        f"📝 **User & Task Breakdown:**\n\n{logged_in_text}"
    )
    await wait.edit(stats_msg)
        
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
    start_time = time.time()
    try:
        await message.copy(chat_id=user_id)
        # Calculates sleep based on work time
        elapsed = time.time() - start_time
        await asyncio.sleep(max(0, 1.5 - elapsed)) 
        return True, "Success"
    except FloodWait as e:
        # If floodwait is huge, just skip this user to save the broadcast
        if e.value > 60:
            return False, "Error"
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
        # (Keep existing setup logic for waiting_id / waiting_speed)
        if PENDING_TASKS[user_id].get("status") == "waiting_id":
            await process_custom_destination(client, message)
            return
        if PENDING_TASKS[user_id].get("status") == "waiting_speed":
            await process_speed_input(client, message)
            return

    link_text = message.text or message.caption
    if not link_text or "https://t.me/" not in link_text:
        return

    # --- NEW: CHECK RESTRICTION FIRST ---
    wait_msg = await message.reply("🔎 **Analyzing Link...**", quote=True)
    is_restricted, status_text = await check_link_restriction(user_id, link_text)
    await wait_msg.delete()
    # ------------------------------------

    PENDING_TASKS[user_id] = {
        "link": link_text, 
        "status": "waiting_choice",
        "is_restricted": is_restricted 
    }
    
    buttons = [
        [InlineKeyboardButton("📂 Send to DM (Here)", callback_data="dest_dm")],
        [InlineKeyboardButton("📢 Send to Channel/Group", callback_data="dest_custom")],
        [InlineKeyboardButton("❌ Cancel Setup", callback_data="cancel_setup")]
    ]
    
    # Add the status text to the reply
    await message.reply(
        f"✨ **Link Detected!**\n\n"
        f"{status_text}\n\n"
        "Where should I send the files?",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )
    
@app.on_message(filters.command(["dl"]) & (filters.private | filters.group))
async def dl_handler(client: Client, message: Message):
    user_id = message.from_user.id
    link_text = ""
    
    # 1. Extract Link (from Reply or Command Argument)
    reply = message.reply_to_message
    if reply and (reply.text or reply.caption):
        link_text = reply.text or reply.caption
    elif len(message.command) > 1:
        link_text = message.text.split(None, 1)[1]
        
    # 2. Validate Link
    if not link_text or "https://t.me/" not in link_text:
        await message.reply_text(
            "**Usage:**\n"
            "• Reply to a link with `/dl`\n"
            "• Or send `/dl https://t.me/...`"
        )
        return

    # --- NEW: CHECK RESTRICTION FIRST ---
    wait_msg = await message.reply("🔎 **Analyzing Link...**", quote=True)
    is_restricted, status_text = await check_link_restriction(user_id, link_text)
    await wait_msg.delete()
    # ------------------------------------

    # 3. Handle Group Chat (Directly ask for Speed)
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        PENDING_TASKS[user_id] = {
            "link": link_text,
            "dest_chat_id": message.chat.id,
            "dest_thread_id": message.message_thread_id,
            "dest_title": message.chat.title or "This Group",
            "status": "waiting_speed",
            "is_restricted": is_restricted # <--- ADDED THIS
        }
        # Send the status info before showing the speed menu
        await message.reply(f"✨ **Link Analyzed!**\n{status_text}", quote=True)
        await ask_for_speed(message)
        return

    # 4. Handle Private Chat (Show Destination Menu)
    # CHANGE: Added "is_restricted" here too
    PENDING_TASKS[user_id] = {
        "link": link_text, 
        "status": "waiting_choice",
        "is_restricted": is_restricted
    }
    
    buttons = [
        [InlineKeyboardButton("📂 Send to DM (Here)", callback_data="dest_dm")],
        [InlineKeyboardButton("📢 Send to Channel/Group", callback_data="dest_custom")],
        [InlineKeyboardButton("❌ Cancel Setup", callback_data="cancel_setup")]  # <-- Added this button
    ]
    
    await message.reply(
        f"✨ **Link Detected!**\n\n"
        f"{status_text}\n\n"
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
    
    # FIX: Enforce minimum safety delay. "0" is dangerous.
    delay = int(text)
    if delay < 3: 
        delay = 3
        await message.reply("⚠️ **Note:** To prevent FloodWait bans, minimum speed has been set to 3 seconds.")
        
    if user_id in PENDING_TASKS:
        task_data = PENDING_TASKS[user_id]
        del PENDING_TASKS[user_id]
        await start_task_final(client, message, task_data, delay, user_id=user_id)
    else:
        await message.reply("❌ Task expired.")

# ==============================================================================
# --- NEW ROBUSTNESS HELPERS ---
# ==============================================================================

async def send_log(text):
    """Sends errors/alerts to the Configured Log Channel/Topic"""
    if not LOG_CHANNEL:
        return
    try:
        chat_id = LOG_CHANNEL
        topic_id = None
        
        # Check if "ID/TOPIC" format
        if "/" in LOG_CHANNEL:
            parts = LOG_CHANNEL.split("/")
            chat_id = int(parts[0])
            topic_id = int(parts[1])
        else:
            chat_id = int(LOG_CHANNEL)

        await app.send_message(chat_id, text, message_thread_id=topic_id)
    except Exception as e:
        print(f"❌ Failed to send log: {e}")

async def check_disk_space():
    """Returns False if free space is < 500MB"""
    try:
        total, used, free = shutil.disk_usage(".")
        free_mb = free / (1024 * 1024)
        if free_mb < 500: # Limit: 500MB
            return False
        return True
    except:
        return True

async def cleanup_watchdog():
    """Runs every 10 mins to clean stuck download folders older than 2 hours"""
    while True:
        await asyncio.sleep(600) # Check every 10 mins
        try:
            download_path = Path("./downloads")
            if not download_path.exists(): continue
            
            current_time = time.time()
            # 2 hours in seconds
            max_age = 2 * 60 * 60 
            
            for user_folder in download_path.iterdir():
                if user_folder.is_dir():
                    for task_folder in user_folder.iterdir():
                        if task_folder.is_dir():
                            folder_time = task_folder.stat().st_mtime
                            if (current_time - folder_time) > max_age:
                                shutil.rmtree(task_folder)
                                await send_log(f"🧹 **Auto-Cleanup:** Deleted stuck folder `{task_folder.name}` (Older than 2h)")
        except Exception as e:
            print(f"Watchdog Error: {e}")
            
async def start_task_final(client: Client, message_context: Message, task_data: dict, delay: int, user_id: int):
    # 1. DISK SPACE PRE-CHECK
    if not await check_disk_space():
        msg = "⚠️ **Server Busy:** Disk is almost full. Please wait for other tasks to finish."
        if isinstance(message_context, Message):
             await message_context.reply(msg, quote=True)
        await send_log("🚨 **Critical:** Disk Space Low (<500MB). Tasks rejected.")
        return

    # 2. QUEUE SYSTEM
    # If user has hit their limit (e.g., 2 tasks), queue this one.
    if user_id not in ADMINS and batch_temp.ACTIVE_TASKS[user_id] >= MAX_CONCURRENT_TASKS_PER_USER:
        TASK_QUEUE[user_id].append({
            "client": client,
            "message": message_context,
            "data": task_data,
            "delay": delay
        })
        position = len(TASK_QUEUE[user_id])
        await message_context.reply(f"⏳ **Added to Queue:** Position #{position}\nTask will start automatically when your current tasks finish.", quote=True)
        return

    # 3. START TASK (Standard Logic)
    task_uuid = uuid.uuid4().hex
    dest = task_data.get("dest_title", "Direct Message")
    
    batch_temp.ACTIVE_TASKS[user_id] += 1
    batch_temp.IS_BATCH[user_id] = False

    start_msg = f"✅ **Task Started!**\nDestination: `{dest}`\nSpeed: `{delay}s` delay\nTask ID: `{task_uuid[:8]}`"
    try:
        if isinstance(message_context, Message):
            if message_context.from_user.is_bot:
                await message_context.edit(start_msg)
            else:
                await message_context.reply(start_msg)
    except: pass
    
    # Log to Channel
    await send_log(f"▶️ **Task Started**\nUser: `{user_id}`\nLink: `{task_data['link'][:40]}...`")

    if user_id not in ACTIVE_PROCESSES:
        ACTIVE_PROCESSES[user_id] = {}
    ACTIVE_PROCESSES[user_id][task_uuid] = {
        "user": task_data.get("dest_title", f"User({user_id})"),
        "dest_title_name": task_data.get("dest_title", "Direct Message"), # Add this
        "item": task_data.get("link", "Unknown"),
        "started": time.time()
    }
    
    # CHANGE: Get the flag we saved earlier
    is_restricted = task_data.get("is_restricted", False)

    asyncio.create_task(
        process_links_logic(
            client,
            message_context,
            task_data["link"],
            dest_chat_id=task_data.get("dest_chat_id"),
            dest_thread_id=task_data.get("dest_thread_id"),
            dest_title=dest, # <--- PASS THIS
            delay=delay,
            acc_user_id=user_id,
            task_uuid=task_uuid,
            is_restricted=is_restricted
        )
    )   
    

# CHANGE: Added is_restricted=False argument
async def process_links_logic(client: Client, message: Message, text: str, dest_chat_id=None, dest_thread_id=None, dest_title="Direct Message", delay=3, acc_user_id=None, task_uuid=None, is_restricted=False):
    # --- 1. SETUP USER & LOGGING ---
    user_id = acc_user_id or (message.from_user.id if message.from_user else 0)
    user_mention = message.from_user.mention if message.from_user else f"User({user_id})"
    
    if user_id not in ACTIVE_PROCESSES: ACTIVE_PROCESSES[user_id] = {}
    if not task_uuid: task_uuid = uuid.uuid4().hex
    
    ACTIVE_PROCESSES[user_id][task_uuid] = {
        "user": user_mention, 
        "dest_title_name": dest_title,
        "item": text[:50]+"...", 
        "started": time.time()
    }

    if dest_chat_id is None: dest_chat_id = message.chat.id
    if dest_thread_id is None: dest_thread_id = message.message_thread_id

    # --- 2. BATCH PROCESSING ---
    if "https://t.me/" in text:
        acc = None
        success_count = 0
        failed_count = 0
        total_count = 0
        status_message = None
        
        start_time = time.time()
        source_title = "Unknown Source"

        try:
            was_cancelled = False
            clean_text = text.replace("https://", "").replace("http://", "").replace("t.me/", "").replace("c/", "")
            parts = clean_text.split("/")

            # Parse range
            last_segment = parts[-1].strip()
            range_match = re.search(r"(\d+)\s*-\s*(\d+)", text)
            if range_match:
                fromID, toID = int(range_match.group(1)), int(range_match.group(2))
            else:
                fromID = toID = int(last_segment)

            total_count = max(1, toID - fromID + 1)

            # Session login
            user_data = await db.get_session(user_id)
            if not user_data:
                await message.reply("**/login First.**")
                return
            
            api_id = await db.get_api_id(user_id)
            api_hash = await db.get_api_hash(user_id)
            
            acc = Client(
                ":memory:", 
                session_string=user_data, 
                api_hash=api_hash, 
                api_id=api_id, 
                no_updates=True,
                workers=4,
                # max_concurrent_transmissions=2,
                ipv6=False
            )
            await acc.start() 
            
            try:
                chatid_check = int("-100" + parts[0]) if "https://t.me/c/" in text else parts[0]
                source_chat = await acc.get_chat(chatid_check)
                source_title = source_chat.title or "Private Chat"
            except: pass

            ACTIVE_PROCESSES[user_id][task_uuid].update({"source_title": source_title, "total": total_count, "current": 0})

            # --- STATUS MESSAGE SETUP ---
            if is_restricted:
                status_message = await client.send_message(
                    message.chat.id,
                    f"⚡ **Initializing Task...**\nSource: {source_title}\nTotal Files: {total_count}",
                    reply_to_message_id=message.id
                )
            else:
                status_text_header = f"**Batch Task Started!** 🚀"
                status_message = await client.send_message(
                    message.chat.id,
                    f"{status_text_header}\n\n{generate_bar(0)}\n\n"
                    f"**Source:** {source_title}\n**Destination :** {dest_title}\n"
                    f"**Total:** {total_count}\n**Processed:** 0\n**Success:** 0\n**Failed:** 0\n**ETA:** ...",
                    reply_to_message_id=message.id
                )

            last_update_time = time.time()

            for index, msgid in enumerate(range(fromID, toID+1), start=1):
                loop_start_time = time.time()

                if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                    ACTIVE_PROCESSES[user_id][task_uuid]["current"] = index

                if batch_temp.IS_BATCH.get(user_id) or CANCEL_FLAGS.get(task_uuid):
                    was_cancelled = True; break

                is_success = False
                try:
                    chatid = int("-100" + parts[0]) if "https://t.me/c/" in text else parts[0]
                    
                    is_success = await handle_private(client, acc, message, chatid, msgid, index, total_count, status_message, dest_chat_id, dest_thread_id, delay, user_id, task_uuid, is_restricted=is_restricted)
                
                except FloodWait as e:
                    if e.value > 300:
                        print(f"FloodWait too long ({e.value}s). Stopping task.")
                        await status_message.edit_text(f"❌ **Task Cancelled automatically**\nReason: FloodWait too long ({e.value}s).")
                        was_cancelled = True
                        break

                    wait_msg = f"⏳ **Rate Limiting Detected**\nSleeping for {e.value} seconds..."
                    try: 
                        if not is_restricted: await status_message.edit_text(wait_msg)
                    except: pass
                    await asyncio.sleep(e.value + 5)
                    
                except Exception as e: 
                    print(f"Error processing {msgid}: {e}")
                    pass

                if is_success: success_count += 1
                else: failed_count += 1

                # --- 3. UNIVERSAL SMART SLEEP ---
                # Applies to BOTH Forwarding and Downloading
                if index < total_count:
                    if is_success:
                        # Success? Wait the full safe delay (e.g. 3s)
                        elapsed_time = time.time() - loop_start_time
                        actual_sleep = max(0, delay - elapsed_time)
                        await asyncio.sleep(actual_sleep)
                    else:
                        # Failed/Deleted? Skip delay instantly (0.05s)
                        await asyncio.sleep(0.05)

                # --- UPDATE DASHBOARD (ONLY IF NOT RESTRICTED) ---
                if not is_restricted:
                    current_now = time.time()
                    if (index % 20 == 0) or (current_now - last_update_time >= 60) or index == total_count:
                        elapsed = current_now - start_time
                        percent = (index / total_count) * 100
                        eta_str = get_readable_time(int(((total_count - index) / (index / elapsed)))) if index > 0 else "..."
                        
                        try:
                            await status_message.edit_text(
                                f"{status_text_header}\n\n{generate_bar(percent)}\n\n"
                                f"**Source:** {source_title}\n**Destination :** {dest_title}\n"
                                f"**Total:** {total_count}\n**Processed:** {index}\n"
                                f"**Success:** {success_count}\n**Failed:** {failed_count}\n**ETA:** {eta_str}"
                            )
                            last_update_time = current_now
                        except: pass
                    
        except Exception as e:
            await send_log(f"❌ **Task Crashed**\nUser: `{user_id}`\nError: `{e}`")

        finally:
            if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                try: del ACTIVE_PROCESSES[user_id][task_uuid]
                except: pass
            
            batch_temp.ACTIVE_TASKS[user_id] -= 1
            if batch_temp.ACTIVE_TASKS[user_id] < 0: batch_temp.ACTIVE_TASKS[user_id] = 0
            if TASK_QUEUE[user_id]:
                next_item = TASK_QUEUE[user_id].pop(0)
                asyncio.create_task(start_task_final(next_item["client"], next_item["message"], next_item["data"], next_item["delay"], user_id))

            if acc:
                try: await acc.stop()
                except: pass

            duration = time.time() - start_time
            time_taken_str = get_readable_time(int(duration))
            
            if 'was_cancelled' in locals() and was_cancelled:
                header = f"Batch was Cancelled! 🛑 {user_mention} ✨"
            else:
                header = f"Batch was Completed! ✅ {user_mention} ✨"

            final_text = (
                f"{header}\n"
                f"📝 **Task :** {source_title} → {dest_title}\n"
                f"⏱ **Time Taken:** `{time_taken_str}`\n"
                f"📊 **Statistics:**\n"
                f"├ 📥 **Total Requested:** `{total_count}`\n"
                f"├ ✅ **Successful:** `{success_count}`\n"
                f"└ ❌ **Failed/Skipped:** `{failed_count}`"
            )
            
            try: await client.send_message(message.chat.id, final_text, reply_to_message_id=message.id)
            except: pass
            try: await status_message.delete()
            except: pass

# ==============================================================================
# --- handle_private: downloads & uploads with per-task cancel checks ---
# ==============================================================================

async def handle_private(client: Client, acc, message: Message, chatid, msgid: int, index: int, total_count: int, status_message: Message, dest_chat_id, dest_thread_id, delay, user_id, task_uuid=None, is_restricted=False):
    msg = None
    try:
        msg = await acc.get_messages(chatid, msgid)
    except UserNotParticipant: return False
    except Exception: return False

    if not msg or msg.empty: return False
    msg_type = get_message_type(msg)
    if not msg_type: return False

    if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)): return False

    # 1. FAST FORWARD (Copy)
    # If is_restricted is True, we skip this block immediately
    if not is_restricted and not getattr(msg, "has_protected_content", False) and not getattr(msg.chat, "has_protected_content", False):
        try:
            await acc.copy_message(chat_id=dest_chat_id, from_chat_id=chatid, message_id=msgid, message_thread_id=dest_thread_id)
            return True 
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            try:
                await acc.copy_message(chat_id=dest_chat_id, from_chat_id=chatid, message_id=msgid, message_thread_id=dest_thread_id)
                return True
            except: pass
        except Exception: pass

    if "Text" == msg_type:
        try: 
            await client.send_message(dest_chat_id, msg.text, entities=msg.entities, message_thread_id=dest_thread_id)
        except: pass
        return True

    # 2. PATHS & FILENAME
    task_id = status_message.id
    task_folder_path = Path(f"./downloads/{user_id}/{task_id}/")
    task_folder_path.mkdir(parents=True, exist_ok=True)

    original_filename = "unknown_file"
    if msg.document and msg.document.file_name: original_filename = msg.document.file_name
    elif msg.video and msg.video.file_name: original_filename = msg.video.file_name
    elif msg.audio and msg.audio.file_name: original_filename = msg.audio.file_name
    elif msg_type == "Photo": original_filename = f"{msgid}.jpg"
    elif msg_type == "Voice": original_filename = f"{msgid}.ogg"

    safe_filename = sanitize_filename(original_filename)
    if not safe_filename.strip(): safe_filename = f"{msgid}.dat"
    file_path_to_save = task_folder_path / safe_filename

    # 3. START DOWNLOAD (Background Task)
    down_task = asyncio.create_task(downstatus(client, status_message, message.chat.id, index, total_count))
    file_path = None
    ph_path = None
    download_success = False

    split_limit = 2000 * 1024 * 1024 
    try:
        me = acc.me if acc.me else await acc.get_me()
        if me.is_premium: split_limit = 4000 * 1024 * 1024 
    except: pass

    try: 
        for attempt in range(3):
            if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)): return False
            try:
                msg_fresh = await acc.get_messages(chatid, msgid)
                if msg_fresh.empty: return False
                
                file_size = 0
                if msg_fresh.document: file_size = msg_fresh.document.file_size
                elif msg_fresh.video: file_size = msg_fresh.video.file_size
                elif msg_fresh.audio: file_size = msg_fresh.audio.file_size

                # LARGE FILE
                if file_size > split_limit:
                    file_path = await acc.download_media(msg_fresh, file_name=str(file_path_to_save), progress=progress, progress_args=[status_message, "down", task_uuid])
                    
                    # --- CRITICAL: INSTANT STATUS SWITCH ---
                    if down_task and not down_task.done(): down_task.cancel()
                    
                    await status_message.edit_text(f"✂️ **Splitting large file ({_pretty_bytes(file_size)})...**")
                    parts = await split_file_python(file_path, chunk_size=1900*1024*1024)
                    
                    up_task = asyncio.create_task(upstatus(client, status_message, message.chat.id, index, total_count))
                    caption = msg.caption[:1024] if msg.caption else ""
                    
                    async with USER_UPLOAD_LOCKS[user_id]:
                        async with UPLOAD_SEMAPHORE:
                            for part in parts:
                                if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)): raise Exception("CANCELLED")
                                while True:
                                    try:
                                        await client.send_document(dest_chat_id, str(part), caption=caption, message_thread_id=dest_thread_id)
                                        break
                                    except FloodWait as e: await asyncio.sleep(e.value + 5)
                                    except Exception: break
                                try: os.remove(part)
                                except: pass
                    
                    if up_task and not up_task.done(): up_task.cancel()
                    try: os.remove(file_path)
                    except: pass
                    return True 

                # NORMAL DOWNLOAD
                else:
                    file_path = await acc.download_media(msg_fresh, file_name=str(file_path_to_save), progress=progress, progress_args=[status_message, "down", task_uuid])
                
                # Thumbnails
                try:
                    thumb = None
                    if msg_fresh.document and msg_fresh.document.thumbs: thumb = msg_fresh.document.thumbs[0]
                    elif msg_fresh.video and msg_fresh.video.thumbs: thumb = msg_fresh.video.thumbs[0]
                    elif msg_fresh.audio and msg_fresh.audio.thumbs: thumb = msg_fresh.audio.thumbs[0]
                    if thumb: ph_path = await acc.download_media(thumb.file_id, file_name=str(task_folder_path / "thumb.jpg"))
                except: pass

                download_success = True
                break
            except FloodWait as e: await asyncio.sleep(e.value + 5)
            except Exception as e:
                if "CANCELLED" in str(e): return False
                await asyncio.sleep(5)

        # --- CRITICAL: INSTANT STATUS SWITCH ---
        if down_task and not down_task.done(): down_task.cancel()
        
        if not download_success: return False
        if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)): return False

        # 4. START UPLOAD IMMEDIATELY
        up_task = asyncio.create_task(upstatus(client, status_message, message.chat.id, index, total_count))
        
        caption = msg.caption[:1024] if msg.caption else None
        uploader = client
        if os.path.getsize(file_path) > 2000 * 1024 * 1024: uploader = acc 

        upload_success = False
        async with USER_UPLOAD_LOCKS[user_id]:
            async with UPLOAD_SEMAPHORE:
                while True:
                    if batch_temp.IS_BATCH.get(user_id) or (task_uuid and CANCEL_FLAGS.get(task_uuid)): break
                    try:
                        if "Document" == msg_type: await uploader.send_document(dest_chat_id, file_path, thumb=ph_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Video" == msg_type: await uploader.send_video(dest_chat_id, file_path, duration=msg.video.duration, width=msg.video.width, height=msg.video.height, thumb=ph_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Audio" == msg_type: await uploader.send_audio(dest_chat_id, file_path, thumb=ph_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Photo" == msg_type: await uploader.send_photo(dest_chat_id, file_path, caption=caption, message_thread_id=dest_thread_id)
                        elif "Voice" == msg_type: await uploader.send_voice(dest_chat_id, file_path, caption=caption, message_thread_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                        elif "Animation" == msg_type: await uploader.send_animation(dest_chat_id, file_path, caption=caption, message_thread_id=dest_thread_id)
                        elif "Sticker" == msg_type: await uploader.send_sticker(dest_chat_id, file_path, message_thread_id=dest_thread_id)
                        upload_success = True
                        break 
                    except FloodWait as e: await asyncio.sleep(e.value + 5)
                    except Exception as e:
                        if "CANCELLED" in str(e): break
                        break
        
        if up_task and not up_task.done(): up_task.cancel()
        return upload_success

    finally:
        try: shutil.rmtree(task_folder_path)
        except: pass
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
            
    # START THE WATCHDOG HERE
    asyncio.create_task(cleanup_watchdog())
    print("🛡️ Auto-Cleanup Watchdog Started")

    await app.start()
    print("Bot Started")
    asyncio.create_task(start_koyeb_health_check())
    await idle()
    await app.stop()
    
if __name__ == "__main__":
    app.run(main())
        
