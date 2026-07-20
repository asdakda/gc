import os
import re
import json
import logging
import sqlite3
import psycopg2
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
import time
import threading
import asyncio
import http.server
import socketserver
import urllib.request
from pyrogram import Client, enums
from pyrogram.errors import SessionPasswordNeeded

# Setup logging with a premium theme look
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [🛡️ GROUP HELPER] - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Config loading helper
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.json")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = os.environ.get("OWNER_ID")

if os.path.exists(CONFIG_PATH):
    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
            if not BOT_TOKEN:
                BOT_TOKEN = config.get("BOT_TOKEN")
            if not OWNER_ID:
                OWNER_ID = config.get("OWNER_ID")
    except Exception as e:
        logger.error(f"Error loading config.json: {e}")

try:
    OWNER_ID = int(OWNER_ID) if OWNER_ID else None
except ValueError:
    OWNER_ID = None

if not BOT_TOKEN or "YOUR_TELEGRAM_BOT_TOKEN_HERE" in BOT_TOKEN:
    logger.warning("BOT_TOKEN is not set properly. Please set it in config.json or environment variables.")

bot = telebot.TeleBot(BOT_TOKEN) if BOT_TOKEN else None

# --- BACKGROUND ASYNCIO LOOP FOR PYROGRAM CLIENTS ---
background_loop = asyncio.new_event_loop()

def start_background_loop(loop_to_run):
    asyncio.set_event_loop(loop_to_run)
    loop_to_run.run_forever()

loop_thread = threading.Thread(target=start_background_loop, args=(background_loop,), daemon=True)
loop_thread.start()

def run_async(coro):
    """Schedules a coroutine to run on the background loop and blocks until it returns a result."""
    future = asyncio.run_coroutine_threadsafe(coro, background_loop)
    return future.result()

async def init_and_connect_client(name, api_id, api_hash, phone):
    client = Client(
        name=name,
        api_id=api_id,
        api_hash=api_hash
    )
    await client.connect()
    code_info = await client.send_code(phone)
    return client, code_info.phone_code_hash

async def sign_in_client(client, phone, phone_code_hash, otp):
    return await client.sign_in(phone_number=phone, phone_code_hash=phone_code_hash, phone_code=otp)

async def check_password_client(client, password):
    return await client.check_password(password)

async def disconnect_client(client):
    try:
        await client.disconnect()
    except Exception:
        pass

async def scrape_members_via_client(session_path, chat_id):
    client = Client(name=session_path)
    await client.connect()
    
    # Force Pyrogram to resolve and cache the group ID
    try:
        await client.get_chat(chat_id)
    except Exception as e:
        logger.warning(f"Direct get_chat failed for {chat_id}, fetching dialogs to cache peers: {e}")
        async for dialog in client.get_dialogs():
            if dialog.chat.id == chat_id:
                break
                
    scraped = 0
    async for member in client.get_chat_members(chat_id):
        if not member.user.is_bot:
            username = member.user.username or member.user.first_name or "Unknown"
            record_member_activity(chat_id, member.user.id, username, is_media=False)
            scraped += 1
    await client.disconnect()
    return scraped

async def auto_discover_groups_via_client(session_path, bot_username):
    client = Client(name=session_path)
    await client.connect()
    discovered = []
    async for dialog in client.get_dialogs():
        chat = dialog.chat
        if chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
            try:
                member = await chat.get_member(bot_username)
                if member.status in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
                    register_group(chat.id, chat.title)
                    discovered.append((chat.id, chat.title))
            except Exception:
                pass
    await client.disconnect()
    return discovered

# Temporary store for ongoing userbot logins
# format: { user_id: { "client": Client, "api_id": str, "api_hash": str, "phone": str, "phone_code_hash": str } }
temp_userbot_logins = {}

# --- DATABASE MANAGER (PostgreSQL with SQLite fallback) ---
class DBManager:
    def __init__(self):
        self.db_url = os.environ.get("DATABASE_URL")
        self.is_postgres = False
        self.init_db()

    def get_connection(self):
        if self.db_url:
            try:
                url = self.db_url
                if url.startswith("postgres://"):
                    url = url.replace("postgres://", "postgresql://", 1)
                conn = psycopg2.connect(url)
                self.is_postgres = True
                return conn
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}. Falling back to SQLite.")
        
        self.is_postgres = False
        sqlite_file = os.path.join(CONFIG_DIR, "bot.db")
        return sqlite3.connect(sqlite_file)

    def execute_query(self, query, params=(), commit=False, fetch=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if not self.is_postgres:
                query = query.replace("%s", "?")
            cursor.execute(query, params)
            if commit:
                conn.commit()
            if fetch == "all":
                return cursor.fetchall()
            elif fetch == "one":
                return cursor.fetchone()
            return None
        except Exception as e:
            logger.error(f"Database Query Error: {e}\nQuery: {query}\nParams: {params}")
            conn.rollback() if self.is_postgres else None
            raise e
        finally:
            cursor.close()
            conn.close()

    def init_db(self):
        # 1. Group settings table
        query_group_settings = """
        CREATE TABLE IF NOT EXISTS group_settings (
            chat_id BIGINT PRIMARY KEY,
            chat_title TEXT,
            remove_links BOOLEAN DEFAULT FALSE,
            inactive_kick_enabled BOOLEAN DEFAULT FALSE,
            inactive_threshold_minutes INTEGER DEFAULT 30,
            delete_service_join_leave BOOLEAN DEFAULT FALSE,
            delete_service_title BOOLEAN DEFAULT FALSE,
            delete_service_photo BOOLEAN DEFAULT FALSE,
            delete_service_pin BOOLEAN DEFAULT FALSE,
            auto_kick_interval_hours INTEGER DEFAULT 0,
            last_auto_kick_run BIGINT DEFAULT 0
        );
        """
        self.execute_query(query_group_settings, commit=True)

        # Apply schema migrations for existing SQLite databases
        if not self.is_postgres:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(group_settings);")
            columns = [col[1] for col in cursor.fetchall()]
            if "inactive_kick_enabled" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN inactive_kick_enabled BOOLEAN DEFAULT FALSE;")
            if "inactive_threshold_minutes" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN inactive_threshold_minutes INTEGER DEFAULT 30;")
            if "delete_service_join_leave" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN delete_service_join_leave BOOLEAN DEFAULT FALSE;")
            if "delete_service_title" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN delete_service_title BOOLEAN DEFAULT FALSE;")
            if "delete_service_photo" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN delete_service_photo BOOLEAN DEFAULT FALSE;")
            if "delete_service_pin" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN delete_service_pin BOOLEAN DEFAULT FALSE;")
            if "auto_kick_interval_hours" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN auto_kick_interval_hours INTEGER DEFAULT 0;")
            if "last_auto_kick_run" not in columns:
                cursor.execute("ALTER TABLE group_settings ADD COLUMN last_auto_kick_run BIGINT DEFAULT 0;")
            conn.commit()
            cursor.close()
            conn.close()
        else:
            for col in ["inactive_kick_enabled", "inactive_threshold_minutes", "delete_service_join_leave", "delete_service_title", "delete_service_photo", "delete_service_pin", "auto_kick_interval_hours", "last_auto_kick_run"]:
                try:
                    self.execute_query(f"ALTER TABLE group_settings ADD COLUMN {col} BIGINT DEFAULT 0;", commit=True)
                except Exception:
                    pass

        # 2. Member activity table
        query_member_activity = """
        CREATE TABLE IF NOT EXISTS member_activity (
            chat_id BIGINT,
            user_id BIGINT,
            username TEXT,
            last_active_media BIGINT,
            PRIMARY KEY (chat_id, user_id)
        );
        """
        self.execute_query(query_member_activity, commit=True)

        # 3. Userbot status table
        query_userbot_status = """
        CREATE TABLE IF NOT EXISTS userbot_config (
            user_id BIGINT PRIMARY KEY,
            is_configured BOOLEAN DEFAULT FALSE
        );
        """
        self.execute_query(query_userbot_status, commit=True)
        logger.info("Database schema initialized and verified.")

db = DBManager()

# Safe callback answer helper
def safe_answer_callback(call_id, text=None, show_alert=False):
    try:
        bot.answer_callback_query(call_id, text=text, show_alert=show_alert)
    except Exception as e:
        logger.debug(f"Callback query answer ignored (likely already answered): {e}")

# Admin verification cache
admin_cache = {}
ADMIN_CACHE_TTL = 300

def is_user_admin(chat_id, user_id):
    if chat_id == user_id:
        return True
    now = time.time()
    cache_key = (chat_id, user_id)
    if cache_key in admin_cache:
        is_admin, timestamp = admin_cache[cache_key]
        if now - timestamp < ADMIN_CACHE_TTL:
            return is_admin
    try:
        member = bot.get_chat_member(chat_id, user_id)
        is_admin = member.status in ["creator", "administrator"]
        admin_cache[cache_key] = (is_admin, now)
        return is_admin
    except ApiTelegramException as e:
        logger.error(f"Error checking admin status for user {user_id} in {chat_id}: {e}")
        return False

def register_group(chat_id, chat_title):
    query = """
    INSERT INTO group_settings (chat_id, chat_title)
    VALUES (%s, %s)
    ON CONFLICT (chat_id) 
    DO UPDATE SET chat_title = EXCLUDED.chat_title;
    """
    db.execute_query(query, (chat_id, chat_title), commit=True)

def record_member_activity(chat_id, user_id, username, is_media):
    now = int(time.time())
    if is_media:
        query = """
        INSERT INTO member_activity (chat_id, user_id, username, last_active_media)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (chat_id, user_id)
        DO UPDATE SET username = EXCLUDED.username, last_active_media = EXCLUDED.last_active_media;
        """
        db.execute_query(query, (chat_id, user_id, username, now), commit=True)
    else:
        query = """
        INSERT INTO member_activity (chat_id, user_id, username, last_active_media)
        VALUES (%s, %s, %s, NULL)
        ON CONFLICT (chat_id, user_id)
        DO UPDATE SET username = EXCLUDED.username;
        """
        db.execute_query(query, (chat_id, user_id, username), commit=True)

# Kick member logic
def kick_member(chat_id, user_id):
    try:
        bot.ban_chat_member(chat_id, user_id)
        bot.unban_chat_member(chat_id, user_id)
        db.execute_query(
            "DELETE FROM member_activity WHERE chat_id = %s AND user_id = %s",
            (chat_id, user_id),
            commit=True
        )
        return True
    except ApiTelegramException as e:
        logger.error(f"Failed to kick user {user_id} from group {chat_id}: {e}")
        return str(e.description)

# Sweep inactive members
def run_inactive_kick_sweep(chat_id, threshold_minutes):
    threshold_timestamp = int(time.time()) - (threshold_minutes * 60)
    
    inactive_members = db.execute_query(
        """
        SELECT user_id, username FROM member_activity 
        WHERE chat_id = %s AND (last_active_media IS NULL OR last_active_media < %s)
        """,
        (chat_id, threshold_timestamp),
        fetch="all"
    )

    logger.info(f"Sweep started for chat {chat_id}. Found {len(inactive_members)} inactive entries.")

    kicked_count = 0
    errors = set()
    for row in inactive_members:
        user_id, username = row[0], row[1]
        
        if is_user_admin(chat_id, user_id):
            logger.info(f"Skipped kicking admin user {user_id} ({username}) in chat {chat_id}")
            continue

        result = kick_member(chat_id, user_id)
        if result is True:
            kicked_count += 1
            logger.info(f"Successfully kicked inactive user {user_id} ({username}) from chat {chat_id}")
        else:
            errors.add(result)
            logger.warning(f"Could not kick user {user_id} ({username}): {result}")
                
    return kicked_count, list(errors)

# Check if Userbot Session is Configured
def is_userbot_configured(user_id):
    row = db.execute_query(
        "SELECT is_configured FROM userbot_config WHERE user_id = %s",
        (user_id,),
        fetch="one"
    )
    if row and row[0]:
        session_file = os.path.join(CONFIG_DIR, f"userbot_{user_id}.session")
        return os.path.exists(session_file)
    return False

# --- BOT HANDLERS ---

if bot:
    # 0. Bot Membership Update Interceptor
    @bot.my_chat_member_handler()
    def handle_bot_membership_update(update):
        global OWNER_ID
        if not OWNER_ID:
            return
        
        old = update.old_chat_member
        new = update.new_chat_member
        chat = update.chat
        
        # If bot is added/promoted as member or administrator
        if new.status in ["member", "administrator"] and old.status not in ["member", "administrator"]:
            try:
                member_count = bot.get_chat_member_count(chat.id)
            except Exception:
                member_count = "Unknown"
                
            type_map = {
                "group": "👥 Group",
                "supergroup": "⚡ Supergroup",
                "channel": "📢 Channel"
            }
            chat_type = type_map.get(chat.type, chat.type)
            
            invite_link = None
            if chat.username:
                invite_link = f"https://t.me/{chat.username}"
            
            if new.status == "administrator":
                if getattr(new, "can_invite_users", False):
                    try:
                        link_obj = bot.create_chat_invite_link(chat.id)
                        invite_link = link_obj.invite_link
                    except Exception as e:
                        logger.warning(f"Could not create invite link: {e}")
                        
            link_section = f"\n🔗 **Join Link**: {invite_link}" if invite_link else "\n🔗 **Join Link**: *Not Available (No permission to add members)*"
            
            alert_msg = (
                f"🤖 **Bot Added to New Chat!**\n\n"
                f"📁 **Title**: **{chat.title}**\n"
                f"🆔 **ID**: `{chat.id}`\n"
                f"🏷️ **Type**: {chat_type}\n"
                f"👥 **Members**: `{member_count}`"
                f"{link_section}"
            )
            
            try:
                bot.send_message(OWNER_ID, alert_msg, parse_mode="Markdown")
                logger.info(f"Sent addition alert to Bot Owner for chat {chat.id}")
            except Exception as e:
                logger.error(f"Failed to send addition alert to owner: {e}")

    # 1. Group Messages and Service Messages Interceptor
    @bot.message_handler(content_types=[
        'text', 'photo', 'video', 'document', 'audio', 'voice', 'sticker', 'video_note', 'animation',
        'new_chat_members', 'left_chat_member', 'new_chat_title', 'new_chat_photo', 'delete_chat_photo', 'pinned_message'
    ], func=lambda message: message.chat.type in ["group", "supergroup"])
    def handle_group_message(message):
        register_group(message.chat.id, message.chat.title)

        # Track joining member activity
        if message.content_type == 'new_chat_members':
            for member in message.new_chat_members:
                if not member.is_bot:
                    username = member.username or member.first_name or "Unknown"
                    record_member_activity(message.chat.id, member.id, username, is_media=False)
                    logger.info(f"Registered new group member {member.id} ({username}) in chat {message.chat.id}")

        # Record activity
        is_media = message.content_type in [
            'photo', 'video', 'document', 'audio', 'voice', 
            'sticker', 'video_note', 'animation'
        ]
        if message.from_user and not message.from_user.is_bot:
            username = message.from_user.username or message.from_user.first_name or "Unknown"
            record_member_activity(message.chat.id, message.from_user.id, username, is_media)

        # Query settings
        row = db.execute_query(
            "SELECT remove_links, delete_service_join_leave, delete_service_title, delete_service_photo, delete_service_pin FROM group_settings WHERE chat_id = %s",
            (message.chat.id,),
            fetch="one"
        )
        if not row:
            return
            
        remove_links, del_jl, del_title, del_photo, del_pin = row

        # Handle Service Message auto-deletion
        should_delete = False
        if message.content_type in ['new_chat_members', 'left_chat_member'] and del_jl:
            should_delete = True
        elif message.content_type == 'new_chat_title' and del_title:
            should_delete = True
        elif message.content_type in ['new_chat_photo', 'delete_chat_photo'] and del_photo:
            should_delete = True
        elif message.content_type == 'pinned_message' and del_pin:
            should_delete = True

        if should_delete:
            try:
                bot.delete_message(message.chat.id, message.message_id)
                logger.info(f"Deleted service message of type {message.content_type} in group {message.chat.id}")
                return
            except ApiTelegramException as e:
                logger.warning(f"Could not delete service message: {e}")

        # Link auto-removal logic
        if remove_links and message.content_type == 'text':
            has_link = False
            extracted_link = None
            if message.entities:
                for entity in message.entities:
                    if entity.type in ["url", "text_link"]:
                        has_link = True
                        if entity.type == "url":
                            extracted_link = message.text[entity.offset:entity.offset+entity.length]
                        elif entity.type == "text_link":
                            extracted_link = entity.url
                        break
            if not has_link and message.text:
                match = re.search(r'(https?://[^\s]+|www\.[^\s]+|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6}/[^\s]*)', message.text)
                if match:
                    has_link = True
                    extracted_link = match.group(0)

            if has_link:
                if not is_user_admin(message.chat.id, message.from_user.id):
                    try:
                        bot.delete_message(message.chat.id, message.message_id)
                        logger.info(f"Deleted link from {message.from_user.id} in group {message.chat.id}")
                        
                        # Forward link to Bot Owner
                        if OWNER_ID:
                            sender_username = message.from_user.username or message.from_user.first_name or "Unknown"
                            alert_msg = (
                                f"⚠️ **Auto-Deleted Link Forwarded**\n\n"
                                f"👥 **Group**: **{message.chat.title}** (`{message.chat.id}`)\n"
                                f"👤 **Sender**: @{sender_username} (ID: `{message.from_user.id}`)\n"
                                f"🔗 **Link**: {extracted_link}"
                            )
                            try:
                                bot.send_message(OWNER_ID, alert_msg)
                            except Exception as alert_err:
                                logger.error(f"Failed to forward link alert to owner: {alert_err}")
                    except ApiTelegramException as e:
                        logger.warning(f"Could not delete message: {e}")

    # 2. /dbinfo Command
    @bot.message_handler(commands=["dbinfo", "list_members"], func=lambda message: message.chat.type == "private")
    def show_db_info(message):
        user_id = message.from_user.id
        groups = db.execute_query("SELECT chat_id, chat_title, inactive_threshold_minutes FROM group_settings", fetch="all")
        admin_groups = []
        for chat_id, title, threshold in groups:
            if is_user_admin(chat_id, user_id):
                admin_groups.append((chat_id, title, threshold))

        if not admin_groups:
            bot.send_message(message.chat.id, "❌ You are not an administrator of any registered groups.")
            return

        response_text = "📊 **Group Helper Database Diagnostics**\n\n"
        for chat_id, title, threshold in admin_groups:
            response_text += f"Group: **{title}** (ID: `{chat_id}`)\n"
            response_text += f"Inactive Period: `{threshold}` minutes\n"
            
            members = db.execute_query(
                "SELECT user_id, username, last_active_media FROM member_activity WHERE chat_id = %s",
                (chat_id,),
                fetch="all"
            )
            
            if not members:
                response_text += "  (No members tracked in DB yet)\n\n"
                continue
                
            response_text += "  **Tracked Members:**\n"
            for u_id, uname, last_media in members:
                is_admin = is_user_admin(chat_id, u_id)
                admin_status = "👑 Admin" if is_admin else "👤 Member"
                
                if last_media:
                    time_diff = int(time.time()) - last_media
                    last_seen_str = f"{time_diff // 60}m ago"
                else:
                    last_seen_str = "Never sent media"
                    
                response_text += f"  - `{uname}` (ID: `{u_id}`) | {admin_status} | Media: {last_seen_str}\n"
            response_text += "\n"

        bot.send_message(message.chat.id, response_text, parse_mode="Markdown")

    # 3. /ubot Command
    @bot.message_handler(commands=["ubot"], func=lambda message: message.chat.type == "private")
    def init_userbot_command(message):
        user_id = message.from_user.id
        if is_userbot_configured(user_id):
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(text="🗑️ Delete Logged-In Userbot", callback_data=f"delete_ubot_{user_id}"))
            bot.send_message(
                message.chat.id,
                "✅ **Userbot Helper is already logged-in and active!**\n\n"
                "You can use it to scrape group member lists directly from the admin dashboard menu.",
                reply_markup=markup,
                parse_mode="Markdown"
            )
            return

        sent_msg = bot.send_message(
            message.chat.id,
            "🔑 **Userbot Setup Process**\n\n"
            "This setup authorizes a user account to scrape all existing members of your groups.\n"
            "First, please send your **API ID** (from my.telegram.org):"
        )
        bot.register_next_step_handler(sent_msg, process_api_id_step)

    def process_api_id_step(message):
        if not message.text or not message.text.strip().isdigit():
            sent_msg = bot.send_message(message.chat.id, "❌ API ID must be a number. Please try again:")
            bot.register_next_step_handler(sent_msg, process_api_id_step)
            return
        
        user_id = message.from_user.id
        temp_userbot_logins[user_id] = {"api_id": int(message.text.strip())}
        
        sent_msg = bot.send_message(message.chat.id, "🔐 Great. Now send your **API HASH**:")
        bot.register_next_step_handler(sent_msg, process_api_hash_step)

    def process_api_hash_step(message):
        if not message.text or len(message.text.strip()) < 10:
            sent_msg = bot.send_message(message.chat.id, "❌ Invalid API Hash. Please try again:")
            bot.register_next_step_handler(sent_msg, process_api_hash_step)
            return
        
        user_id = message.from_user.id
        temp_userbot_logins[user_id]["api_hash"] = message.text.strip()
        
        sent_msg = bot.send_message(
            message.chat.id,
            "📱 Now, send your **PHONE NUMBER** including country code (e.g. `+1234567890`):"
        )
        bot.register_next_step_handler(sent_msg, process_phone_step)

    def process_phone_step(message):
        if not message.text or not message.text.strip().startswith("+"):
            sent_msg = bot.send_message(message.chat.id, "❌ Phone number must start with '+' and include country code. Try again:")
            bot.register_next_step_handler(sent_msg, process_phone_step)
            return
        
        user_id = message.from_user.id
        phone = message.text.strip()
        temp_userbot_logins[user_id]["phone"] = phone
        data = temp_userbot_logins[user_id]

        bot.send_message(message.chat.id, "🔌 Initializing userbot client connection...")
        try:
            client, phone_code_hash = run_async(init_and_connect_client(
                os.path.join(CONFIG_DIR, f"userbot_{user_id}"),
                data["api_id"],
                data["api_hash"],
                phone
            ))
            data["client"] = client
            data["phone_code_hash"] = phone_code_hash
            
            sent_msg = bot.send_message(
                message.chat.id,
                "📩 **Verification Code Sent!**\n"
                "Please enter the OTP verification code received on Telegram:\n"
                "*(Tip: If the code is '12345', send it as '1 2 3 4 5' or directly '12345')*"
            )
            bot.register_next_step_handler(sent_msg, process_otp_step)
        except Exception as e:
            logger.error(f"Error starting userbot client: {e}")
            bot.send_message(message.chat.id, f"❌ Connection Error: {e}\nSetup cancelled. Send `/ubot` to restart.")
            temp_userbot_logins.pop(user_id, None)

    def process_otp_step(message):
        user_id = message.from_user.id
        if user_id not in temp_userbot_logins:
            bot.send_message(message.chat.id, "❌ Setup state lost. Send `/ubot` to restart.")
            return

        otp = message.text.strip().replace(" ", "")
        data = temp_userbot_logins[user_id]
        client = data["client"]

        try:
            run_async(sign_in_client(
                client=client,
                phone=data["phone"],
                phone_code_hash=data["phone_code_hash"],
                otp=otp
            ))
            
            db.execute_query(
                "INSERT INTO userbot_config (user_id, is_configured) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET is_configured = TRUE;",
                (user_id, True),
                commit=True
            )
            
            bot.send_message(
                message.chat.id,
                "🎉 **Success! Userbot Helper is fully logged-in and active.**\n\n"
                "The bot is now equipped to scrape group members dynamically."
            )
            run_async(disconnect_client(client))
            temp_userbot_logins.pop(user_id, None)
        except SessionPasswordNeeded:
            sent_msg = bot.send_message(message.chat.id, "🔐 **Two-Factor Authentication (2FA) is enabled.**\nPlease enter your 2FA password:")
            bot.register_next_step_handler(sent_msg, process_2fa_step)
        except Exception as e:
            logger.error(f"Sign-in error: {e}")
            bot.send_message(message.chat.id, f"❌ Authentication failed: {e}\nSend `/ubot` to restart.")
            run_async(disconnect_client(client))
            temp_userbot_logins.pop(user_id, None)

    def process_2fa_step(message):
        user_id = message.from_user.id
        if user_id not in temp_userbot_logins:
            bot.send_message(message.chat.id, "❌ Setup state lost. Send `/ubot` to restart.")
            return

        password = message.text.strip()
        data = temp_userbot_logins[user_id]
        client = data["client"]

        try:
            run_async(check_password_client(client, password))
            db.execute_query(
                "INSERT INTO userbot_config (user_id, is_configured) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET is_configured = TRUE;",
                (user_id, True),
                commit=True
            )
            bot.send_message(
                message.chat.id,
                "🎉 **Success! Userbot Helper is fully logged-in (2FA verified) and active.**"
            )
            run_async(disconnect_client(client))
            temp_userbot_logins.pop(user_id, None)
        except Exception as e:
            logger.error(f"2FA sign-in error: {e}")
            bot.send_message(message.chat.id, f"❌ 2FA verification failed: {e}\nSend `/ubot` to restart.")
            run_async(disconnect_client(client))
            temp_userbot_logins.pop(user_id, None)

    # 4. /admin Dashboard Command
    @bot.message_handler(commands=["start", "admin"], func=lambda message: message.chat.type == "private")
    def send_admin_dashboard(message):
        user_id = message.from_user.id
        groups = db.execute_query("SELECT chat_id, chat_title FROM group_settings", fetch="all")
        admin_groups = []
        for chat_id, title in groups:
            if is_user_admin(chat_id, user_id):
                admin_groups.append((chat_id, title))

        if not admin_groups:
            markup = types.InlineKeyboardMarkup(row_width=1)
            if is_userbot_configured(user_id):
                markup.add(types.InlineKeyboardButton(text="🔍 Auto-Discover Groups (Userbot)", callback_data="discover_ubot"))
            
            bot.send_message(
                message.chat.id,
                "👋 **Welcome to Group Helper Bot!**\n\n"
                "To manage a group, follow these steps:\n"
                "1. Add me to your group/supergroup as an **Administrator**.\n"
                "2. Ensure I have **Delete Messages** and **Ban Users** permissions.\n"
                "3. Send any message in the group, or configure/use the userbot auto-discover below.\n"
                "4. Use the `/admin` command here in private chat to open the controller panel.",
                reply_markup=markup if is_userbot_configured(user_id) else None,
                parse_mode="Markdown"
            )
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for chat_id, title in admin_groups:
            markup.add(types.InlineKeyboardButton(text=f"⚙️ {title}", callback_data=f"manage_{chat_id}"))

        if is_userbot_configured(user_id):
            markup.add(types.InlineKeyboardButton(text="🔍 Auto-Discover Groups (Userbot)", callback_data="discover_ubot"))

        bot.send_message(
            message.chat.id,
            "👑 **Group Helper Admin Dashboard**\n\n"
            "Select a group below to customize its management settings:",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    # Async group discover thread
    def run_async_discover_groups(user_id, admin_chat_id):
        try:
            bot.send_message(admin_chat_id, "🔍 **Userbot is scanning your dialogues to discover groups...**")
            bot_username = bot.get_me().username
            discovered = run_async(auto_discover_groups_via_client(
                os.path.join(CONFIG_DIR, f"userbot_{user_id}"),
                bot_username
            ))
            
            if discovered:
                group_list = "\n".join([f"- **{title}** (`{c_id}`)" for c_id, title in discovered])
                bot.send_message(
                    admin_chat_id,
                    f"✅ **Auto-Discovery Complete!**\n"
                    f"Successfully discovered and registered **{len(discovered)}** groups:\n{group_list}\n\n"
                    f"Use `/admin` to open the control panel.",
                    parse_mode="Markdown"
                )
            else:
                bot.send_message(
                    admin_chat_id,
                    "⚠️ **No new groups discovered.**\n"
                    "Ensure the bot is added to your groups as an Administrator."
                )
        except Exception as e:
            logger.error(f"Discovery thread failed: {e}")
            bot.send_message(admin_chat_id, f"❌ Userbot Auto-Discovery failed: {e}")

    # Async member scraper thread
    def run_async_scrape_members(chat_id, user_id, admin_chat_id):
        try:
            bot.send_message(admin_chat_id, "📥 **Userbot is starting member scraper...**")
            scraped = run_async(scrape_members_via_client(
                os.path.join(CONFIG_DIR, f"userbot_{user_id}"),
                chat_id
            ))
            bot.send_message(
                admin_chat_id,
                f"✅ **Scraping Complete!**\n"
                f"Successfully imported **{scraped}** members from the group into the tracking database."
            )
        except Exception as e:
            logger.error(f"Scraper thread failed: {e}")
            bot.send_message(admin_chat_id, f"❌ Userbot Member Scraping failed: {e}")

    # 5. Callback Handlers
    @bot.callback_query_handler(func=lambda call: True)
    def handle_callbacks(call):
        user_id = call.from_user.id
        data = call.data

        # --- MAIN GROUP SETTINGS MENU ---
        if data.startswith("manage_"):
            chat_id = int(data.split("_")[1])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            row = db.execute_query(
                "SELECT chat_title, remove_links FROM group_settings WHERE chat_id = %s",
                (chat_id,),
                fetch="one"
            )
            if not row:
                safe_answer_callback(call.id, "❌ Group not found.", show_alert=True)
                return

            title, remove_links = row
            status_links = "🟢 ON" if remove_links else "🔴 OFF"

            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton(text=f"🔗 Link Auto-Remove: {status_links}", callback_data=f"toggle_links_{chat_id}"),
                types.InlineKeyboardButton(text="🚷 Inactive Kick Settings", callback_data=f"inactive_menu_{chat_id}"),
                types.InlineKeyboardButton(text="🔙 Back to Groups", callback_data="back_to_dashboard")
            )

            try:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"🛠️ **Settings for: {title}**\n\nConfigure group control settings below:",
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
            safe_answer_callback(call.id)

        # --- INACTIVE KICK SETTINGS SUB-MENU ---
        elif data.startswith("inactive_menu_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            row = db.execute_query(
                "SELECT chat_title, inactive_threshold_minutes, auto_kick_interval_hours FROM group_settings WHERE chat_id = %s",
                (chat_id,),
                fetch="one"
            )
            title, threshold, autokick_hrs = row
            st_autokick = f"🟢 ON ({autokick_hrs}h)" if autokick_hrs > 0 else "🔴 OFF"

            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton(text=f"⏱️ Inactive Period: {threshold} Min", callback_data=f"threshold_menu_{chat_id}"),
                types.InlineKeyboardButton(text=f"🤖 Auto Kick: {st_autokick}", callback_data=f"setup_autokick_{chat_id}"),
                types.InlineKeyboardButton(text="📊 Show Activity Status", callback_data=f"status_kick_{chat_id}"),
                types.InlineKeyboardButton(text="💥 Kick Inactive Now", callback_data=f"force_kick_{chat_id}")
            )

            if is_userbot_configured(user_id):
                markup.add(types.InlineKeyboardButton(text="📥 Scrape Member List (Userbot)", callback_data=f"scrape_ubot_{chat_id}"))

            markup.add(types.InlineKeyboardButton(text="🔙 Back to Main Settings", callback_data=f"manage_{chat_id}"))

            try:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"🚷 **Inactive Kick Controls - {title}**\n\nManage inactive members removal criteria:",
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
            safe_answer_callback(call.id)

        # --- SERVICE MESSAGE DELETION SUB-MENU ---
        elif data.startswith("delmenu_"):
            chat_id = int(data.split("_")[1])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            row = db.execute_query(
                "SELECT chat_title, delete_service_join_leave, delete_service_title, delete_service_photo, delete_service_pin FROM group_settings WHERE chat_id = %s",
                (chat_id,),
                fetch="one"
            )
            if not row:
                safe_answer_callback(call.id, "❌ Settings not found.", show_alert=True)
                return
            title, j_l, title_ch, photo_ch, pin_ch = row
            
            st_jl = "🟢 ON" if j_l else "🔴 OFF"
            st_title = "🟢 ON" if title_ch else "🔴 OFF"
            st_photo = "🟢 ON" if photo_ch else "🔴 OFF"
            st_pin = "🟢 ON" if pin_ch else "🔴 OFF"

            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton(text=f"👋 Join/Leave Msg: {st_jl}", callback_data=f"togglesrv_jl_{chat_id}"),
                types.InlineKeyboardButton(text=f"📝 Title Change Msg: {st_title}", callback_data=f"togglesrv_title_{chat_id}"),
                types.InlineKeyboardButton(text=f"🖼️ Photo Change Msg: {st_photo}", callback_data=f"togglesrv_photo_{chat_id}"),
                types.InlineKeyboardButton(text=f"📌 Pinned Msg Alerts: {st_pin}", callback_data=f"togglesrv_pin_{chat_id}"),
                types.InlineKeyboardButton(text="🔙 Back to Main Settings", callback_data=f"manage_{chat_id}")
            )

            try:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"🗑️ **Service Message Deletion - {title}**\n\nToggle which service messages should be deleted automatically:",
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
            safe_answer_callback(call.id)

        # --- TOGGLE SERVICE MESSAGE SETTING ---
        elif data.startswith("togglesrv_"):
            parts = data.split("_")
            setting_type = parts[1]
            chat_id = int(parts[2])
            
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            col_map = {
                "jl": "delete_service_join_leave",
                "title": "delete_service_title",
                "photo": "delete_service_photo",
                "pin": "delete_service_pin"
            }
            col_name = col_map[setting_type]

            row = db.execute_query(f"SELECT {col_name} FROM group_settings WHERE chat_id = %s", (chat_id,), fetch="one")
            current = row[0] if row else False
            new_val = not current
            db.execute_query(f"UPDATE group_settings SET {col_name} = %s WHERE chat_id = %s", (new_val, chat_id), commit=True)
            
            safe_answer_callback(call.id, "Setting updated!")
            handle_callbacks(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"delmenu_{chat_id}", chat_instance=call.chat_instance))

        # --- THRESHOLD (INACTIVE PERIOD) SELECTOR MENU ---
        elif data.startswith("threshold_menu_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            row = db.execute_query("SELECT chat_title, inactive_threshold_minutes FROM group_settings WHERE chat_id = %s", (chat_id,), fetch="one")
            title, current_val = row

            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton(text="30 Min", callback_data=f"set_thresh_{chat_id}_30"),
                types.InlineKeyboardButton(text="60 Min", callback_data=f"set_thresh_{chat_id}_60"),
                types.InlineKeyboardButton(text="120 Min", callback_data=f"set_thresh_{chat_id}_120"),
                types.InlineKeyboardButton(text="✏️ Custom", callback_data=f"custom_thresh_{chat_id}")
            )
            markup.add(types.InlineKeyboardButton(text="🔙 Back to Controls", callback_data=f"inactive_menu_{chat_id}"))

            try:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"⏱️ **Select Inactive Period**\nGroup: {title}\n\nCurrent Inactive Period: `{current_val}` minutes",
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
            safe_answer_callback(call.id)

        # --- SET PREDEFINED THRESHOLD ---
        elif data.startswith("set_thresh_"):
            parts = data.split("_")
            chat_id = int(parts[2])
            mins = int(parts[3])

            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            db.execute_query("UPDATE group_settings SET inactive_threshold_minutes = %s WHERE chat_id = %s", (mins, chat_id), commit=True)
            safe_answer_callback(call.id, f"Inactive period updated to {mins} minutes.")
            handle_callbacks(types.CallbackQuery(id=call.id, from_user=call.from_user, message=call.message, data=f"threshold_menu_{chat_id}", chat_instance=call.chat_instance))

        # --- CUSTOM THRESHOLD HANDLER ---
        elif data.startswith("custom_thresh_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            sent_msg = bot.send_message(
                call.message.chat.id,
                "✏️ **Please enter the custom inactive period in minutes**:\n"
                "*(Send a number, e.g. 45, or send 'cancel')*",
                parse_mode="Markdown"
            )
            safe_answer_callback(call.id)
            bot.register_next_step_handler(sent_msg, process_custom_threshold_input, chat_id)

        # --- SETUP AUTO-KICK INTERVAL ---
        elif data.startswith("setup_autokick_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            sent_msg = bot.send_message(
                call.message.chat.id,
                "🤖 **Setup Automated Auto-Kick Hours**\n\n"
                "Please enter the interval in hours for auto-kicking (e.g. `24` for once every day, or enter `off` to disable auto-kicking):",
                parse_mode="Markdown"
            )
            safe_answer_callback(call.id)
            bot.register_next_step_handler(sent_msg, process_autokick_interval_input, chat_id)

        # --- ACTION CALLBACKS ---
        elif data.startswith("toggle_links_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            row = db.execute_query("SELECT remove_links FROM group_settings WHERE chat_id = %s", (chat_id,), fetch="one")
            current = row[0] if row else False
            new_val = not current
            db.execute_query("UPDATE group_settings SET remove_links = %s WHERE chat_id = %s", (new_val, chat_id), commit=True)
            
            safe_answer_callback(call.id, f"Link auto-removal: {'Enabled' if new_val else 'Disabled'}")
            
            # Edit the reply markup dynamically
            status_links = "🟢 ON" if new_val else "🔴 OFF"
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton(text=f"🔗 Link Auto-Remove: {status_links}", callback_data=f"toggle_links_{chat_id}"),
                types.InlineKeyboardButton(text="🚷 Inactive Kick Settings", callback_data=f"inactive_menu_{chat_id}"),
                types.InlineKeyboardButton(text="🔙 Back to Groups", callback_data="back_to_dashboard")
            )
            try:
                bot.edit_message_reply_markup(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=markup
                )
            except Exception as e:
                logger.error(f"Error editing reply markup: {e}")

        elif data.startswith("status_kick_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            row = db.execute_query("SELECT chat_title, inactive_threshold_minutes FROM group_settings WHERE chat_id = %s", (chat_id,), fetch="one")
            title, threshold = row
            threshold_timestamp = int(time.time()) - (threshold * 60)

            active_count = db.execute_query(
                "SELECT COUNT(*) FROM member_activity WHERE chat_id = %s AND last_active_media >= %s",
                (chat_id, threshold_timestamp),
                fetch="one"
            )[0]

            inactive_count = db.execute_query(
                "SELECT COUNT(*) FROM member_activity WHERE chat_id = %s AND (last_active_media IS NULL OR last_active_media < %s)",
                (chat_id, threshold_timestamp),
                fetch="one"
            )[0]

            bot.send_message(
                call.message.chat.id,
                f"📊 **Activity Status - {title}**\n"
                f"*(Inactive Period: {threshold} Minutes)*\n\n"
                f"🟢 **Active (Sent Media):** {active_count}\n"
                f"🔴 **Inactive (No Media):** {inactive_count}\n\n"
                f"_Note: The bot tracks users who sent messages since the bot was added._",
                parse_mode="Markdown"
            )
            safe_answer_callback(call.id)

        elif data.startswith("force_kick_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            row = db.execute_query("SELECT chat_title, inactive_threshold_minutes FROM group_settings WHERE chat_id = %s", (chat_id,), fetch="one")
            title, threshold = row

            safe_answer_callback(call.id, "🧹 Kicking inactive members...")
            kicked, errors = run_inactive_kick_sweep(chat_id, threshold)

            err_msg = ""
            if errors:
                err_msg = f"\n⚠️ **Failed to kick some members due to:**\n" + "\n".join([f"- {err}" for err in errors])

            bot.send_message(
                call.message.chat.id,
                f"💥 **Immediate Kick Complete** in **{title}**!\n"
                f"Kicked **{kicked}** members who did not send any media in the last {threshold} minutes.{err_msg}",
                parse_mode="Markdown"
            )

        elif data.startswith("scrape_ubot_"):
            chat_id = int(data.split("_")[2])
            if not is_user_admin(chat_id, user_id):
                safe_answer_callback(call.id, "❌ Access Denied.", show_alert=True)
                return

            threading.Thread(
                target=run_async_scrape_members,
                args=(chat_id, user_id, call.message.chat.id),
                daemon=True
            ).start()
            safe_answer_callback(call.id, "📥 Scraper started in background.")

        elif data == "show_ubot_info":
            bot.send_message(
                call.message.chat.id,
                "ℹ️ **Userbot Scraper Information**\n\n"
                "To scrape all members who were already in the group before the bot was added, you need to configure a helper Userbot.\n\n"
                "To do this:\n"
                "1. Send the command `/ubot` here.\n"
                "2. Provide your API ID, API Hash, Phone number, and OTP.\n"
                "3. Once authenticated, a new button will appear in the settings menu allowing you to import all members into the tracking database with one click."
            )
            safe_answer_callback(call.id)

        elif data.startswith("delete_ubot_"):
            u_id = int(data.split("_")[2])
            db.execute_query("DELETE FROM userbot_config WHERE user_id = %s", (u_id,), commit=True)
            session_file = os.path.join(CONFIG_DIR, f"userbot_{u_id}.session")
            if os.path.exists(session_file):
                try:
                    os.remove(session_file)
                except Exception:
                    pass
            bot.send_message(call.message.chat.id, "🗑️ Userbot helper configuration and session deleted successfully.")
            safe_answer_callback(call.id)

        elif data == "discover_ubot":
            if not is_userbot_configured(user_id):
                safe_answer_callback(call.id, "❌ Userbot is not configured. Send /ubot first.", show_alert=True)
                return

            threading.Thread(
                target=run_async_discover_groups,
                args=(user_id, call.message.chat.id),
                daemon=True
            ).start()
            safe_answer_callback(call.id, "🔍 Group auto-discovery started in background.")

        elif data == "back_to_dashboard":
            groups = db.execute_query("SELECT chat_id, chat_title FROM group_settings", fetch="all")
            admin_groups = []
            for chat_id, title in groups:
                if is_user_admin(chat_id, user_id):
                    admin_groups.append((chat_id, title))

            if not admin_groups:
                try:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text="❌ You are no longer an admin in any registered groups."
                    )
                except Exception as e:
                    logger.error(f"Error editing message: {e}")
                safe_answer_callback(call.id)
                return

            markup = types.InlineKeyboardMarkup(row_width=1)
            for chat_id, title in admin_groups:
                markup.add(types.InlineKeyboardButton(text=f"⚙️ {title}", callback_data=f"manage_{chat_id}"))

            try:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text="👑 **Group Helper Admin Dashboard**\n\nSelect a group to customize settings:",
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Error editing message: {e}")
            safe_answer_callback(call.id)

# --- PROCESS CUSTOM USER THRESHOLD INPUT ---
def process_custom_threshold_input(message, chat_id):
    if not message.text:
        sent_msg = bot.send_message(message.chat.id, "❌ Invalid input. Please send a valid number of minutes:")
        bot.register_next_step_handler(sent_msg, process_custom_threshold_input, chat_id)
        return

    text = message.text.strip().lower()
    if text == "cancel":
        bot.send_message(message.chat.id, "Cancelled configuration.")
        return

    if not text.isdigit():
        sent_msg = bot.send_message(message.chat.id, "❌ That's not a number. Please enter a valid number of minutes (or 'cancel'):")
        bot.register_next_step_handler(sent_msg, process_custom_threshold_input, chat_id)
        return

    mins = int(text)
    if mins <= 0:
        sent_msg = bot.send_message(message.chat.id, "❌ Minutes must be greater than 0. Try again:")
        bot.register_next_step_handler(sent_msg, process_custom_threshold_input, chat_id)
        return

    db.execute_query("UPDATE group_settings SET inactive_threshold_minutes = %s WHERE chat_id = %s", (mins, chat_id), commit=True)
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(text="🔙 Back to Settings", callback_data=f"inactive_menu_{chat_id}"))
    
    bot.send_message(
        message.chat.id,
        f"✅ Inactive period has been set to **{mins}** minutes!",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# --- PROCESS AUTOMATED AUTO-KICK INTERVAL INPUT ---
def process_autokick_interval_input(message, chat_id):
    if not message.text:
        sent_msg = bot.send_message(message.chat.id, "❌ Invalid input. Please enter a valid number of hours or 'off':")
        bot.register_next_step_handler(sent_msg, process_autokick_interval_input, chat_id)
        return

    text = message.text.strip().lower()
    if text in ["off", "disable"]:
        db.execute_query(
            "UPDATE group_settings SET auto_kick_interval_hours = 0 WHERE chat_id = %s",
            (chat_id,),
            commit=True
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(text="🔙 Back to Settings", callback_data=f"inactive_menu_{chat_id}"))
        bot.send_message(
            message.chat.id,
            "✅ **Automated Auto-Kick is now disabled!**",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        return

    if not text.isdigit():
        sent_msg = bot.send_message(message.chat.id, "❌ That's not a valid number. Please enter the interval in hours (e.g. 24) or send 'off':")
        bot.register_next_step_handler(sent_msg, process_autokick_interval_input, chat_id)
        return

    hours = int(text)
    if hours <= 0:
        sent_msg = bot.send_message(message.chat.id, "❌ Hours must be greater than 0. Try again:")
        bot.register_next_step_handler(sent_msg, process_autokick_interval_input, chat_id)
        return

    # Update hours and reset timer
    db.execute_query(
        "UPDATE group_settings SET auto_kick_interval_hours = %s, last_auto_kick_run = %s WHERE chat_id = %s",
        (hours, int(time.time()), chat_id),
        commit=True
    )

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(text="🔙 Back to Settings", callback_data=f"inactive_menu_{chat_id}"))
    
    bot.send_message(
        message.chat.id,
        f"✅ **Automated Auto-Kick has been scheduled!**\n\nThe bot will automatically sweep and kick inactive members once every **{hours}** hours.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# --- BACKGROUND AUTO-KICK PROCESS (SWEEP BY SCHEDULE INTERVAL RUNNING EVERY 1 MINUTE) ---
def run_auto_kick_loop():
    while True:
        try:
            # Fetch all chats where auto-kick is scheduled
            active_groups = db.execute_query(
                "SELECT chat_id, inactive_threshold_minutes, auto_kick_interval_hours, last_auto_kick_run FROM group_settings WHERE auto_kick_interval_hours > 0",
                fetch="all"
            )

            now = int(time.time())
            for row in active_groups:
                chat_id, threshold, interval_hours, last_run = row
                
                # Check if the scheduled interval has passed
                if now - last_run >= (interval_hours * 3600):
                    logger.info(f"Running scheduled auto-kick sweep for chat {chat_id} (Every {interval_hours} hours)...")
                    kicked, errors = run_inactive_kick_sweep(chat_id, threshold)
                    
                    # Update last run timestamp
                    db.execute_query(
                        "UPDATE group_settings SET last_auto_kick_run = %s WHERE chat_id = %s",
                        (now, chat_id),
                        commit=True
                    )
                    
                    if kicked > 0:
                        logger.info(f"Successfully auto-kicked {kicked} members in scheduled sweep.")
                    if errors:
                        logger.warning(f"Auto-kick sweep encountered errors: {errors}")
        except Exception as e:
            logger.error(f"Error in background auto-kick loop: {e}")

        # Check schedule once every 60 seconds
        time.sleep(60)

# Start auto kick loop thread
auto_kick_thread = threading.Thread(target=run_auto_kick_loop, daemon=True)
auto_kick_thread.start()

# --- WEB SERVICE KEEP-ALIVE SYSTEM ---
def run_ping_server():
    port = int(os.environ.get("PORT", 8080))
    class PingHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot is alive and running!")
            
    socketserver.TCPServer.allow_reuse_address = True
    try:
        with socketserver.TCPServer(("", port), PingHandler) as httpd:
            logger.info(f"Keep-alive HTTP server started on port {port}")
            httpd.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start HTTP keep-alive server: {e}")

def run_self_ping_loop():
    ping_url = os.environ.get("WEB_URL") or os.environ.get("PING_URL")
    if not ping_url:
        logger.info("No WEB_URL or PING_URL configured. Self-ping keep-alive loop skipped.")
        return
        
    logger.info(f"Self-ping keep-alive loop started for: {ping_url}")
    while True:
        # Sleep 10 minutes (600 seconds)
        time.sleep(600)
        try:
            req = urllib.request.Request(
                ping_url, 
                headers={'User-Agent': 'Bot-Keep-Alive'}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                response.read()
            logger.info("Keep-alive self-ping successful.")
        except Exception as e:
            logger.warning(f"Keep-alive self-ping failed: {e}")

# --- STARTUP FUNCTION ---
if __name__ == "__main__":
    if not BOT_TOKEN:
        logger.error("Failed to start bot: BOT_TOKEN is missing!")
    else:
        # Start keep-alive server thread
        logger.info("Starting keep-alive HTTP server...")
        threading.Thread(target=run_ping_server, daemon=True).start()
        
        # Start self-ping loop thread
        logger.info("Starting self-ping keep-alive loop...")
        threading.Thread(target=run_self_ping_loop, daemon=True).start()

        while True:
            try:
                logger.info("Starting Group Helper Bot polling...")
                bot.infinity_polling(timeout=60, long_polling_timeout=5)
            except Exception as e:
                logger.error(f"Polling crash detected: {e}. Restarting polling loop in 5 seconds...")
                time.sleep(5)
