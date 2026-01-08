import asyncio
import logging
import aiosqlite
import aiohttp
import time
import hashlib
import json
import math
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, CommandObject, ChatMemberUpdatedFilter
from aiohttp import web
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, ChatJoinRequest
from tonsdk.utils import Address
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# Импорт аналитики
from analytics import init_analytics_db, handle_analytics, handle_analytics_funnel

from config import (
    BOT_TOKEN,
    ADMIN_ID,
    CONTRACT_ADDRESS,
    TONCENTER_API_KEY,
    API_URL,
)

# ==========================================
# 📢 НАСТРОЙКИ
# ==========================================
LIVE_CHANNEL_ID = "@tw10x"       
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = "https://tw10x.app/webhook"
WEB_SERVER_HOST = "127.0.0.1"
WEB_SERVER_PORT = 8080
GAS_RESERVE = 0.2 

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==========================================
# 🧠 UTILS & DB
# ==========================================
def normalize_address(addr_str):
    try:
        return Address(addr_str).to_string(is_user_friendly=False)
    except Exception:
        return None

def display_addr(raw_addr):
    try:
        return Address(raw_addr).to_string(is_user_friendly=True, is_url_safe=True, is_bounceable=False)
    except:
        return raw_addr

def short_addr(addr):
    if not addr: return "Unknown"
    readable = display_addr(addr)
    return f"{readable[:4]}...{readable[-4:]}"

def json_no_cache(data):
    return web.json_response(data, headers={
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0"
    })

async def init_db():
    async with aiosqlite.connect("lottery.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER,
                sender TEXT,
                amount REAL,
                tx_hash TEXT UNIQUE,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                wallet_address TEXT,
                referrer_id INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT,
                start_ts_ms INTEGER,
                end_ts_ms INTEGER,
                tickets_count INTEGER DEFAULT 0,
                tickets_commit_hash TEXT,
                winner_wallet TEXT,
                winner_ticket_tx TEXT,
                prize_expected_ton REAL,
                withdraw_script TEXT,
                withdraw_tx_hash TEXT,
                completed_ts_ms INTEGER
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER,
                event TEXT,
                payload_json TEXT,
                ts_ms INTEGER
            )
        """)
        
        async with db.execute("SELECT COUNT(*) FROM rounds") as cursor:
            if (await cursor.fetchone())[0] == 0:
                now = int(time.time() * 1000)
                end = now + (7 * 24 * 3600 * 1000)
                await db.execute("INSERT INTO rounds (status, start_ts_ms, end_ts_ms) VALUES (?, ?, ?)", 
                                 ('ACTIVE', now, end))
                await db.commit()
                logging.info("🆕 Created First Round #1")
        await db.commit()

async def log_audit(round_id, event, payload):
    async with aiosqlite.connect("lottery.db") as db:
        await db.execute("INSERT INTO audit_log (round_id, event, payload_json, ts_ms) VALUES (?, ?, ?, ?)",
                         (round_id, event, json.dumps(payload), int(time.time()*1000)))
        await db.commit()

# --- CORE LOGIC (STRICT) ---

async def get_active_round_id():
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT id FROM rounds WHERE status='ACTIVE' ORDER BY id DESC LIMIT 1") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None 

async def add_ticket(sender, amount, tx_hash):
    sender_raw = normalize_address(sender)
    round_id = await get_active_round_id()
    
    if not round_id:
        logging.error(f"❌ REJECTED TICKET {tx_hash}: No Active Round")
        return False

    async with aiosqlite.connect("lottery.db") as db:
        try:
            await db.execute("INSERT INTO tickets (round_id, sender, amount, tx_hash) VALUES (?, ?, ?, ?)", 
                             (round_id, sender_raw, amount, tx_hash))
            await db.commit()
            return True
        except aiosqlite.IntegrityError: return False

async def set_user_wallet(user_id, wallet_address):
    clean_addr = normalize_address(wallet_address)
    if not clean_addr: return False
    async with aiosqlite.connect("lottery.db") as db:
        await db.execute("""
            INSERT INTO users (user_id, wallet_address) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET wallet_address=excluded.wallet_address
        """, (user_id, clean_addr))
        await db.commit()
    return True

async def register_referral(user_id, referrer_id):
    if user_id == referrer_id: return False 
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]: return False 
        await db.execute("""
            INSERT INTO users (user_id, referrer_id) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET referrer_id=excluded.referrer_id
        """, (user_id, referrer_id))
        await db.commit()
        return True

async def get_referrer_wallet(user_id):
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or not row[0]: return None
            referrer_id = row[0]
        async with db.execute("SELECT wallet_address FROM users WHERE user_id = ?", (referrer_id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]: return row[0]
    return None

async def get_ref_stats(user_id):
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (user_id,)) as cursor:
            count = (await cursor.fetchone())[0]
    earnings = count * 0.5
    return {"invited": count, "earned": earnings}

async def has_active_ticket(user_id):
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT wallet_address FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or not row[0]: return False 
            wallet = row[0]
        
        round_id = await get_active_round_id()
        if not round_id: return False

        async with db.execute("SELECT COUNT(*) FROM tickets WHERE sender = ? AND round_id = ?", (wallet, round_id)) as cursor:
            count = (await cursor.fetchone())[0]
            return count > 0

# --- ANALYTICS ---
async def get_analytics_24h():
    since_ts = int((time.time() - 24 * 3600) * 1000)
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall("SELECT event, COUNT(*) AS cnt FROM analytics_events WHERE ts_ms >= ? GROUP BY event", (since_ts,))
    stats = {r["event"]: r["cnt"] for r in rows}
    def v(key): return stats.get(key, 0)
    opened = max(v("open_rules"), 1)
    return {
        "open": v("open_rules"), "rules": v("open_rules"), "wallet": v("connect_wallet"),
        "enter": v("enter_game_click"), "success": v("tx_success"),
        "p_success": round(v("tx_success") / opened * 100),
        "p_rules": round(v("open_rules") / opened * 100),
        "p_wallet": round(v("connect_wallet") / opened * 100),
        "p_enter": round(v("enter_game_click") / opened * 100),
    }

async def get_analytics_7d():
    since_ts = int((time.time() - 7 * 24 * 3600) * 1000)
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall("SELECT event, COUNT(*) AS cnt FROM analytics_events WHERE ts_ms >= ? GROUP BY event", (since_ts,))
    stats = {r["event"]: r["cnt"] for r in rows}
    def v(key): return stats.get(key, 0)
    base = max(v("open_rules"), 1)
    return {
        "open": v("open_rules"), "success": v("tx_success"), "p_success": round(v("tx_success") / base * 100),
        "p_rules": round(v("open_rules") / base * 100),
        "p_wallet": round(v("connect_wallet") / base * 100),
        "p_enter": round(v("enter_game_click") / base * 100),
        "rules": v("open_rules"), "wallet": v("connect_wallet"), "enter": v("enter_game_click"),
    }

# ==========================================
# 🔁 SCANNER & BALANCE
# ==========================================
async def get_contract_balance():
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://toncenter.com/api/v2/getAddressBalance?address={CONTRACT_ADDRESS}&api_key={TONCENTER_API_KEY}"
            async with session.get(url) as r:
                data = await r.json()
                if data["ok"]:
                    return int(data["result"]) / 1e9
    except Exception as e: logging.error(f"Balance check error: {e}")
    return 0.0

async def send_to_channel(sender, amount, tx_hash):
    current_jackpot = await get_contract_balance()
    full_addr = display_addr(sender)
    
    text = (
        f"🎟 <b>Новый участник!</b>\n\n"
        f"👤 <code>{full_addr}</code>\n"
        f"💰 Вход: <b>{amount} TON</b>\n"
        f"🏦 <b>Банк игры: {current_jackpot:.1f} TON</b>\n\n"
        f"🔗 <a href='https://tonviewer.com/transaction/{tx_hash}'>Explorer</a>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎰 Участвовать", url="https://t.me/tw10x_official_bot")]])
    try: await bot.send_message(chat_id=LIVE_CHANNEL_ID, text=text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except: pass

async def check_deposits():
    params = {"address": CONTRACT_ADDRESS, "limit": 20, "archival": "true"}
    if TONCENTER_API_KEY: params["api_key"] = TONCENTER_API_KEY
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL, params=params) as r:
                data = await r.json()
                if not data.get("ok"): return
                for tx in reversed(data["result"]):
                    in_msg = tx.get("in_msg")
                    if not in_msg: continue
                    value = int(in_msg.get("value", 0))
                    source = in_msg.get("source")
                    tx_hash = tx["transaction_id"]["hash"]
                    if value >= 5_000_000_000 and source:
                        ton_amount = value / 1_000_000_000
                        if await add_ticket(source, ton_amount, tx_hash):
                            logging.info(f"🎟 Ticket: {ton_amount} TON from {source}")
                            await send_to_channel(source, ton_amount, tx_hash)
    except Exception: pass

async def scheduler():
    while True:
        await check_deposits()
        await asyncio.sleep(15)

# ==========================================
# 📐 INTERNAL HELPERS (LOGIC LAYER)
# ==========================================
async def _get_round_data():
    balance = await get_contract_balance()
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM rounds WHERE status='ACTIVE' ORDER BY id DESC LIMIT 1") as c:
            row = await c.fetchone()
        
        async with db.execute("SELECT COUNT(*) FROM users") as c: 
            total_protocol_users = (await c.fetchone())[0]

    if not row: return None

    round_id = row['id']
    start_ts = row['start_ts_ms'] // 1000
    end_ts = row['end_ts_ms'] // 1000
    now_ts = int(time.time())

    progress = 0
    if end_ts > start_ts:
        progress = min(100.0, max(0.0, (now_ts - start_ts) / (end_ts - start_ts) * 100))

    async with aiosqlite.connect("lottery.db") as db:
         async with db.execute("SELECT COUNT(*) FROM tickets WHERE round_id=?", (round_id,)) as c: 
             active_tickets = (await c.fetchone())[0]
         async with db.execute("SELECT COUNT(DISTINCT sender) FROM tickets WHERE round_id=?", (round_id,)) as c: 
             active_players = (await c.fetchone())[0]

    return {
        "id": round_id,
        "status": "ACTIVE",
        "start_ts": start_ts,
        "end_ts": end_ts,
        "duration_sec": end_ts - start_ts,
        "progress": round(progress, 2),
        "pool": {
            "total": balance,
            "jackpot": balance * 0.4,
            "holders": balance * 0.3,
            "treasury": balance * 0.2,
            "dev": balance * 0.1
        },
        "stats": {
            "tickets": active_tickets,
            "players": active_players,
            "total_protocol_users": total_protocol_users
        }
    }

async def _get_user_tickets_internal(addr):
    if not addr: return {"address": None, "active_round_id": None, "tickets": []}
    clean_addr = normalize_address(addr)
    active_round_id = await get_active_round_id()

    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT round_id, amount, tx_hash, timestamp FROM tickets WHERE sender=? ORDER BY id DESC", 
                              (clean_addr,)) as cursor:
            rows = await cursor.fetchall()

    tickets = []
    for r in rows:
        t_round_id = r[0]
        status = "ACTIVE" if active_round_id and t_round_id == active_round_id else "ARCHIVED"
        tickets.append({
            "round_id": t_round_id, "amount": r[1], "tx_hash": r[2], "status": status, "timestamp": r[3]
        })

    return {"address": clean_addr, "active_round_id": active_round_id, "tickets": tickets}

# ==========================================
# 🌐 API HANDLERS - STRICT v1
# ==========================================
async def handle_index(request): return web.FileResponse("./webapp/index.html")

async def handle_v1_current_round(request):
    data = await _get_round_data()
    if not data: return json_no_cache({"round": {"status": "NONE"}})
    
    return json_no_cache({
        "round_id": data["id"],
        "status": "ACTIVE",
        "start_ts": data["start_ts"],
        "end_ts": data["end_ts"],
        "duration_sec": data["duration_sec"],
        "progress": data["progress"],
        "pool": data["pool"],
        "stats": {"tickets": data["stats"]["tickets"], "players": data["stats"]["players"]},
        "source": "on-chain",
        "contract": CONTRACT_ADDRESS
    })

async def handle_v1_completed_rounds(request):
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall("""
            SELECT id, winner_wallet, prize_expected_ton, withdraw_tx_hash, completed_ts_ms 
            FROM rounds 
            WHERE status='COMPLETED' AND completed_ts_ms IS NOT NULL
            ORDER BY id DESC LIMIT 10
        """)
    result = []
    for r in rows:
        result.append({
            "round_id": r["id"],
            "completed_ts": r["completed_ts_ms"] // 1000,
            "winner_wallet": short_addr(r["winner_wallet"]),
            "prize_ton": r["prize_expected_ton"],
            "payout_tx": r["withdraw_tx_hash"],
            "proof_url": f"https://tonviewer.com/transaction/{r['withdraw_tx_hash']}"
        })
    return json_no_cache({"rounds": result})

async def handle_v1_user_tickets(request):
    addr = request.query.get("address")
    data = await _get_user_tickets_internal(addr)
    return json_no_cache(data)

async def handle_v1_user_chance(request):
    addr = request.query.get("address")
    if not addr: return json_no_cache({"chance_percent": 0})
    clean_addr = normalize_address(addr)
    active_round_id = await get_active_round_id()
    if not active_round_id: return json_no_cache({"chance_percent": 0})

    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT COUNT(*) FROM tickets WHERE round_id=?", (active_round_id,)) as c:
            total_tickets = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM tickets WHERE round_id=? AND sender=?", (active_round_id, clean_addr)) as c:
            user_tickets = (await c.fetchone())[0]

    chance = 0.0
    if total_tickets > 0: chance = (user_tickets / total_tickets) * 100
    return json_no_cache({
        "round_id": active_round_id, "tickets_user": user_tickets, "tickets_total": total_tickets, "chance_percent": round(chance, 2)
    })

# ==========================================
# 🦖 LEGACY ADAPTERS (FIXED FOR LANDING)
# ==========================================

async def handle_legacy_global_stats(request):
    data = await _get_round_data()
    if not data: return json_no_cache({"status": "NONE"})

    # 🔥 FIX: Manually constructing the structure expected by index.html
    return json_no_cache({
        "round_id": data["id"],
        "round_start": data["start_ts"],
        "round_end": data["end_ts"],
        "round_end_ts": data["end_ts"], 
        "roundStart": data["start_ts"],
        "roundEnd": data["end_ts"],
        "current_pool": data["pool"]["total"],
        "jackpot": data["pool"]["total"],
        "total_tickets": data["stats"]["tickets"],
        "total_users": data["stats"]["total_protocol_users"],
        "balances": {
            "prize": data["pool"]["total"],      # <--- index.html needs "prize"
            "jackpot": data["pool"]["jackpot"],
            "holder": data["pool"]["holders"],   # <--- index.html needs "holder" (singular)
            "holders": data["pool"]["holders"],
            "treasury": data["pool"]["treasury"],
            "dev": data["pool"]["dev"],
            "total": data["pool"]["total"]
        },
        "contract": CONTRACT_ADDRESS,
        "source": "on-chain"
    })

# 🔥 FIX: New Adapter for History to match index.html keys
async def handle_legacy_rounds(request):
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall("""
            SELECT id, winner_wallet, prize_expected_ton, withdraw_tx_hash, completed_ts_ms 
            FROM rounds 
            WHERE status='COMPLETED' AND completed_ts_ms IS NOT NULL
            ORDER BY id DESC LIMIT 10
        """)
    
    # Map to format: id, date, winner, prize, tx
    rounds = []
    for r in rows:
        rounds.append({
            "id": r["id"],
            "date": r["completed_ts_ms"] // 1000,
            "winner": short_addr(r["winner_wallet"]),
            "prize": r["prize_expected_ton"],
            "tx": r["withdraw_tx_hash"]
        })
    return json_no_cache({"rounds": rounds})

async def handle_legacy_user_tickets(request):
    addr = request.query.get("address")
    v1_data = await _get_user_tickets_internal(addr)
    history = []
    for t in v1_data.get("tickets", []):
        history.append({
            "amount": t["amount"],
            "hash": t["tx_hash"][:8]+"...",
            "time": t["timestamp"],
            "status": t["status"],
            "is_active": (t["status"] == "ACTIVE")
        })
    return json_no_cache({"history": history})

# Legacy Utils
async def handle_save_wallet(request):
    try:
        data = await request.json()
        await set_user_wallet(int(data.get("user_id")), data.get("wallet"))
        return json_no_cache({"status": "ok"})
    except: return json_no_cache({"error": "err"})

async def handle_get_referrer(request):
    uid = request.query.get("user_id")
    if not uid: return json_no_cache({})
    return json_no_cache({"ref_wallet": await get_referrer_wallet(int(uid))})

async def handle_ref_stats(request):
    uid = request.query.get("user_id")
    if not uid: return json_no_cache({"invited": 0, "earned": 0})
    return json_no_cache(await get_ref_stats(int(uid)))

@dp.chat_join_request()
async def join_request_handler(update: ChatJoinRequest, bot: Bot):
    user_id = update.from_user.id
    if await has_active_ticket(user_id):
        try:
            await update.approve()
            await bot.send_message(user_id, "✅ <b>VIP-клуб открыт!</b>", parse_mode="HTML")
        except: pass
    else:
        try:
            await update.decline()
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Купить билет", web_app=WebAppInfo(url="https://tw10x.app"))]])
            await bot.send_message(user_id, "⛔️ <b>Нужен билет!</b>", parse_mode="HTML", reply_markup=kb)
        except: pass

# ==========================================
# 👮‍♂️ COMMANDS
# ==========================================
@dp.message(Command("stop_round"))
async def stop_round(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM rounds WHERE status='ACTIVE' ORDER BY id DESC LIMIT 1") as cursor:
            round_row = await cursor.fetchone()
        if not round_row: return await message.answer("❌ Нет активного раунда.")
        round_id = round_row["id"]
        async with db.execute("SELECT tx_hash FROM tickets WHERE round_id=? ORDER BY tx_hash ASC", (round_id,)) as cursor:
            tickets = await cursor.fetchall()
        if not tickets:
            await db.execute("UPDATE rounds SET status='STOPPED', tickets_count=0 WHERE id=?", (round_id,))
            await db.commit()
            await message.answer(f"🛑 Раунд #{round_id} остановлен (пусто).")
            return
        joined = "".join(t["tx_hash"] for t in tickets)
        commit_hash = hashlib.sha256(joined.encode()).hexdigest()
        await db.execute("UPDATE rounds SET status='STOPPED', tickets_count=?, tickets_commit_hash=? WHERE id=?", (len(tickets), commit_hash, round_id))
        await db.commit()
        await log_audit(round_id, "STOP_ROUND", {"tickets": len(tickets), "commit_hash": commit_hash})
    await message.answer(f"🛑 <b>Раунд #{round_id} ОСТАНОВЛЕН</b>\n🎟 Билетов: {len(tickets)}\n🔐 Commit Hash: <code>{commit_hash}</code>", parse_mode="HTML")

@dp.message(Command("select_winner"))
async def select_winner(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM rounds WHERE status='STOPPED' ORDER BY id DESC LIMIT 1") as cursor:
            round_row = await cursor.fetchone()
        if not round_row: return await message.answer("❌ Нет раунда STOPPED.")
        round_id = round_row["id"]
        commit_hash = round_row["tickets_commit_hash"]
        async with db.execute("SELECT tx_hash, sender FROM tickets WHERE round_id=? ORDER BY tx_hash ASC", (round_id,)) as cursor:
            tickets = await cursor.fetchall()
        last_tx = tickets[-1]["tx_hash"]
        raw_entropy = f"{last_tx}{commit_hash}{round_id}"
        entropy_hash = hashlib.sha256(raw_entropy.encode()).hexdigest()
        winner_index = int(entropy_hash, 16) % len(tickets)
        winner = tickets[winner_index]
        await db.execute("UPDATE rounds SET status='WINNER_SELECTED', winner_wallet=?, winner_ticket_tx=? WHERE id=?", (winner["sender"], winner["tx_hash"], round_id))
        await db.commit()
        await log_audit(round_id, "SELECT_WINNER", {"winner": winner["sender"], "tx": winner["tx_hash"], "entropy_src": raw_entropy})
    await message.answer(f"🏆 <b>Победитель выбран!</b>\n👤 <code>{display_addr(winner['sender'])}</code>\n🎟 TX: <code>{winner['tx_hash']}</code>\n\n👉 Введи: <code>/prepare_withdraw</code>", parse_mode="HTML")

@dp.message(Command("prepare_withdraw"))
async def prepare_withdraw(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    current_balance = await get_contract_balance()
    if current_balance <= GAS_RESERVE: return await message.answer(f"❌ Баланс ({current_balance}) мал.")
    payout_amount = round(current_balance - GAS_RESERVE, 2)
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM rounds WHERE status='WINNER_SELECTED' ORDER BY id DESC LIMIT 1") as cursor:
            round_row = await cursor.fetchone()
        if not round_row: return await message.answer("❌ Нет раунда WINNER_SELECTED.")
        round_id = round_row["id"]
        winner_raw = round_row["winner_wallet"]
        winner_friendly = display_addr(winner_raw)
        script = f"""import {{ Address, beginCell, toNano }} from '@ton/core'; import {{ NetworkProvider }} from '@ton/blueprint'; export async function run(provider: NetworkProvider) {{ const sender = provider.sender(); const body = beginCell().storeUint(1, 32).storeCoins(toNano('{payout_amount}')).storeAddress(Address.parse('{winner_friendly}')).endCell(); await sender.send({{ to: Address.parse('{CONTRACT_ADDRESS}'), value: toNano('0.05'), body, }}); console.log('✅ Transaction sent'); }}"""
        await db.execute("UPDATE rounds SET status='WITHDRAW_PREPARED', prize_expected_ton=?, withdraw_script=? WHERE id=?", (payout_amount, script, round_id))
        await db.commit()
        await log_audit(round_id, "PREPARE_WITHDRAW", {"amount": payout_amount, "winner": winner_friendly})
    await message.answer(f"📦 <b>Скрипт создан:</b> <code>withdraw_round_{round_id}.ts</code>\n💰 К выплате: {payout_amount} TON\n<pre>{script}</pre>", parse_mode="HTML")

@dp.message(Command("confirm_withdraw"))
async def confirm_withdraw(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    parts = message.text.split()
    if len(parts) != 2: return await message.answer("⚠️ Введи хэш: /confirm_withdraw TX_HASH")
    tx_hash = parts[1]
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM rounds WHERE status='WITHDRAW_PREPARED' ORDER BY id DESC LIMIT 1") as cursor:
            round_row = await cursor.fetchone()
        if not round_row: return await message.answer("❌ Нет раунда в ожидании.")
        round_id = round_row['id']
        winner_full_addr = display_addr(round_row['winner_wallet'])
        await db.execute("UPDATE rounds SET status='COMPLETED', withdraw_tx_hash=?, completed_ts_ms=? WHERE id=?", (tx_hash, int(time.time() * 1000), round_id))
        now = int(time.time() * 1000)
        end = now + (7 * 24 * 3600 * 1000)
        await db.execute("INSERT INTO rounds (status, start_ts_ms, end_ts_ms, tickets_count) VALUES (?, ?, ?, ?)", ('ACTIVE', now, end, 0))
        await db.commit()
        await log_audit(round_id, "CONFIRM_WITHDRAW", {"tx_hash": tx_hash})
    try:
        text = (f"🎉 <b>РАУНД #{round_id} ЗАВЕРШЕН!</b>\n\n🏆 Победитель: <code>{winner_full_addr}</code>\n💰 Приз отправлен: <b>{round_row['prize_expected_ton']} TON</b>\n🔗 <a href='https://tonviewer.com/transaction/{tx_hash}'>Proof of Payout</a>\n\n🚀 <b>Новый раунд уже начался!</b>")
        await bot.send_message(LIVE_CHANNEL_ID, text, parse_mode="HTML", disable_web_page_preview=True)
    except: pass
    await message.answer("✅ Раунд закрыт. Новый создан.")

@dp.message(Command("analytics"))
async def analytics_cmd(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    data = await get_analytics_24h()
    text = (f"📊 <b>Analytics (24h)</b>\n\n👀 Open: {data['open']}\n📜 Rules: {data['rules']} ({data['p_rules']}%)\n🔗 Wallet: {data['wallet']} ({data['p_wallet']}%)\n🎟 Enter: {data['enter']} ({data['p_enter']}%)\n💸 Success: {data['success']} ({data['p_success']}%)")
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("analytics_week"))
async def analytics_week_cmd(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    data = await get_analytics_7d()
    text = (f"📊 <b>Analytics (7 days)</b>\n\n👀 Open: {data['open']}\n📜 Rules: {data['rules']} ({data['p_rules']}%)\n🔗 Wallet: {data['wallet']} ({data['p_wallet']}%)\n🎟 Enter: {data['enter']} ({data['p_enter']}%)\n💸 Success: {data['success']} ({data['p_success']}%)")
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("id"))
async def get_id_cmd(message: types.Message):
    await message.answer(f"🆔 Chat ID: {message.chat.id}", parse_mode="Markdown")

@dp.message(Command("stats"))
async def admin_stats(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    bal = await get_contract_balance()
    await message.answer(f"📊 <b>ADMIN STATS</b>\n💰 Balance: {bal} TON", parse_mode="HTML")

@dp.message(Command("broadcast"))
async def broadcast_cmd(message: types.Message, command: CommandObject):
    if message.from_user.id != int(ADMIN_ID): return
    if not command.args: return
    text = command.args
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT user_id FROM users") as cursor: users = await cursor.fetchall()
    count = 0
    for row in users:
        try:
            await bot.send_message(chat_id=row[0], text=text, parse_mode="HTML")
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    await message.answer(f"✅ Sent to {count} users")

@dp.message(CommandStart())
async def start_cmd(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    args = command.args
    if args and args.startswith("ref_"):
        try:
            referrer_id = int(args.split("_")[1])
            await register_referral(user_id, referrer_id)
        except: pass
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎰 ИГРАТЬ (Mini App)", web_app=WebAppInfo(url="https://tw10x.app/game.html"))]])
    await message.answer("👋 <b>TW10X Game</b>\n\nУчаствуй & Побеждай!\nЖивой лог: " + LIVE_CHANNEL_ID + "\n\nЖми кнопку ниже, чтобы начать.", reply_markup=kb, parse_mode="HTML")

# ==========================================
# 🚀 MAIN (REGISTERING ROUTES)
# ==========================================
async def main():
    await init_db()
    await init_analytics_db()
    
    app = web.Application()
    
    # 1. CLEAN API v1 (The Truth)
    app.router.add_get("/api/v1/round/current", handle_v1_current_round)
    app.router.add_get("/api/v1/rounds/completed", handle_v1_completed_rounds)
    app.router.add_get("/api/v1/user/tickets", handle_v1_user_tickets)
    app.router.add_get("/api/v1/user/chance", handle_v1_user_chance)
    
    # 2. LEGACY ADAPTERS (For Landing)
    app.router.add_get("/api/public/overview", handle_legacy_global_stats) # Main Landing Stats
    app.router.add_get("/api/public/rounds", handle_legacy_rounds)         # 🔥 FIX: History
    app.router.add_get("/api/global_stats", handle_legacy_global_stats)     # Fallback
    app.router.add_get("/api/user", handle_legacy_user_tickets)             # Legacy
    
    # 3. UTILS
    app.router.add_post("/api/save_wallet", handle_save_wallet)
    app.router.add_get("/api/referrer", handle_get_referrer)
    app.router.add_get("/api/ref_stats", handle_ref_stats)
    app.router.add_post("/api/analytics", handle_analytics)
    app.router.add_get("/api/analytics_funnel", handle_analytics_funnel)
    
    # Static & Index
    app.router.add_get("/", handle_index)
    app.router.add_static("/webapp", path="./webapp")

    handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_SERVER_HOST, WEB_SERVER_PORT)
    await site.start()
    
    await bot.delete_webhook() 
    await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True, allowed_updates=["message", "chat_join_request", "callback_query"])
    
    asyncio.create_task(scheduler())
    logging.info(f"🚀 SYSTEM ONLINE v5.3 (LEGACY FIX). Live: {LIVE_CHANNEL_ID}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
