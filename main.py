import asyncio
import logging
import aiosqlite
import aiohttp
import time
import hashlib
from analytics import init_analytics_db, handle_analytics, handle_analytics_funnel
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, CommandObject, ChatMemberUpdatedFilter
from aiohttp import web
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, ChatJoinRequest
from tonsdk.utils import Address
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.exceptions import TelegramForbiddenError

from config import (
    BOT_TOKEN,
    ADMIN_ID,
    CONTRACT_ADDRESS,
    TONCENTER_API_KEY,
    API_URL,
    WALLET_DEV, WALLET_TREASURY, WALLET_JACKPOT, WALLET_HOLDER_DROP
)

# ==========================================
# 📢 НАСТРОЙКИ КАНАЛОВ
# ==========================================
LIVE_CHANNEL_ID = "@tw10x"       
# ==========================================

# === CONFIG ===
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = "https://tw10x.app/webhook"
WEB_SERVER_HOST = "127.0.0.1"
WEB_SERVER_PORT = 8080

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

CACHE_BALANCE = {"value": 0.0, "time": 0} 
CACHE_TTL = 60 

# Кэш для публичных балансов (v3.0 PR-3)
CACHE_BALANCES = {
    "data": {
        "prize": 0, "dev": 0, "treasury": 0, "jackpot": 0, "holder": 0
    },
    "last_update": 0
}

# ==========================================
# 🧠 UTILS & DB
# ==========================================
def normalize_address(addr_str):
    try:
        return Address(addr_str).to_string(is_user_friendly=True, is_url_safe=True, is_bounceable=True)
    except Exception:
        return None

def short_addr(addr):
    if not addr: return "Unknown"
    return f"{addr[:4]}...{addr[-4:]}"

async def init_db():
    async with aiosqlite.connect("lottery.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender TEXT,
                amount REAL,
                tx_hash TEXT UNIQUE,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                round_id INTEGER
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
        # rounds создается через migrate_v3.py
        await db.commit()

# ==========================================
# 🧱 ROUND ENGINE v3.0
# ==========================================

async def get_active_round():
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM rounds WHERE status='ACTIVE' ORDER BY id DESC LIMIT 1") as cursor:
            return await cursor.fetchone()

async def create_new_round():
    now_ms = int(time.time() * 1000)
    end_ms = now_ms + (7 * 24 * 3600 * 1000)
    async with aiosqlite.connect("lottery.db") as db:
        await db.execute("INSERT INTO rounds (status, start_ts_ms, end_ts_ms, tickets_count) VALUES (?, ?, ?, ?)", 
                         ('ACTIVE', now_ms, end_ms, 0))
        await db.commit()
    logging.info("♻️ New Round Created")

async def close_round_logic(round_id):
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        
        async with db.execute("SELECT * FROM tickets WHERE round_id=? ORDER BY tx_hash ASC", (round_id,)) as cursor:
            tickets = await cursor.fetchall()
            
        if not tickets:
            logging.warning(f"Round {round_id} empty. Restarting.")
            await db.execute("UPDATE rounds SET status='FINISHED_EMPTY', closed_ts_ms=? WHERE id=?", (int(time.time()*1000), round_id))
            await db.commit()
            await create_new_round()
            return

        # Variant A: Lexicographically last tx_hash
        seed_source = tickets[-1]['tx_hash'] 
        seed_hash = hashlib.sha256(seed_source.encode()).hexdigest()
        
        tickets_count = len(tickets)
        winner_index = int(seed_hash, 16) % tickets_count
        winner = tickets[winner_index]
        
        try:
            url = f"https://toncenter.com/api/v2/getAddressBalance?address={CONTRACT_ADDRESS}&api_key={TONCENTER_API_KEY}"
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url) as r:
                    d = await r.json()
                    prize_pool = int(d["result"]) / 1e9 if d.get("ok") else 0
        except: prize_pool = 0

        await db.execute("""
            UPDATE rounds SET 
                status='AWAITING_PAYOUT', 
                closed_ts_ms=?, 
                tickets_count=?,
                seed_source_tx_hash=?, 
                seed_hash=?,
                winner_wallet=?, 
                winner_ticket_tx_hash=?, 
                prize_amount_ton=?
            WHERE id=?
        """, (int(time.time()*1000), tickets_count, seed_source, seed_hash, winner['sender'], winner['tx_hash'], prize_pool, round_id))
        await db.commit()
        
        msg = (
            f"🏁 <b>РАУНД #{round_id} ЗАВЕРШЕН!</b>\n\n"
            f"🎟 Билетов: {tickets_count}\n"
            f"🏆 Победитель: <code>{winner['sender']}</code>\n"
            f"💰 Приз (Pool): {prize_pool:.1f} TON\n"
            f"🎲 Seed Source: ...{seed_source[-8:]}\n\n"
            f"⚠️ <b>ДЕЙСТВИЕ:</b> Переведи приз и подтверди:\n"
            f"<code>/payout {round_id} TX_HASH</code>"
        )
        try: await bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
        except: pass
        
        await create_new_round()

# --- ЛОГИКА ---
async def add_ticket(sender, amount, tx_hash):
    sender = normalize_address(sender)
    
    round_data = await get_active_round()
    if not round_data:
        await create_new_round()
        round_data = await get_active_round()
    
    current_round_id = round_data['id']

    async with aiosqlite.connect("lottery.db") as db:
        try:
            await db.execute("INSERT INTO tickets (sender, amount, tx_hash, round_id) VALUES (?, ?, ?, ?)", 
                             (sender, amount, tx_hash, current_round_id))
            await db.execute("UPDATE rounds SET tickets_count = tickets_count + 1 WHERE id=?", (current_round_id,))
            await db.commit()

            if (round_data['tickets_count'] + 1) >= 100:
                logging.info("🚀 Round Limit Reached (100 tickets). Closing...")
                asyncio.create_task(close_round_logic(current_round_id))

            return True
        except aiosqlite.IntegrityError: return False

async def get_user_tickets(address):
    address = normalize_address(address)
    if not address: return {"history": []}
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT amount, tx_hash, timestamp FROM tickets WHERE sender=? ORDER BY id DESC", (address,)) as cursor:
            rows = await cursor.fetchall()
    return {"history": [{"amount": r[0], "hash": r[1][:8]+"...", "time": r[2]} for r in rows]}

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
        async with db.execute("SELECT COUNT(*) FROM tickets WHERE sender = ?", (wallet,)) as cursor:
            count = (await cursor.fetchone())[0]
            return count > 0

async def get_analytics_24h():
    since_ts = int((time.time() - 24 * 3600) * 1000)
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall("SELECT event, COUNT(*) AS cnt FROM analytics_events WHERE ts_ms >= ? GROUP BY event", (since_ts,))
    stats = {r["event"]: r["cnt"] for r in rows}
    def v(key): return stats.get(key, 0)
    opened = max(v("open_rules"), 1)
    return {
        "open": v("open_rules"), "rules": v("open_rules"), "wallet": v("connect_wallet"), "enter": v("enter_game_click"), "success": v("tx_success"),
        "p_rules": round(v("open_rules") / opened * 100), "p_wallet": round(v("connect_wallet") / opened * 100),
        "p_enter": round(v("enter_game_click") / opened * 100), "p_success": round(v("tx_success") / opened * 100),
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
        "open": v("open_rules"), "rules": v("open_rules"), "wallet": v("connect_wallet"), "enter": v("enter_game_click"), "success": v("tx_success"),
        "p_rules": round(v("open_rules") / base * 100), "p_wallet": round(v("connect_wallet") / base * 100),
        "p_enter": round(v("enter_game_click") / base * 100), "p_success": round(v("tx_success") / base * 100),
    }

# ==========================================
# 📊 API & PUBLIC DATA (PR-3 FIXED)
# ==========================================

async def public_balances_worker():
    """Фоновый воркер для обновления балансов раз в 60 сек"""
    while True:
        try:
            # Словарь для итерации и проверок
            targets = {
                "prize": CONTRACT_ADDRESS,
                "dev": WALLET_DEV,
                "treasury": WALLET_TREASURY,
                "jackpot": WALLET_JACKPOT,
                "holder": WALLET_HOLDER_DROP
            }
            
            async with aiohttp.ClientSession() as sess:
                for key, addr in targets.items():
                    # FIX #2: Проверка на None/Empty
                    if not addr:
                        logging.warning(f"⚠️ Public API Warning: WALLET_{key.upper()} is missing in env!")
                        CACHE_BALANCES['data'][key] = 0
                        continue

                    try:
                        url = f"https://toncenter.com/api/v2/getAddressBalance?address={addr}&api_key={TONCENTER_API_KEY}"
                        async with sess.get(url) as r:
                            d = await r.json()
                            if d.get("ok"):
                                CACHE_BALANCES['data'][key] = int(d["result"]) / 1e9
                            else:
                                CACHE_BALANCES['data'][key] = 0
                    except Exception as e:
                        logging.error(f"Balance fetch error for {key}: {e}")
            
            CACHE_BALANCES['last_update'] = time.time()
            
        except Exception as e:
            logging.error(f"Public Balances Worker Crash: {e}")
        
        # Ждем 60 секунд перед следующим обновлением
        await asyncio.sleep(60)

# FIX #1: Убрали вызов update из хендлера
async def handle_public_overview(request):
    r = await get_active_round()
    
    if not r: 
        return web.json_response({
            "round_id": 0,
            "round_end_ts": int(time.time() + 86400),
            "tickets_sold": 0,
            "balances": CACHE_BALANCES['data']
        })

    return web.json_response({
        "round_id": r['id'],
        "round_end_ts": int(r['end_ts_ms'] / 1000),
        "tickets_sold": r['tickets_count'],
        "balances": CACHE_BALANCES['data']
    })

async def handle_public_rounds(request):
    async with aiosqlite.connect("lottery.db") as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT id, closed_ts_ms, winner_wallet, prize_amount_ton, payout_tx_hash, tickets_count 
            FROM rounds 
            WHERE status='PAID' 
            ORDER BY id DESC LIMIT 10
        """) as c:
            rows = await c.fetchall()
            
    result = []
    for row in rows:
        result.append({
            "id": row['id'],
            "date": int(row['closed_ts_ms'] / 1000),
            "winner": short_addr(row['winner_wallet']),
            "prize": row['prize_amount_ton'],
            "tx": row['payout_tx_hash'],
            "tickets": row['tickets_count']
        })
        
    return web.json_response({"rounds": result})


# ==========================================
# 🔁 SCANNER & SCHEDULER
# ==========================================
async def get_contract_balance():
    # Legacy for admin stats
    if time.time() - CACHE_BALANCE["time"] < CACHE_TTL: return CACHE_BALANCE["value"]
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://toncenter.com/api/v2/getAddressBalance?address={CONTRACT_ADDRESS}&api_key={TONCENTER_API_KEY}"
            async with session.get(url) as r:
                data = await r.json()
                if data["ok"]:
                    val = int(data["result"]) / 1e9
                    CACHE_BALANCE["value"] = val
                    CACHE_BALANCE["time"] = time.time()
                    return val
    except Exception: pass
    return CACHE_BALANCE["value"]

def get_next_round_end():
    # Legacy helper
    now = datetime.utcnow()
    days_ahead = 6 - now.weekday()
    if days_ahead < 0: days_ahead += 7
    target = now + timedelta(days=days_ahead)
    target = target.replace(hour=15, minute=0, second=0, microsecond=0)
    if target < now: target += timedelta(days=7)
    return int(target.timestamp())

async def send_to_channel(sender, amount, tx_hash):
    current_jackpot = await get_contract_balance()
    text = (
        f"🎟 <b>Новый участник!</b>\n\n"
        f"👤 <code>{short_addr(sender)}</code>\n"
        f"💰 Вход: <b>{amount} TON</b>\n"
        f"🏦 <b>Банк игры: {current_jackpot:.1f} TON</b>\n\n"
        f"🔗 <a href='https://tonviewer.com/transaction/{tx_hash}'>Explorer</a>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎰 Участвовать", url="https://t.me/tw10x_official_bot")]])
    try: await bot.send_message(chat_id=LIVE_CHANNEL_ID, text=text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except Exception: pass

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
                    if value >= 4_000_000_000 and source:
                        ton_amount = value / 1_000_000_000
                        if await add_ticket(source, ton_amount, tx_hash):
                            logging.info(f"🎟 Ticket: {ton_amount} TON from {source}")
                            CACHE_BALANCE["time"] = 0 
                            await send_to_channel(source, ton_amount, tx_hash)
    except Exception: pass

async def scheduler():
    while True:
        await check_deposits()
        try:
            round_data = await get_active_round()
            if round_data:
                now_ms = int(time.time() * 1000)
                if now_ms >= round_data['end_ts_ms']:
                    logging.info("⏰ Round Time Limit Reached. Closing...")
                    await close_round_logic(round_data['id'])
        except Exception as e:
            logging.error(f"Scheduler Error: {e}")
            
        await asyncio.sleep(15)

# ==========================================
# 🌐 API HANDLERS
# ==========================================
async def handle_index(request): return web.FileResponse("./webapp/index.html")

async def handle_save_wallet(request):
    try:
        data = await request.json()
        await set_user_wallet(int(data.get("user_id")), data.get("wallet"))
        return web.json_response({"status": "ok"})
    except: return web.json_response({"error": "err"}, status=400)

async def handle_get_referrer(request):
    uid = request.query.get("user_id")
    if not uid: return web.json_response({})
    return web.json_response({"ref_wallet": await get_referrer_wallet(int(uid))})

async def handle_user_stats(request):
    addr = request.query.get("address")
    if not addr: return web.json_response({"history": []})
    return web.json_response(await get_user_tickets(addr))

async def handle_global_stats(request):
    # Legacy endpoint для MiniApp
    balance = await get_contract_balance()
    async with aiosqlite.connect("lottery.db") as db:
        async with db.execute("SELECT COUNT(*) FROM tickets") as c: total_tickets = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users") as c: total_users = (await c.fetchone())[0]
    return web.json_response({
        "jackpot": balance, "round_end": get_next_round_end(), "total_tickets": total_tickets, "total_users": total_users
    })

async def handle_ref_stats(request):
    uid = request.query.get("user_id")
    if not uid: return web.json_response({"invited": 0, "earned": 0})
    return web.json_response(await get_ref_stats(int(uid)))

@dp.chat_join_request()
async def join_request_handler(update: ChatJoinRequest, bot: Bot):
    user_id = update.from_user.id
    logging.info(f"🚪 Join Request from {user_id}")
    if await has_active_ticket(user_id):
        try:
            await update.approve()
            await bot.send_message(user_id, "✅ <b>Доступ в VIP-клуб открыт!</b>\nДобро пожаловать в элиту.", parse_mode="HTML")
            logging.info(f"✅ Approved {user_id}")
        except Exception as e: logging.error(f"Approve Error: {e}")
    else:
        try:
            await update.decline()
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 Купить билет", web_app=WebAppInfo(url="https://tw10x.app"))]])
            await bot.send_message(user_id, "⛔️ <b>Доступ только для игроков!</b>\n\nВ этом раунде у тебя нет билета.\nКупи билет, чтобы попасть в закрытый чат.", parse_mode="HTML", reply_markup=kb)
            logging.info(f"⛔️ Declined {user_id}")
        except Exception as e: logging.error(f"Decline Error: {e}")

@dp.message(Command("id"))
async def get_id_cmd(message: types.Message):
    await message.answer(f"🆔 Chat ID: `{message.chat.id}`", parse_mode="Markdown")

@dp.message(Command("stats"))
async def admin_stats(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    bal = await get_contract_balance()
    await message.answer(f"📊 <b>ADMIN STATS</b>\n💰 Balance: {bal} TON", parse_mode="HTML")

@dp.message(Command("payout"))
async def payout_cmd(message: types.Message, command: CommandObject):
    if message.from_user.id != int(ADMIN_ID): return
    args = command.args.split() if command.args else []
    if len(args) != 2:
        await message.answer("Usage: `/payout <round_id> <tx_hash>`", parse_mode="Markdown")
        return
    round_id, tx_hash = args[0], args[1]
    async with aiosqlite.connect("lottery.db") as db:
        await db.execute("UPDATE rounds SET status='PAID', payout_tx_hash=? WHERE id=?", (tx_hash, round_id))
        await db.commit()
    await message.answer(f"✅ Round {round_id} marked as PAID.")

@dp.message(Command("round"))
async def round_info_cmd(message: types.Message):
    r = await get_active_round()
    if not r: await message.answer("No active round."); return
    await message.answer(f"🟢 <b>Active Round #{r['id']}</b>\n🎟 Tickets: {r['tickets_count']}/100\n⏳ Ends: {datetime.fromtimestamp(r['end_ts_ms']/1000)}", parse_mode="HTML")

@dp.message(Command("analytics"))
async def analytics_cmd(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    data = await get_analytics_24h()
    text = (f"📊 <b>Analytics (24h)</b>\n\n👀 Open: {data['open']}\n📜 Rules: {data['rules']} ({data['p_rules']}%)\n"
            f"🔗 Wallet: {data['wallet']} ({data['p_wallet']}%)\n🎟 Enter: {data['enter']} ({data['p_enter']}%)\n"
            f"💸 Success: {data['success']} ({data['p_success']}%)")
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("analytics_week"))
async def analytics_week_cmd(message: types.Message):
    if message.from_user.id != int(ADMIN_ID): return
    data = await get_analytics_7d()
    text = (f"📊 <b>Analytics (7 days)</b>\n\n👀 Open: {data['open']}\n📜 Rules: {data['rules']} ({data['p_rules']}%)\n"
            f"🔗 Wallet: {data['wallet']} ({data['p_wallet']}%)\n🎟 Enter: {data['enter']} ({data['p_enter']}%)\n"
            f"💸 Success: {data['success']} ({data['p_success']}%)")
    await message.answer(text, parse_mode="HTML")

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

async def main():
    await init_db()
    await init_analytics_db()
    
    app = web.Application()
    
    # 🟢 PR-3 API
    app.router.add_get("/api/public/overview", handle_public_overview)
    app.router.add_get("/api/public/rounds", handle_public_rounds)

    # Legacy & Pages
    app.router.add_get("/", handle_index)
    app.router.add_post("/api/save_wallet", handle_save_wallet)
    app.router.add_get("/api/referrer", handle_get_referrer)
    app.router.add_get("/api/user", handle_user_stats)
    app.router.add_get("/api/global_stats", handle_global_stats)
    app.router.add_get("/api/ref_stats", handle_ref_stats)
    app.router.add_post("/api/analytics", handle_analytics)
    app.router.add_get("/api/analytics_funnel", handle_analytics_funnel)
    
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
    # FIX: Запускаем воркер балансов фоном
    asyncio.create_task(public_balances_worker())
    
    logging.info(f"🚀 SYSTEM ONLINE v3.0 (Public API + Worker). Live Channel: {LIVE_CHANNEL_ID}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
