import asyncio
import logging
import aiosqlite
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiohttp import web
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from tonsdk.utils import Address
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from config import (
    BOT_TOKEN,
    ADMIN_ID,
    CONTRACT_ADDRESS,
    TONCENTER_API_KEY,
    API_URL,
)

# === WEBHOOK CONFIG ===
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = "https://tw10x.app/webhook"
WEB_SERVER_HOST = "127.0.0.1"
WEB_SERVER_PORT = 8080

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==========================================
# 🧠 ADDRESS NORMALIZER
# ==========================================
def normalize_address(addr_str):
    try:
        return Address(addr_str).to_string(
            is_user_friendly=True,
            is_url_safe=True,
            is_bounceable=True
        )
    except Exception as e:
        logging.error(f"Address normalize error: {e}")
        return addr_str

# ==========================================
# 🗄 DATABASE
# ==========================================
async def init_db():
    async with aiosqlite.connect("lottery.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender TEXT,
                amount REAL,
                tx_hash TEXT UNIQUE,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def add_ticket(sender, amount, tx_hash):
    sender = normalize_address(sender)
    async with aiosqlite.connect("lottery.db") as db:
        try:
            await db.execute(
                "INSERT INTO tickets (sender, amount, tx_hash) VALUES (?, ?, ?)",
                (sender, amount, tx_hash)
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def get_stats():
    async with aiosqlite.connect("lottery.db") as db:
        rows = await db.execute_fetchall("SELECT sender, amount FROM tickets")

    return {
        "bank": round(sum(r[1] for r in rows), 2),
        "tickets": len(rows),
        "players": len(set(r[0] for r in rows))
    }

async def get_user_stats(address):
    address = normalize_address(address)

    async with aiosqlite.connect("lottery.db") as db:
        total = await db.execute_fetchone("SELECT sum(amount) FROM tickets")
        rows = await db.execute_fetchall(
            "SELECT amount, tx_hash, timestamp FROM tickets WHERE sender=? ORDER BY id DESC",
            (address,)
        )

    bank = total[0] or 0
    user_sum = sum(r[0] for r in rows)

    return {
        "address": address,
        "total_invested": round(user_sum, 2),
        "ticket_count": len(rows),
        "chance": round((user_sum / bank * 100) if bank else 0, 2),
        "history": [
            {"amount": r[0], "hash": r[1][:8] + "...", "time": r[2]}
            for r in rows[:5]
        ]
    }

# ==========================================
# 🌐 HTTP HANDLERS
# ==========================================
async def handle_index(request):
    return web.FileResponse("./webapp/index.html")

async def handle_status(request):
    return web.json_response(await get_stats())

async def handle_user(request):
    addr = request.query.get("address")
    if not addr:
        return web.json_response({"error": "address required"}, status=400)
    return web.json_response(await get_user_stats(addr))

# ==========================================
# 🔁 TON MONITOR (BACKGROUND)
# ==========================================
async def check_deposits():
    params = {"address": CONTRACT_ADDRESS, "limit": 50, "archival": "true"}
    if TONCENTER_API_KEY:
        params["api_key"] = TONCENTER_API_KEY

    async with aiohttp.ClientSession() as session:
        async with session.get(API_URL, params=params) as r:
            data = await r.json()
            if not data.get("ok"):
                return

            for tx in reversed(data["result"]):
                msg = tx.get("in_msg")
                if not msg:
                    continue

                value = int(msg.get("value", 0))
                source = msg.get("source")
                tx_hash = tx["transaction_id"]["hash"]

                if value >= 5_000_000_000 and source:
                    ton = value / 1_000_000_000
                    if await add_ticket(source, ton, tx_hash):
                        await bot.send_message(
                            ADMIN_ID,
                            f"💰 <b>+{ton} TON</b>\n🎫 Билет куплен!",
                            parse_mode="HTML"
                        )

async def scheduler():
    logging.info("⏳ Scheduler started")
    while True:
        try:
            await check_deposits()
        except Exception as e:
            logging.error(e)
        await asyncio.sleep(30)

# ==========================================
# 🤖 BOT COMMANDS
# ==========================================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🎰 ИГРАТЬ",
            web_app=WebAppInfo(url="https://tw10x.app")
        )]
    ])
    await message.answer("Добро пожаловать в TW10X", reply_markup=kb)

# ==========================================
# 🚀 MAIN (WEBHOOK MODE)
# ==========================================
async def main():
    await init_db()

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/user", handle_user)

    handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_SERVER_HOST, WEB_SERVER_PORT)
    await site.start()

    await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)

    asyncio.create_task(scheduler())

    logging.info("🚀 BOT STARTED IN WEBHOOK MODE")

    await asyncio.Event().wait()  # держим процесс живым

if __name__ == "__main__":
    asyncio.run(main())
