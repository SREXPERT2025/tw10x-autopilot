import asyncio
import logging
import aiosqlite
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiohttp import web
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from tonsdk.utils import Address 

# 👇 ИМПОРТИРУЕМ НАСТРОЙКИ ИЗ config.py
from config import (
    BOT_TOKEN,
    ADMIN_ID,
    CONTRACT_ADDRESS,
    TONCENTER_API_KEY,
    API_URL,
    WEB_SERVER_PORT
)

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==========================================
# 🧠 УМНЫЙ ПЕРЕВОДЧИК АДРЕСОВ
# ==========================================
def normalize_address(addr_str):
    """Превращает любой формат в единый стандарт EQ..."""
    try:           
        return Address(addr_str).to_string(is_user_friendly=True, is_url_safe=True, is_bounceable=True)
    except Exception as e:
        logging.error(f"Ошибка нормализации адреса {addr_str}: {e}")
        return addr_str

# ==========================================
# 🗄 БАЗА ДАННЫХ
# ==========================================
async def init_db():
    async with aiosqlite.connect('lottery.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender TEXT,
                amount REAL,
                tx_hash TEXT UNIQUE,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.commit()

async def add_ticket(sender, amount, tx_hash):
    clean_sender = normalize_address(sender)
    async with aiosqlite.connect('lottery.db') as db:
        try:
            await db.execute('INSERT INTO tickets (sender, amount, tx_hash) VALUES (?, ?, ?)', (clean_sender, amount, tx_hash))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def get_stats():
    async with aiosqlite.connect('lottery.db') as db:
        async with db.execute('SELECT sender, amount FROM tickets') as cursor:
            tickets = await cursor.fetchall()
            
    count = len(tickets)
    total_bank = sum(t[1] for t in tickets)
    unique_players = len(set(t[0] for t in tickets))
    
    return {
        "bank": round(total_bank, 2),
        "tickets": count,
        "players": unique_players
    }

async def get_user_stats(user_address):
    clean_address = normalize_address(user_address)
    
    async with aiosqlite.connect('lottery.db') as db:
        async with db.execute('SELECT sum(amount) FROM tickets') as cursor:
            res = await cursor.fetchone()
            total_bank = res[0] if res and res[0] else 0

        async with db.execute('SELECT amount, tx_hash, timestamp FROM tickets WHERE sender = ? ORDER BY id DESC', (clean_address,)) as cursor:
            tickets = await cursor.fetchall()

    user_total = sum(t[0] for t in tickets)
    count = len(tickets)
    chance = (user_total / total_bank * 100) if total_bank > 0 else 0

    return {   
        "address": clean_address,
        "total_invested": round(user_total, 2),
        "ticket_count": count,
        "chance": round(chance, 2),
        "history": [
            {"amount": t[0], "hash": t[1][:8]+"...", "time": t[2]} 
            for t in tickets[:5]
        ]
    }

async def clear_tickets():
    async with aiosqlite.connect('lottery.db') as db:
        await db.execute('DELETE FROM tickets')
        await db.commit()

# ==========================================
# 🌐 WEB SERVER (API)
# ==========================================
async def handle_status(request):
    stats = await get_stats()
    return web.json_response(stats, headers={'Access-Control-Allow-Origin': '*'})

async def handle_index(request):
    return web.FileResponse('./webapp/index.html')

async def handle_user_info(request):
    address = request.query.get('address')
    if not address:
        return web.json_response({"error": "No address"}, status=400)
    stats = await get_user_stats(address)
    return web.json_response(stats, headers={'Access-Control-Allow-Origin': '*'})

# ==========================================
# 🤖 БОТ И МОНИТОРИНГ
# ==========================================
async def check_deposits():
    params = {"address": CONTRACT_ADDRESS, "limit": 100, "archival": "true"}
    if TONCENTER_API_KEY: params["api_key"] = TONCENTER_API_KEY

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(API_URL, params=params) as resp:
                data = await resp.json()
                if not data.get("ok"): return

                for tx in reversed(data["result"]):
                    in_msg = tx.get("in_msg")
                    if not in_msg: continue
                    
                    value = int(in_msg.get("value", 0))
                    source = in_msg.get("source")
                    tx_hash = tx.get("transaction_id", {}).get("hash")

                    if value > 100_000_000 and source: 
                        amount = value / 1_000_000_000
                        if await add_ticket(source, amount, tx_hash):
                            logging.info(f"💰 +{amount} TON от {source}")
                            await bot.send_message(ADMIN_ID, f"💰 <b>+{amount} TON</b>\n🎫 Билет куплен!", parse_mode="HTML")
        except Exception as e:
            logging.error(f"Err: {e}")

async def scheduler():
    while True:
        await check_deposits()
        await asyncio.sleep(30)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    # Тут замени ссылку google.com на https://tw10x.app
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎰 ИГРАТЬ (Открыть Лобби)", web_app=WebAppInfo(url="https://tw10x.app"))]
    ])
    await message.answer("👋 Добро пожаловать в TW10X Lottery!", reply_markup=kb)

@dp.message(Command("debug"))
async def cmd_debug(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect('lottery.db') as db:
        async with db.execute('SELECT sender, amount FROM tickets') as cursor:
            rows = await cursor.fetchall()
    
    if not rows:
        await message.answer("🤷‍♂️ База пуста")
        return

    text = "<b>📋 БАЗА ДАННЫХ:</b>\n"
    for row in rows:
        text += f"<code>{row[0]}</code> : {row[1]} TON\n"
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("clear"))
async def cmd_clear(message: types.Message):
    if message.from_user.id == ADMIN_ID:
        await clear_tickets()
        await message.answer("🧹 База очищена")

# ==========================================
# 🚀 ЗАПУСК
# ==========================================
async def main():
    await init_db()
    
    # Настраиваем веб-сервер
    app = web.Application()
    app.router.add_get('/', handle_index)
    app.router.add_get('/api/status', handle_status)
    app.router.add_get('/api/user', handle_user_info)
    app.router.add_static('/webapp', path='./webapp')
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    # 🔒 ВОТ ОН, ФИКС БЕЗОПАСНОСТИ ОТ ИВАНА:
    site = web.TCPSite(runner, '127.0.0.1', 8080)
    await site.start()
    
    logging.info(f"🌍 Сервер запущен локально: http://127.0.0.1:8080")
    
    # Запускаем фоновые задачи
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped!")
