import aiosqlite
import asyncio
import logging

logging.basicConfig(level=logging.INFO)

async def migrate():
    print("🚀 Starting Database Migration to v3.0...")
    
    async with aiosqlite.connect("lottery.db") as db:
        # 1. Создаем таблицу ROUNDS
        print("🛠 Creating table 'rounds'...")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT DEFAULT 'ACTIVE',
                start_ts_ms INTEGER,
                end_ts_ms INTEGER,
                closed_ts_ms INTEGER,
                tickets_count INTEGER DEFAULT 0,
                seed_source_tx_hash TEXT,
                seed_hash TEXT,
                winner_wallet TEXT,
                winner_ticket_tx_hash TEXT,
                prize_amount_ton REAL,
                payout_tx_hash TEXT
            )
        """)
        
        # 2. Добавляем round_id в TICKETS
        print("🛠 Altering table 'tickets'...")
        try:
            await db.execute("ALTER TABLE tickets ADD COLUMN round_id INTEGER")
            print("✅ Column 'round_id' added.")
        except Exception as e:
            if "duplicate column" in str(e):
                print("ℹ️ Column 'round_id' already exists.")
            else:
                print(f"⚠️ Warning: {e}")

        # Индексы
        await db.execute("CREATE INDEX IF NOT EXISTS idx_rounds_status ON rounds(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tickets_round ON tickets(round_id)")
        
        await db.commit()
    
    print("✅ MIGRATION COMPLETE. Database is ready for v3.0 logic.")

if __name__ == "__main__":
    asyncio.run(migrate())
