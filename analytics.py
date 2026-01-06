import json
import time
import aiosqlite
from aiohttp import web

DB_PATH = "lottery.db"

# Разрешённые события (строгая схема, чтобы не засорять БД мусором)
ALLOWED_EVENTS = {
    "open_rules",
    "switch_tab",
    "connect_wallet",
    "enter_game_click",
    "tx_success",
    "tx_cancel",
}

def _safe_int(v):
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None

def _safe_str(v, max_len=256):
    if v is None:
        return None
    s = str(v)
    if len(s) > max_len:
        s = s[:max_len]
    return s

async def init_analytics_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER NOT NULL,
                event TEXT NOT NULL,
                user_id INTEGER,
                wallet TEXT,
                extra_json TEXT,
                ip TEXT,
                ua TEXT
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_analytics_event ON analytics_events(event)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_analytics_user ON analytics_events(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_analytics_ts ON analytics_events(ts_ms)")
        await db.commit()

async def handle_analytics(request: web.Request) -> web.Response:
    """
    Принимает события из webapp/index.html:
    {
      event: "open_rules",
      user_id: 123,
      wallet: "EQ....",
      ts: 173... (ms),
      extra: { ... }
    }
    """
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

    event = _safe_str(data.get("event"), max_len=64)
    if not event or event not in ALLOWED_EVENTS:
        return web.json_response({"ok": False, "error": "invalid_event"}, status=400)

    ts_ms = _safe_int(data.get("ts"))
    if not ts_ms:
        ts_ms = int(time.time() * 1000)

    user_id = _safe_int(data.get("user_id"))
    wallet = _safe_str(data.get("wallet"), max_len=128)

    extra = data.get("extra") if isinstance(data.get("extra"), dict) else {}
    # Лёгкая нормализация, чтобы не писать огромные payload
    norm_extra = {}
    for k, v in list(extra.items())[:20]:
        key = _safe_str(k, max_len=64)
        if key is None:
            continue
        if isinstance(v, (int, float, bool)) or v is None:
            norm_extra[key] = v
        else:
            norm_extra[key] = _safe_str(v, max_len=256)

    extra_json = json.dumps(norm_extra, ensure_ascii=False)

    # диагностика/защита
    ip = request.headers.get("X-Forwarded-For", request.remote)
    ip = _safe_str(ip, max_len=128)
    ua = _safe_str(request.headers.get("User-Agent"), max_len=256)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO analytics_events (ts_ms, event, user_id, wallet, extra_json, ip, ua)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts_ms, event, user_id, wallet, extra_json, ip, ua),
        )
        await db.commit()

    return web.json_response({"ok": True})

async def handle_analytics_funnel(request: web.Request) -> web.Response:
    """
    Простейшая агрегированная статистика для проверки,
    что события приходят и можно строить воронку.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall(
            """
            SELECT event, COUNT(*) AS cnt
            FROM analytics_events
            GROUP BY event
            ORDER BY cnt DESC
            """
        )
    return web.json_response({"ok": True, "by_event": [dict(r) for r in rows]})
