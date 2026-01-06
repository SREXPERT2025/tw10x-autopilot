import os
from dotenv import load_dotenv
from pathlib import Path

# Определяем путь к .env
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

# Загружаем переменные окружения
load_dotenv(dotenv_path=ENV_PATH)

# === CORE ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# === API ===
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY")
API_URL = os.getenv("API_URL", "https://toncenter.com/api/v2/getTransactions")
WEB_SERVER_PORT = int(os.getenv("WEB_SERVER_PORT", 8080))

# === WALLETS v3.0 ===
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
WALLET_DEV = os.getenv("WALLET_DEV")
WALLET_TREASURY = os.getenv("WALLET_TREASURY")
WALLET_JACKPOT = os.getenv("WALLET_JACKPOT")
WALLET_HOLDER_DROP = os.getenv("WALLET_HOLDER_DROP")

# Проверка критических переменных
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing in .env")
if not CONTRACT_ADDRESS:
    raise ValueError("CONTRACT_ADDRESS is missing in .env")
