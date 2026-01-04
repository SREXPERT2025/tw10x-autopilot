import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

# Загружаем ТОЛЬКО этот .env
load_dotenv(dotenv_path=ENV_PATH)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY")

API_URL = os.getenv("API_URL", "https://toncenter.com/api/v2/getTransactions")
WEB_SERVER_PORT = int(os.getenv("WEB_SERVER_PORT", 8080))

print("ENV PATH:", ENV_PATH)
print("BOT_TOKEN PREFIX:", BOT_TOKEN[:10])
