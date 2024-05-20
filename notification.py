import requests
from dotenv import load_dotenv
import os


load_dotenv()

TOKEN = os.environ.get("TELEGRAM_TOKEN")

url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"

print(requests.get(url).json())