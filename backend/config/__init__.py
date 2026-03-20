import os
from pathlib import Path
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

BASE_DIR   = Path(__file__).parent.parent
if load_dotenv:
    load_dotenv(BASE_DIR / ".env")

API_KEY    = os.environ.get("MAAIKE_API_KEY", "rue-pinard-2025")
SECRET_KEY = os.environ.get("FLASK_SECRET",   "maaike-secret-2025")
