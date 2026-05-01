import os
import sys
import json
import time
import re
import gc
import logging
import random
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
import fitz  # PyMuPDF
import gdown
from flask import Flask
from threading import Thread
import google.generativeai as genai

# Optional OCR
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("⚠️ OCR not available – will use embedded text only")

# ========================
# CONFIG & START PAGE
# ========================
START_PAGE_1BASED = 975          # CHANGE THIS TO ANY PAGE YOU WANT
START_PAGE_0BASED = START_PAGE_1BASED - 1

# ========================
# LOGGING
# ========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========================
# ENV VARIABLES
# ========================
CLAUDE_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
NVIDIA_KEY = os.getenv("NVIDIA_API_KEY", "").strip()
OPENROUTER_KEYS = [k.strip() for k in os.getenv("OPENROUTER_KEYS", "").split(",") if k.strip()]
GEMINI_KEYS = [k.strip() for k in os.getenv("GEMINI_KEYS", "").split(",") if k.strip()]

MONGO_URI = os.getenv("MONGO_URI", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
DRIVE_FILE_ID = os.getenv("DRIVE_FILE_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "")

if not MONGO_URI or not SHEET_ID or not DRIVE_FILE_ID or not SERVICE_ACCOUNT_JSON:
    logger.error("❌ Missing required env vars. Exiting.")
    sys.exit(1)

# ========================
# KEY ROTATION
# ========================
class KeyRotation:
    def __init__(self):
        self.openrouter_idx = 0
        self.gemini_idx = 0

    def get_next_openrouter_key(self):
        if not OPENROUTER_KEYS:
            return None
        key = OPENROUTER_KEYS[self.openrouter_idx]
        self.openrouter_idx = (self.openrouter_idx + 1) % len(OPENROUTER_KEYS)
        return key

    def get_next_gemini_key(self):
        if not GEMINI_KEYS:
            return None
        key = GEMINI_KEYS[self.gemini_idx]
        self.gemini_idx = (self.gemini_idx + 1) % len(GEMINI_KEYS)
        return key

key_rotation = KeyRotation()

# ========================
# SECTION RANGES (1‑based)
# ========================
SECTION_RANGES = [
    (1, 75, "Agronomy"), (76, 242, "Horticulture"), (243, 308, "Entomology"),
    (309, 389, "Fisheries"), (390, 517, "Animal Husbandry"), (518, 557, "Plant Pathology"),
    (558, 585, "Agricultural Economics"), (586, 704, "General Agriculture"),
    (705, 727, "Seed Technology"), (728, 759, "Weed Science"), (760, 771, "Apiculture"),
    (772, 803, "Forestry"), (804, 839, "Meteorology"), (840, 860, "Genetics and Breeding"),
    (861, 931, "Agricultural Engineering"), (932, 941, "Extension Education"),
    (942, 946, "Mushroom Cultivation"), (947, 964, "Sericulture"), (965, 966, "Lac Culture"),
    (967, 1075, "Soil Science")
]

# ========================
# DATABASE & SHEETS
# ========================
logger.info("Connecting to MongoDB...")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
try:
    mongo_client.server_info()
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    sys.exit(1)
db = mongo_client['agri_data_bank']
tracker_col = db['process_tracker']

logger.info("Connecting to Google Sheets...")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1

# ========================
# TRACKER (NEW NAME TO RESET)
# ========================
TRACKER_NAME = "pdf_tracker_v2"   # new tracker → ignores old 1076

def init_tracker():
    tracker = tracker_col.find_one({"_id": TRACKER_NAME})
    db_page = tracker.get("current_page", 0) if tracker else 0
    if db_page > START_PAGE_0BASED:
        logger.info(f"✅ Resuming from page {db_page+1} (MongoDB)")
        return db_page
    else:
        update_tracker(START_PAGE_0BASED)
        logger.info(f"🚀 Forced start from page {START_PAGE_1BASED} (hardcoded)")
        return START_PAGE_0BASED

def update_tracker(page_idx):
    tracker_col.update_one({"_id": TRACKER_NAME}, {"$set": {"current_page": page_idx}}, upsert=True)

def get_section(page_idx):
    human = page_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human <= e:
            return name
    return "General Agriculture"

# ========================
# TEXT EXTRACTION (OCR fallback)
# ========================
def extract_text(doc, page_idx):
    page = doc.load_page(page_idx)
    text = page.get_text()
    if text and len(text.split()) > 15:
        return text

    # OCR fallback
    if OCR_AVAILABLE:
        logger.info(f"Page {page_idx+1} low text, trying OCR...")
        try:
            pix = page.get_pixmap(dpi=150)
            img = pix.tobytes("png")
            # use pytesseract directly on bytes
            ocr_text = pytesseract.image_to_string(img)
            if ocr_text and len(ocr_text.split()) > 15:
                logger.info(f"OCR success on page {page_idx+1}")
                return ocr_text
        except Exception as e:
            logger.warning(f"OCR failed: {e}")
    return ""

# ========================
# PROMPT (RELAXED)
# ========================
def build_prompt(text, section):
    examples = """
REFERENCE QUESTION STYLE:
The excretory organ of silkworm which is located at the junction of the midgut and hindgut is known as
Options: Proboscis | Malpighian tubule | Nephridia | Green glands | None
Answer: Malpighian tubule
"""
    return f"""You are an expert agriculture mentor setting a mock paper.

Generate up to 10 multiple-choice questions from the text.

RULES:
- English only, moderate difficulty
- Exactly 5 options per question
- 'answer' must exactly match one option
- Return a JSON array (no extra text)

Topic: {section}
{examples}

Schema example:
[
  {{
    "section": "{section}",
    "question": "...",
    "opt1": "...", "opt2": "...", "opt3": "...", "opt4": "...", "opt5": "...",
    "answer": "...",
    "explanation": "..."
  }}
]

Content:
{text[:5000]}
"""

# ========================
# JSON CLEANER (ROBUST)
# ========================
def extract_and_clean_json(raw):
    if not raw:
        return None
    raw = raw.strip()
    # remove common prefixes
    raw = re.sub(r'^(?i)(here is your json|```json|```)\s*', '', raw)
    raw = re.sub(r'```$', '', raw)
    try:
        data = json.loads(raw)
    except:
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
        except:
            return None
    if not isinstance(data, list):
        data = [data]
    data = data[:10]

    def clean_opt(opt):
        opt = str(opt).strip()
        return re.sub(r'^(option\s*\d+\s*:|^\d+\.\s*|^[a-e]\)\s*)', '', opt, flags=re.I).strip()

    result = []
    for item in data:
        q = str(item.get("question", "")).strip()
        opt1 = clean_opt(item.get("opt1", ""))
        opt2 = clean_opt(item.get("opt2", ""))
        opt3 = clean_opt(item.get("opt3", ""))
        opt4 = clean_opt(item.get("opt4", ""))
        opt5 = clean_opt(item.get("opt5", ""))
        ans = clean_opt(item.get("answer", ""))
        expl = str(item.get("explanation", "")).strip()
        if not q or not ans:
            continue
        if ans.lower() not in [opt1.lower(), opt2.lower(), opt3.lower(), opt4.lower(), opt5.lower()]:
            # fuzzy match if needed
            if not any(ans.lower() in v.lower() or v.lower() in ans.lower() for v in [opt1, opt2, opt3, opt4, opt5]):
                continue
        result.append({
            "section": str(item.get("section", "")).strip() or "General Agriculture",
            "question": q,
            "opt1": opt1, "opt2": opt2, "opt3": opt3, "opt4": opt4, "opt5": opt5,
            "answer": ans,
            "explanation": expl
        })
    return result if result else None

# ========================
# API PROVIDERS (FIXED)
# ========================

def call_openrouter(prompt):
    if not OPENROUTER_KEYS:
        return None
    url = "https://openrouter.ai/api/v1/chat/completions"
    models = ["openrouter/auto", "meta-llama/llama-3.1-70b-instruct"]
    for _ in range(len(OPENROUTER_KEYS)):
        key = key_rotation.get_next_openrouter_key()
        if not key:
            continue
        for model in models:
            try:
                resp = requests.post(
                    url,
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 2500,      # ✅ CRITICAL: reduces credit usage
                        "temperature": 0.4
                    },
                    timeout=60
                )
                if resp.status_code == 200:
                    return resp.json()["choices"][0]["message"]["content"]
                elif resp.status_code == 429:
                    logger.warning("OpenRouter rate limit, switching key")
                    break
                else:
                    logger.warning(f"OpenRouter {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.error(f"OpenRouter error: {e}")
                continue
    return None

def call_nvidia(prompt):
    if not NVIDIA_KEY:
        return None
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    # Only use the valid, non-EOL model
    models = ["nvidia/nemotron-4-340b-instruct"]
    for model in models:
        try:
            resp = requests.post(
                url,
                headers={"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 2500,
                    "temperature": 0.4
                },
                timeout=60
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
            else:
                logger.warning(f"NVIDIA {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.error(f"NVIDIA error: {e}")
    return None

def call_claude(prompt):
    if not CLAUDE_KEY:
        return None
    url = "https://api.anthropic.com/v1/messages"
    try:
        resp = requests.post(
            url,
            headers={
                "x-api-key": CLAUDE_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-3-5-sonnet-20241022",
                "max_tokens": 2500,
                "temperature": 0.4,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=60
        )
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
        else:
            logger.warning(f"Claude {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.error(f"Claude error: {e}")
    return None

def call_gemini(prompt):
    if not GEMINI_KEYS:
        return None
    models = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for _ in range(len(GEMINI_KEYS)):
        key = key_rotation.get_next_gemini_key()
        if not key:
            continue
        for model_name in models:
            try:
                genai.configure(api_key=key)
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(
                    prompt + "\n\nReturn ONLY valid JSON array.",
                    generation_config={"response_mime_type": "text/plain"}
                )
                if response and response.text:
                    return response.text
            except Exception as e:
                if "429" in str(e):
                    logger.warning(f"Gemini quota for key {key[:8]}..., switching")
                    break
                else:
                    logger.error(f"Gemini error: {e}")
    return None

# ========================
# ORCHESTRATOR WITH RETRIES
# ========================
def generate_questions(text, section):
    prompt = build_prompt(text, section)
    start_time = time.time()
    # Order: OpenRouter → Gemini → Claude → NVIDIA
    providers = [
        ("OpenRouter", call_openrouter),
        ("Gemini", call_gemini),
        ("Claude", call_claude),
        ("NVIDIA", call_nvidia)
    ]
    for attempt in range(3):
        logger.info(f"--- Attempt {attempt+1} ---")
        random.shuffle(providers)
        for name, func in providers:
            if time.time() - start_time > 120:
                logger.warning("Global timeout")
                return None
            logger.info(f"Trying {name}...")
            raw = func(prompt)
            logger.info(f"{name} raw (first 300): {str(raw)[:300]}")
            if raw:
                cleaned = extract_and_clean_json(raw)
                if cleaned:
                    logger.info(f"✅ {name} success: {len(cleaned)} questions")
                    return cleaned
                else:
                    logger.warning(f"{name} returned invalid JSON")
            time.sleep(2)
    # fallback with shorter text
    if len(text) > 2000:
        logger.info("Retrying with shorter text")
        return generate_questions(text[:2000], section)
    return None

# ========================
# SHEETS APPEND
# ========================
def append_to_sheet(rows):
    for i in range(3):
        try:
            sheet.append_rows(rows, value_input_option="RAW")
            logger.info(f"✓ Saved {len(rows)} rows to Sheets")
            return True
        except Exception as e:
            logger.warning(f"Sheets attempt {i+1} failed: {e}")
            time.sleep(5)
    return False

# ========================
# MAIN WORKFLOW
# ========================
def main_workflow():
    pdf_path = "book.pdf"
    if not os.path.exists(pdf_path):
        logger.info("Downloading PDF...")
        try:
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=False)
            if os.path.getsize(pdf_path) < 1e6:
                raise Exception("Download failed")
            logger.info("PDF ready")
        except Exception as e:
            logger.error(f"Download error: {e}")
            return

    with fitz.open(pdf_path) as doc:
        total = doc.page_count
    logger.info(f"Total pages: {total}")

    current = init_tracker()
    if current >= total:
        return

    buffer = []
    while current < total:
        nxt = min(current + 2, total)
        section = get_section(current)
        logger.info(f"Pages {current+1}-{nxt} | {section}")

        combined = ""
        with fitz.open(pdf_path) as doc:
            for i in range(current, nxt):
                txt = extract_text(doc, i)
                if txt:
                    combined += txt + "\n"
        combined = combined[:15000]

        if len(combined.split()) < 50:
            logger.warning("Too little text, skipping")
        else:
            questions = generate_questions(combined, section)
            if questions:
                for q in questions:
                    buffer.append([
                        q["section"], q["question"], q["opt1"], q["opt2"], q["opt3"],
                        q["opt4"], q["opt5"], q["answer"], q["explanation"]
                    ])
                if len(buffer) >= 50:
                    if append_to_sheet(buffer):
                        buffer = []
            else:
                logger.warning("No questions this batch")

        update_tracker(nxt)
        current = nxt
        gc.collect()
        time.sleep(15)   # rate limit protection

    if buffer:
        append_to_sheet(buffer)
    logger.info("✅ Processing complete")

# ========================
# FLASK SERVER
# ========================
app = Flask(__name__)

@app.route('/')
def home():
    return "AGTA 2026 Engine - Page 975 start, all fixes applied ✅"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    Thread(target=main_workflow, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
