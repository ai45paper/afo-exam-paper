import os
import sys
import json
import time
import re
import gc
import logging
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
import fitz
import gdown
from flask import Flask
from threading import Thread
import google.generativeai as genai
import anthropic

# ========================
# CONFIG
# ========================
START_PAGE_1BASED = 970   # change as needed
START_PAGE_0BASED = START_PAGE_1BASED - 1

try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

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
POPPLER_PATH = os.getenv("POPPLER_PATH", "/usr/bin")

if not MONGO_URI or not SHEET_ID or not DRIVE_FILE_ID or not SERVICE_ACCOUNT_JSON:
    logger.error("Missing required env vars")
    sys.exit(1)

# ========================
# KEY ROTATION TRACKING
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
# SECTION RANGES
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
# TRACKER
# ========================
def init_tracker():
    last = START_PAGE_0BASED
    update_tracker(last)
    logger.info(f"Forced start from page {last+1} (hardcoded)")
    return last

def update_tracker(page_idx):
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_idx}}, upsert=True)

def get_section(page_idx):
    human = page_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human <= e:
            return name
    return "General Agriculture"

# ========================
# TEXT EXTRACTION (OCR only when needed)
# ========================
def extract_text_with_ocr(doc, pdf_path, page_idx):
    page = doc.load_page(page_idx)
    text = page.get_text()
    if text and len(text.strip()) > 100:
        return text
    if not OCR_AVAILABLE:
        return ""
    logger.info(f"OCR on page {page_idx+1}")
    try:
        images = convert_from_path(pdf_path, first_page=page_idx+1, last_page=page_idx+1,
                                   dpi=200, poppler_path=POPPLER_PATH)
        ocr_text = ""
        for img in images:
            ocr_text += pytesseract.image_to_string(img.convert("L"), lang="eng", config="--oem 3 --psm 6")
        return ocr_text
    except Exception as e:
        logger.warning(f"OCR failed: {e}")
        return ""

# ========================
# PROMPT BUILDING (relaxed)
# ========================
def build_prompt(text, section):
    examples = """
REFERENCE QUESTION STYLE (Follow exactly):

The excretory organ of silkworm which is located at the junction of the midgut and hindgut is known as
Options: Proboscis | Malpighian tubule | Nephridia | Green glands | None
Answer: Malpighian tubule

Type of silviculture system which can regenerate through seeds and where the majority have a long life is
Options: Pollarding | High forest | Coppicing | Forking | None
Answer: High forest

The process of removing the green colouring (known as chlorophyll) from the skin of citrus fruit by introducing measured amounts of ethylene gas is known as
Options: Ripening | Degreening | Physiological maturity | Denavelling | Dehusking
Answer: Degreening
"""
    return f"""You are Satyam Sir, an expert agriculture mentor setting a mock paper for the AGTA 2026 and IBPS AFO Mains batches.

YOUR TASK: Generate exactly 10 high-quality multiple-choice questions from the provided text.

RULES:
1. Language: 100% STRICTLY ENGLISH ONLY. No Hindi.
2. Difficulty: Moderate level.
3. Options: Exactly 5 options per question.
4. Answer Match: The 'answer' field MUST exactly match the text of one of the 5 options.
5. Question Length: 20 to 35 words.
6. Explanation: Max 20 words.
7. Format: Return a JSON array. Do not include explanations outside JSON.

Topic: {section}

{examples}

EXPECTED JSON SCHEMA:
[
  {{
    "section": "{section}",
    "question": "...",
    "opt1": "...",
    "opt2": "...",
    "opt3": "...",
    "opt4": "...",
    "opt5": "...",
    "answer": "...",
    "explanation": "..."
  }}
]

Content:
{text[:5000]}
"""

# ========================
# RELAXED JSON PARSER
# ========================
def extract_and_clean_json(raw):
    if not raw:
        return None
    raw = raw.strip()
    # Remove common Gemini prefix
    raw = re.sub(r'^Here is your JSON:\s*', '', raw, flags=re.IGNORECASE)
    # Try direct parse
    try:
        data = json.loads(raw)
    except:
        # fallback: extract first JSON array
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
        opt = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', opt)
        return opt.strip()

    result = []
    for item in data:
        q = item.get("question", "").strip()
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
# AI PROVIDERS (with fresh key per model)
# ========================

def call_openrouter(prompt):
    if not OPENROUTER_KEYS:
        return None
    models = ["openrouter/auto", "meta-llama/llama-3.1-70b-instruct", "anthropic/claude-3.5-sonnet"]
    url = "https://openrouter.ai/api/v1/chat/completions"
    for model in models:
        key = key_rotation.get_next_openrouter_key()
        if not key:
            continue
        try:
            resp = requests.post(url,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}]},
                timeout=60)
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
            else:
                logger.warning(f"OpenRouter {model} HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.error(f"OpenRouter error: {e}")
    return None

def call_nvidia(prompt):
    if not NVIDIA_KEY:
        return None
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    models = ["nvidia/nemotron-4-340b-instruct", "meta/llama3-70b-instruct"]
    for model in models:
        try:
            resp = requests.post(url,
                headers={"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}]},
                timeout=60)
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"NVIDIA error: {e}")
    return None

def call_claude(prompt):
    if not CLAUDE_KEY:
        return None
    try:
        client = anthropic.Anthropic(api_key=CLAUDE_KEY)
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2500,
            temperature=0.4,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude error: {e}")
        return None

def call_gemini(prompt):
    if not GEMINI_KEYS:
        return None
    models = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for model_name in models:
        key = key_rotation.get_next_gemini_key()
        if not key:
            continue
        try:
            genai.configure(api_key=key)
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(
                prompt + "\n\nReturn a JSON array. Do not include explanations outside JSON.",
                generation_config={"response_mime_type": "text/plain"},
                request_options={"timeout": 60}
            )
            if response and response.text:
                return response.text
        except Exception as e:
            logger.error(f"Gemini {model_name} error: {e}")
            if "429" in str(e):
                continue
    return None

# ========================
# GENERATE QUESTIONS with retry, raw logging, timeout, fallback
# ========================
def generate_questions(text, section):
    prompt = build_prompt(text, section)
    start_time = time.time()
    providers = [("OpenRouter", call_openrouter), ("NVIDIA", call_nvidia),
                 ("Claude", call_claude), ("Gemini", call_gemini)]

    for name, func in providers:
        if time.time() - start_time > 120:
            logger.warning("Global timeout (120s) – moving to next batch")
            return None

        for attempt in range(2):   # retry each provider twice
            logger.info(f"Trying {name} (attempt {attempt+1})...")
            try:
                raw = func(prompt)
                # RAW LOGGING (critical for debugging)
                logger.info(f"{name} RAW (first 1000 chars):\n{raw[:1000] if raw else 'None'}")
                if raw:
                    cleaned = extract_and_clean_json(raw)
                    if cleaned:
                        logger.info(f"✓ {name} succeeded: {len(cleaned)} questions")
                        return cleaned
                    else:
                        logger.warning(f"{name} returned invalid JSON (after cleaning)")
                else:
                    logger.warning(f"{name} returned None")
            except Exception as e:
                logger.error(f"{name} exception: {e}", exc_info=True)

    # Fallback with shorter text (if full text failed)
    if len(text) > 2000:
        logger.info("Retrying with shorter prompt (first 2000 chars)")
        return generate_questions(text[:2000], section)

    return None

# ========================
# GOOGLE SHEETS WRITE with retry
# ========================
def append_to_sheet(rows):
    for attempt in range(3):
        try:
            sheet.append_rows(rows, value_input_option="RAW")
            logger.info(f"✓ Appended {len(rows)} rows to Google Sheets")
            return True
        except Exception as e:
            logger.warning(f"Sheets write attempt {attempt+1} failed: {e}")
            time.sleep(5 * (attempt + 1))
    logger.error("Failed to write to Google Sheets after 3 retries")
    return False

# ========================
# MAIN WORKFLOW
# ========================
def main_workflow():
    pdf_path = "book.pdf"

    if not os.path.exists(pdf_path):
        logger.info("Downloading PDF from Google Drive...")
        try:
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=False)
            if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) == 0:
                raise Exception("Empty download")
            logger.info("✅ PDF downloaded")
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return

    with fitz.open(pdf_path) as doc:
        total_pages = doc.page_count
    logger.info(f"📄 Total PDF pages: {total_pages}")

    current = init_tracker()
    if current >= total_pages:
        logger.info("Already completed")
        return

    buffer = []
    while current < total_pages:
        next_page = min(current + 2, total_pages)
        section = get_section(current)
        logger.info(f"📖 Processing pages {current+1}-{next_page} | {section}")

        combined = ""
        with fitz.open(pdf_path) as doc:
            for i in range(current, next_page):
                page_text = extract_text_with_ocr(doc, pdf_path, i)
                if page_text:
                    combined += page_text + "\n"
        combined = combined[:15000]

        # Quality check – skip if too little text or garbage
        word_count = len(combined.split())
        if word_count < 80:
            logger.warning(f"Low quality text on pages {current+1}-{next_page} (words {word_count}) – skipping AI")
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
                logger.warning(f"No valid questions for batch {current+1}-{next_page}")

        update_tracker(next_page)
        current = next_page
        gc.collect()
        logger.info("Waiting 20 seconds before next batch (rate limit protection)...")
        time.sleep(20)

    if buffer:
        append_to_sheet(buffer)
    logger.info("✅ All pages processed successfully!")

# ========================
# FLASK SERVER
# ========================
app = Flask(__name__)

@app.route('/')
def home():
    return "AGTA 2026 Engine LIVE - Production Ready (all fixes applied) ✅"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    Thread(target=main_workflow, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
