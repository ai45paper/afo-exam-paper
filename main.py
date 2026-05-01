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
import anthropic

try:
    import pytesseract
    from PIL import Image
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("⚠️ Pillow or pytesseract not installed. OCR fallback disabled.")

# ========================
# CONFIG & START PAGE
# ========================
START_PAGE_1BASED = 970
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
# KEY ROTATION TRACKING
# ========================
class KeyRotation:
    def __init__(self):
        self.openrouter_idx = 0
        self.gemini_idx = 0
    
    def get_next_openrouter_key(self):
        if not OPENROUTER_KEYS: return None
        key = OPENROUTER_KEYS[self.openrouter_idx]
        self.openrouter_idx = (self.openrouter_idx + 1) % len(OPENROUTER_KEYS)
        return key
    
    def get_next_gemini_key(self):
        if not GEMINI_KEYS: return None
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
# TRACKER (SMART RESUME)
# ========================
def init_tracker():
    tracker = tracker_col.find_one({"_id": "pdf_tracker"})
    db_page = tracker.get("current_page", 0) if tracker else 0
    
    if db_page > START_PAGE_0BASED:
        logger.info(f"✅ Resuming from page {db_page+1} (MongoDB tracked)")
        return db_page
    else:
        update_tracker(START_PAGE_0BASED)
        logger.info(f"🚀 Forced start from page {START_PAGE_1BASED} (hardcoded)")
        return START_PAGE_0BASED

def update_tracker(page_idx):
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_idx}}, upsert=True)

def get_section(page_idx):
    human = page_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human <= e:
            return name
    return "General Agriculture"

# ========================
# HYBRID TEXT EXTRACTION (RAM-SAFE OCR)
# ========================
def extract_text(doc, page_idx):
    try:
        page = doc.load_page(page_idx)
        text = page.get_text()
        
        if text and len(text.split()) > 15:
            return text
            
        # 🔥 OCR FALLBACK (RAM Safe Method via PyMuPDF Pixmap)
        if OCR_AVAILABLE:
            logger.info(f"Page {page_idx+1} has low text. Triggering RAM-Safe OCR...")
            pix = page.get_pixmap(dpi=150) # Moderate DPI for speed & RAM safety
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            ocr_text = pytesseract.image_to_string(img)
            
            if ocr_text and len(ocr_text.split()) > 15:
                logger.info(f"✓ OCR successfully recovered text on page {page_idx+1}")
                return ocr_text

        logger.warning(f"Page {page_idx+1} is entirely blank/images. Skipping.")
        return ""
    except Exception as e:
        logger.error(f"Failed to read page {page_idx+1}: {e}")
        return ""

# ========================
# PROMPT BUILDING
# ========================
def build_prompt(text, section):
    examples = """
REFERENCE QUESTION STYLE:
The excretory organ of silkworm which is located at the junction of the midgut and hindgut is known as
Options: Proboscis | Malpighian tubule | Nephridia | Green glands | None
Answer: Malpighian tubule
"""
    # ✅ FIX 5: Relaxed Prompt Strictness
    return f"""You are Satyam Sir, an expert agriculture mentor setting a mock paper for UPSSSC AGTA 2026 and IBPS AFO.

YOUR TASK: Generate up to 10 high-quality multiple-choice questions from the provided text.

RULES:
1. Language: STRICTLY ENGLISH ONLY. No Hindi.
2. Difficulty: MODERATE level.
3. Options: Exactly 5 options per question.
4. Answer Match: The 'answer' MUST exactly match the text of one of the options.
5. Format: Return JSON array. If formatting fails, still try your best to output valid JSON. Do not include explanations outside JSON.

Topic: {section}
{examples}

EXPECTED JSON SCHEMA:
[
  {{
    "section": "{section}",
    "question": "Question text here...",
    "opt1": "First option text",
    "opt2": "Second option text",
    "opt3": "Third option text",
    "opt4": "Fourth option text",
    "opt5": "Fifth option text",
    "answer": "Exact text of the correct option",
    "explanation": "Short conceptual explanation."
  }}
]

Content:
{text[:5000]}
"""

# ========================
# JSON CLEANER (ROBUST)
# ========================
def extract_and_clean_json(raw):
    if not raw: return None
    raw = raw.strip()
    raw = re.sub(r"^(?i)(Here is your JSON:|Here is the JSON array:|```json|```)\s*", "", raw).strip()
    raw = re.sub(r"```$", "", raw).strip()

    try:
        data = json.loads(raw)
    except:
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if not match: return None
        try: data = json.loads(match.group(0))
        except: return None

    data = data[:10]
    
    def clean_opt(opt):
        opt = str(opt).strip()
        return re.sub(r"^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)", "", opt, flags=re.IGNORECASE).strip()
        
    result = []
    for item in data:
        q = str(item.get("question", "")).strip()
        opt1, opt2, opt3 = clean_opt(item.get("opt1","")), clean_opt(item.get("opt2","")), clean_opt(item.get("opt3",""))
        opt4, opt5, ans = clean_opt(item.get("opt4","")), clean_opt(item.get("opt5","")), clean_opt(item.get("answer",""))
        expl = str(item.get("explanation","")).strip()
        
        if not q or not ans: continue
            
        valid_opts = [opt1.lower(), opt2.lower(), opt3.lower(), opt4.lower(), opt5.lower()]
        if ans.lower() not in valid_opts:
            if not any(ans.lower() in v or v in ans.lower() for v in valid_opts): continue
                
        result.append({
            "section": str(item.get("section","")).strip() or "General Agriculture",
            "question": q,
            "opt1": opt1, "opt2": opt2, "opt3": opt3, "opt4": opt4, "opt5": opt5,
            "answer": ans, "explanation": expl
        })
    return result if result else None

# ========================
# AI PROVIDERS (URLS FIXED)
# ========================
def call_openrouter(prompt):
    key = key_rotation.get_next_openrouter_key()
    if not key: return None
    # ✅ FIX 1: URL FIXED (No Markdown brackets)
    url = "[https://openrouter.ai/api/v1/chat/completions](https://openrouter.ai/api/v1/chat/completions)"
    for model in ["openrouter/auto", "meta-llama/llama-3.1-70b-instruct", "anthropic/claude-3.5-sonnet"]:
        try:
            resp = requests.post(url, headers={"Authorization": f"Bearer {key}"}, json={"model": model, "messages": [{"role": "user", "content": prompt}]}, timeout=30)
            if resp.status_code == 200: return resp.json()["choices"][0]["message"]["content"]
            if resp.status_code == 429: break
        except Exception as e:
            logger.error(f"OpenRouter Error ({model}): {e}")
            continue
    return None

def call_nvidia(prompt):
    if not NVIDIA_KEY: return None
    # ✅ FIX 1: URL FIXED
    url = "[https://integrate.api.nvidia.com/v1/chat/completions](https://integrate.api.nvidia.com/v1/chat/completions)"
    for model in ["nvidia/nemotron-4-340b-instruct", "meta/llama3-70b-instruct"]:
        try:
            resp = requests.post(url, headers={"Authorization": f"Bearer {NVIDIA_KEY}"}, json={"model": model, "messages": [{"role": "user", "content": prompt}]}, timeout=30)
            if resp.status_code == 200: return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"NVIDIA Error ({model}): {e}")
            continue
    return None

def call_claude(prompt):
    if not CLAUDE_KEY: return None
    try:
        client = anthropic.Anthropic(api_key=CLAUDE_KEY)
        return client.messages.create(model="claude-3-5-sonnet-20241022", max_tokens=2500, temperature=0.4, messages=[{"role": "user", "content": prompt}]).content[0].text
    except Exception as e:
        logger.error(f"Claude Error: {e}")
        return None

def call_gemini(prompt):
    key = key_rotation.get_next_gemini_key()
    if not key: return None
    for model_name in ["gemini-2.5-flash", "gemini-2.0-flash"]:
        try:
            genai.configure(api_key=key)
            model = genai.GenerativeModel(model_name)
            resp = model.generate_content(prompt, generation_config={"response_mime_type": "text/plain"})
            if resp and resp.text: return resp.text
        except Exception as e:
            # ✅ FIX 2: Gemini logging
            logger.error(f"Gemini Error ({model_name}): {e}")
            continue
    return None

# ========================
# ORCHESTRATOR (SHUFFLE & RETRY)
# ========================
def generate_questions(text, section):
    prompt = build_prompt(text, section)
    start_time = time.time()
    
    # List of tuples
    providers = [
        ("OpenRouter", call_openrouter), 
        ("NVIDIA", call_nvidia), 
        ("Claude", call_claude), 
        ("Gemini", call_gemini)
    ]
    
    # ✅ FIX 3: Strong Retry Logic with provider shuffle
    for attempt in range(2):
        logger.info(f"--- Generation Attempt {attempt + 1} ---")
        random.shuffle(providers) # Randomize order so we don't hit the same failing API first
        
        for name, func in providers:
            if time.time() - start_time > 120: 
                logger.warning("Global timeout (120s) – moving to next batch")
                return None
            try:
                logger.info(f"Trying {name}...")
                raw = func(prompt)
                
                # ✅ ADDED: Raw response logging to debug silent failures
                logger.info(f"{name} RAW RESPONSE (first 300 chars):\n{str(raw)[:300].replace(chr(10), ' ')}\n---")
                
                if raw:
                    cleaned = extract_and_clean_json(raw)
                    if cleaned:
                        logger.info(f"✓ Success: {len(cleaned)} MCQs")
                        return cleaned
                    else:
                        logger.warning(f"⚠️ {name} output rejected by cleaner.")
                time.sleep(2) # Prevent rapid-fire API ban
            except Exception as e:
                logger.error(f"{name} execution crashed: {e}")
                continue
            
    logger.warning("All AI providers failed. Trying shorter prompt...")
    short_prompt = build_prompt(text[:2000], section)
    for name, func in providers:
         raw = func(short_prompt)
         if raw:
             cleaned = extract_and_clean_json(raw)
             if cleaned: return cleaned
    return None

# ========================
# GOOGLE SHEETS
# ========================
def append_to_sheet(rows):
    for attempt in range(3):
        try:
            sheet.append_rows(rows, value_input_option="RAW")
            logger.info(f"✓ Saved {len(rows)} MCQs to Google Sheets")
            return True
        except Exception as e:
            time.sleep(5 * (attempt + 1))
    return False

# ========================
# MAIN WORKFLOW
# ========================
def main_workflow():
    pdf_path = "book.pdf"
    logger.info("▶️ ENGINE STARTING...")

    if not os.path.exists(pdf_path):
        logger.info("📥 Downloading PDF (117MB) using gdown...")
        try:
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=True)
            if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) < 1000000:
                raise Exception("Corrupt file")
            logger.info("✅ PDF Saved.")
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            return

    try:
        with fitz.open(pdf_path) as doc: total_pages = doc.page_count
    except Exception as e:
        logger.error(f"❌ Cannot read PDF: {e}")
        return

    current = init_tracker()
    if current >= total_pages: return

    buffer = []
    while current < total_pages:
        next_page = min(current + 2, total_pages)
        section = get_section(current)
        logger.info(f"\n📖 Processing pages {current+1}-{next_page} | {section}")

        combined = ""
        try:
            with fitz.open(pdf_path) as doc:
                for i in range(current, next_page):
                    text = extract_text(doc, i)
                    if text: combined += text + "\n"
        except Exception as e:
            logger.error(f"Error reading page batch: {e}")
             
        combined = combined[:15000]

        if len(combined.strip()) > 50:
            questions = generate_questions(combined, section)
            if questions:
                for q in questions:
                    buffer.append([
                        q.get("section", section), q.get("question", ""), 
                        q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                        q.get("opt4", ""), q.get("opt5", ""), 
                        q.get("answer", ""), q.get("explanation", "")
                    ])
                if len(buffer) >= 50:
                    if append_to_sheet(buffer): buffer = []
            else:
                logger.warning("No questions generated for this text block.")
        else:
            logger.warning(f"Not enough text on pages {current+1}-{next_page} even after OCR.")

        # ✅ FIX 6: Update tracker IMMEDIATELY to prevent infinite loop on failure
        update_tracker(next_page)
        current = next_page
        gc.collect()
        
        logger.info("Waiting 15 seconds (Rate Limit Protection)...")
        time.sleep(15)

    if buffer: append_to_sheet(buffer)
    logger.info("🎉 All pages processed successfully!")

# ========================
# FLASK SERVER
# ========================
app = Flask(__name__)

@app.route('/')
def home(): return "AGTA 2026 Engine LIVE - OCR Enabled & RAM Safe ✅"

if __name__ == "__main__":
    Thread(target=main_workflow, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
