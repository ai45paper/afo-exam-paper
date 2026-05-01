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
START_PAGE_1BASED = 970
START_PAGE_0BASED = START_PAGE_1BASED - 1

try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("⚠️ pdf2image or pytesseract not installed. OCR fallback disabled.")

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
        self.openrouter_last_used = 0
        self.gemini_last_used = 0
    
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
# TEXT EXTRACTION
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
        images = convert_from_path(pdf_path, first_page=page_idx+1, last_page=page_idx+1, dpi=200, poppler_path=POPPLER_PATH)
        ocr_text = ""
        for img in images:
            ocr_text += pytesseract.image_to_string(img.convert("L"), lang="eng", config="--oem 3 --psm 6")
        return ocr_text
    except Exception as e:
        logger.warning(f"OCR failed: {e}")
        return ""

# ========================
# PROMPT BUILDING
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
"""
    # ✅ FIX 3: Realistic Prompt (Less strict formatting rules)
    return f"""You are Satyam Sir, an expert agriculture mentor setting a mock paper for the AGTA 2026 and IBPS AFO Mains batches.

YOUR TASK: Generate exactly 10 high-quality multiple-choice questions from the provided text.

RULES:
1. Language: 100% STRICTLY ENGLISH ONLY. No Hindi.
2. Difficulty: Moderate level. Keep them engaging.
3. Options: Exactly 5 options per question.
4. Answer Match: The text in the 'answer' field MUST exactly match the text of one of the 5 options.
5. Format: Return a JSON array. Do not include explanations outside JSON.

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
# JSON CLEANER
# ========================
def extract_and_clean_json(raw):
    # ✅ FIX 2: Robust JSON extraction
    if not raw:
        return None

    raw = raw.strip()
    
    # ✅ FIX 7: Clean common Gemini/Claude prefixes
    raw = re.sub(r'^(Here is your JSON:|Here is the JSON array:|```json|```)', '', raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r'```$', '', raw).strip()

    try:
        # Direct parse attempt
        data = json.loads(raw)
    except:
        # Fallback extraction (grab everything between first [ and last ])
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            logger.debug("Failed regex match for JSON array bounds.")
            return None
        try:
            data = json.loads(match.group(0))
        except Exception as e:
            logger.debug(f"JSON load failed on matched segment: {e}")
            return None

    data = data[:10]
    
    def clean_opt(opt):
        opt = str(opt).strip()
        opt = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', opt, flags=re.IGNORECASE)
        return opt.strip()
        
    result = []
    for item in data:
        q = str(item.get("question", "")).strip()
        opt1 = clean_opt(item.get("opt1",""))
        opt2 = clean_opt(item.get("opt2",""))
        opt3 = clean_opt(item.get("opt3",""))
        opt4 = clean_opt(item.get("opt4",""))
        opt5 = clean_opt(item.get("opt5",""))
        ans = clean_opt(item.get("answer",""))
        expl = str(item.get("explanation","")).strip()
        
        if not q or not ans:
            continue
            
        # Flexible answer validation
        valid_options = [opt1.lower(), opt2.lower(), opt3.lower(), opt4.lower(), opt5.lower()]
        if ans.lower() not in valid_options:
            matched = False
            for v_opt in valid_options:
                if ans.lower() in v_opt or v_opt in ans.lower():
                    matched = True
                    break
            if not matched:
                logger.debug(f"Question rejected: Answer '{ans}' not in options.")
                continue
                
        result.append({
            "section": str(item.get("section","")).strip() or "General Agriculture",
            "question": q,
            "opt1": opt1, "opt2": opt2, "opt3": opt3, "opt4": opt4, "opt5": opt5,
            "answer": ans,
            "explanation": expl
        })
    return result if result else None

# ========================
# AI PROVIDERS WITH KEY ROTATION
# ========================

def call_openrouter(prompt):
    key = key_rotation.get_next_openrouter_key()
    if not key: return None
    models = ["openrouter/auto", "meta-llama/llama-3.1-70b-instruct", "anthropic/claude-3.5-sonnet"]
    url = "[https://openrouter.ai/api/v1/chat/completions](https://openrouter.ai/api/v1/chat/completions)"
    for model in models:
        try:
            resp = requests.post(url, 
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}]}, 
                timeout=30)
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
            if resp.status_code == 429:
                break
        except:
            continue
    return None

def call_nvidia(prompt):
    if not NVIDIA_KEY: return None
    url = "[https://integrate.api.nvidia.com/v1/chat/completions](https://integrate.api.nvidia.com/v1/chat/completions)"
    models = ["nvidia/nemotron-4-340b-instruct", "meta/llama3-70b-instruct"]
    for model in models:
        try:
            resp = requests.post(url, 
                headers={"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}]}, 
                timeout=30)
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
        except:
            continue
    return None

def call_claude(prompt):
    if not CLAUDE_KEY: return None
    try:
        client = anthropic.Anthropic(api_key=CLAUDE_KEY)
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2500,
            temperature=0.4,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except:
        return None

def call_gemini(prompt):
    key = key_rotation.get_next_gemini_key()
    if not key: return None
    models = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for model_name in models:
        try:
            genai.configure(api_key=key)
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(
                prompt + "\n\nReturn strictly valid JSON array only.",
                generation_config={"response_mime_type": "text/plain"}
            )
            if response and response.text:
                return response.text
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                continue
            continue
    return None

def generate_questions(text, section):
    # ✅ FIX 4: Check Text Quality
    word_count = len(text.split())
    if word_count < 80:
        logger.warning(f"Low quality/short text ({word_count} words) — skipping AI generation")
        return None

    prompt = build_prompt(text, section)
    start_time = time.time()
    
    providers = [
        ("OpenRouter", call_openrouter),
        ("NVIDIA", call_nvidia),
        ("Claude", call_claude),
        ("Gemini", call_gemini)
    ]
    
    # ✅ FIX 6: Retry per provider
    for attempt in range(2):
        logger.info(f"--- Generation Attempt {attempt + 1} ---")
        for provider_name, provider_func in providers:
            # ✅ FIX 5: Increased Timeout
            if time.time() - start_time > 120:
                logger.warning("Global timeout (120s) – moving to next batch")
                return None
            
            try:
                logger.info(f"Trying {provider_name}...")
                raw = provider_func(prompt)
                
                # ✅ FIX 1: Add RAW logging
                logger.info(f"{provider_name} RAW RESPONSE (first 500 chars):\n{raw[:500] if raw else 'None'}\n---")
                
                if raw:
                    cleaned = extract_and_clean_json(raw)
                    if cleaned:
                        logger.info(f"✓ Success with {provider_name}: {len(cleaned)} questions generated")
                        return cleaned
                    else:
                        logger.warning(f"⚠️ {provider_name} response failed JSON parsing.")
            except Exception as e:
                logger.debug(f"{provider_name} failed: {e}")
                continue
                
    # Bonus Feature: Try a shorter prompt if everything failed
    logger.warning("All AI providers failed. Retrying with shorter prompt...")
    short_prompt = build_prompt(text[:2000], section)
    for provider_name, provider_func in providers:
         raw = provider_func(short_prompt)
         if raw:
             cleaned = extract_and_clean_json(raw)
             if cleaned:
                 logger.info(f"✓ Success on shorter prompt with {provider_name}")
                 return cleaned
    
    logger.warning("❌ All AI providers failed completely for this batch")
    return None

# ========================
# GOOGLE SHEETS
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
# MAIN WORKFLOW (STREAMING DOWNLOAD)
# ========================
def main_workflow():
    pdf_path = "book.pdf"
    logger.info("▶️ ENGINE STARTING...")

    if not os.path.exists(pdf_path):
        logger.info("📥 Streaming PDF Download (117MB) in chunks...")
        try:
            download_url = "".join(["https://", "drive.google.com", "/uc?id=", DRIVE_FILE_ID, "&export=download"])
            session = requests.Session()
            response = session.get(download_url, stream=True)
            
            if "confirm=" not in download_url and response.text.find("confirm=") != -1:
                match = re.search(r'confirm=([a-zA-Z0-9_-]+)', response.text)
                if match:
                    token = match.group(1)
                    download_url += f"&confirm={token}"
                    response = session.get(download_url, stream=True)

            with open(pdf_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024): 
                    if chunk: f.write(chunk)
                    
            if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) == 0:
                raise Exception("Downloaded file is empty or missing.")
            logger.info("✅ PDF Streaming Saved to Disk.")
            gc.collect()
            time.sleep(3)
        except Exception as e:
            logger.error(f"❌ PDF download failed: {e}")
            return

    try:
        with fitz.open(pdf_path) as doc:
            total_pages = doc.page_count
        logger.info(f"📄 Total PDF pages: {total_pages}")
    except Exception as e:
        logger.error(f"❌ Cannot read PDF: {e}")
        return

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
        try:
            with fitz.open(pdf_path) as doc:
                for i in range(current, next_page):
                    page_text = extract_text_with_ocr(doc, pdf_path, i)
                    if page_text:
                        combined += page_text + "\n"
        except Exception as e:
             logger.error(f"Error reading pages: {e}")
             
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
                    if append_to_sheet(buffer):
                        buffer = []
            else:
                logger.warning("No valid questions generated for this batch")
        else:
            logger.warning(f"Insufficient text on pages {current+1}-{next_page}")

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
    return "AGTA 2026 Engine LIVE - Production Ready ✅"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    Thread(target=main_workflow, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
