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

# ========================
# CONFIG & START PAGE
# ========================
START_PAGE_1BASED = 1020          # HARDCODED START PAGE
START_PAGE_0BASED = START_PAGE_1BASED - 1

# ========================
# LOGGING
# ========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========================
# ENV VARIABLES
# ========================
OPENROUTER_KEYS = [k.strip() for k in os.getenv("OPENROUTER_KEYS", "").split(",") if k.strip()]
GEMINI_KEYS = [k.strip() for k in os.getenv("GEMINI_KEYS", "").split(",") if k.strip()]
NVIDIA_KEYS = [k.strip() for k in os.getenv("NVIDIA_KEYS", "").split(",") if k.strip()]

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
        self.nvidia_idx = 0

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

    def get_next_nvidia_key(self):
        if not NVIDIA_KEYS:
            return None
        key = NVIDIA_KEYS[self.nvidia_idx]
        self.nvidia_idx = (self.nvidia_idx + 1) % len(NVIDIA_KEYS)
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
# TRACKER
# ========================
TRACKER_NAME = "pdf_tracker_v3"

def init_tracker():
    """Force start from hardcoded page, override MongoDB."""
    force_page = START_PAGE_0BASED
    update_tracker(force_page)
    logger.info(f"🚀 FORCED START from page {force_page+1} (hardcoded: {START_PAGE_1BASED})")
    return force_page

def update_tracker(page_idx):
    """Update MongoDB tracker with current page."""
    tracker_col.update_one({"_id": TRACKER_NAME}, {"$set": {"current_page": page_idx}}, upsert=True)

def get_section(page_idx):
    """Get section name for a given page index."""
    human = page_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human <= e:
            return name
    return "General Agriculture"

# ========================
# TEXT EXTRACTION (improved)
# ========================
def extract_text_from_page(pdf_path, page_idx):
    """Extract embedded text from PDF page. If insufficient, try to extract any content."""
    try:
        doc = fitz.open(pdf_path)
        page = doc.load_page(page_idx)
        text = page.get_text()
        doc.close()
        
        word_count = len(text.split())
        
        # Return text if we have at least 30 words
        if word_count >= 30:
            return text.strip()
        # If less than 30 words, try to get some context from surrounding content
        elif word_count > 0:
            logger.warning(f"Page {page_idx+1} has {word_count} words (below 30). Using as-is.")
            return text.strip()
        else:
            logger.warning(f"Page {page_idx+1} has 0 words. Skipping.")
            return ""
    except Exception as e:
        logger.error(f"Error extracting page {page_idx+1}: {e}")
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
    return f"""You are an expert agriculture mentor setting a mock paper.

Generate up to 10 multiple-choice questions from the text provided.

RULES:
- English only, moderate difficulty
- Exactly 5 options per question
- 'answer' must exactly match one option
- Return ONLY a valid JSON array (no markdown, no extra text)

Topic: {section}
{examples}

Schema:
[
  {{
    "section": "{section}",
    "question": "...",
    "opt1": "...", "opt2": "...", "opt3": "...", "opt4": "...", "opt5": "...",
    "answer": "...",
    "explanation": "..."
  }}
]

Content to create questions from:
{text[:6000]}
"""

# ========================
# JSON CLEANER (ROBUST)
# ========================
def extract_and_clean_json(raw, default_section):
    """Extract and clean JSON from API response."""
    if not raw:
        return None
    
    raw = raw.strip()
    # Remove markdown code blocks
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
    raw = re.sub(r"^here is your json\s*", "", raw, flags=re.I)
    raw = re.sub(r'```$', '', raw)
    
    try:
        data = json.loads(raw)
    except:
        # Try to find JSON array in the text
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
        """Clean option text."""
        opt = str(opt).strip()
        return re.sub(r'^(option\s*\d+\s*:|^\d+\.\s*|^[a-e]\)\s*)', '', opt, flags=re.I).strip()

    result = []
    for item in data:
        try:
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
            
            # Validate answer matches one of the options
            if ans.lower() not in [opt1.lower(), opt2.lower(), opt3.lower(), opt4.lower(), opt5.lower()]:
                # Fuzzy match fallback
                if not any(ans.lower() in v.lower() or v.lower() in ans.lower() 
                          for v in [opt1, opt2, opt3, opt4, opt5]):
                    continue
            
            result.append({
                "section": str(item.get("section", "")).strip() or default_section,
                "question": q,
                "opt1": opt1, "opt2": opt2, "opt3": opt3, "opt4": opt4, "opt5": opt5,
                "answer": ans,
                "explanation": expl
            })
        except Exception as e:
            logger.warning(f"Error processing item: {e}")
            continue
    
    return result if result else None

# ========================
# API PROVIDERS (OPTIMIZED ORDER)
# ========================

def call_openrouter(prompt):
    """Call OpenRouter with auto model and fallback."""
    if not OPENROUTER_KEYS:
        return None
    
    url = "https://openrouter.ai/api/v1/chat/completions"
    models = ["openrouter/auto"]
    
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
                        "max_tokens": 900,
                        "temperature": 0.3
                    },
                    timeout=60
                )
                
                if resp.status_code == 200:
                    return resp.json()["choices"][0]["message"]["content"]
                elif resp.status_code == 402:
                    logger.warning(f"OpenRouter 402 (insufficient credits) for key {key[:8]}...")
                    break
                elif resp.status_code == 429:
                    logger.warning(f"OpenRouter 429 (rate limit) for key {key[:8]}...")
                    break
                else:
                    logger.warning(f"OpenRouter {resp.status_code}: {resp.text[:150]}")
            except Exception as e:
                logger.error(f"OpenRouter error: {e}")
                continue
    
    return None

def call_gemini(prompt):
    """Call Gemini with multiple keys and models."""
    if not GEMINI_KEYS:
        return None
    
    import google.generativeai as genai
    models = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-2.0-flash-exp"]
    
    for _ in range(len(GEMINI_KEYS)):
        key = key_rotation.get_next_gemini_key()
        if not key:
            continue
        
        for model_name in models:
            try:
                genai.configure(api_key=key)
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(
                    prompt + "\n\nReturn ONLY valid JSON array with no markdown.",
                    generation_config={"response_mime_type": "text/plain"}
                )
                
                if response and response.text:
                    return response.text
            except Exception as e:
                error_str = str(e).lower()
                if "403" in error_str or "denied access" in error_str:
                    logger.warning(f"Gemini 403 (denied) for key {key[:8]}..., switching")
                    break
                elif "429" in error_str or "quota" in error_str:
                    logger.warning(f"Gemini quota exceeded for key {key[:8]}...")
                    break
                else:
                    logger.error(f"Gemini error: {e}")
    
    return None

def call_nvidia(prompt):
    """Call NVIDIA with multiple models."""
    if not NVIDIA_KEYS:
        return None
    
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    models = [
        "nvidia/nemotron-4-340b-instruct",
        "nvidia/llama-2-70b",
        "nvidia/mistral-large"
    ]
    
    for _ in range(len(NVIDIA_KEYS)):
        key = key_rotation.get_next_nvidia_key()
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
                        "max_tokens": 900,
                        "temperature": 0.3
                    },
                    timeout=60
                )
                
                if resp.status_code == 200:
                    return resp.json()["choices"][0]["message"]["content"]
                elif resp.status_code == 404:
                    logger.warning(f"NVIDIA 404 (not found) for {model}")
                    break
                elif resp.status_code == 429:
                    logger.warning(f"NVIDIA 429 (rate limit)")
                    break
                else:
                    logger.warning(f"NVIDIA {resp.status_code}: {resp.text[:150]}")
            except Exception as e:
                logger.error(f"NVIDIA error: {e}")
    
    return None

# ========================
# ORCHESTRATOR
# ========================
def generate_questions(text, section):
    """Generate questions with provider fallback chain."""
    prompt = build_prompt(text, section)
    start_time = time.time()
    
    # Provider chain: OpenRouter -> Gemini -> NVIDIA
    providers = [
        ("OpenRouter", call_openrouter, 3),  # 3 attempts
        ("Gemini", call_gemini, 3),
        ("NVIDIA", call_nvidia, 2)
    ]
    
    for provider_name, func, max_attempts in providers:
        for attempt in range(max_attempts):
            if time.time() - start_time > 180:  # 3 min timeout
                logger.warning("⏱️ Global timeout reached")
                return None
            
            logger.info(f"📡 {provider_name} attempt {attempt+1}/{max_attempts}...")
            
            try:
                raw = func(prompt)
                
                if raw:
                    logger.info(f"   Raw response (first 200 chars): {raw[:200]}")
                    cleaned = extract_and_clean_json(raw, section)
                    
                    if cleaned:
                        logger.info(f"✅ {provider_name} SUCCESS: {len(cleaned)} questions generated")
                        return cleaned
                    else:
                        logger.warning(f"   ⚠️ {provider_name} returned invalid JSON")
                else:
                    logger.warning(f"   ⚠️ {provider_name} returned None")
            except Exception as e:
                logger.error(f"   ❌ {provider_name} exception: {e}")
            
            time.sleep(2)
    
    logger.error("❌ All providers exhausted, no questions generated")
    return None

# ========================
# GOOGLE SHEETS APPEND
# ========================
def append_to_sheet(rows):
    """Append rows to Google Sheets with retry."""
    for attempt in range(3):
        try:
            sheet.append_rows(rows, value_input_option="RAW")
            logger.info(f"✅ Saved {len(rows)} rows to Google Sheets")
            return True
        except Exception as e:
            logger.warning(f"   Sheets attempt {attempt+1}/3 failed: {e}")
            time.sleep(5)
    
    return False

# ========================
# MAIN WORKFLOW
# ========================
def main_workflow():
    pdf_path = "book.pdf"
    
    # Download PDF if not present
    if not os.path.exists(pdf_path):
        logger.info("📥 Downloading PDF from Google Drive...")
        try:
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=False)
            if os.path.getsize(pdf_path) < 1_000_000:
                raise Exception("Downloaded file too small (<1MB)")
            logger.info("✅ PDF ready")
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            return
    
    # Verify PDF
    try:
        with fitz.open(pdf_path) as doc:
            total_pages = doc.page_count
        logger.info(f"📄 Total pages in PDF: {total_pages}")
    except Exception as e:
        logger.error(f"❌ Cannot read PDF: {e}")
        return
    
    # Initialize tracker
    current = init_tracker()
    
    if current >= total_pages:
        logger.info("✅ Already completed all pages")
        return
    
    buffer = []
    pages_processed = 0
    questions_generated = 0
    
    while current < total_pages:
        # Process single page (not pairs, to avoid skipping)
        page_idx = current
        section = get_section(page_idx)
        logger.info(f"\n📖 Processing page {page_idx+1}/{total_pages} | Section: {section}")
        
        # Extract text
        page_text = extract_text_from_page(pdf_path, page_idx)
        
        if page_text:
            logger.info(f"   ✓ Extracted {len(page_text.split())} words")
            
            # Generate questions
            questions = generate_questions(page_text, section)
            
            if questions:
                for q in questions:
                    buffer.append([
                        q["section"], q["question"], q["opt1"], q["opt2"], q["opt3"],
                        q["opt4"], q["opt5"], q["answer"], q["explanation"]
                    ])
                    questions_generated += 1
                
                logger.info(f"   ➕ {len(questions)} questions added to buffer")
            else:
                logger.warning(f"   ⚠️ No questions generated for page {page_idx+1}")
        else:
            logger.warning(f"   ⚠️ Page {page_idx+1} has no readable text")
        
        # Save buffer if it reaches 50+ rows
        if len(buffer) >= 50:
            if append_to_sheet(buffer):
                buffer = []
            else:
                logger.error("Failed to save to sheets, keeping in buffer")
        
        pages_processed += 1
        update_tracker(page_idx + 1)
        current = page_idx + 1
        gc.collect()
        
        logger.info(f"   ⏳ Waiting 10 seconds (rate limit protection)...")
        time.sleep(10)
    
    # Save remaining buffer
    if buffer:
        logger.info(f"\n📤 Saving final {len(buffer)} rows...")
        append_to_sheet(buffer)
    
    logger.info(f"\n🎉 COMPLETED!")
    logger.info(f"   Pages processed: {pages_processed}")
    logger.info(f"   Questions generated: {questions_generated}")

# ========================
# FLASK SERVER
# ========================
app = Flask(__name__)

@app.route('/')
def home():
    return "AGTA 2026 Engine v3.0 - Professional Edition ✅"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    Thread(target=main_workflow, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
