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
import fitz  # PyMuPDF
import gdown
from flask import Flask
from threading import Thread
import google.generativeai as genai
import anthropic

# Optional OCR
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------- ENV VARS ----------
CLAUDE_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
NVIDIA_KEY = os.getenv("NVIDIA_API_KEY", "").strip()
OPENROUTER_KEYS = [k.strip() for k in os.getenv("OPENROUTER_KEYS", "").split(",") if k.strip()]
GEMINI_KEYS = [k.strip() for k in os.getenv("GEMINI_KEYS", "").split(",") if k.strip()]

MONGO_URI = os.getenv("MONGO_URI", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
DRIVE_FILE_ID = os.getenv("DRIVE_FILE_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "")

MANUAL_START_PAGE = int(os.getenv("MANUAL_START_PAGE", "969")) - 1   # 0‑based
POPPLER_PATH = os.getenv("POPPLER_PATH", "/usr/bin")

if not MONGO_URI or not SHEET_ID or not DRIVE_FILE_ID or not SERVICE_ACCOUNT_JSON:
    logger.error("Missing required environment variables. Exiting.")
    sys.exit(1)

# ---------- SECTIONS ----------
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

# ---------- DB & SHEETS ----------
logger.info("Connecting to MongoDB...")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
try:
    mongo_client.server_info()  # force connection check
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

# ---------- TRACKER ----------
def init_tracker():
    tracker = tracker_col.find_one({"_id": "pdf_tracker"})
    last = tracker.get("current_page", 0) if tracker else 0
    if last < MANUAL_START_PAGE:
        last = MANUAL_START_PAGE
        update_tracker(last)
        logger.info(f"Manual override: starting from page {last+1}")
    else:
        logger.info(f"Resuming from page {last+1}")
    return last

def update_tracker(page_idx):
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_idx}}, upsert=True)

def get_section(page_idx):
    human = page_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human <= e:
            return name
    return "General Agriculture"

# ---------- TEXT EXTRACTION ----------
def extract_text_with_ocr(doc, pdf_path, page_idx):
    page = doc.load_page(page_idx)
    text = page.get_text()
    if text and len(text.strip()) > 100:
        return text
    if not OCR_AVAILABLE or page_idx > 1200:   # skip OCR after page 1200
        return ""
    logger.info(f"OCR on page {page_idx+1}")
    try:
        images = convert_from_path(pdf_path, first_page=page_idx+1, last_page=page_idx+1, dpi=200, poppler_path=POPPLER_PATH)
        ocr = ""
        for img in images:
            ocr += pytesseract.image_to_string(img.convert("L"), lang="eng", config="--oem 3 --psm 6")
        return ocr
    except Exception as e:
        logger.warning(f"OCR failed: {e}")
        return ""

# ---------- PROMPT ----------
def build_prompt(text, section):
    examples = """... (same as before) ..."""  # keep your examples
    return f"""You are Satyam Sir... (same prompt but add: Return STRICT valid JSON only without explanation)
    
    Topic: {section}
    {examples}
    
    Expected JSON schema: [...]
    Content: {text[:5000]}
    """

# ---------- JSON CLEANER ----------
def extract_and_clean_json(raw):
    if not raw:
        return None
    clean = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw.strip(), flags=re.MULTILINE)
    match = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', clean)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except:
        return None
    data = data[:10]
    result = []
    def clean_opt(opt):
        opt = str(opt).strip()
        opt = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', opt)
        return opt.strip()
    for item in data:
        q = item.get("question", "").strip()
        opt1 = clean_opt(item.get("opt1",""))
        opt2 = clean_opt(item.get("opt2",""))
        opt3 = clean_opt(item.get("opt3",""))
        opt4 = clean_opt(item.get("opt4",""))
        opt5 = clean_opt(item.get("opt5",""))
        ans = clean_opt(item.get("answer",""))
        if not q or not ans:
            continue
        if ans.lower() not in [opt1.lower(), opt2.lower(), opt3.lower(), opt4.lower(), opt5.lower()]:
            continue
        result.append({
            "section": str(item.get("section","")).strip() or "General Agriculture",
            "question": q,
            "opt1": opt1, "opt2": opt2, "opt3": opt3, "opt4": opt4, "opt5": opt5,
            "answer": ans,
            "explanation": str(item.get("explanation","")).strip()
        })
    return result if result else None

# ---------- AI PROVIDERS (with 60s global timeout) ----------
def call_gemini(prompt):
    if not GEMINI_KEYS:
        return None
    for key in GEMINI_KEYS:
        try:
            genai.configure(api_key=key)
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(
                prompt + "\n\nReturn STRICT valid JSON only without explanation.",
                generation_config={"response_mime_type": "text/plain"}
            )
            if response and response.text:
                return response.text
        except Exception as e:
            logger.warning(f"Gemini error: {e}")
            continue
    return None

def call_claude(prompt):
    if not CLAUDE_KEY:
        return None
    try:
        client = anthropic.Anthropic(api_key=CLAUDE_KEY)
        resp = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=2500,
            temperature=0.4,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text
    except Exception as e:
        logger.warning(f"Claude error: {e}")
        return None

def call_openrouter(prompt):
    if not OPENROUTER_KEYS:
        return None
    models = ["openrouter/auto", "meta-llama/llama-3.1-70b-instruct", "anthropic/claude-3.5-sonnet", "mistralai/mixtral-8x7b-instruct"]
    url = "https://openrouter.ai/api/v1/chat/completions"
    for key in OPENROUTER_KEYS:          # keys first (avoid slow loop)
        for model in models:
            try:
                resp = requests.post(url, headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                                     json={"model": model, "messages": [{"role": "user", "content": prompt}]}, timeout=30)
                if resp.status_code == 200:
                    return resp.json()["choices"][0]["message"]["content"]
                if resp.status_code == 429:
                    time.sleep(2)
            except:
                continue
    return None

def call_nvidia(prompt):
    if not NVIDIA_KEY:
        return None
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    models = ["meta/llama3-70b-instruct", "nvidia/nemotron-4-340b-instruct"]
    for model in models:
        try:
            resp = requests.post(url, headers={"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"},
                                 json={"model": model, "messages": [{"role": "user", "content": prompt}]}, timeout=30)
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
        except:
            continue
    return None

def generate_questions(text, section):
    prompt = build_prompt(text, section)
    start_time = time.time()
    # Priority: Gemini -> Claude -> OpenRouter -> NVIDIA
    for func in [call_gemini, call_claude, call_openrouter, call_nvidia]:
        if time.time() - start_time > 60:
            logger.warning("Global timeout (60s) reached")
            return None
        raw = func(prompt)
        if raw:
            cleaned = extract_and_clean_json(raw)
            if cleaned:
                return cleaned
    return None

# ---------- SHEETS WRITE WITH RETRY ----------
def append_to_sheet(rows):
    for attempt in range(3):
        try:
            sheet.append_rows(rows, value_input_option="RAW")
            logger.info(f"Appended {len(rows)} rows to Google Sheets")
            return True
        except Exception as e:
            logger.warning(f"Sheets write attempt {attempt+1} failed: {e}")
            time.sleep(5 * (attempt + 1))
    logger.error("Failed to write to Google Sheets after 3 retries")
    return False

# ---------- MAIN WORKFLOW ----------
def main_workflow():
    pdf_path = "book.pdf"
    # Download if missing
    if not os.path.exists(pdf_path):
        logger.info("Downloading PDF from Google Drive...")
        try:
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=False)
            if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) == 0:
                raise Exception("Empty download")
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return

    with fitz.open(pdf_path) as doc:
        total = doc.page_count
    logger.info(f"Total pages: {total}")

    curr = init_tracker()
    if curr >= total:
        logger.info("Already completed")
        return

    buffer = []
    while curr < total:
        nxt = min(curr + 2, total)
        section = get_section(curr)
        logger.info(f"Processing pages {curr+1}-{nxt} | {section}")

        combined = ""
        with fitz.open(pdf_path) as doc:
            for i in range(curr, nxt):
                text = extract_text_with_ocr(doc, pdf_path, i)
                if text:
                    combined += text + "\n"
        # Memory protection
        combined = combined[:15000]

        if len(combined.strip()) > 50:
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
                logger.warning("No valid questions generated")
        else:
            logger.warning(f"Insufficient text on pages {curr+1}-{nxt}")

        update_tracker(nxt)
        curr = nxt
        gc.collect()
        time.sleep(8)

    if buffer:
        append_to_sheet(buffer)
    logger.info("All pages processed successfully!")

# ---------- FLASK APP ----------
app = Flask(__name__)
@app.route('/')
def home():
    return "AGTA 2026 Engine LIVE (production grade)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    Thread(target=main_workflow, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
