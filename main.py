import os
import sys
import json
import time
import re
import gc
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
import pypdf
import gdown
from flask import Flask
from threading import Thread
from pdf2image import convert_from_path
import pytesseract
from PIL import Image

# ==========================================
# 1. ENVIRONMENT VARIABLES
# ==========================================
NVIDIA_KEY = os.getenv("NVIDIA_API_KEY", "").strip()
OPENROUTER_KEYS = [k.strip() for k in os.getenv("OPENROUTER_KEYS", "").split(",") if k.strip()]
GEMINI_KEYS = [k.strip() for k in os.getenv("GEMINI_KEYS", "").split(",") if k.strip()]

MONGO_URI = os.getenv("MONGO_URI", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
DRIVE_FILE_ID = os.getenv("DRIVE_FILE_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "")

if not MONGO_URI or not SHEET_ID or not DRIVE_FILE_ID or not SERVICE_ACCOUNT_JSON:
    print("❌ Missing required environment variables. Exiting.")
    sys.exit(1)

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

# ==========================================
# 2. DATABASE & SHEETS CONNECTION
# ==========================================
print("🔄 Connecting to MongoDB...")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo_client['agri_data_bank']
tracker_col = db['process_tracker']

print("🔄 Connecting to Google Sheets...")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1

# ==========================================
# 3. TRACKER - ALWAYS START FROM PAGE 1
# ==========================================
def init_tracker_and_sheet():
    try:
        print("🔁 Reset Mode: Starting from Page 1 (Index 0)")
        # Force reset to page 1
        tracker_col.update_one(
            {"_id": "pdf_tracker"},
            {"$set": {"current_page": 0}},
            upsert=True
        )
        # Clear sheet and add headers only first time
        if tracker_col.find_one({"_id": "pdf_tracker_reset_done"}) is None:
            sheet.clear()
            headers = ["Section", "Question", "Option 1", "Option 2", "Option 3", "Option 4", "Option 5", "Answer", "Explanation"]
            sheet.append_row(headers)
            tracker_col.insert_one({"_id": "pdf_tracker_reset_done", "done": True})
        return 0
    except Exception as e:
        print(f"❌ Error in tracker reset: {e}")
        sys.exit(1)

def update_tracker(page_num):
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}})

def get_section(p_idx):
    human_page = p_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human_page <= e:
            return name
    return "General Agriculture"

# ==========================================
# 4. OCR EXTRACTION
# ==========================================
def extract_text_with_ocr(reader, pdf_path, page_index):
    text = reader.pages[page_index].extract_text()
    if text and len(text.strip()) > 100:
        return text

    print(f"🔍 High-Res OCR activated for Page {page_index+1}")
    try:
        images = convert_from_path(
            pdf_path,
            first_page=page_index + 1,
            last_page=page_index + 1,
            dpi=300,
            poppler_path="/usr/bin"
        )
        ocr_text = ""
        for img in images:
            gray = img.convert("L")
            ocr_text += pytesseract.image_to_string(gray, lang="eng", config="--oem 3 --psm 6")
        return ocr_text
    except Exception as e:
        print(f"⚠️ OCR Error on Page {page_index+1}: {e}")
        return ""

# ==========================================
# 5. AI PROMPT & JSON
# ==========================================
def build_afo_prompt(text, section):
    return f"""
You are a Professional Agriculture Examiner and Senior Question Setter 
for competitive exams such as IBPS AFO Mains and UPSSSC AGTA.

TASK: Read the text carefully and generate high-quality professional MCQ questions.

STRICT RULE: Use ONLY the information present in the provided text.

Topic / Section: {section}

LANGUAGE: Professional Examiner-Level English.

REQUIREMENTS:
- Question Length: 20–35 words.
- Mix: 60% Conceptual, 10% Statement-based, 30% Fact-based.
- Exactly 5 options.
- Explanation: 1–2 lines.

QUESTION COUNT:
- Limited info → 5–8
- Moderate → 8–12
- Rich data → 12–20

OUTPUT FORMAT: ONLY valid JSON array.

JSON STRUCTURE:
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

SOURCE TEXT:
{text[:7000]}
"""

def extract_json_from_response(raw_text):
    try:
        match = re.search(r'\[\s*{.*}\s*\]', raw_text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        else:
            return json.loads(raw_text)
    except Exception as e:
        print(f"⚠️ JSON Parsing failed: {e}")
        return None

# ==========================================
# 6. API ROTATION
# ==========================================
def call_nvidia(prompt):
    print("🧠 Using Primary Brain: NVIDIA (Nemotron-70B)...")
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": "nvidia/llama-3.1-nemotron-70b-instruct",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.4
    }
    r = requests.post(url, headers=headers, json=payload, timeout=50)
    r.raise_for_status()
    return r.json()['choices'][0]['message']['content']

def call_openrouter(prompt):
    for key in OPENROUTER_KEYS:
        try:
            print(f"🔄 Using OpenRouter (Key: {key[:5]}...)")
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            payload = {"model": "meta-llama/llama-3.1-70b-instruct", "messages": [{"role": "user", "content": prompt}]}
            r = requests.post(url, headers=headers, json=payload, timeout=50)
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
        except:
            continue
    raise Exception("All OpenRouter keys failed.")

def call_gemini(prompt):
    for key in GEMINI_KEYS:
        for model in ["gemini-1.5-pro", "gemini-1.5-flash", "gemini-2.0-flash"]:
            try:
                print(f"⚔️ Using Gemini ({model})")
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
                payload = {"contents": [{"parts": [{"text": prompt}]}]}
                r = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=50)
                if r.status_code == 200:
                    return r.json()['candidates'][0]['content']['parts'][0]['text']
            except:
                continue
    raise Exception("All Gemini keys failed.")

def generate_questions(text, section):
    prompt = build_afo_prompt(text, section)
    raw_response = None
    try:
        raw_response = call_nvidia(prompt)
    except Exception as e:
        print(f"⚠️ NVIDIA Error: {e}")
        try:
            raw_response = call_openrouter(prompt)
        except Exception as e2:
            print(f"⚠️ OpenRouter Error: {e2}")
            try:
                raw_response = call_gemini(prompt)
            except Exception as e3:
                print(f"❌ All AI providers failed: {e3}")
                return None
    if raw_response:
        print("🧠 AI Response Received")
        return extract_json_from_response(raw_response)
    return None

# ==========================================
# 7. MAIN WORKFLOW (OPTIMIZED)
# ==========================================
def main_workflow():
    pdf_path = "book.pdf"
    if not os.path.exists(pdf_path):
        print("📥 Downloading PDF Book...")
        gdown.download(f"https://drive.google.com/uc?id={DRIVE_FILE_ID}", pdf_path, quiet=False)
        print("✅ PDF download completed.")
    else:
        print("✅ PDF already exists.")

    print("\n🚀 Starting PDF Processing Engine...")
    sys.stdout.flush()

    # Load PdfReader once (optimization)
    reader = pypdf.PdfReader(pdf_path)
    total_pages = len(reader.pages)
    print(f"📄 Total pages in PDF: {total_pages}")

    # Force start from page 1 (index 0)
    curr_page = init_tracker_and_sheet()

    while curr_page < total_pages:
        try:
            next_page = min(curr_page + 2, total_pages)
            section = get_section(curr_page)

            print(f"\n📖 Scanning Pages: {curr_page+1} to {next_page} | Topic: {section}")
            sys.stdout.flush()

            # Extract text from current chunk
            text = ""
            for i in range(curr_page, next_page):
                extracted = extract_text_with_ocr(reader, pdf_path, i)
                if extracted:
                    text += extracted + "\n"

            if not text or len(text.strip()) < 50:
                print("⚠️ Page is blank or unreadable. Skipping chunk.")
                curr_page = next_page
                update_tracker(curr_page)
                continue

            questions = generate_questions(text, section)

            if questions and isinstance(questions, list) and len(questions) > 0:
                rows_to_insert = []
                for q in questions:
                    rows_to_insert.append([
                        q.get("section", section), q.get("question", ""),
                        q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                        q.get("opt4", ""), q.get("opt5", ""),
                        q.get("answer", ""), q.get("explanation", "")
                    ])
                sheet.append_rows(rows_to_insert, value_input_option="RAW")
                print(f"✅ Appended {len(rows_to_insert)} questions to Sheet.")
            else:
                print("⚠️ AI gave invalid format. Skipping chunk.")

            curr_page = next_page
            update_tracker(curr_page)
            print(f"⏳ Progress: {curr_page}/{total_pages} pages done. Cooling 20 sec...")
            time.sleep(20)

        except Exception as e:
            print(f"❌ Loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(60)
        finally:
            gc.collect()

    print("🏁 Book completely processed!")

# ==========================================
# 8. FLASK SERVER
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Agri-Bot Mains Engine is LIVE and Processing 24/7!"

@app.route('/status')
def status():
    try:
        tracker = tracker_col.find_one({"_id": "pdf_tracker"})
        current_page = tracker.get("current_page", 0) if tracker else 0
        return {"status": "running", "current_page": current_page + 1}
    except:
        return {"status": "error"}

def run_server():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, threaded=True)

# ==========================================
# 9. MAIN
# ==========================================
if __name__ == "__main__":
    print("🚀 Starting Agri AI Engine")
    workflow_thread = Thread(target=main_workflow, daemon=True)
    workflow_thread.start()
    run_server()
