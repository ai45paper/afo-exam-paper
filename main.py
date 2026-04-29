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

# --- IMPORTS FOR OCR ---
from pdf2image import convert_from_path
import pytesseract
from PIL import Image

# ==========================================
# 1. SETUP & ENVIRONMENT VARIABLES
# ==========================================
NVIDIA_KEY = os.getenv("NVIDIA_API_KEY", "").strip()
OPENROUTER_KEYS = [k.strip() for k in os.getenv("OPENROUTER_KEYS", "").split(",") if k.strip()]
GEMINI_KEYS = [k.strip() for k in os.getenv("GEMINI_KEYS", "").split(",") if k.strip()]

MONGO_URI = os.getenv("MONGO_URI", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
DRIVE_FILE_ID = os.getenv("DRIVE_FILE_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "")

# Strict Section Mapping
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
# 2. DATABASE & GOOGLE SHEETS CONNECTION
# ==========================================
print("🔄 Connecting to MongoDB...")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['agri_data_bank']
tracker_col = db['process_tracker']

print("🔄 Connecting to Google Sheets...")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1

# ==========================================
# 3. TRACKER & ONE-TIME SHEET CLEAR LOGIC
# ==========================================
def init_tracker_and_sheet():
    tracker = tracker_col.find_one({"_id": "pdf_tracker"})
    if not tracker:
        print("⚠️ First Time Run Detected! Clearing Sheet & Setting Page 2...")
        sheet.clear()
        headers = ["Section", "Question", "Option 1", "Option 2", "Option 3", "Option 4", "Option 5", "Answer", "Explanation"]
        sheet.append_row(headers)
        tracker_col.insert_one({"_id": "pdf_tracker", "current_page": 1, "has_cleared_sheet": True})
        return 1
    else:
        p = tracker.get("current_page", 1)
        print(f"✅ Restarting safely from Page {p+1} (Index {p}). Data is protected.")
        return p

def update_tracker(page_num):
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}})

def get_section(p_idx):
    human_page = p_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human_page <= e: return name
    return "General Agriculture"

# ==========================================
# 3.5 OCR TEXT EXTRACTION (PRO OPTIMIZED)
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
            dpi=300
        )
        
        ocr_text = ""
        for img in images:
            gray = img.convert("L")  # Convert to Grayscale for better contrast
            ocr_text += pytesseract.image_to_string(
                gray,
                lang="eng",
                config="--oem 3 --psm 6"
            )
        return ocr_text
    except Exception as e:
        print(f"⚠️ OCR Error on Page {page_index+1}: {e}")
        return ""

# ==========================================
# 4. THE AI BRAIN (PROMPT & PARSING)
# ==========================================
def build_afo_prompt(text, section):
    return f"""
You are a Professional Agriculture Examiner and Senior Question Setter 
for competitive exams such as IBPS AFO Mains and UPSSSC AGTA.

TASK
You will receive TEXT extracted from TWO BOOK PAGES.
Your job is to read the text carefully and generate high-quality 
professional MCQ questions strictly from the provided content.

STRICT RULE
• Use ONLY the information present in the provided text.
• Do NOT add external knowledge.
• Do NOT assume facts not written in the text.
• Extract exam-oriented facts, concepts, numbers, varieties, diseases, etc.

Topic / Section: {section}

LANGUAGE
All questions must be written in **Professional Examiner-Level English**.

QUESTION REQUIREMENTS
1. Question Length: Each question must contain **20–35 words**.
2. Mix Ratio: 60% Conceptual, 10% Statement-based, 30% Fact-based.
3. Options: Each question MUST contain **exactly 5 distinct options**.
4. Explanation: Provide a **1–2 line conceptual explanation** for the correct answer.

QUESTION COUNT RULE (IMPORTANT)
• Limited info → **5–8 questions**
• Moderate info → **8–12 questions**
• Rich technical data → **12–20 questions**

OUTPUT FORMAT (CRITICAL)
Return ONLY a valid **JSON array**. Do NOT include Markdown, Code blocks, or text outside JSON.

JSON STRUCTURE
[
  {{
    "section": "{section}",
    "question": "Question text here...",
    "opt1": "Option A",
    "opt2": "Option B",
    "opt3": "Option C",
    "opt4": "Option D",
    "opt5": "Option E",
    "answer": "Exact text of the correct option",
    "explanation": "Short conceptual explanation."
  }}
]

SOURCE TEXT
{text[:7000]}
"""

def extract_json_from_response(raw_text):
    try:
        match = re.search(r'\[\s*{.*}\s*\]', raw_text, re.DOTALL)
        if match: return json.loads(match.group(0))
        else: return json.loads(raw_text)
    except Exception as e:
        print(f"⚠️ JSON Parsing failed. Error: {e}")
        return None

# ==========================================
# 5. API ROTATION LOGIC
# ==========================================
def call_nvidia(prompt):
    print("🧠 Using Primary Brain: NVIDIA (Nemotron-70B)...")
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"}
    payload = {"model": "nvidia/llama-3.1-nemotron-70b-instruct", "messages": [{"role": "user", "content": prompt}], "temperature": 0.4}
    r = requests.post(url, headers=headers, json=payload, timeout=50)
    r.raise_for_status()
    return r.json()['choices'][0]['message']['content']

def call_openrouter(prompt):
    for key in OPENROUTER_KEYS:
        try:
            print(f"🔄 Using Secondary Brain: OpenRouter (Key: {key[:5]}...)")
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            payload = {"model": "meta-llama/llama-3.1-70b-instruct", "messages": [{"role": "user", "content": prompt}]}
            r = requests.post(url, headers=headers, json=payload, timeout=50)
            if r.status_code == 200: return r.json()['choices'][0]['message']['content']
        except: continue
    raise Exception("All OpenRouter keys failed.")

def call_gemini(prompt):
    for key in GEMINI_KEYS:
        for model in ["gemini-1.5-pro", "gemini-1.5-flash", "gemini-2.0-flash"]:
            try:
                print(f"⚔️ Using Army Backup: Gemini ({model})")
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
                payload = {"contents": [{"parts": [{"text": prompt}]}]}
                r = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=50)
                if r.status_code == 200: return r.json()['candidates'][0]['content']['parts'][0]['text']
            except: continue
    raise Exception("All Gemini keys failed.")

def generate_questions(text, section):
    prompt = build_afo_prompt(text, section)
    raw_response = None
    try: raw_response = call_nvidia(prompt)
    except Exception as e:
        print(f"⚠️ NVIDIA Error: {e}")
        try: raw_response = call_openrouter(prompt)
        except Exception as e2:
            print(f"⚠️ OpenRouter Error: {e2}")
            try: raw_response = call_gemini(prompt)
            except Exception as e3:
                print(f"❌ Critical: All AI Providers failed.")
                return None
    if raw_response: return extract_json_from_response(raw_response)
    return None

# ==========================================
# 6. MASTER WORKFLOW
# ==========================================
def main_workflow():
    pdf_path = "book.pdf"
    if not os.path.exists(pdf_path):
        print("📥 Downloading PDF Book...")
        gdown.download(f"https://drive.google.com/uc?id={DRIVE_FILE_ID}", pdf_path, quiet=False)

    print("\n🚀 Starting PDF Processing Engine...")
    reader = pypdf.PdfReader(pdf_path)
    
    while True:
        try:
            curr_page = init_tracker_and_sheet()
            next_page = curr_page + 2
            section = get_section(curr_page)
            
            print(f"\n📖 Scanning Pages: {curr_page+1} to {next_page} | Topic: {section}")
            
            if curr_page >= len(reader.pages):
                print("🏁 Book completely processed!")
                break
                
            text = ""
            for i in range(curr_page, min(next_page, len(reader.pages))):
                extracted = extract_text_with_ocr(reader, pdf_path, i)
                if extracted:
                    text += extracted + "\n"
            
            if not text or len(text.strip()) < 50:
                print("⚠️ Page is completely blank or unreadable. Skipping chunk.")
                update_tracker(next_page)
                continue

            questions = generate_questions(text, section)
            
            if questions and isinstance(questions, list) and len(questions) > 0:
                rows_to_insert = []
                for q in questions:
                    rows_to_insert.append([
                        q.get("section", section), q.get("question", ""),
                        q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""), q.get("opt4", ""), q.get("opt5", ""),
                        q.get("answer", ""), q.get("explanation", "")
                    ])
                
                sheet.append_rows(rows_to_insert, value_input_option="RAW")
                print(f"✅ Success: Appended {len(rows_to_insert)} questions to Sheet.")
                update_tracker(next_page)
            else:
                print("⚠️ AI generated invalid format or empty list. Skipping chunk.")
                update_tracker(next_page)
                
            print("⏳ Cooldown for 20 seconds...")
            time.sleep(20)
            
        except Exception as e:
            print(f"❌ Main Loop Exception: {e}")
            time.sleep(60)
            
        finally:
            gc.collect()

# ==========================================
# 7. FLASK SERVER (KEEP-ALIVE FOR RENDER)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Agri-Bot Mains Engine is LIVE and Processing 24/7!"

def run_server():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    server_thread = Thread(target=run_server)
    server_thread.daemon = True
    server_thread.start()
    
    main_workflow()
