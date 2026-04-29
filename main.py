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
import google.generativeai as genai

# ==========================================
# 1. ENVIRONMENT VARIABLES (with validation)
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
# 2. DATABASE & GOOGLE SHEETS
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
# 3. TRACKER - FORCE START FROM PAGE 1
# ==========================================
def init_tracker_and_sheet():
    try:
        print("🔁 Reset Mode: Starting from Page 1 (Index 0)")
        tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": 0}}, upsert=True)
        if tracker_col.find_one({"_id": "pdf_tracker_reset_done"}) is None:
            sheet.clear()
            headers = ["Section", "Question", "Option 1", "Option 2", "Option 3", "Option 4", "Option 5", "Answer", "Explanation"]
            sheet.append_row(headers)
            tracker_col.insert_one({"_id": "pdf_tracker_reset_done", "done": True})
        return 0
    except Exception as e:
        print(f"❌ Tracker error: {e}")
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
# 4. OCR WITH POPPLER PATH
# ==========================================
def extract_text_with_ocr(reader, pdf_path, page_index):
    text = reader.pages[page_index].extract_text()
    if text and len(text.strip()) > 100:
        return text
    print(f"🔍 OCR Page {page_index+1}")
    try:
        images = convert_from_path(
            pdf_path,
            first_page=page_index+1,
            last_page=page_index+1,
            dpi=300,
            poppler_path="/usr/bin"
        )
        ocr_text = ""
        for img in images:
            gray = img.convert("L")
            ocr_text += pytesseract.image_to_string(gray, lang="eng", config="--oem 3 --psm 6")
        return ocr_text
    except Exception as e:
        print(f"⚠️ OCR error: {e}")
        return ""

# ==========================================
# 5. IMPROVED AFO-LEVEL PROMPT (20-35 words)
# ==========================================
def build_afo_prompt(text, section):
    return f"""
You are an expert agriculture exam paper setter for IBPS AFO Mains, NABARD Grade A, and ICAR JRF level.

Generate high quality MCQs from the given agriculture content.

STRICT RULES:

1. Each question MUST be 20–35 words long.
2. Questions must be conceptual, analytical, and exam-oriented.
3. DO NOT use phrases like:
   - "According to the text"
   - "Based on the passage"
   - "From the given text"
4. Questions must resemble real competitive exam questions (IBPS AFO Mains standard).
5. Each question must have exactly 5 distinct options.
6. Provide correct answer and a short conceptual explanation (20–30 words).

QUESTION COUNT RULE (based on content density):
- Rich technical content → 12–15 MCQs
- Moderate content → 6–10 MCQs
- Limited content → 3–5 MCQs

DO NOT create unnecessary or repetitive questions.

OUTPUT FORMAT (CSV only):
Section,Question,Option1,Option2,Option3,Option4,Option5,Answer,Explanation

Topic/Section: {section}

Agriculture Content:
{text[:7000]}
"""

def extract_csv_from_response(raw_text):
    lines = raw_text.strip().split('\n')
    if lines and 'Section' in lines[0]:
        lines = lines[1:]
    data = []
    for line in lines:
        if line.strip() and ',' in line:
            parts = line.split(',')
            if len(parts) >= 9:
                data.append({
                    "section": parts[0],
                    "question": parts[1],
                    "opt1": parts[2],
                    "opt2": parts[3],
                    "opt3": parts[4],
                    "opt4": parts[5],
                    "opt5": parts[6],
                    "answer": parts[7],
                    "explanation": parts[8]
                })
    if data:
        return data
    # Fallback to JSON
    try:
        match = re.search(r'\[\s*{.*}\s*\]', raw_text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except:
        pass
    return None

# ==========================================
# 6. NVIDIA MULTI-MODEL (Correct models)
# ==========================================
def call_nvidia(prompt):
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {NVIDIA_KEY}",
        "Content-Type": "application/json"
    }
    NVIDIA_MODELS = [
        "nvidia/llama-3.1-nemotron-70b-instruct",
        "meta/llama-3.1-70b-instruct",
        "meta/llama-3.1-8b-instruct",
        "mistralai/mixtral-8x7b-instruct",
        "mistralai/mistral-7b-instruct"
    ]
    for model in NVIDIA_MODELS:
        try:
            print(f"🧠 Trying NVIDIA model: {model}")
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.4,
                "top_p": 0.9,
                "max_tokens": 2500
            }
            r = requests.post(url, headers=headers, json=payload, timeout=60)
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
            else:
                print(f"⚠️ NVIDIA {model} HTTP {r.status_code}")
        except Exception as e:
            print(f"⚠️ NVIDIA {model} error: {e}")
            continue
    return None

# ==========================================
# 7. OPENROUTER MULTI-MODEL (as you provided)
# ==========================================
def call_openrouter(prompt):
    OPENROUTER_MODELS = [
        "mistralai/mixtral-8x7b-instruct",
        "mistralai/mistral-7b-instruct",
        "meta-llama/llama-3-70b-instruct",
        "meta-llama/llama-3-8b-instruct",
        "google/gemma-7b-it"
    ]
    url = "https://openrouter.ai/api/v1/chat/completions"
    for key in OPENROUTER_KEYS:
        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        }
        for model in OPENROUTER_MODELS:
            try:
                print(f"🔄 Trying OpenRouter model: {model}")
                payload = {
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.35,
                    "top_p": 0.9,
                    "max_tokens": 2500
                }
                r = requests.post(url, headers=headers, json=payload, timeout=90)
                if r.status_code == 200:
                    data = r.json()
                    if "choices" in data and data["choices"]:
                        return data["choices"][0]["message"]["content"]
            except Exception as e:
                print(f"⚠️ OpenRouter {model} failed: {e}")
                continue
    return None

# ==========================================
# 8. GEMINI MULTI-MODEL (as you provided)
# ==========================================
def call_gemini(prompt):
    GEMINI_MODELS = [
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.0-pro",
        "gemini-2.0-flash",
        "gemini-1.5-pro",
        "gemini-1.5-flash"
    ]
    for key in GEMINI_KEYS:
        try:
            genai.configure(api_key=key)
            for model_name in GEMINI_MODELS:
                try:
                    print(f"🤖 Trying Gemini {model_name}")
                    model = genai.GenerativeModel(model_name)
                    response = model.generate_content(prompt)
                    if response and response.text:
                        return response.text
                except Exception as e:
                    print(f"⚠️ Gemini {model_name} failed: {e}")
                    continue
        except:
            continue
    return None

# ==========================================
# 9. MASTER AI ROUTER
# ==========================================
def generate_questions(text, section):
    prompt = build_afo_prompt(text, section)
    raw = None
    # Primary: NVIDIA multi-model
    raw = call_nvidia(prompt)
    if not raw:
        # Secondary: OpenRouter multi-model
        raw = call_openrouter(prompt)
    if not raw:
        # Final: Gemini multi-model
        raw = call_gemini(prompt)
    if not raw:
        print("❌ All AI providers failed")
        return None
    print("🧠 AI Response received")
    return extract_csv_from_response(raw)

# ==========================================
# 10. MAIN WORKFLOW (Optimized)
# ==========================================
def main_workflow():
    pdf_path = "book.pdf"
    if not os.path.exists(pdf_path):
        print("📥 Downloading PDF...")
        gdown.download(f"https://drive.google.com/uc?id={DRIVE_FILE_ID}", pdf_path, quiet=False)
    print("✅ PDF ready")

    reader = pypdf.PdfReader(pdf_path)
    total_pages = len(reader.pages)
    print(f"📄 Total pages: {total_pages}")

    curr_page = init_tracker_and_sheet()

    while curr_page < total_pages:
        try:
            next_page = min(curr_page + 2, total_pages)
            section = get_section(curr_page)
            print(f"\n📖 Pages {curr_page+1}-{next_page} | {section}")

            text = ""
            for i in range(curr_page, next_page):
                extracted = extract_text_with_ocr(reader, pdf_path, i)
                if extracted:
                    text += extracted + "\n"

            if len(text.strip()) < 50:
                print("⚠️ Skipping blank/unreadable chunk")
                curr_page = next_page
                update_tracker(curr_page)
                continue

            questions = generate_questions(text, section)
            if questions and isinstance(questions, list) and len(questions) > 0:
                rows = []
                for q in questions:
                    rows.append([
                        q.get("section", section),
                        q.get("question", ""),
                        q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                        q.get("opt4", ""), q.get("opt5", ""),
                        q.get("answer", ""), q.get("explanation", "")
                    ])
                sheet.append_rows(rows, value_input_option="RAW")
                print(f"✅ Added {len(rows)} MCQs to Sheet")
            else:
                print("⚠️ No valid MCQs generated")

            curr_page = next_page
            update_tracker(curr_page)
            print(f"⏳ Progress: {curr_page}/{total_pages} pages. Cooling 20s...")
            time.sleep(20)
        except Exception as e:
            print(f"❌ Loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(60)
        finally:
            gc.collect()
    print("🏁 Processing complete!")

# ==========================================
# 11. FLASK SERVER
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Agri-Bot AFO Engine is LIVE (NVIDIA → OpenRouter → Gemini, multi-model)"

@app.route('/status')
def status():
    try:
        t = tracker_col.find_one({"_id": "pdf_tracker"})
        page = t.get("current_page", 0) if t else 0
        return {"status": "running", "current_page": page + 1}
    except:
        return {"status": "error"}

def run_server():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, threaded=True)

# ==========================================
# 12. MAIN ENTRY
# ==========================================
if __name__ == "__main__":
    print("🚀 Starting AFO MCQ Generator Engine (Multi-Model Fallback)")
    Thread(target=main_workflow, daemon=True).start()
    run_server()
