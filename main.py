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
import fitz  # PyMuPDF
import gdown
from flask import Flask
from threading import Thread
from pdf2image import convert_from_path
import pytesseract
import google.generativeai as genai
import anthropic

# ==========================================
# 1. ENVIRONMENT VARIABLES
# ==========================================
CLAUDE_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
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
# 3. TRACKER & APPEND-ONLY LOGIC
# ==========================================
def init_tracker_and_sheet():
    tracker = tracker_col.find_one({"_id": "pdf_tracker"})
    page = tracker.get("current_page", 0) if tracker else 0
    
    # 🔥 MANUAL OVERRIDE: Index 969 = Page 970
    MANUAL_START_PAGE = 969 
    
    if page < MANUAL_START_PAGE:
        page = MANUAL_START_PAGE
        update_tracker(page)
        print(f"🚀 MANUAL OVERRIDE: Jumping directly to Page {page+1}")
    else:
        print(f"✅ Safe Restart: Resuming from Page {page+1} (MongoDB Tracked)")

    # 🛑 SHEET DELETE FUNCTION IS REMOVED
    print("📝 Google Sheet check complete. Existing data is SAFE. Appending new data at the bottom...")
    
    return page

def update_tracker(page_num):
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)

def get_section(p_idx):
    human_page = p_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human_page <= e: return name
    return "General Agriculture"

# ==========================================
# 4. FAST TEXT EXTRACTION
# ==========================================
def extract_text_with_ocr(doc, pdf_path, page_index):
    page = doc.load_page(page_index)
    text = page.get_text()
    if text and len(text.strip()) > 100:
        return text
    
    print(f"🔍 OCR activated for Page {page_index+1}")
    try:
        images = convert_from_path(pdf_path, first_page=page_index+1, last_page=page_index+1, dpi=200, poppler_path="/usr/bin")
        ocr_text = ""
        for img in images:
            ocr_text += pytesseract.image_to_string(img.convert("L"), lang="eng", config="--oem 3 --psm 6")
        return ocr_text
    except: return ""

# ==========================================
# 5. PROFESSIONAL PROMPT
# ==========================================
def build_afo_prompt(text, section):
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

STRICT RULES:
1. Language: 100% STRICTLY ENGLISH ONLY. No Hindi.
2. Difficulty: Moderate level. Keep them engaging.
3. Options: Exactly 5 options per question.
4. Answer Match: The text in the 'answer' field MUST exactly match the text of one of the 5 options.
5. Question Length: 20 to 35 words.
6. Explanation: Max 20 words.
7. Format: Return ONLY a raw JSON array. DO NOT use markdown code blocks (like ```json). No introductory or concluding text.

Topic: {section}

{examples}

EXPECTED JSON SCHEMA:
[
  {{
    "section": "{section}",
    "question": "Question text here strictly in English...",
    "opt1": "First option text",
    "opt2": "Second option text",
    "opt3": "Third option text",
    "opt4": "Fourth option text",
    "opt5": "Fifth option text",
    "answer": "Exact text of the correct option",
    "explanation": "Short conceptual explanation strictly in English."
  }}
]

Content:
{text[:5000]}
"""

# ==========================================
# 6. JSON PARSER
# ==========================================
def extract_and_clean_json(raw_text):
    if not raw_text: return None
    try:
        clean_text = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw_text.strip(), flags=re.MULTILINE).strip()
        match = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', clean_text)
        data = json.loads(match.group(0)) if match else json.loads(clean_text)
        data = data[:10]  
        
        cleaned_data = []
        for item in data:
            def clean_opt(opt):
                return re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', str(opt), flags=re.IGNORECASE).strip()
            
            opt1, opt2, opt3 = clean_opt(item.get("opt1", "")), clean_opt(item.get("opt2", "")), clean_opt(item.get("opt3", ""))
            opt4, opt5, ans = clean_opt(item.get("opt4", "")), clean_opt(item.get("opt5", "")), clean_opt(item.get("answer", ""))
            
            if not item.get("question") or not ans: continue
            
            valid_options_lower = [opt1.lower(), opt2.lower(), opt3.lower(), opt4.lower(), opt5.lower()]
            if ans.lower() not in valid_options_lower: continue
                
            cleaned_data.append({
                "section": str(item.get("section", "")).strip(),
                "question": str(item.get("question", "")).strip(),
                "opt1": opt1, "opt2": opt2, "opt3": opt3, "opt4": opt4, "opt5": opt5,
                "answer": ans, "explanation": str(item.get("explanation", "")).strip()
            })
        return cleaned_data
    except: return None

# ==========================================
# 7. AI FIX SECTION
# ==========================================
def call_claude(prompt):
    if not CLAUDE_KEY: return None
    try:
        print("🤖 Claude: claude-opus-4-20250805")
        client = anthropic.Anthropic(api_key=CLAUDE_KEY)
        response = client.messages.create(
            model="claude-opus-4-20250805", max_tokens=2500, temperature=0.4,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        print(f"⚠️ Claude Error: {e}")
        return None

def call_openrouter(prompt):
    models = ["openrouter/auto", "meta-llama/llama-3.1-70b-instruct", "anthropic/claude-3.5-sonnet", "mistralai/mixtral-8x7b-instruct"]
    url = "".join(["h", "t", "t", "p", "s", "://", "openrouter.ai", "/api/v1/chat/completions"])
    for model in models:
        for key in OPENROUTER_KEYS:
            try:
                print(f"🔄 OpenRouter: {model}")
                r = requests.post(url, headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, 
                                 json={"model": model, "messages": [{"role": "user", "content": prompt}]}, timeout=60)
                if r.status_code == 200: return r.json()["choices"][0]["message"]["content"]
                if r.status_code == 429: time.sleep(5)
            except: continue
    return None

def call_nvidia(prompt):
    url = "".join(["h", "t", "t", "p", "s", "://", "integrate.api.nvidia.com", "/v1/chat/completions"])
    models = ["meta/llama3-70b-instruct", "nvidia/nemotron-4-340b-instruct"]
    for model in models:
        try:
            print(f"🧠 NVIDIA: {model}")
            r = requests.post(url, headers={"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"},
                             json={"model": model, "messages": [{"role": "user", "content": prompt}]}, timeout=60)
            if r.status_code == 200: return r.json()['choices'][0]['message']['content']
        except: continue
    return None

def call_gemini(prompt):
    models = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for key in GEMINI_KEYS:
        if not key: continue
        try:
            genai.configure(api_key=key)
            for model_name in models:
                try:
                    print(f"🤖 Gemini: {model_name}")
                    model = genai.GenerativeModel(model_name)
                    response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
                    if response and response.text: return response.text
                except Exception as e:
                    if "429" in str(e) or "quota" in str(e).lower():
                        print("⏸️ Quota Full. Switching Key...")
                        break
                    continue
        except: continue
    return None

def generate_questions(text, section):
    prompt = build_afo_prompt(text, section)
    for func in [call_claude, call_openrouter, call_nvidia, call_gemini]:
        raw = func(prompt)
        if raw:
            cleaned = extract_and_clean_json(raw)
            if cleaned: return cleaned
    return None

# ==========================================
# 8. MAIN WORKFLOW
# ==========================================
def main_workflow():
    try:
        pdf_path = "book.pdf"
        print("▶️ ENGINE STARTING...")
        
        if not os.path.exists(pdf_path):
            print("📥 Downloading PDF (117MB)...")
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=True)
            print("✅ PDF Saved.")
            gc.collect()

        with fitz.open(pdf_path) as temp_doc:
            total_pages = temp_doc.page_count
        
        curr_page = init_tracker_and_sheet()
        buffer = []

        while curr_page < total_pages:
            next_page = min(curr_page + 2, total_pages)
            section = get_section(curr_page)
            
            try:
                print(f"\n📖 Pages {curr_page+1}-{next_page} | {section}")

                text = ""
                with fitz.open(pdf_path) as doc:
                    for i in range(curr_page, next_page):
                        extracted = extract_text_with_ocr(doc, pdf_path, i)
                        if extracted: text += extracted + "\n"

                if len(text.strip()) > 50:
                    questions = generate_questions(text, section)
                    if questions:
                        for q in questions:
                            buffer.append([q.get("section", section), q.get("question"), q.get("opt1"), q.get("opt2"), q.get("opt3"), q.get("opt4"), q.get("opt5"), q.get("answer"), q.get("explanation")])
                        
                        if len(buffer) >= 50:
                            sheet.append_rows(buffer, value_input_option="RAW")
                            print(f"✅ Batch Saved: {len(buffer)} MCQs")
                            buffer = []
                
                update_tracker(next_page)
                curr_page = next_page
                gc.collect()
                time.sleep(8)
                
            except Exception as e:
                print(f"❌ Error on Page {curr_page+1}: {e}")
                print("⏭️ Skipping to next page to prevent infinite loop...")
                update_tracker(next_page)
                curr_page = next_page
                time.sleep(10)
        
        if buffer: sheet.append_rows(buffer, value_input_option="RAW")
        print("🎉 FINISHED!")
    except Exception as e:
        print(f"❌ CRITICAL CRASH: {e}")

# ==========================================
# 9. FLASK SERVER
# ==========================================
app = Flask(__name__)
@app.route('/')
def home(): return "AGTA 2026 Engine LIVE!"

if __name__ == "__main__":
    Thread(target=main_workflow, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
