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
# 3. TRACKER & SHEET LOGIC (Safe Reset)
# ==========================================
def init_tracker_and_sheet():
    reset_flag = tracker_col.find_one({"_id": "sheet_init_v6"})
    
    if not reset_flag:
        print("🔁 CLEAN START: Wiping Google Sheet and resetting to Page 1...")
        sheet.clear()
        headers = ["Topic", "Question", "Option A", "Option B", "Option C", "Option D", "Option E", "Answer", "Explanation"]
        sheet.append_row(headers)
        
        tracker_col.update_one({"_id": "sheet_init_v6"}, {"$set": {"done": True}}, upsert=True)
        tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": 0}}, upsert=True)
        return 0
    else:
        tracker = tracker_col.find_one({"_id": "pdf_tracker"})
        page = tracker.get("current_page", 0) if tracker else 0
        print(f"✅ Safe Restart: Resuming from Page {page+1} (Index {page})")
        return page

def update_tracker(page_num):
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}})

def get_section(p_idx):
    human_page = p_idx + 1
    for s, e, name in SECTION_RANGES:
        if s <= human_page <= e: return name
    return "General Agriculture"

# ==========================================
# 4. FAST TEXT EXTRACTION (Optimized DPI)
# ==========================================
def extract_text_with_ocr(doc, pdf_path, page_index):
    page = doc.load_page(page_index)
    text = page.get_text()
    
    if text and len(text.strip()) > 100:
        return text
        
    print(f"🔍 Empty Page! OCR activated for Page {page_index+1}")
    try:
        images = convert_from_path(pdf_path, first_page=page_index+1, last_page=page_index+1, dpi=200, poppler_path="/usr/bin")
        ocr_text = ""
        for img in images:
            gray = img.convert("L")
            ocr_text += pytesseract.image_to_string(gray, lang="eng", config="--oem 3 --psm 6")
        return ocr_text
    except Exception as e:
        print(f"⚠️ OCR error: {e}")
        return ""

# ==========================================
# 5. PROFESSIONAL PROMPT (STRICT ENGLISH)
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

The term used to define the variation derived from any form of the cell or tissue culture is known as
Options: Genetic Engineering | Somaclonal variation | Genetic Variation | Environmental Variation | None
Answer: Somaclonal variation

A destructive polyhedrosis disease of silkworm that is related to wilt and is marked by spotty yellowing of the skin and internal liquefaction also called jaundice is
Options: Pebrine | Grasseries | Flacherie | Aspergillosis | None
Answer: Grasseries

The process of removing the green colouring (known as chlorophyll) from the skin of citrus fruit by introducing measured amounts of ethylene gas is known as
Options: Ripening | Degreening | Physiological maturity | Denavelling | Dehusking
Answer: Degreening
"""

    return f"""
You are Satyam Sir, an expert agriculture mentor setting a mock paper for the AGTA 2026 and IBPS AFO Mains batches.

Your task: Generate high-quality multiple-choice questions from the given agricultural content.

STRICT RULES:
1. Language: STRICTLY ENGLISH ONLY. Do NOT use Hindi or any other language. All questions, options, and explanations must be entirely in English.
2. Difficulty: Moderate level. Keep them engaging and relevant for competitive exams.
3. Question Length: Each question MUST be strictly between 20 to 35 words. 
4. Structure: Exactly 5 options per question.
5. Formatting: Do NOT output prefixes like "Option A:" or "1)". Provide plain text.
6. Output Limit: Maximum 10 questions per chunk.

Topic: {section}

{examples}

OUTPUT FORMAT:
You MUST return ONLY a RAW JSON ARRAY. No markdown block formatting (do not use ```json).
[
  {{
    "section": "{section}",
    "question": "Question text here strictly in English (20-35 words)...",
    "opt1": "First option text",
    "opt2": "Second option text",
    "opt3": "Third option text",
    "opt4": "Fourth option text",
    "opt5": "Fifth option text",
    "answer": "Exact text of the correct option",
    "explanation": "Short conceptual explanation strictly in English (20 words max)."
  }}
]

Content:
{text[:5000]}
"""

# ==========================================
# 6. JSON PARSER (Non-Greedy & Answer Validation)
# ==========================================
def extract_and_clean_json(raw_text):
    if not raw_text:
        return None
    try:
        match = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', raw_text)
        if match:
            data = json.loads(match.group(0))
        else:
            data = json.loads(raw_text)
            
        data = data[:10]  
        
        cleaned_data = []
        for item in data:
            opt1 = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', str(item.get("opt1", "")), flags=re.IGNORECASE).strip()
            opt2 = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', str(item.get("opt2", "")), flags=re.IGNORECASE).strip()
            opt3 = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', str(item.get("opt3", "")), flags=re.IGNORECASE).strip()
            opt4 = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', str(item.get("opt4", "")), flags=re.IGNORECASE).strip()
            opt5 = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', str(item.get("opt5", "")), flags=re.IGNORECASE).strip()
            ans = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', str(item.get("answer", "")), flags=re.IGNORECASE).strip()
            
            # Answer Validation Rule
            if ans not in [opt1, opt2, opt3, opt4, opt5]:
                continue
                
            cleaned_data.append({
                "section": str(item.get("section", "")).strip(),
                "question": str(item.get("question", "")).strip(),
                "opt1": opt1,
                "opt2": opt2,
                "opt3": opt3,
                "opt4": opt4,
                "opt5": opt5,
                "answer": ans,
                "explanation": str(item.get("explanation", "")).strip()
            })
        return cleaned_data
    except Exception as e:
        print(f"⚠️ JSON Parsing failed: {e}")
        return None

# ==========================================
# 7. AI FALLBACK ENGINE (CLAUDE FIRST!)
# ==========================================
def call_claude(prompt):
    """Call Claude API - FIXED VERSION without proxies parameter"""
    if not CLAUDE_KEY:
        print("⚠️ Claude API Key not found")
        return None
    
    try:
        print("🤖 Claude Model: claude-opus-4-20250805")
        # Initialize client without proxies parameter (not supported in SDK 0.28.0)
        client = anthropic.Anthropic(api_key=CLAUDE_KEY)
        
        response = client.messages.create(
            model="claude-opus-4-20250805",
            max_tokens=2500,
            temperature=0.4,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        return response.content[0].text
    except Exception as e:
        print(f"⚠️ Claude Error: {e}")
        return None

def call_openrouter(prompt):
    url = "https://openrouter.ai/api/v1/chat/completions"
    models = [
        "openrouter/auto",
        "meta-llama/llama-3.1-70b-instruct",
        "anthropic/claude-3.5-sonnet",
        "mistralai/mixtral-8x7b-instruct"
    ]
    for model in models:
        for key in OPENROUTER_KEYS:
            try:
                print(f"🔄 OpenRouter Model: {model}")
                headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
                payload = {
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.4,
                    "max_tokens": 2500
                }
                r = requests.post(url, headers=headers, json=payload, timeout=60)
                if r.status_code == 200:
                    return r.json()["choices"][0]["message"]["content"]
                time.sleep(5)
            except Exception as e:
                print(f"⚠️ OpenRouter Error ({model}): {e}")
                continue
    return None

def call_nvidia(prompt):
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"}
    models = [
        "meta/llama3-70b-instruct",
        "nvidia/nemotron-4-340b-instruct"
    ]
    for model in models:
        try:
            print(f"🧠 NVIDIA Model: {model}")
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.35,
                "max_tokens": 2500
            }
            r = requests.post(url, headers=headers, json=payload, timeout=60)
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
            time.sleep(5)
        except Exception as e:
            print(f"⚠️ NVIDIA Error ({model}): {e}")
            continue
    return None

def call_gemini(prompt):
    # Reverted to original models as the user's comment was not part of the original code
    models = [
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.0-flash",
        "gemini-1.5-pro",
        "gemini-1.5-flash"
    ]
    for key in GEMINI_KEYS:
        try:
            genai.configure(api_key=key)
            for model_name in models:
                try:
                    print(f"🤖 Gemini Model: {model_name}")
                    model = genai.GenerativeModel(model_name)
                    response = model.generate_content(
                        prompt,
                        generation_config=genai.types.GenerationConfig(
                            temperature=0.4,
                            response_mime_type="application/json"
                        )
                    )
                    if response and response.text:
                        return response.text
                    time.sleep(5)
                except Exception as e:
                    print(f"⚠️ Gemini Error ({model_name}): {e}")
                    continue
        except Exception as e:
            continue
    return None

def generate_questions(text, section):
    """Generate questions with Claude as primary provider"""
    prompt = build_afo_prompt(text, section)
    
    # ✅ TRY CLAUDE FIRST (SAHI MODEL)
    raw = call_claude(prompt)
    if raw:
        cleaned = extract_and_clean_json(raw)
        if cleaned:
            print(f"✅ Claude generated {len(cleaned)} questions")
            return cleaned
    
    print("⚠️ Claude failed → waiting before OpenRouter")
    time.sleep(5)
    
    # Fallback to OpenRouter
    raw = call_openrouter(prompt)
    if raw:
        cleaned = extract_and_clean_json(raw)
        if cleaned:
            print(f"✅ OpenRouter generated {len(cleaned)} questions")
            return cleaned
        
    print("⚠️ OpenRouter failed → waiting before NVIDIA")
    time.sleep(10)
    
    # Fallback to NVIDIA
    raw = call_nvidia(prompt)
    if raw:
        cleaned = extract_and_clean_json(raw)
        if cleaned:
            print(f"✅ NVIDIA generated {len(cleaned)} questions")
            return cleaned
        
    print("⚠️ NVIDIA failed → waiting before Gemini")
    time.sleep(10)
    
    # Last resort: Gemini
    raw = call_gemini(prompt)
    if raw:
        cleaned = extract_and_clean_json(raw)
        if cleaned:
            print(f"✅ Gemini generated {len(cleaned)} questions")
            return cleaned
        
    print("❌ All AI providers failed")
    return None

# ==========================================
# 8. MAIN WORKFLOW (Crash-Proof & Batching)
# ==========================================
def main_workflow():
    pdf_path = "book.pdf"
    
    # 1. Download Step with Try/Except Safety
    if not os.path.exists(pdf_path):
        print("📥 Downloading PDF...")
        try:
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=False)
            print("✅ PDF Downloaded Successfully!")
        except Exception as e:
            print(f"❌ CRITICAL ERROR: PDF Download Failed! {e}")
            return

    # 2. PDF Open Step with Try/Except Safety
    try:
        doc = fitz.open(pdf_path)
        total_pages = doc.page_count
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Failed to open PDF! {e}")
        return

    curr_page = init_tracker_and_sheet()
    buffer = []

    # 3. Main Processing Loop
    while curr_page < total_pages:
        try:
            next_page = min(curr_page + 2, total_pages)
            section = get_section(curr_page)
            print(f"\n📖 Pages {curr_page+1}-{next_page} | Topic: {section}")

            text = ""
            for i in range(curr_page, next_page):
                extracted = extract_text_with_ocr(doc, pdf_path, i)
                if extracted: text += extracted + "\n"

            if len(text.strip()) < 50:
                print("⚠️ Skipping blank chunk")
                update_tracker(next_page)
                curr_page = next_page
                continue

            questions = generate_questions(text, section)

            if questions and len(questions) > 0:
                for q in questions:
                    buffer.append([
                        q["section"], q["question"], 
                        q["opt1"], q["opt2"], q["opt3"], q["opt4"], q["opt5"], 
                        q["answer"], q["explanation"]
                    ])
                
                # Batch Buffer Logic (Append every 50 questions)
                if len(buffer) >= 50:
                    sheet.append_rows(buffer, value_input_option="RAW")
                    print(f"✅ Batch Appended {len(buffer)} MCQs to Sheet")
                    buffer = []
            
            update_tracker(next_page)
            curr_page = next_page
            print("⏳ Cooldown for 8 seconds...")
            time.sleep(8)

        except Exception as e:
            print(f"❌ Loop error: {e}")
            time.sleep(60)
        finally:
            text = None
            questions = None
            gc.collect()

    # Flush remaining buffer at the end
    if len(buffer) > 0:
        sheet.append_rows(buffer, value_input_option="RAW")
        print(f"✅ Final Batch Appended {len(buffer)} MCQs to Sheet")
    
    doc.close()

# ==========================================
# 9. FLASK SERVER
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "AGTA 2026 Engine LIVE!"

if __name__ == "__main__":
    print("🚀 Starting Agri AI Engine with Claude")
    Thread(target=main_workflow, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)), threaded=True)
