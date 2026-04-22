import os
import sys
import json
import time
import re
from datetime import datetime, timedelta
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
from google import genai
import pypdf
import gdown
from keep_alive import keep_alive

sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# 1. CONFIGURATION
# ==========================================
OPENROUTER_KEYS = os.getenv("OPENROUTER_KEYS", "").split(",")
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# OpenRouter: auto‑select free model
OPENROUTER_MODEL = "openrouter/free"
OPENROUTER_TEMPERATURE = 0.4

# Gemini models
GEMINI_MODELS = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro"
]
GEMINI_TEMPERATURE = 0.4

# Validation
if not GEMINI_KEYS or GEMINI_KEYS == ['']:
    raise ValueError("❌ GEMINI_KEYS not set")
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI not set")
if not SERVICE_ACCOUNT_JSON:
    raise ValueError("❌ SERVICE_ACCOUNT_JSON not set")

OPENROUTER_KEYS = [k.strip() for k in OPENROUTER_KEYS if k.strip()]
GEMINI_KEYS = [k.strip() for k in GEMINI_KEYS if k.strip()]

# MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['agri_data_bank']
progress_collection = db['process_tracker']
config_collection = db['config']
print("✅ MongoDB Connection: SUCCESS")

# Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1
print("✅ Google Sheets Connection: SUCCESS")

# ==========================================
# 2. RESET (ONLY ONCE – MONGO FLAG)
# ==========================================
def is_reset_done():
    doc = config_collection.find_one({"_id": "reset_flag"})
    return doc.get("done", False) if doc else False

def mark_reset_done():
    config_collection.update_one({"_id": "reset_flag"}, {"$set": {"done": True}}, upsert=True)

def get_current_page():
    try:
        tracker = progress_collection.find_one({"_id": "pdf_tracker"})
        return tracker.get("current_page", 0) if tracker else 0
    except:
        return 0

def update_current_page(page_num):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)
    print(f"📌 Page tracker updated to {page_num}")

def reset_and_start_fresh():
    print("🔄 Resetting all data – starting fresh from page 1...")
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": 0}}, upsert=True)
    if 'questions_db' in db.list_collection_names():
        db['questions_db'].drop()
    sheet.clear()
    sheet.append_row(["Section", "Question", "Option1", "Option2", "Option3", "Option4", "Option5", "Answer"])
    print("✅ Reset complete. Starting from page 0.")
    mark_reset_done()

# ==========================================
# 3. WAIT UNTIL 5:30 AM IST
# ==========================================
def wait_until_5_30_am_ist():
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    target_ist = now_ist.replace(hour=5, minute=30, second=0, microsecond=0)
    if now_ist >= target_ist:
        target_ist += timedelta(days=1)
    wait_seconds = (target_ist - now_ist).total_seconds()
    print(f"⏰ Waiting until {target_ist.strftime('%Y-%m-%d %H:%M:%S')} IST ({wait_seconds/3600:.1f} hours)")
    time.sleep(wait_seconds)

# ==========================================
# 4. PDF EXTRACTION (3 PAGES)
# ==========================================
def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    if not os.path.exists(pdf_path):
        return ""
    reader = pypdf.PdfReader(pdf_path)
    total_pages = len(reader.pages)
    if start_page >= total_pages:
        return None
    actual_end = min(end_page, total_pages)
    print(f"📖 Reading pages {start_page} to {actual_end} (total {total_pages})")
    text = ""
    for i in range(start_page, actual_end):
        try:
            page_text = reader.pages[i].extract_text()
            if page_text:
                text += page_text + "\n"
        except:
            continue
    return text.strip() if text.strip() else ""

# ==========================================
# 5. OPENROUTER API CALL
# ==========================================
def call_openrouter(api_key, prompt):
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": OPENROUTER_TEMPERATURE,
        "max_tokens": 2000
    }
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    elif response.status_code == 402:
        raise Exception("INSUFFICIENT_CREDITS")
    else:
        raise Exception(f"OpenRouter error {response.status_code}: {response.text[:200]}")

# ==========================================
# 6. PROFESSIONAL PROMPT – SHORT Q & SHORT OPTIONS
# ==========================================
def build_prompt(text_chunk):
    truncated = text_chunk[:6000]
    return f"""You are a professional agriculture exam question setter for UPSSSC AGTA and IBPS AFO (Mains level).
Based on the provided text, generate between 15 and 20 high‑quality conceptual questions.

CRITICAL RULES (STRICTLY FOLLOW):
1. Each question MUST be **at most 2 lines** (concise, to the point). Never make questions longer than 2 lines.
2. Options (opt1 to opt5) must be **very short** – preferably 1 to 3 words, or a short phrase (e.g., "Fluchloralin", "Pre-emergence", "2 kg/ha", "At sowing", "Root uptake"). Do NOT write long sentences as options.
3. Question tone: professional, direct, exam‑oriented (like AFO Mains).
4. Do NOT use phrases like "According to the text" or "Based on the above".
5. Return ONLY a valid JSON list (no markdown, no extra text, no explanation).
6. Each object must have exactly: section, question, opt1, opt2, opt3, opt4, opt5, answer.
7. Section should be the subject (Agronomy, Soil Science, Horticulture, Genetics, Plant Pathology, etc.).

STYLE EXAMPLES (short questions, short options):
- "Which herbicide is used as pre‑emergence in sunflower at 2 kg ha⁻¹?"
  Options: ["Fluchloralin", "Pendimethalin", "Atrazine", "Glyphosate", "2,4-D"]
- "What is the critical timing for pre‑emergence herbicide application to avoid crop injury?"
  Options: ["Before sowing", "At sowing", "After emergence", "At flowering", "At maturity"]
- "Which nutrient deficiency causes Khaira disease in rice?"
  Options: ["Zinc", "Iron", "Manganese", "Copper", "Boron"]
- "What is the LD50 value of a pesticide an indicator of?"
  Options: ["Acute toxicity", "Chronic toxicity", "Bioaccumulation", "Persistence", "Synergism"]
- "Which method is used to determine available phosphorus in neutral to alkaline soils?"
  Options: ["Olsen's", "Bray's", "Mehlich's", "Truog's", "Morgan's"]
- "What is the recommended percentage of male plants in papaya orchard for proper pollination?"
  Options: ["10%", "20%", "30%", "40%", "50%"]
- "Which biotechnique was used to clone Dolly the sheep?"
  Options: ["Somatic cell nuclear transfer", "Embryo splitting", "Gene editing", "Artificial insemination", "Cloning vector"]

You MUST generate at least 15 questions, maximum 20.

Text source:
{truncated}

JSON template:
[
  {{
    "section": "Agronomy",
    "question": "...",
    "opt1": "...", "opt2": "...", "opt3": "...", "opt4": "...", "opt5": "...",
    "answer": "..."
  }}
]"""

# ==========================================
# 7. GENERATE QUESTIONS (OPENROUTER + GEMINI)
# ==========================================
def generate_questions(text_chunk):
    prompt = build_prompt(text_chunk)
    total_attempts = 0
    max_attempts = (len(OPENROUTER_KEYS) + len(GEMINI_KEYS) * len(GEMINI_MODELS))

    # LAYER 1: OPENROUTER
    if OPENROUTER_KEYS:
        print(f"🌐 OpenRouter layer: {len(OPENROUTER_KEYS)} keys with auto model (temp={OPENROUTER_TEMPERATURE})")
        for key_idx, api_key in enumerate(OPENROUTER_KEYS):
            total_attempts += 1
            print(f"🌐 Attempt {total_attempts}/{max_attempts}: OpenRouter key {key_idx}")
            try:
                response_text = call_openrouter(api_key, prompt)
                clean = re.sub(r'```json\n|\n```|```', '', response_text).strip()
                json_match = re.search(r'\[[\s\S]*\]', clean)
                if json_match:
                    clean = json_match.group(0)
                questions = json.loads(clean)
                if not isinstance(questions, list):
                    questions = [questions]
                if len(questions) < 15:
                    print(f"⚠️ Only {len(questions)} questions, retrying same key...")
                    time.sleep(60)
                    continue
                print(f"✅ Generated {len(questions)} questions using OpenRouter (auto model)")
                return questions[:20]
            except Exception as e:
                err = str(e)
                print(f"⚠️ OpenRouter key {key_idx} failed: {err[:150]}")
                if "INSUFFICIENT_CREDITS" in err or "402" in err:
                    print("⏳ Insufficient credits – moving to next key (short wait)")
                    time.sleep(5)
                    continue
                if "JSON" in err or "Expecting value" in err:
                    print("⏳ JSON parse error – moving to next key")
                    time.sleep(5)
                    continue
                print("⏳ Waiting 60 seconds before next key...")
                time.sleep(60)
                continue

    # LAYER 2: GEMINI
    print(f"🤖 Gemini layer: {len(GEMINI_KEYS)} keys × {len(GEMINI_MODELS)} models = {len(GEMINI_KEYS)*len(GEMINI_MODELS)} attempts (temp={GEMINI_TEMPERATURE})")
    for key_idx, api_key in enumerate(GEMINI_KEYS):
        for model in GEMINI_MODELS:
            total_attempts += 1
            print(f"🤖 Attempt {total_attempts}/{max_attempts}: Gemini key {key_idx}, model {model}")
            try:
                gemini_client = genai.Client(api_key=api_key)
                response = gemini_client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config={
                        "temperature": GEMINI_TEMPERATURE,
                        "max_output_tokens": 2000
                    }
                )
                raw = response.text
                clean = re.sub(r'```json\n|\n```|```', '', raw).strip()
                json_match = re.search(r'\[[\s\S]*\]', clean)
                if json_match:
                    clean = json_match.group(0)
                questions = json.loads(clean)
                if not isinstance(questions, list):
                    questions = [questions]
                if len(questions) < 15:
                    print(f"⚠️ Only {len(questions)} questions, retrying same model...")
                    time.sleep(60)
                    continue
                print(f"✅ Generated {len(questions)} questions using Gemini/{model}")
                return questions[:20]
            except Exception as e:
                err = str(e)
                print(f"⚠️ Gemini {model} failed: {err[:150]}")
                if "429" in err or "503" in err:
                    print("⏳ Quota/overload – waiting 60 seconds")
                    time.sleep(60)
                    continue
                if "404" in err:
                    print("⏳ Model not found – moving to next model")
                    time.sleep(5)
                    continue
                print("⏳ Waiting 10 seconds")
                time.sleep(10)
                continue

    # ALL ATTEMPTS EXHAUSTED
    print(f"🚨 All {max_attempts} attempts exhausted. Waiting 1 hour...")
    time.sleep(3600)
    try:
        test_client = genai.Client(api_key=GEMINI_KEYS[0])
        test_client.models.generate_content(model=GEMINI_MODELS[0], contents="test")
    except Exception as test_err:
        if "429" in str(test_err):
            print("⚠️ Quota still exhausted. Waiting until 5:30 AM IST.")
            wait_until_5_30_am_ist()
    return generate_questions(text_chunk)

# ==========================================
# 8. MAIN LOOP
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    if not is_reset_done():
        reset_and_start_fresh()
    else:
        print(f"✅ Reset already performed. Resuming from page {get_current_page()} (no reset)")
    
    pdf = "book.pdf"
    if not os.path.exists(pdf):
        print("📥 Downloading book...")
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
        gdown.download(url, pdf, quiet=False)
        if os.path.exists(pdf):
            print(f"✅ Downloaded: {os.path.getsize(pdf)} bytes")
        else:
            print("❌ Download failed.")
            return
    else:
        print(f"✅ PDF exists: {pdf}")
    
    print("\n" + "="*60)
    print("📖 PROCESSING (3 pages/chunk, 15–20 questions)")
    print(f"🌐 OpenRouter: {len(OPENROUTER_KEYS)} keys with auto model (temp={OPENROUTER_TEMPERATURE})")
    print(f"🤖 Gemini: {len(GEMINI_KEYS)} keys × {len(GEMINI_MODELS)} models = {len(GEMINI_KEYS)*len(GEMINI_MODELS)} attempts (temp={GEMINI_TEMPERATURE})")
    print(f"🔁 Total attempts per chunk: {len(OPENROUTER_KEYS) + len(GEMINI_KEYS)*len(GEMINI_MODELS)}")
    print("⏱️ Gaps: 5s for credits/parse/404 errors, 60s for quota/other errors")
    print("🚨 After all fails: wait 1h, then if still 429 wait until 5:30 AM IST")
    print("="*60 + "\n")
    
    total_q = 0
    errors = 0
    
    while True:
        try:
            page = get_current_page()
            next_page = page + 3
            print(f"\n🔍 Chunk: pages {page} to {next_page-1}")
            text = extract_pdf_text(page, next_page, pdf)
            if text is None:
                print("🏁 End of PDF.")
                break
            if len(text) < 150:
                print(f"⚠️ Low text ({len(text)} chars). Skipping.")
                update_current_page(next_page)
                continue
            
            print(f"🧠 Generating 15–20 questions ({len(text)} chars)")
            questions = generate_questions(text)
            if not questions:
                print("⚠️ No questions. Advancing.")
                update_current_page(next_page)
                continue
            
            rows = []
            for q in questions:
                rows.append([
                    q.get("section", "General"),
                    q.get("question", ""),
                    q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                    q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                ])
            sheet.append_rows(rows, value_input_option="RAW")
            total_q += len(questions)
            print(f"✅ Appended {len(questions)} questions (total {total_q})")
            update_current_page(next_page)
            errors = 0
            print("⏳ Success gap: 30 seconds")
            time.sleep(30)
        except Exception as e:
            print(f"❌ Loop error: {e}")
            errors += 1
            if errors > 5:
                break
            time.sleep(60)
    
    print(f"\n📊 FINAL: {total_q} questions in Google Sheets.")

if __name__ == "__main__":
    main()
