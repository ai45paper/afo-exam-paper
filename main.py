import os
import sys
import json
import time
import re
import gc
from datetime import datetime, timedelta
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
from google import genai
import pypdf
import gdown

# Try pymupdf for better PDF extraction (optional)
try:
    import pymupdf
    PYMU = True
except ImportError:
    PYMU = False
    print("⚠️ pymupdf not installed. Install with: pip install pymupdf (better text extraction)")

sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# 1. MASTER CONTROL (Disabled to protect your data)
# ==========================================
TOTAL_WIPE_OUT = False   # Set to False completely to ensure your data is safe.

# ==========================================
# 2. SECTION MAPPING (your page ranges)
# ==========================================
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

def get_section_by_page(page_num):
    actual_page = page_num + 1
    for start, end, name in SECTION_RANGES:
        if start <= actual_page <= end:
            return name
    return "General Agriculture"

# ==========================================
# 3. ENVIRONMENT & CONNECTIONS
# ==========================================
OPENROUTER_KEYS = os.getenv("OPENROUTER_KEYS", "").split(",")
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

OPENROUTER_MODEL = "openrouter/free"
OPENROUTER_TEMPERATURE = 0.4

GEMINI_MODELS = ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-pro"]
GEMINI_TEMPERATURE = 0.4

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
# 4. TRACKER & RESET (persistent)
# ==========================================
def get_current_page():
    try:
        tracker = progress_collection.find_one({"_id": "pdf_tracker"})
        return tracker.get("current_page", 0) if tracker else 0
    except:
        return 0

def update_current_page(page_num):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)
    print(f"📌 Page tracker updated to {page_num} (page {page_num+1} in 1‑index)")

def perform_total_wipeout():
    # Intentionally disabled to protect your data
    print("⚠️ Wipeout function called but disabled for safety. No data deleted.")
    pass

# ==========================================
# 5. PDF TEXT EXTRACTION
# ==========================================
def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    if not os.path.exists(pdf_path):
        return ""
    try:
        reader = pypdf.PdfReader(pdf_path)
        total_pages = len(reader.pages)
    except:
        total_pages = 0
        
    if start_page >= total_pages:
        return None
        
    actual_end = min(end_page, total_pages)
    print(f"📖 Extracting pages {start_page+1} to {actual_end}")
    all_text = []
    
    for p in range(start_page, actual_end):
        page_text = ""
        # Try pypdf first
        try:
            page_text = reader.pages[p].extract_text()
            if page_text and page_text.strip():
                all_text.append(page_text.strip())
                continue
        except:
            pass
            
        # Try pymupdf if available (better for formatted text)
        if PYMU:
            try:
                doc = pymupdf.open(pdf_path)
                page = doc[p]
                page_text = page.get_text("text") 
                doc.close()
                if page_text and page_text.strip():
                    all_text.append(page_text.strip())
                    continue
            except:
                pass
                
        print(f"⚠️ No text extracted from page {p+1} (It might be a scanned image/blank)")
        
    combined = "\n".join(all_text).strip()
    if not combined:
        print(f"❌ No text at all for pages {start_page+1}-{actual_end}")
    return combined if combined else ""

# ==========================================
# 6. FULL PROMPT
# ==========================================
def build_prompt(text_chunk, section_name):
    truncated = text_chunk[:6000]
    full_prompt = f"""You are a professional agriculture exam question setter for UPSSSC AGTA and IBPS AFO (Mains level).
Based on the provided text, generate between 15 and 20 high‑quality conceptual questions.

CRITICAL RULES:
1. Level: MODERATE (conceptual and professional).
2. Questions MUST be 2 to 3 lines long. DO NOT use phrases like "According to the text".
3. Return ONLY a valid JSON list. No code blocks, no markdown, no text explanations.
4. Provide exactly 5 options as fields named opt1, opt2, opt3, opt4, opt5. The correct answer must be placed in the "answer" field as the exact text of the correct option.
5. The "section" field must be set to the value we provide: "{section_name}".
6. Each object must have: section, question, opt1, opt2, opt3, opt4, opt5, answer.

Now, generate between 15 and 20 questions from the text below. Follow the exact format: each question as a JSON object with section (set to "{section_name}"), question, opt1, opt2, opt3, opt4, opt5, answer.

Text Source:
{truncated}
"""
    return full_prompt

# ==========================================
# 7. OPENROUTER & GEMINI QUESTION GENERATION
# ==========================================
def parse_questions(response_text, default_section):
    clean = re.sub(r'```json\n|\n```|```', '', response_text).strip()
    json_match = re.search(r'\[[\s\S]*\]', clean)
    if not json_match:
        raise ValueError("No JSON array found")
    try:
        raw = json.loads(json_match.group(0))
    except json.JSONDecodeError as e:
        fixed = re.sub(r',\s*}', '}', json_match.group(0))
        fixed = re.sub(r',\s*\]', ']', fixed)
        try:
            raw = json.loads(fixed)
        except:
            raise ValueError(f"JSON decode error: {e}")
            
    if not isinstance(raw, list):
        raw = [raw]
        
    valid = []
    for q in raw:
        if not isinstance(q, dict):
            continue
        if 'options' in q and isinstance(q['options'], list):
            opts = q['options']
            while len(opts) < 5:
                opts.append("")
            for i, opt in enumerate(opts[:5], 1):
                q[f'opt{i}'] = opt
            del q['options']
            
        required = ['question', 'opt1', 'opt2', 'opt3', 'opt4', 'opt5', 'answer']
        if not all(k in q for k in required):
            continue
        if 'section' not in q or not q['section']:
            q['section'] = default_section
        valid.append(q)
        
    if len(valid) < 15:
        raise ValueError(f"Only {len(valid)} valid questions (need 15)")
    return valid[:20]

def call_openrouter(api_key, prompt):
    url = "[https://openrouter.ai/api/v1/chat/completions](https://openrouter.ai/api/v1/chat/completions)"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
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

def generate_questions(text_chunk, section_name):
    prompt = build_prompt(text_chunk, section_name)
    max_attempts = len(OPENROUTER_KEYS) + len(GEMINI_KEYS) * len(GEMINI_MODELS)
    total = 0

    # OpenRouter
    if OPENROUTER_KEYS:
        for key_idx, api_key in enumerate(OPENROUTER_KEYS):
            total += 1
            for retry in range(2):
                print(f"🌐 Attempt {total}/{max_attempts} (retry {retry+1}/2): OpenRouter key {key_idx}")
                try:
                    resp = call_openrouter(api_key, prompt)
                    qs = parse_questions(resp, section_name)
                    print(f"✅ Generated {len(qs)} questions using OpenRouter")
                    return qs
                except Exception as e:
                    err = str(e)
                    print(f"⚠️ OpenRouter key {key_idx} failed: {err[:150]}")
                    if "INSUFFICIENT_CREDITS" in err or "402" in err:
                        break
                    time.sleep(5)
            time.sleep(2)

    # Gemini
    for key_idx, api_key in enumerate(GEMINI_KEYS):
        for model in GEMINI_MODELS:
            total += 1
            print(f"🤖 Attempt {total}/{max_attempts}: Gemini key {key_idx}, model {model}")
            try:
                client = genai.Client(api_key=api_key)
                resp = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config={"temperature": GEMINI_TEMPERATURE, "max_output_tokens": 2000}
                )
                qs = parse_questions(resp.text, section_name)
                print(f"✅ Generated {len(qs)} questions using Gemini/{model}")
                return qs
            except Exception as e:
                err = str(e)
                print(f"⚠️ Gemini {model} failed: {err[:150]}")
                if "429" in err or "RESOURCE_EXHAUSTED" in err:
                    print("⏳ API Limit hit! Waiting for 60 seconds before retrying...")
                    time.sleep(60)
                else:
                    time.sleep(5)
                continue

    print(f"🚨 All attempts exhausted. Waiting 2 minutes to cool down API...")
    time.sleep(120)
    return generate_questions(text_chunk, section_name)

# ==========================================
# 8. MAIN LOOP
# ==========================================
def main():
    try:
        from keep_alive import keep_alive
        keep_alive()
    except ImportError:
        pass
        
    print("🚀 Agri-Bot System Initiated.")
    
    # ---------------------------------------------------------
    # SAFETY LOCK & FORCE PAGE 23
    # ---------------------------------------------------------
    print("🛑 WIPE OUT DISABLED: Aapka Google Sheet data safe hai.")
    
    # Force the tracker to Page 23 (Index 22)
    print("⏩ Forcing script to start from Page 23 as requested...")
    update_current_page(22) 
    
    pdf = "book.pdf"
    if not os.path.exists(pdf):
        print("📥 Downloading book...")
        # Fixed URL string issue here (No markdown hidden links)
        url = "[https://drive.google.com/uc?id=](https://drive.google.com/uc?id=)" + DRIVE_FILE_ID.strip()
        gdown.download(url, pdf, quiet=False)
        if not os.path.exists(pdf):
            print("❌ Download failed.")
            return
        print(f"✅ Downloaded: {os.path.getsize(pdf)} bytes")
    else:
        print(f"✅ PDF exists: {pdf}")
    
    print("\n" + "="*60)
    print("📖 PROCESSING (3 pages/chunk, 15–20 questions)")
    print("="*60 + "\n")
    
    total_q = 0
    errors = 0
    
    while True:
        try:
            page = get_current_page()
            next_page = page + 3
            section = get_section_by_page(page)
            print(f"\n🔍 Chunk: pages {page+1} to {next_page} | Section: {section}")
            
            text = extract_pdf_text(page, next_page, pdf)
            if text is None:
                print("🏁 End of PDF reached.")
                break
            
            # If PDF is scanned images, it will trigger this block
            if len(text.strip()) < 150:
                print(f"⚠️ Insufficient text ({len(text)} chars) on pages {page+1}-{next_page}. These pages might be scanned photos.")
                print("⏩ Skipping to next chunk...")
                update_current_page(next_page)
                gc.collect()
                time.sleep(3) 
                continue
            
            print(f"🧠 Generating questions ({len(text)} chars) for {section}")
            questions = generate_questions(text, section)
            
            if not questions:
                print("⚠️ No questions generated. Skipping chunk.")
                update_current_page(next_page)
                gc.collect()
                time.sleep(5)
                continue
            
            rows = []
            for q in questions:
                rows.append([
                    q.get("section", section),
                    q.get("question", ""),
                    q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                    q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                ])
            sheet.append_rows(rows, value_input_option="RAW")
            total_q += len(questions)
            print(f"✅ Appended {len(questions)} questions to {section} (total {total_q})")
            
            update_current_page(next_page)
            errors = 0
            del text, questions, rows
            gc.collect()
            print("⏳ Success gap: 30 seconds")
            time.sleep(30)
            
        except Exception as e:
            print(f"❌ Loop error: {e}")
            errors += 1
            if errors > 10:
                print("Too many errors, stopping.")
                break
            time.sleep(60)
    
    print(f"\n📊 FINAL: {total_q} questions in Google Sheets.")

if __name__ == "__main__":
    main()
