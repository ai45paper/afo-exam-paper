import os
import sys
import json
import time
import re
import gc
import base64
from datetime import datetime, timedelta
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
from google import genai
import pypdf
import gdown

# ==========================================
# 0. TRY TO IMPORT PYMUPDF (for image conversion & OCR)
# ==========================================
try:
    import pymupdf
    PYMU = True
except ImportError:
    PYMU = False
    print("⚠️ pymupdf not installed. OCR fallback will not work. Install with: pip install pymupdf")

sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# 1. MASTER CONTROL – NO AUTOMATIC WIPE AFTER FIRST RUN
# ==========================================
TOTAL_WIPE_OUT = True   # Ignored after first run due to MongoDB flag

# ==========================================
# 2. PAGE-BASED SECTION MAPPING (your ranges)
# ==========================================
SECTION_RANGES = [
    (1, 75, "Agronomy"),
    (76, 242, "Horticulture"),
    (243, 308, "Entomology"),
    (309, 389, "Fisheries"),
    (390, 517, "Animal Husbandry"),
    (518, 557, "Plant Pathology"),
    (558, 585, "Agricultural Economics"),
    (586, 704, "General Agriculture"),
    (705, 727, "Seed Technology"),
    (728, 759, "Weed Science"),
    (760, 771, "Apiculture"),
    (772, 803, "Forestry"),
    (804, 839, "Meteorology"),
    (840, 860, "Genetics and Breeding"),
    (861, 931, "Agricultural Engineering"),
    (932, 941, "Extension Education"),
    (942, 946, "Mushroom Cultivation"),
    (947, 964, "Sericulture"),
    (965, 966, "Lac Culture"),
    (967, 1075, "Soil Science")
]

def get_section_by_page(page_num):
    """page_num is 0‑indexed internal page number -> returns section name"""
    actual_page = page_num + 1
    for start, end, name in SECTION_RANGES:
        if start <= actual_page <= end:
            return name
    return "General Agriculture"

# ==========================================
# 3. CONFIGURATION & ENVIRONMENT
# ==========================================
OPENROUTER_KEYS = os.getenv("OPENROUTER_KEYS", "").split(",")
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

OPENROUTER_MODEL = "openrouter/free"
OPENROUTER_TEMPERATURE = 0.4

GEMINI_MODELS = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro"
]
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
# 4. TRACKER FUNCTIONS (PAGE ONLY)
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

# ==========================================
# 5. PERSISTENT MASTER RESET FLAG (ONCE)
# ==========================================
def is_master_reset_done():
    doc = config_collection.find_one({"_id": "master_reset_flag"})
    return doc.get("done", False) if doc else False

def mark_master_reset_done():
    config_collection.update_one({"_id": "master_reset_flag"}, {"$set": {"done": True}}, upsert=True)

def perform_total_wipeout():
    print("🧹 [MASTER RESET] पुरानी शीट और MongoDB डेटा साफ़ किया जा रहा है...")
    progress_collection.delete_many({})
    if 'questions_db' in db.list_collection_names():
        db['questions_db'].drop()
    sheet.clear()
    sheet.append_row(["Section", "Question", "Option1", "Option2", "Option3", "Option4", "Option5", "Answer"])
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": 0}}, upsert=True)
    mark_master_reset_done()
    print("✅ सब कुछ क्लीन हो गया। अब Page 1 से शुरू होगा।")

# ==========================================
# 6. WAIT UNTIL 5:30 AM IST (QUOTA RESET)
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
# 7. PDF TEXT EXTRACTION WITH OCR FALLBACK (FULLPROOF)
# ==========================================
def extract_text_from_pdf_page(page_num, pdf_path, gemini_client):
    """Extract text from a single PDF page using pypdf -> pymupdf -> Gemini Vision OCR."""
    # Try pypdf
    try:
        reader = pypdf.PdfReader(pdf_path)
        if page_num < len(reader.pages):
            text = reader.pages[page_num].extract_text()
            if text and text.strip():
                return text.strip()
    except:
        pass
    # Try pymupdf (fitz)
    if PYMU:
        try:
            doc = pymupdf.open(pdf_path)
            if page_num < len(doc):
                text = doc[page_num].get_text()
                doc.close()
                if text and text.strip():
                    return text.strip()
            else:
                if 'doc' in locals():
                    doc.close()
        except:
            pass
    # Fallback: Use Gemini Vision OCR (convert page to image)
    if PYMU and gemini_client:
        try:
            doc = pymupdf.open(pdf_path)
            if page_num < len(doc):
                pix = doc[page_num].get_pixmap()
                img_bytes = pix.tobytes("png")
                doc.close()
                b64 = base64.b64encode(img_bytes).decode('utf-8')
                response = gemini_client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=[
                        "Extract all readable text from this image of an agricultural book page. Return only the extracted text, no extra comments.",
                        {"mime_type": "image/png", "data": b64}
                    ]
                )
                text = response.text.strip()
                if text:
                    print(f"✅ OCR extracted {len(text)} chars from page {page_num+1}")
                    return text
                else:
                    print(f"⚠️ OCR gave empty result for page {page_num+1}")
            else:
                doc.close()
        except Exception as e:
            print(f"❌ OCR failed for page {page_num+1}: {e}")
    return ""

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    """Extract text from a range of pages (max 3 pages). Returns None if end of PDF."""
    if not os.path.exists(pdf_path):
        return ""
    # Get Gemini client for OCR (use first key)
    gemini_client = None
    try:
        gemini_client = genai.Client(api_key=GEMINI_KEYS[0])
    except:
        pass
    # Get total pages
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
        text = extract_text_from_pdf_page(p, pdf_path, gemini_client)
        if text:
            all_text.append(text)
        else:
            print(f"⚠️ No text extracted from page {p+1} even after OCR")
    combined = "\n".join(all_text).strip()
    if not combined:
        print(f"❌ No text at all for pages {start_page+1}-{actual_end}")
    return combined if combined else ""

# ==========================================
# 8. OPENROUTER API CALL
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
# 9. FULL PROMPT (48 EXAMPLES) – condensed for brevity; include full in final
# ==========================================
def build_prompt(text_chunk, section_name):
    truncated = text_chunk[:6000]
    prompt = f"""You are a professional agriculture exam question setter for UPSSSC AGTA and IBPS AFO (Mains level).
Based on the provided text, generate between 15 and 20 high‑quality conceptual questions.

CRITICAL RULES:
1. Level: MODERATE (conceptual and professional).
2. Questions MUST be 2 to 3 lines long (exactly like the examples below). DO NOT use phrases like "According to the text".
3. Return ONLY a valid JSON list. No code blocks, no markdown, no text explanations.
4. Provide exactly 5 options as fields named opt1, opt2, opt3, opt4, opt5. Do NOT use an "options" array. The correct answer must be placed in the "answer" field as the exact text of the correct option.
5. The "section" field must be set to the value we provide: "{section_name}". Use this exact section name for all questions in this chunk.
6. Each object must have: section, question, opt1, opt2, opt3, opt4, opt5, answer.

[FULL LIST OF 48 EXAMPLES – include all from previous code for style matching]

Text Source:
{truncated}
"""
    return prompt

# ==========================================
# 10. ROBUST QUESTION PARSER
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

# ==========================================
# 11. GENERATE QUESTIONS (OpenRouter + Gemini)
# ==========================================
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
                    if "No JSON array" in err or "JSON decode error" in err:
                        time.sleep(5)
                        continue
                    time.sleep(10)
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
                if "429" in err or "503" in err:
                    time.sleep(60)
                else:
                    time.sleep(5)
                continue

    print(f"🚨 All {max_attempts} attempts exhausted. Waiting 1 hour...")
    time.sleep(3600)
    return generate_questions(text_chunk, section_name)

# ==========================================
# 12. MAIN LOOP – RESUME, NO SKIP, OCR FALLBACK
# ==========================================
def main():
    from keep_alive import keep_alive
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    # Master reset only once (persistent flag)
    if not is_master_reset_done():
        perform_total_wipeout()
        print("⚠️ First run complete. Sheet will NEVER be cleared again.")
    else:
        current = get_current_page()
        print(f"✅ Resume Mode: Page {current+1} (0‑index {current}) – Sheet data preserved, no reset.")
    
    pdf = "book.pdf"
    if not os.path.exists(pdf):
        print("📥 Downloading book...")
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
        gdown.download(url, pdf, quiet=False)
        if not os.path.exists(pdf):
            print("❌ Download failed.")
            return
        print(f"✅ Downloaded: {os.path.getsize(pdf)} bytes")
    else:
        print(f"✅ PDF exists: {pdf}")
    
    print("\n" + "="*60)
    print("📖 PROCESSING (3 pages/chunk, 15–20 questions)")
    print("🗂️ Section mapping based on page ranges.")
    print("🚫 OCR fallback enabled – scanned pages will be converted to text.")
    print("🔄 Master reset flag stored – sheet NEVER cleared again.")
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
            if len(text.strip()) < 150:
                print(f"⚠️ Very little text ({len(text)} chars). Skipping chunk (after OCR).")
                update_current_page(next_page)
                gc.collect()
                time.sleep(10)
                continue
            
            print(f"🧠 Generating 15–20 questions ({len(text)} chars) for section: {section}")
            questions = generate_questions(text, section)
            if not questions:
                print("⚠️ No questions generated. Skipping chunk.")
                update_current_page(next_page)
                gc.collect()
                time.sleep(10)
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
            # Free memory
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
