import os
import sys
import json
import time
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
from google import genai
import pypdf
import gdown
from keep_alive import keep_alive

# Force flush prints
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# 1. CONFIGURATION
# ==========================================
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

if not GEMINI_KEYS or GEMINI_KEYS == ['']:
    raise ValueError("❌ GEMINI_KEYS not set")
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI not set")
if not SERVICE_ACCOUNT_JSON:
    raise ValueError("❌ SERVICE_ACCOUNT_JSON not set")

# MongoDB
client = MongoClient(MONGO_URI)
db = client['agri_data_bank']
progress_collection = db['process_tracker']
print("✅ MongoDB Connection: SUCCESS")

# Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1
print("✅ Google Sheets Connection: SUCCESS")

# ==========================================
# 2. RESET FUNCTION (ONCE)
# ==========================================
def reset_and_start_fresh():
    print("🔄 Resetting all data – starting fresh from page 1...")
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": 0}}, upsert=True)
    if 'questions_db' in db.list_collection_names():
        db['questions_db'].drop()
        print("✅ Dropped old questions from MongoDB")
    sheet.clear()
    sheet.append_row(["Section", "Question", "Option1", "Option2", "Option3", "Option4", "Option5", "Answer"])
    print("✅ Cleared Google Sheets and added header row")
    print("✅ Reset complete. Starting from page 0.")

# ==========================================
# 3. UTILITY FUNCTIONS
# ==========================================
def get_current_page():
    try:
        tracker = progress_collection.find_one({"_id": "pdf_tracker"})
        return tracker.get("current_page", 0) if tracker else 0
    except:
        return 0

def update_current_page(page_num):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)
    print(f"📌 Page tracker updated to {page_num}")

def get_gemini_key(key_index):
    key = GEMINI_KEYS[key_index % len(GEMINI_KEYS)].strip()
    print(f"🔑 Using key index {key_index % len(GEMINI_KEYS)} (key attempt {key_index})")
    return key

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    if not os.path.exists(pdf_path):
        return ""
    reader = pypdf.PdfReader(pdf_path)
    total_pages = len(reader.pages)
    if start_page >= total_pages:
        return None
    actual_end = min(end_page, total_pages)
    print(f"📖 Reading pages {start_page} to {actual_end} (total pages: {total_pages})")
    text = ""
    for i in range(start_page, actual_end):
        try:
            page_text = reader.pages[i].extract_text()
            if page_text:
                text += page_text + "\n"
        except Exception as e:
            print(f"⚠️ Page {i} extraction error: {e}")
            continue
    return text.strip() if text.strip() else ""

# ==========================================
# 4. AI GENERATION – 5 MODELS × 9 KEYS (45 attempts)
# ==========================================
# All available free models (in fallback order)
FREE_MODELS = [
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
    "gemini-3-flash"   # may not exist, but included for completeness
]

def generate_questions(text_chunk, key_attempt=0, model_attempt=0):
    """
    Try models sequentially. After trying all models with current key,
    move to next key. After all keys exhausted, wait 1 hour.
    """
    truncated_text = text_chunk[:15000]
    
    prompt = f"""You are a professional agriculture exam question setter for UPSSSC AGTA and IBPS AFO.
Based on the provided text (from an agriculture book), generate between 25 and 35 high‑quality conceptual questions.
- Each question must be 2–3 lines long.
- Do NOT use phrases like "According to the text".
- Return ONLY a valid JSON list (no extra text, no markdown).
- Each object must have fields: section, question, opt1, opt2, opt3, opt4, opt5, answer.
- The "section" should be the subject (e.g., Agronomy, Soil Science, Horticulture, Genetics).
- Provide exactly 5 options (opt1 to opt5).

STYLE EXAMPLES (match this tone):
- "Which soil science branch specifically focuses on the origin, morphological characteristics, classification processes, and geographical distribution of soils?"
- "Dolly the sheep became the first mammal cloned successfully. Which advanced biotechnological technique was utilized to produce this clone?"
- "The deficiency of which essential micronutrient leads to the manifestation of Khaira disease in rice, characterized by chlorotic leaves and stunted growth?"

You MUST generate at least 25 questions, maximum 35.
If the text is short, use your agricultural knowledge to fill gaps.

Text source:
{truncated_text}

JSON template:
[
  {{
    "section": "Agronomy",
    "question": "...",
    "opt1": "...", "opt2": "...", "opt3": "...", "opt4": "...", "opt5": "...",
    "answer": "..."
  }}
]"""

    # Loop through models starting from current model_attempt
    for m_idx in range(model_attempt, len(FREE_MODELS)):
        model = FREE_MODELS[m_idx]
        try:
            client = genai.Client(api_key=get_gemini_key(key_attempt))
            print(f"🤖 Trying model: {model} (key attempt {key_attempt}, model index {m_idx})")
            response = client.models.generate_content(model=model, contents=prompt)
            raw = response.text
            clean = re.sub(r'```json\n|\n```|```', '', raw).strip()
            questions = json.loads(clean)
            if not isinstance(questions, list):
                questions = [questions]
            if len(questions) < 25:
                print(f"⚠️ Only {len(questions)} questions – retrying same model (need 25+)")
                time.sleep(10)  # 10 sec gap before retrying same model
                return generate_questions(text_chunk, key_attempt, m_idx)  # retry same model
            print(f"✅ Generated {len(questions)} questions using {model}")
            return questions[:35]  # cap at 35
        except Exception as e:
            error_str = str(e)
            print(f"⚠️ {model} failed: {error_str[:200]}")
            # If it's a 429 (quota) or 503 (overload), wait longer and switch key immediately
            if "429" in error_str or "503" in error_str:
                print("⏳ Quota/overload error. Waiting 60 seconds before switching key...")
                time.sleep(60)
                # Move to next key (reset model index to 0)
                if key_attempt + 1 < len(GEMINI_KEYS) * len(FREE_MODELS):
                    print(f"🔄 Switching to next key (key attempt {key_attempt+1})")
                    time.sleep(60)  # 60 sec gap between key switches
                    return generate_questions(text_chunk, key_attempt + 1, 0)
                else:
                    print("🚨 All keys exhausted. Waiting 1 hour...")
                    time.sleep(3600)
                    return generate_questions(text_chunk, 0, 0)
            # For other errors (404, etc.), try next model within same key
            print(f"⏳ Waiting 10 seconds before trying next model...")
            time.sleep(10)  # 10 sec gap between model attempts
            continue
    
    # If all models tried with this key, move to next key
    if key_attempt + 1 < len(GEMINI_KEYS) * len(FREE_MODELS):
        print(f"🔄 All models failed with key attempt {key_attempt}. Switching to next key (total attempts: {key_attempt+1})")
        time.sleep(60)  # 60 sec gap when switching keys
        return generate_questions(text_chunk, key_attempt + 1, 0)
    else:
        print("🚨 All 45 attempts (9 keys × 5 models) exhausted. Waiting 1 hour before restart.")
        time.sleep(3600)
        return generate_questions(text_chunk, 0, 0)

# ==========================================
# 5. MAIN FUNCTION – PROCESS 3 PAGES PER CHUNK
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    # Reset only once (marker file)
    if not os.path.exists("reset_done.marker"):
        reset_and_start_fresh()
        with open("reset_done.marker", "w") as f:
            f.write("done")
    else:
        print("✅ Reset already performed. Resuming from saved page.")
    
    pdf_filename = "book.pdf"
    if not os.path.exists(pdf_filename):
        print("📥 Downloading book from Google Drive...")
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
        gdown.download(url, pdf_filename, quiet=False)
        if os.path.exists(pdf_filename):
            print(f"✅ Downloaded: {os.path.getsize(pdf_filename)} bytes")
        else:
            print("❌ Download failed. Exiting.")
            return
    else:
        print(f"✅ PDF already exists: {pdf_filename}")
    
    print("\n" + "="*60)
    print("📖 STARTING PAGE-BY-PAGE PROCESSING (3 pages per chunk, 25–35 questions)")
    print("📋 Fallback: 5 models × 9 keys = 45 total attempts per chunk")
    print("⏱️ Model gap: 10s | Key switch gap: 60s")
    print("="*60 + "\n")
    
    total_questions = 0
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5
    
    while True:
        try:
            current_page = get_current_page()
            next_page = current_page + 3  # Process 3 pages at a time
            print(f"\n🔍 Processing chunk: pages {current_page} to {next_page-1}")
            
            text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
            if text_chunk is None:
                print("🏁 End of PDF reached. Mission complete!")
                break
            
            if len(text_chunk.strip()) < 150:
                print(f"⚠️ Insufficient text on these pages ({len(text_chunk)} chars). Skipping chunk.")
                update_current_page(next_page)
                continue
            
            print(f"🧠 Generating 25–35 questions from {len(text_chunk)} characters...")
            questions = generate_questions(text_chunk)
            
            if not questions:
                print(f"⚠️ No questions generated for chunk {current_page}-{next_page-1}. Skipping.")
                update_current_page(next_page)
                continue
            
            # Batch append to Google Sheets
            rows = []
            for q in questions:
                rows.append([
                    q.get("section", "General"),
                    q.get("question", ""),
                    q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                    q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                ])
            sheet.append_rows(rows)
            total_questions += len(questions)
            print(f"✅ Appended {len(questions)} questions to Google Sheets (total so far: {total_questions})")
            
            # Update progress and reset error counter
            update_current_page(next_page)
            consecutive_errors = 0
            
            # Success gap: wait 30 seconds to avoid rate limiting
            print("⏳ Success Gap: Waiting 30 seconds before next chunk...")
            time.sleep(30)
            
        except Exception as e:
            print(f"❌ Main loop error: {e}")
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print("🚨 Too many consecutive errors. Stopping.")
                break
            print("⏳ Waiting 60 seconds before retry...")
            time.sleep(60)
    
    print(f"\n📊 FINAL STATISTICS: {total_questions} questions generated and stored in Google Sheets.")
    print("✅ All questions saved. Only page tracker kept in MongoDB.")

if __name__ == "__main__":
    main()
