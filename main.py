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

# Force flush prints so logs appear immediately
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

# ---------- MongoDB ----------
client = MongoClient(MONGO_URI)
db = client['agri_data_bank']
progress_collection = db['process_tracker']
print("✅ MongoDB Connection: SUCCESS")

# ---------- Google Sheets ----------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1
print("✅ Google Sheets Connection: SUCCESS")

# ==========================================
# 2. RESET FUNCTION (clear all old data)
# ==========================================
def reset_and_start_fresh():
    """Delete all previous questions from MongoDB and Google Sheets, reset page tracker to 0."""
    print("🔄 Resetting all data – starting fresh from page 1...")
    # Reset page tracker to 0
    progress_collection.update_one(
        {"_id": "pdf_tracker"},
        {"$set": {"current_page": 0}},
        upsert=True
    )
    # Delete all questions from MongoDB (if any collection exists)
    if 'questions_db' in db.list_collection_names():
        db['questions_db'].drop()
        print("✅ Dropped old questions from MongoDB")
    # Clear Google Sheets (keep only header row)
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
    progress_collection.update_one(
        {"_id": "pdf_tracker"},
        {"$set": {"current_page": page_num}},
        upsert=True
    )
    print(f"📌 Page tracker updated to {page_num}")

def get_gemini_key(attempt):
    key = GEMINI_KEYS[attempt % len(GEMINI_KEYS)].strip()
    print(f"🔑 Using key index {attempt % len(GEMINI_KEYS)} (attempt {attempt})")
    return key

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    """Extract text from a range of pages. Returns None if end of PDF."""
    if not os.path.exists(pdf_path):
        return ""
    reader = pypdf.PdfReader(pdf_path)
    total_pages = len(reader.pages)
    if start_page >= total_pages:
        return None
    end = min(end_page, total_pages)
    print(f"📖 Reading pages {start_page} to {end} (total {total_pages})")
    text = ""
    for i in range(start_page, end):
        try:
            page_text = reader.pages[i].extract_text()
            if page_text:
                text += page_text + "\n"
        except:
            continue
    return text.strip() if text.strip() else ""

# ==========================================
# 4. AI GENERATION – FORCED 25–35 QUESTIONS
# ==========================================
# List of all free Gemini models (fallback order)
FREE_MODELS = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
    "gemini-2.0-flash-lite"
]

def generate_questions(text_chunk, key_attempt=0, model_attempt=0):
    """
    Generate exactly 25–35 questions from the given text.
    Uses fallback across models and API keys.
    """
    # Prompt that explicitly demands 25-35 questions
    prompt = f"""You are a professional agriculture exam question setter for UPSSSC AGTA and IBPS AFO.
Based on the provided text, generate between 25 and 35 high‑quality conceptual questions.
- Each question must be 2–3 lines long.
- Do NOT use phrases like "According to the text".
- Return ONLY a valid JSON list (no extra text, no markdown).
- Each object must have fields: section, question, opt1, opt2, opt3, opt4, opt5, answer.
- The "section" should be the subject (e.g., Agronomy, Soil Science, Horticulture, Genetics).
- Provide exactly 5 options (opt1 to opt5).

Style examples (match this tone):
- "Which soil science branch specifically focuses on the origin, morphological characteristics, classification processes, and geographical distribution of soils?"
- "Dolly the sheep became the first mammal cloned successfully. Which advanced biotechnological technique was utilized to produce this clone?"
- "The deficiency of which essential micronutrient leads to the manifestation of Khaira disease in rice, characterized by chlorotic leaves and stunted growth?"
- "The traditional shifting cultivation system known as Jhum is also referred to as 'Bewar' and 'Dahiya.' In which Indian state are these local names used?"

You MUST generate at least 25 questions, maximum 35.
Do not generate fewer than 25 even if the text is short – use your knowledge of agriculture to fill gaps.

Text source:
{text_chunk[:20000]}

JSON template:
[
  {{
    "section": "Agronomy",
    "question": "...",
    "opt1": "...", "opt2": "...", "opt3": "...", "opt4": "...", "opt5": "...",
    "answer": "..."
  }}
]"""

    for idx in range(model_attempt, len(FREE_MODELS)):
        model = FREE_MODELS[idx]
        try:
            client = genai.Client(api_key=get_gemini_key(key_attempt))
            response = client.models.generate_content(model=model, contents=prompt)
            raw = response.text
            # Clean markdown
            clean = re.sub(r'```json\n|\n```|```', '', raw).strip()
            questions = json.loads(clean)
            if not isinstance(questions, list):
                questions = [questions]
            # Validate count
            if len(questions) < 25:
                print(f"⚠️ Only {len(questions)} questions generated – retrying with same model (need 25+)")
                # Retry with same model (sometimes it obeys better on second try)
                time.sleep(2)
                return generate_questions(text_chunk, key_attempt, idx)  # retry same model
            print(f"✅ Generated {len(questions)} questions using {model}")
            return questions[:35]  # cap at 35
        except Exception as e:
            print(f"⚠️ {model} failed: {e}")
            # If fatal error (404, 429, 503) switch to next key
            if "404" in str(e) or "429" in str(e) or "503" in str(e):
                if key_attempt < len(GEMINI_KEYS) - 1:
                    print(f"🔄 Switching to next API key (error: {e})")
                    time.sleep(2)
                    return generate_questions(text_chunk, key_attempt + 1, 0)
                else:
                    print("🚨 All keys exhausted. Waiting 30 minutes...")
                    time.sleep(1800)
                    return generate_questions(text_chunk, 0, 0)
            continue

    # If all models tried with this key, switch key
    if key_attempt < len(GEMINI_KEYS) - 1:
        print(f"🔄 All models failed with key {key_attempt}. Switching to next key.")
        time.sleep(2)
        return generate_questions(text_chunk, key_attempt + 1, 0)
    else:
        print("🚨 All keys exhausted. Waiting 30 minutes before restart.")
        time.sleep(1800)
        return generate_questions(text_chunk, 0, 0)

# ==========================================
# 5. MAIN FUNCTION (PROCESSING LOOP)
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    # Ask user if reset is needed? For automation, we always reset because requirement says "page 1 se start kro previous sara data delete kro"
    # To avoid accidental reset on every deploy, we'll check a marker file. But for safety, we'll reset only once.
    # We'll check if a reset has been done already.
    reset_flag = os.path.exists("reset_done.marker")
    if not reset_flag:
        reset_and_start_fresh()
        # Create marker file so reset happens only once
        with open("reset_done.marker", "w") as f:
            f.write("done")
    else:
        print("✅ Reset already performed. Resuming from saved page.")
    
    pdf_filename = "book.pdf"
    
    # Download PDF if missing
    if not os.path.exists(pdf_filename):
        print("📥 Downloading book from Google Drive...")
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
        gdown.download(url, pdf_filename, quiet=False)
        time.sleep(1)
        if os.path.exists(pdf_filename):
            print(f"✅ Downloaded: {os.path.getsize(pdf_filename)} bytes")
        else:
            print("❌ Download failed. Exiting.")
            return
    else:
        print(f"✅ PDF already exists: {pdf_filename}")
    
    # ========== PROCESSING LOOP ==========
    print("\n" + "="*60)
    print("📖 STARTING PAGE-BY-PAGE PROCESSING LOOP (25–35 questions per 5 pages)")
    print("="*60 + "\n")
    
    total_questions = 0
    consecutive_errors = 0
    
    while True:
        try:
            current_page = get_current_page()
            next_page = current_page + 5
            print(f"\n🔍 Processing pages {current_page} to {next_page}")
            
            text = extract_pdf_text(current_page, next_page, pdf_filename)
            if text is None:
                print("🏁 End of PDF reached. Mission complete!")
                break
            
            if len(text) < 150:
                print(f"⚠️ Very little text on these pages ({len(text)} chars). Skipping to next chunk.")
                update_current_page(next_page)
                continue
            
            print(f"🧠 Generating 25–35 questions from {len(text)} characters...")
            questions = generate_questions(text)
            
            if not questions:
                print("⚠️ No questions returned. Advancing anyway.")
                update_current_page(next_page)
                continue
            
            # Append to Google Sheets
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
            
            # Update progress in MongoDB (only page number, no questions stored)
            update_current_page(next_page)
            consecutive_errors = 0
            
            # Optional: small delay to respect rate limits
            print("⏳ Pausing for 2 seconds before next chunk...")
            time.sleep(2)
            
        except Exception as e:
            print(f"❌ Loop error: {e}")
            consecutive_errors += 1
            if consecutive_errors >= 5:
                print("🚨 Too many consecutive errors. Stopping.")
                break
            time.sleep(30)
    
    print(f"\n📊 FINAL: {total_questions} questions generated and stored in Google Sheets.")
    print("✅ All questions have been saved, and MongoDB contains only the page tracker.")

if __name__ == "__main__":
    main()
