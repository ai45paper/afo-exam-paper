import os
import json
import time
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
from google import genai  # Latest SDK
import pypdf # PyPDF2 की जगह तेज़ 'pypdf' का उपयोग
import gdown
from keep_alive import keep_alive

# ==========================================
# 1. CONFIGURATION & ENVIRONMENT SETUP
# ==========================================
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# Validate critical environment variables
if not GEMINI_KEYS or GEMINI_KEYS == ['']:
    raise ValueError("❌ GEMINI_KEYS not set properly")
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI not set")
if not SERVICE_ACCOUNT_JSON:
    raise ValueError("❌ SERVICE_ACCOUNT_JSON not set")

# MongoDB Setup
try:
    client = MongoClient(MONGO_URI)
    db = client['agri_data_bank']
    progress_collection = db['process_tracker']
    questions_collection = db['questions_db']
    print("✅ MongoDB Connection: SUCCESS")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")
    raise

# Google Sheets Setup
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gsheet_client = gspread.authorize(creds)
    sheet = gsheet_client.open_by_key(SHEET_ID).sheet1
    print("✅ Google Sheets Connection: SUCCESS")
except Exception as e:
    print(f"❌ Google Sheets Error: {e}")
    raise

# ==========================================
# 2. CORE UTILITY FUNCTIONS
# ==========================================
def get_current_page():
    try:
        tracker = progress_collection.find_one({"_id": "pdf_tracker"})
        if tracker:
            return tracker.get("current_page", 0)
        else:
            progress_collection.insert_one({"_id": "pdf_tracker", "current_page": 0})
            return 0
    except Exception as e:
        print(f"⚠️ Error getting current page: {e}")
        return 0

def update_current_page(page_num):
    try:
        progress_collection.update_one(
            {"_id": "pdf_tracker"}, 
            {"$set": {"current_page": page_num}},
            upsert=True
        )
    except Exception as e:
        print(f"⚠️ Error updating current page: {e}")

def get_active_gemini_key(attempt=0):
    key_index = attempt % len(GEMINI_KEYS)
    current_key = GEMINI_KEYS[key_index].strip()
    return current_key

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    text = ""
    try:
        if not os.path.exists(pdf_path):
            print(f"❌ File not found: {pdf_path}")
            return ""
        
        # Using pypdf for faster processing of large files
        reader = pypdf.PdfReader(pdf_path)
        total_pages = len(reader.pages)
        
        if start_page >= total_pages:
            return None 
            
        actual_end = min(end_page, total_pages)
        print(f"📖 Reading pages {start_page} to {actual_end}...")
        
        for i in range(start_page, actual_end):
            page_text = reader.pages[i].extract_text()
            if page_text:
                text += page_text + "\n"
    except Exception as e:
        print(f"❌ PDF Reading Error: {e}")
    return text if text.strip() else ""

# ==========================================
# 3. AI GENERATION LOGIC
# ==========================================
def generate_questions(text_chunk, key_attempt=0):
    try:
        current_key = get_active_gemini_key(key_attempt)
        ai_client = genai.Client(api_key=current_key)
        
        prompt = f"""Role: Professional Agriculture Exam Paper Setter.
Task: Create 15-35 high-quality questions for UPSSSC AGTA / IBPS AFO level based on the text.

STYLE EXAMPLES:
- "Which soil science branch specifically focuses on the origin, morphological characteristics, classification processes, and geographical distribution of soils?"
- "The deficiency of which essential micronutrient leads to the manifestation of Khaira disease in rice, characterized by chlorotic leaves and stunted growth?"

JSON Template:
[
  {{
    "section": "Agronomy",
    "question": "Question text...",
    "opt1": "Choice A", "opt2": "Choice B", "opt3": "Choice C", "opt4": "Choice D", "opt5": "Choice E",
    "answer": "Correct Choice"
  }}
]

Text Source: {text_chunk}"""

        models_to_try = ['gemini-2.0-flash', 'gemini-1.5-flash']
        response_text = None
        
        for model_name in models_to_try:
            try:
                response = ai_client.models.generate_content(model=model_name, contents=prompt)
                response_text = response.text
                break
            except:
                continue
        
        if not response_text:
            raise Exception("AI failed to generate response.")

        clean_text = re.sub(r'```json\n|\n```|```', '', response_text).strip()
        questions = json.loads(clean_text)
        return questions if isinstance(questions, list) else [questions]
    
    except Exception as e:
        if key_attempt < len(GEMINI_KEYS) - 1:
            time.sleep(2)
            return generate_questions(text_chunk, key_attempt + 1)
        else:
            print("🚨 Cooldown: Waiting 30 minutes...")
            time.sleep(1800)
            return generate_questions(text_chunk, 0)

# ==========================================
# 4. MAIN ENGINE
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    pdf_filename = "book.pdf"
    if not os.path.exists(pdf_filename):
        print("📥 Starting Secure Book Download...")
        try:
            # ✅ URL फिक्स: अब कोई ब्रैकेट नहीं है
            clean_id = DRIVE_FILE_ID.strip()
            download_url = f"[https://drive.google.com/uc?id=](https://drive.google.com/uc?id=){clean_id}"
            
            gdown.download(download_url, pdf_filename, quiet=False)
            
            if os.path.exists(pdf_filename):
                print(f"✅ Downloaded: {os.path.getsize(pdf_filename)} bytes")
            else:
                raise Exception("File not saved.")
        except Exception as e:
            print(f"❌ Download Failed: {e}")
            return

    page_count = 0
    error_count = 0
    
    while True:
        try:
            current_page = get_current_page()
            next_page = current_page + 5
            
            text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
            
            if text_chunk is None:
                print("🏁 MISSION COMPLETE.")
                break
            
            if len(text_chunk.strip()) > 150:
                print("🤖 AI Processing...")
                questions = generate_questions(text_chunk)
                
                if questions:
                    questions_collection.insert_many(questions)
                    sheet_data = [[q.get("section", "General"), q.get("question", ""), q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""), q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")] for q in questions]
                    sheet.append_rows(sheet_data)
                    print(f"✅ Success: {len(questions)} items added.")
                    page_count += len(questions)
            
            update_current_page(next_page)
            print("⏳ 2 Minute Break...")
            time.sleep(120)
        except Exception as main_err:
            print(f"❌ Error: {main_err}")
            time.sleep(30)

if __name__ == "__main__":
    main()
