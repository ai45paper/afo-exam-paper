import os
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

# ==========================================
# 1. CONFIGURATION & ENVIRONMENT SETUP
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
try:
    client = MongoClient(MONGO_URI)
    db = client['agri_data_bank']
    progress_collection = db['process_tracker']
    questions_collection = db['questions_db']
    print("✅ MongoDB Connection: SUCCESS")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")
    raise

# Google Sheets
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
# 2. UTILITY FUNCTIONS
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
        print(f"⚠️ Error getting page: {e}")
        return 0

def update_current_page(page_num):
    try:
        progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)
    except Exception as e:
        print(f"⚠️ Error updating page: {e}")

def get_active_gemini_key(attempt=0):
    return GEMINI_KEYS[attempt % len(GEMINI_KEYS)].strip()

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    text = ""
    try:
        if not os.path.exists(pdf_path):
            print(f"❌ File not found: {pdf_path}")
            return ""
        reader = pypdf.PdfReader(pdf_path)
        total_pages = len(reader.pages)
        if start_page >= total_pages:
            return None
        actual_end = min(end_page, total_pages)
        print(f"📖 Reading pages {start_page} to {actual_end}...")
        for i in range(start_page, actual_end):
            try:
                page_text = reader.pages[i].extract_text()
                if page_text:
                    text += page_text + "\n"
            except Exception as page_error:
                print(f"⚠️ Error on page {i}: {page_error}")
                continue
    except Exception as e:
        print(f"❌ PDF Error: {e}")
    return text if text.strip() else ""

def generate_questions(text_chunk, key_attempt=0):
    try:
        current_key = get_active_gemini_key(key_attempt)
        ai_client = genai.Client(api_key=current_key)
        
        prompt = f"""Create 15-35 agriculture exam questions based on this text. Return ONLY valid JSON list. Each question must have: section, question, opt1-opt5, answer.

Text: {text_chunk}"""
        
        models_to_try = ['gemini-1.5-flash', 'gemini-2.0-flash']
        response_text = None
        
        for model_name in models_to_try:
            try:
                response = ai_client.models.generate_content(model=model_name, contents=prompt)
                response_text = response.text
                print(f"🤖 Generated using {model_name}")
                break
            except Exception as e:
                print(f"⚠️ {model_name} failed: {e}")
                continue
        
        if not response_text:
            raise Exception("All models failed")
        
        clean_text = re.sub(r'```json\n|\n```|```', '', response_text).strip()
        questions = json.loads(clean_text)
        if not isinstance(questions, list):
            questions = [questions]
        return questions
        
    except Exception as e:
        print(f"⚠️ Error with key {key_attempt}: {e}")
        if key_attempt < len(GEMINI_KEYS) - 1:
            time.sleep(2)
            return generate_questions(text_chunk, key_attempt + 1)
        else:
            print("🚨 All keys failed. Waiting 30 minutes...")
            time.sleep(1800)
            return generate_questions(text_chunk, 0)

# ==========================================
# 3. MAIN FUNCTION - ALL LOOP INSIDE HERE
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    pdf_filename = "book.pdf"
    
    # Download PDF if not exists
    if not os.path.exists(pdf_filename):
        print("📥 Downloading book...")
        try:
            download_url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
            gdown.download(download_url, pdf_filename, quiet=False)
            if os.path.exists(pdf_filename):
                print(f"✅ Downloaded: {os.path.getsize(pdf_filename)} bytes")
            else:
                raise Exception("Download failed")
        except Exception as e:
            print(f"❌ Download error: {e}")
            return
    
    # ==========================================
    # THIS IS THE MAIN PROCESSING LOOP
    # ==========================================
    page_count = 0
    error_count = 0
    MAX_ERRORS = 5
    
    print("📖 Starting page processing loop...")  # <-- DEBUG LOG
    
    while True:
        try:
            current_page = get_current_page()
            next_page = current_page + 5
            
            print(f"🔍 Processing from page {current_page} to {next_page}")  # <-- DEBUG LOG
            
            text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
            
            if text_chunk is None:
                print("🏁 Book complete!")
                break
            
            if len(text_chunk.strip()) > 150:
                print(f"🧠 Generating questions for pages {current_page}-{next_page}...")
                questions = generate_questions(text_chunk)
                
                if questions and len(questions) > 0:
                    try:
                        questions_collection.insert_many(questions)
                        sheet_data = []
                        for q in questions:
                            sheet_data.append([
                                q.get("section", "General"),
                                q.get("question", ""),
                                q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                                q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                            ])
                        sheet.append_rows(sheet_data)
                        print(f"✅ Added {len(questions)} questions")
                        page_count += len(questions)
                        error_count = 0
                        update_current_page(next_page)
                    except Exception as e:
                        print(f"❌ Save error: {e}")
                        error_count += 1
                else:
                    update_current_page(next_page)
            else:
                print(f"⚠️ Not enough text on pages {current_page}-{next_page}, skipping...")
                update_current_page(next_page)
            
            print("⏳ Waiting 2 minutes...")
            time.sleep(120)
            
        except Exception as e:
            print(f"❌ Loop error: {e}")
            error_count += 1
            if error_count >= MAX_ERRORS:
                print("🚨 Too many errors, stopping.")
                break
            time.sleep(30)
    
    print(f"\n📊 Done! {page_count} questions generated.")

if __name__ == "__main__":
    main()
