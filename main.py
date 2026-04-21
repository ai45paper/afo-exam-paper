import os
import json
import time
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
import google.generativeai as genai
import PyPDF2
import gdown
from keep_alive import keep_alive

# ==========================================
# 1. CONFIGURATION & ENVIRONMENT SETUP
# ==========================================
# Render के Environment Variables से डेटा उठाना
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# MongoDB Setup
try:
    client = MongoClient(MONGO_URI)
    db = client['agri_data_bank']
    progress_collection = db['process_tracker']
    questions_collection = db['questions_db']
    print("✅ MongoDB Connection: SUCCESS")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")

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

# ==========================================
# 2. CORE UTILITY FUNCTIONS
# ==========================================
def get_current_page():
    tracker = progress_collection.find_one({"_id": "pdf_tracker"})
    if tracker:
        return tracker["current_page"]
    else:
        progress_collection.insert_one({"_id": "pdf_tracker", "current_page": 0})
        return 0

def update_current_page(page_num):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}})

def get_active_gemini_key(attempt=0):
    key_index = attempt % len(GEMINI_KEYS)
    current_key = GEMINI_KEYS[key_index].strip()
    genai.configure(api_key=current_key)
    return current_key

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    text = ""
    try:
        if not os.path.exists(pdf_path):
            print(f"❌ File not found: {pdf_path}")
            return ""
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            total_pages = len(reader.pages)
            if start_page >= total_pages:
                return None 
            
            actual_end = min(end_page, total_pages)
            for i in range(start_page, actual_end):
                page_text = reader.pages[i].extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception as e:
        print(f"❌ PDF Reading Error: {e}")
    return text

# ==========================================
# 3. AI GENERATION LOGIC (FEW-SHOT PROMPT)
# ==========================================
def generate_questions(text_chunk, key_attempt=0):
    get_active_gemini_key(key_attempt)
    model = genai.GenerativeModel('gemini-2.5-flash') 
    
    prompt = f"""
    Role: Professional Agriculture Exam Paper Setter.
    Task: Create 15-35 high-quality questions for UPSSSC AGTA / IBPS AFO level.
    
    CRITICAL RULES:
    1. Level: MODERATE (neither too easy nor extremely hard).
    2. Questions must be 2 to 3 lines long.
    3. Return ONLY a valid JSON list. No code blocks, no text explanations.
    4. Provide exactly 5 options (opt1 to opt5).
    
    STYLE EXAMPLES (Follow this Tone):
    - "Which soil science branch specifically focuses on the origin, morphological characteristics, classification processes, and geographical distribution of soils?"
    - "The deficiency of which essential micronutrient leads to the manifestation of Khaira disease in rice, characterized by chlorotic leaves and stunted growth?"
    - "The traditional shifting cultivation system known as Jhum is also referred to as 'Bewar' and 'Dahiya.' In which Indian state are these local names used?"
    
    JSON Template:
    [
      {{
        "section": "Agronomy/Soil Science/Horticulture",
        "question": "Question text...",
        "opt1": "Choice A", "opt2": "Choice B", "opt3": "Choice C", "opt4": "Choice D", "opt5": "Choice E",
        "answer": "Correct Choice"
      }}
    ]
    
    Text Source:
    {text_chunk}
    """
    
    try:
        response = model.generate_content(prompt)
        # Clean response string from any markdown formatting
        clean_text = re.sub(r'```json\n|\n```|```', '', response.text).strip()
        return json.loads(clean_text)
    except Exception as e:
        print(f"⚠️ Generation Warning (Key {key_attempt % len(GEMINI_KEYS)}): {e}")
        if key_attempt < len(GEMINI_KEYS) - 1:
            print("🔄 Rotating to next API key...")
            return generate_questions(text_chunk, key_attempt + 1)
        else:
            print("🚨 30-MINUTE COOL DOWN: All keys exhausted.")
            time.sleep(1800)
            return generate_questions(text_chunk, 0)

# ==========================================
# 4. MAIN ENGINE
# ==========================================
def main():
    # Keep Render alive
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    # Secure Download Block
    pdf_filename = "book.pdf"
    if not os.path.exists(pdf_filename):
        print("📥 Starting Secure Book Download...")
        try:
            # Constructing URL manually to ensure it's clean
            clean_id = DRIVE_FILE_ID.strip()
            download_url = f"[https://drive.google.com/uc?id=](https://drive.google.com/uc?id=){clean_id}"
            
            gdown.download(download_url, pdf_filename, quiet=False)
            
            if os.path.exists(pdf_filename):
                print("✅ Book Downloaded Successfully.")
            else:
                raise Exception("File not found after download.")
        except Exception as e:
            print(f"❌ CRITICAL ERROR: Download Failed! -> {e}")
            return

    # Process Pages
    while True:
        current_page = get_current_page()
        next_page = current_page + 5
        print(f"\n📑 Working on Pages: {current_page} to {next_page}...")
        
        text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
        
        if text_chunk is None:
            print("🏁 MISSION COMPLETE: Entire book digitized.")
            break
        
        if len(text_chunk.strip()) > 150:
            print("🤖 AI is thinking...")
            questions = generate_questions(text_chunk)
            
            if questions:
                try:
                    # Database Backup
                    questions_collection.insert_many(questions)
                    
                    # Formatting for Sheet
                    sheet_data = []
                    for q in questions:
                        sheet_data.append([
                            q.get("section", "General"),
                            q.get("question", ""),
                            q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                            q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                        ])
                    
                    # Batch Update to Sheet
                    sheet.append_rows(sheet_data)
                    print(f"✅ Success: {len(questions)} items added to Sheet.")
                except Exception as e:
                    print(f"❌ Sheet Update Error: {e}")
        
        # Save Progress
        update_current_page(next_page)
        
        # 2 Minute Break for stability
        print("⏳ Pause for 2 minutes (Protecting Tokens)...")
        time.sleep(120)

if __name__ == "__main__":
    main()
