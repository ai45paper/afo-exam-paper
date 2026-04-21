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
# 1. ENVIRONMENT VARIABLES & SETUP
# ==========================================
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
    print("✅ MongoDB Connected Successfully")
except Exception as e:
    print(f"❌ MongoDB Connection Error: {e}")

# Google Sheets Setup
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gsheet_client = gspread.authorize(creds)
    sheet = gsheet_client.open_by_key(SHEET_ID).sheet1
    print("✅ Google Sheets Connected Successfully")
except Exception as e:
    print(f"❌ Google Sheets Connection Error: {e}")

# ==========================================
# 2. HELPER FUNCTIONS
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
    genai.configure(api_key=GEMINI_KEYS[key_index].strip())
    return GEMINI_KEYS[key_index]

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    text = ""
    try:
        if not os.path.exists(pdf_path):
            return ""
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            total_pages = len(reader.pages)
            if start_page >= total_pages:
                return None 
            
            actual_end = min(end_page, total_pages)
            for i in range(start_page, actual_end):
                text += reader.pages[i].extract_text() + "\n"
    except Exception as e:
        print(f"❌ PDF Read Error: {e}")
    return text

# ==========================================
# 3. AI PROMPT & QUESTION GENERATION
# ==========================================
def generate_questions(text_chunk, key_attempt=0):
    get_active_gemini_key(key_attempt)
    model = genai.GenerativeModel('gemini-2.5-flash') 
    
    prompt = f"""
    You are an expert Agriculture Exam Paper Setter for UPSSSC AGTA and IBPS AFO exams.
    Read the following text and generate between 15 to 35 high-quality questions.
    
    STRICT RULES:
    1. Difficulty Level: MODERATE.
    2. Format: Return ONLY a valid JSON array of objects. No extra text.
    3. Question Length: Minimum 2 lines, Maximum 3 lines. DO NOT use phrases like "According to the text".
    4. Options: Exactly 5 options per question.
    5. Section Detection: Identify the subject (e.g., Agronomy, Horticulture) based on the text.
    
    MATCH THIS STYLE (FEW-SHOT EXAMPLES):
    - "Which soil science branch specifically focuses on the origin, morphological characteristics, classification processes, and geographical distribution of soils?"
    - "The deficiency of which essential micronutrient leads to the manifestation of Khaira disease in rice, characterized by chlorotic leaves and stunted growth?"
    - "The traditional shifting cultivation system known as Jhum is also referred to as 'Bewar' and 'Dahiya.' In which Indian state are these local names used?"
    
    JSON Format:
    [
      {{
        "section": "Agronomy",
        "question": "Question text here...",
        "opt1": "Choice 1", "opt2": "Choice 2", "opt3": "Choice 3", "opt4": "Choice 4", "opt5": "Choice 5",
        "answer": "Correct Choice"
      }}
    ]
    
    Text: {text_chunk}
    """
    
    try:
        response = model.generate_content(prompt)
        clean_text = re.sub(r'```json\n|\n```|```', '', response.text).strip()
        return json.loads(clean_text)
    except Exception as e:
        if key_attempt < len(GEMINI_KEYS) - 1:
            print(f"🔄 Switching Key... (Attempt {key_attempt + 1})")
            return generate_questions(text_chunk, key_attempt + 1)
        else:
            print("🚨 30 MINUTE RECOVERY SLEEP...")
            time.sleep(1800)
            return generate_questions(text_chunk, 0)

# ==========================================
# 4. MAIN EXECUTION ENGINE
# ==========================================
def main():
    keep_alive()
    print("🚀 System active.")
    
    pdf_filename = "book.pdf"
    if not os.path.exists(pdf_filename):
        print("📥 Downloading Book...")
        # URL को पूरी तरह से क्लीन कर दिया गया है
        url = f'[https://drive.google.com/uc?id=](https://drive.google.com/uc?id=){DRIVE_FILE_ID}'
        try:
            # fuzzy=True हटा दिया गया है
            gdown.download(url, pdf_filename, quiet=False)
            print("✅ Download Complete.")
        except Exception as e:
            print(f"❌ Download Failed: {e}")
            return
    
    while True:
        current_page = get_current_page()
        next_page = current_page + 5
        print(f"\n📖 Pages: {current_page} to {next_page}...")
        
        text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
        
        if text_chunk is None:
            print("🎉 Book Completed!")
            break
        
        if len(text_chunk.strip()) > 100:
            questions = generate_questions(text_chunk)
            if questions:
                try:
                    questions_collection.insert_many(questions)
                    sheet_data = [[q.get("section"), q.get("question"), q.get("opt1"), q.get("opt2"), q.get("opt3"), q.get("opt4"), q.get("opt5"), q.get("answer")] for q in questions]
                    sheet.append_rows(sheet_data)
                    print(f"✅ Added {len(questions)} questions.")
                except Exception as e:
                    print(f"❌ Save Error: {e}")
        
        update_current_page(next_page)
        print("⏳ 2 MINUTE BREAK...")
        time.sleep(120)

if __name__ == "__main__":
    main()
