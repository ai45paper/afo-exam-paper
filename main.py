import os
import json
import time
import random
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
import google.generativeai as genai
import PyPDF2
import gdown

# ==========================================
# 1. ENVIRONMENT VARIABLES & SETUP
# ==========================================
# Render पर Environment Variables में ये वैल्यूज डालनी हैं
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# MongoDB Setup
client = MongoClient(MONGO_URI)
db = client['agri_data_bank']
progress_collection = db['process_tracker']
questions_collection = db['questions_db']

# Google Sheets Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_current_page():
    # MongoDB से चेक करें कि कोड कहाँ तक चला था
    tracker = progress_collection.find_one({"_id": "pdf_tracker"})
    if tracker:
        return tracker["current_page"]
    else:
        progress_collection.insert_one({"_id": "pdf_tracker", "current_page": 0})
        return 0

def update_current_page(page_num):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}})

def get_active_gemini_key(attempt=0):
    # Key Rotation Logic
    key_index = attempt % len(GEMINI_KEYS)
    genai.configure(api_key=GEMINI_KEYS[key_index])
    return GEMINI_KEYS[key_index]

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    # 5 पेज का टेक्स्ट निकालना
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            total_pages = len(reader.pages)
            if start_page >= total_pages:
                return None # किताब खत्म
            
            actual_end = min(end_page, total_pages)
            for i in range(start_page, actual_end):
                text += reader.pages[i].extract_text() + "\n"
    except Exception as e:
        print(f"PDF Read Error: {e}")
    return text

# ==========================================
# 3. AI PROMPT & QUESTION GENERATION
# ==========================================
def generate_questions(text_chunk, key_attempt=0):
    get_active_gemini_key(key_attempt)
    
    # AI Model Selection (Fallback setup)
    model = genai.GenerativeModel('gemini-2.5-flash') # अगर 2.5 नहीं चला तो कोड में 1.5 लगा सकते हैं
    
    prompt = f"""
    You are an expert Agriculture Exam Paper Setter for UPSSSC AGTA and IBPS AFO exams.
    Read the following text and generate between 15 to 35 high-quality questions.
    
    STRICT RULES:
    1. Difficulty Level: MODERATE. Questions should not be overly hard, students should be able to understand them.
    2. Format: Return ONLY a valid JSON array of objects. No markdown, no extra text.
    3. Question Length: Minimum 2 lines, Maximum 3 lines. Do NOT use phrases like "According to the text".
    4. Options: Exactly 5 options per question.
    5. Section Detection: Identify the core subject (e.g., Agronomy, Soil Science, Horticulture) based on the text. If a page header indicates a new section, use that.
    
    JSON Format required:
    [
      {{
        "section": "Agronomy",
        "question": "Which specific irrigation method is considered most efficient for sandy soils where water-holding capacity is low and frequent light applications are required?",
        "opt1": "Drip Irrigation",
        "opt2": "Check Basin",
        "opt3": "Furrow Method",
        "opt4": "Sprinkler Irrigation",
        "opt5": "Flooding",
        "answer": "Drip Irrigation"
      }}
    ]
    
    Text to process:
    {text_chunk}
    """
    
    try:
        response = model.generate_content(prompt)
        # Clean JSON text
        json_text = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Gemini API Error with Key {key_attempt % len(GEMINI_KEYS)}: {e}")
        # अगर कोटा खत्म या एरर आए, तो अगली Key (Next Key) से ट्राई करें
        if key_attempt < len(GEMINI_KEYS) * 2:
            time.sleep(5)
            return generate_questions(text_chunk, key_attempt + 1)
        return []

# ==========================================
# 4. MAIN EXECUTION ENGINE
# ==========================================
def main():
    print("Starting Agri-Data-Bank Processor...")
    
    # 1. Download Book (One time per session)
    pdf_filename = "book.pdf"
    if not os.path.exists(pdf_filename):
        print("Downloading Book from Google Drive...")
        url = f'https://drive.google.com/uc?id={DRIVE_FILE_ID}'
        gdown.download(url, pdf_filename, quiet=False)
    
    # 2. Main Loop
    while True:
        current_page = get_current_page()
        next_page = current_page + 5
        print(f"Processing pages: {current_page} to {next_page}...")
        
        # Extract Text
        text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
        
        if text_chunk is None:
            print("Book Completed! All 1075 pages processed.")
            break
        
        if len(text_chunk.strip()) > 100: # अगर पेज खाली नहीं है
            # Generate Questions
            questions = generate_questions(text_chunk)
            
            if questions:
                # 1. Save to MongoDB (Backup)
                questions_collection.insert_many(questions)
                
                # 2. Push to Google Sheets (Column A to H)
                sheet_data = []
                for q in questions:
                    sheet_data.append([
                        q.get("section", "General Agriculture"),
                        q.get("question", ""),
                        q.get("opt1", ""),
                        q.get("opt2", ""),
                        q.get("opt3", ""),
                        q.get("opt4", ""),
                        q.get("opt5", ""),
                        q.get("answer", "")
                    ])
                
                # Append rows directly to Google Sheet
                sheet.append_rows(sheet_data)
                print(f"Successfully added {len(questions)} questions to Sheet.")
            else:
                print("No questions generated for this chunk. Skipping.")
                
        # Update progress and wait before next chunk (Token management)
        update_current_page(next_page)
        print("Sleeping for 15 seconds to manage API limits...")
        time.sleep(15)

if __name__ == "__main__":
    main()
