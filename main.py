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
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            total_pages = len(reader.pages)
            if start_page >= total_pages:
                return None  # Book is complete
            
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
    
    # AI Model Selection (Priority: 2.5 flash)
    model = genai.GenerativeModel('gemini-2.5-flash') 
    
    prompt = f"""
    You are an expert Agriculture Exam Paper Setter for UPSSSC AGTA and IBPS AFO exams.
    Read the following text and generate between 15 to 35 high-quality questions.
    
    STRICT RULES:
    1. Difficulty Level: MODERATE. Questions should require conceptual understanding, not just rote facts.
    2. Format: Return ONLY a valid JSON array of objects. No markdown, no extra text, no explanations.
    3. Question Length: Minimum 2 lines, Maximum 3 lines. DO NOT use words like "According to the text" or "In the table". Make it look like a real competitive exam question.
    4. Options: Exactly 5 options per question (opt1 to opt5).
    5. Section Detection: Identify the core subject (e.g., Agronomy, Soil Science, Horticulture, Entomology) based on the text. If a page header indicates a new section, use that.
    
    HERE ARE EXAMPLES OF THE EXACT QUESTION STYLE, TONE, AND LEVEL YOU MUST MATCH:
    - "Which soil science branch specifically focuses on the origin, morphological characteristics, classification processes, and geographical distribution of soils?"
    - "The deficiency of which essential micronutrient leads to the manifestation of Khaira disease in rice, characterized by chlorotic leaves and stunted growth?"
    - "In papaya cultivation, a proportion of male plants must be retained to ensure adequate pollination for fruit development. What is the recommended percentage of male plants?"
    - "The traditional shifting cultivation system known as Jhum is also referred to as 'Bewar' and 'Dahiya.' In which Indian state are these local names used?"
    - "Which of the following statements is NOT correct regarding forest soils?"
    
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
        
        # Robust JSON cleaning
        raw_text = response.text
        # Remove markdown code blocks if AI adds them
        clean_text = re.sub(r'```json\n|\n```|```', '', raw_text).strip()
        
        return json.loads(clean_text)
        
    except json.JSONDecodeError as je:
        print(f"⚠️ JSON Parse Error: AI didn't return proper format. Retrying... Error: {je}")
        time.sleep(5)
        # If JSON fails, try with same key once more
        return []
        
    except Exception as e:
        print(f"⚠️ API Error with Key {key_attempt % len(GEMINI_KEYS)}: {e}")
        
        # Key Rotation Logic
        if key_attempt < len(GEMINI_KEYS) - 1:
            print(f"🔄 Switching to next Key... (Attempt {key_attempt + 1})")
            time.sleep(5)
            return generate_questions(text_chunk, key_attempt + 1)
        
        # All Keys Exhausted
        else:
            print("🚨 ALL KEYS EXHAUSTED OR RATE LIMITED!")
            print("💤 Sleeping for 30 MINUTES to let tokens recover...")
            time.sleep(1800) # 30 mins sleep
            print("☀️ Waking up... Trying again with Key 0")
            return generate_questions(text_chunk, 0)

# ==========================================
# 4. MAIN EXECUTION ENGINE
# ==========================================
def main():
    # 1. Start the Keep Alive Server
    keep_alive()
    print("🚀 Keep-Alive Server Started. System is active.")
    
    # 2. Download Book (Runs only once if file doesn't exist)
    pdf_filename = "book.pdf"
    if not os.path.exists(pdf_filename):
        print("📥 Downloading Book from Google Drive... Please wait.")
        url = f'[https://drive.google.com/uc?id=](https://drive.google.com/uc?id=){DRIVE_FILE_ID}'
        try:
            gdown.download(url, pdf_filename, quiet=False)
            print("✅ Download Complete.")
        except Exception as e:
            print(f"❌ Download Failed: {e}")
            return
    
    # 3. Main Processing Loop
    while True:
        current_page = get_current_page()
        next_page = current_page + 5
        print(f"\n📖 Processing pages: {current_page} to {next_page}...")
        
        # Extract Text
        text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
        
        # Check if book is finished
        if text_chunk is None:
            print("🎉 SUCCESS: Book Completed! All 1075 pages processed.")
            break
        
        # If page has readable text
        if len(text_chunk.strip()) > 100:
            print("🧠 Generating questions...")
            questions = generate_questions(text_chunk)
            
            if questions:
                try:
                    # 1. Save to MongoDB Backup
                    questions_collection.insert_many(questions)
                    
                    # 2. Prepare Data for Google Sheets
                    sheet_data = []
                    for q in questions:
                        sheet_data.append([
                            q.get("section", "General Agriculture"),
                            q.get("question", "N/A"),
                            q.get("opt1", ""),
                            q.get("opt2", ""),
                            q.get("opt3", ""),
                            q.get("opt4", ""),
                            q.get("opt5", ""),
                            q.get("answer", "")
                        ])
                    
                    # 3. Push to Google Sheet
                    sheet.append_rows(sheet_data)
                    print(f"✅ Successfully added {len(questions)} questions to Database & Sheet.")
                except Exception as e:
                    print(f"❌ Error saving data: {e}")
            else:
                print("⚠️ No valid questions generated for this chunk (might be an empty page or table).")
                
        # Update progress tracking
        update_current_page(next_page)
        
        # Safe Break to prevent token exhaustion
        print("⏳ Chunk complete. Sleeping for 2 MINUTES...")
        time.sleep(120)

if __name__ == "__main__":
    main()
