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
# 1. CONFIGURATION & ENVIRONMENT VALIDATION
# ==========================================
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

if not GEMINI_KEYS or GEMINI_KEYS == ['']:
    raise ValueError("❌ GEMINI_KEYS environment variable not set properly")
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI environment variable not set")
if not SERVICE_ACCOUNT_JSON:
    raise ValueError("❌ SERVICE_ACCOUNT_JSON environment variable not set")

# ==========================================
# 2. MONGODB SETUP
# ==========================================
try:
    client = MongoClient(MONGO_URI)
    db = client['agri_data_bank']
    progress_collection = db['process_tracker']
    questions_collection = db['questions_db']
    print("✅ MongoDB Connection: SUCCESS")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")
    raise

# ==========================================
# 3. GOOGLE SHEETS SETUP
# ==========================================
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
# 4. UTILITY FUNCTIONS
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
        print(f"📌 Progress saved: page {page_num}")
    except Exception as e:
        print(f"⚠️ Error updating current page: {e}")

def get_active_gemini_key(attempt=0):
    key_index = attempt % len(GEMINI_KEYS)
    current_key = GEMINI_KEYS[key_index].strip()
    print(f"🔑 Using Gemini key index {key_index} (attempt {attempt})")
    return current_key

def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    text = ""
    try:
        if not os.path.exists(pdf_path):
            print(f"❌ File not found: {pdf_path}")
            return ""
        
        reader = pypdf.PdfReader(pdf_path)
        total_pages = len(reader.pages)
        
        if start_page >= total_pages:
            return None  # End of document
        
        actual_end = min(end_page, total_pages)
        print(f"📖 Reading pages {start_page} to {actual_end} (total pages in PDF: {total_pages})...")
        
        for i in range(start_page, actual_end):
            try:
                page_text = reader.pages[i].extract_text()
                if page_text:
                    text += page_text + "\n"
                else:
                    print(f"⚠️ Page {i} extracted empty text")
            except Exception as page_error:
                print(f"⚠️ Error extracting page {i}: {page_error}")
                continue
    except Exception as e:
        print(f"❌ PDF Reading Error: {e}")
    
    if text.strip():
        print(f"✅ Extracted {len(text)} characters from pages {start_page}-{actual_end-1}")
    else:
        print(f"⚠️ No text extracted from pages {start_page}-{actual_end-1}")
    return text if text.strip() else ""

# ==========================================
# 5. AI GENERATION – FULL BRAIN + MODEL FALLBACK + KEY ROTATION
# ==========================================
def generate_questions(text_chunk, key_attempt=0, model_attempt=0):
    """
    Generate questions with:
    - 3 model fallback (2.5 -> 2.0 -> 1.5 flash)
    - 9 API key rotation (when all models fail with a key)
    """
    try:
        current_key = get_active_gemini_key(key_attempt)
        ai_client = genai.Client(api_key=current_key)
        
        # ========== COMPLETE PROMPT WITH 21 EXAMPLES (FULL BRAIN) ==========
        prompt = f"""Role: Professional Agriculture Exam Paper Setter for UPSSSC AGTA and IBPS AFO.
Task: Create 15-35 high-quality conceptual questions based on the provided text.

CRITICAL RULES:
1. Level: MODERATE (conceptual and professional).
2. Questions MUST be 2 to 3 lines long. DO NOT use phrases like "According to the text".
3. Return ONLY a valid JSON list. No code blocks, no markdown, no text explanations.
4. Provide exactly 5 options (opt1 to opt5).
5. Section Detection: Detect the subject (Agronomy, Soil Science, Horticulture, Genetics, etc.).

STYLE EXAMPLES (YOUR BRAIN MUST MATCH THIS EXACT TONE AND FORMAT):
- "Which soil science branch specifically focuses on the origin, morphological characteristics, classification processes, and geographical distribution of soils?"
- "Dolly the sheep became the first mammal cloned successfully. Which advanced biotechnological technique was utilized to produce this clone?"
- "The deficiency of which essential micronutrient leads to the manifestation of Khaira disease in rice, characterized by chlorotic leaves and stunted growth?"
- "The traditional shifting cultivation system known as Jhum is also referred to as 'Bewar' and 'Dahiya.' In which Indian state are these local names used?"
- "In papaya cultivation, a proportion of male plants must be retained to ensure adequate pollination for fruit development. What is the recommended percentage of male plants?"
- "Among domestic animals, cow milk is known to be comparatively low in which essential mineral, making supplementation important for infants and certain populations?"
- "LD50 is a standard toxicological parameter used to express the potency of pesticides. What does LD50 specifically measure?"
- "Olsen's extractant method is widely used to determine the availability of which nutrient in neutral to alkaline soils?"
- "Anthrax, a highly contagious disease affecting livestock, can also be transmitted to humans. By what alternate name is this zoonotic disease known?"
- "Blanching of vegetables prior to freezing is carried out primarily to achieve which purpose?"
- "Which organization in India specifically focuses on strengthening and promoting small-scale shrimp farming through technical support and cooperative development?"
- "Which Indian buffalo breed is regarded as the best globally due to milk production and is extensively used for grading up various local buffalo populations?"
- "The certification required to declare plants or planting material as disease-free for international export is known as which certificate?"
- "Which prestigious North Indian mango cultivar is famous for its sweet flavour, pleasant aroma, fiberless pulp, thin stone, and excellent transport quality?"
- "What is the primary advantage of vegetative (clonal) propagation of plants compared to seed propagation?"
- "Which of the following statements is NOT correct regarding forest soils?"
- "In diffusion of innovations, what term is used for the group of individuals who are traditional and the last to adopt new technology and often show resistance until the idea is fully established?"
- "A mating or crossing between two individuals differing in only one pair of contrasting alleles results in which type of genetic cross?"
- "The stable, dark, amorphous, colloidal product of organic matter decomposition that is resistant to microbial breakdown is known as what?"
- "The conversion of nitrite or nitrate into gaseous nitrogen during the nitrogen cycle is known as what process?"
- "The certification tag colour associated with Foundation Seed under seed certification standards is which of the following?"

JSON Template:
[
  {{
    "section": "Agronomy",
    "question": "Question text...",
    "opt1": "Choice A", "opt2": "Choice B", "opt3": "Choice C", "opt4": "Choice D", "opt5": "Choice E",
    "answer": "Correct Choice"
  }}
]

Text Source:
{text_chunk}
"""
        # Model list in fallback order
        models_to_try = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-flash']
        
        # Start from current model_attempt index
        for idx in range(model_attempt, len(models_to_try)):
            model_name = models_to_try[idx]
            try:
                print(f"🤖 Trying model: {model_name} (key attempt {key_attempt}, model index {idx})")
                response = ai_client.models.generate_content(model=model_name, contents=prompt)
                response_text = response.text
                # Clean markdown
                clean_text = re.sub(r'```json\n|\n```|```', '', response_text).strip()
                questions = json.loads(clean_text)
                if not isinstance(questions, list):
                    questions = [questions]
                print(f"✅ Successfully generated {len(questions)} questions using {model_name}")
                return questions
            except Exception as model_err:
                print(f"⚠️ Model {model_name} failed: {model_err}")
                continue
        
        # If all models failed with current key, try next key
        if key_attempt < len(GEMINI_KEYS) - 1:
            print(f"🔄 All models failed with key index {key_attempt}. Switching to next API key.")
            time.sleep(2)
            return generate_questions(text_chunk, key_attempt + 1, 0)  # reset model_attempt
        else:
            # All keys exhausted: long cooldown and restart from first key
            print("🚨 All Gemini keys exhausted. Waiting 30 minutes before retry...")
            time.sleep(1800)
            return generate_questions(text_chunk, 0, 0)
        
    except json.JSONDecodeError as json_error:
        print(f"⚠️ JSON Parse Error: {json_error}")
        # Retry with same key but next model
        if model_attempt < 2:
            return generate_questions(text_chunk, key_attempt, model_attempt + 1)
        elif key_attempt < len(GEMINI_KEYS) - 1:
            return generate_questions(text_chunk, key_attempt + 1, 0)
        else:
            print("🚨 Cooldown: Waiting 30 minutes...")
            time.sleep(1800)
            return generate_questions(text_chunk, 0, 0)
    
    except Exception as e:
        print(f"⚠️ Generation Error (Key {key_attempt}, Model attempt {model_attempt}): {e}")
        if model_attempt < 2:
            return generate_questions(text_chunk, key_attempt, model_attempt + 1)
        elif key_attempt < len(GEMINI_KEYS) - 1:
            return generate_questions(text_chunk, key_attempt + 1, 0)
        else:
            print("🚨 Cooldown: Waiting 30 minutes...")
            time.sleep(1800)
            return generate_questions(text_chunk, 0, 0)

# ==========================================
# 6. MAIN ENGINE – GUARANTEED LOOP EXECUTION
# ==========================================
def main():
    # Start the keep-alive Flask server in background thread
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    pdf_filename = "book.pdf"
    
    # ---------- PDF DOWNLOAD (if not already present) ----------
    if not os.path.exists(pdf_filename):
        print("📥 Starting Secure Book Download from Google Drive...")
        try:
            clean_id = DRIVE_FILE_ID.strip()
            download_url = f"https://drive.google.com/uc?id={clean_id}"
            print(f"📍 Download URL: {download_url}")
            gdown.download(download_url, pdf_filename, quiet=False)
            if os.path.exists(pdf_filename):
                file_size = os.path.getsize(pdf_filename)
                print(f"✅ Book Downloaded Successfully. Size: {file_size} bytes")
            else:
                raise Exception("File not found after download attempt.")
        except Exception as e:
            print(f"❌ CRITICAL ERROR: Download Failed! -> {e}")
            # Even if download fails, we attempt to continue (maybe file already exists)
            # but we must exit if no PDF at all.
            if not os.path.exists(pdf_filename):
                print("❌ No PDF file available. Exiting.")
                return
    else:
        print(f"✅ PDF file already exists: {pdf_filename} (size: {os.path.getsize(pdf_filename)} bytes)")
    
    # ========== MAIN PROCESSING LOOP ==========
    print("\n" + "="*70)
    print("📖 STARTING PAGE-BY-PAGE PROCESSING LOOP")
    print("="*70 + "\n")
    
    total_questions_generated = 0
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5
    
    while True:
        try:
            current_page = get_current_page()
            next_page = current_page + 5
            print(f"\n🔍 [CHUNK] Processing pages {current_page} to {next_page}")
            
            # Extract text from PDF
            text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
            
            # Check for end of PDF
            if text_chunk is None:
                print("🏁 MISSION COMPLETE: Entire book has been digitized.")
                break
            
            # Only process if we have substantial text
            if len(text_chunk.strip()) > 150:
                print(f"🧠 AI is generating questions for pages {current_page}-{next_page} (text length: {len(text_chunk)} characters)")
                questions = generate_questions(text_chunk)
                
                if questions and len(questions) > 0:
                    try:
                        # Insert into MongoDB
                        questions_collection.insert_many(questions)
                        print(f"✅ Inserted {len(questions)} questions into MongoDB")
                        
                        # Prepare rows for Google Sheets
                        sheet_data = []
                        for q in questions:
                            sheet_data.append([
                                q.get("section", "General"),
                                q.get("question", ""),
                                q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                                q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                            ])
                        
                        # Append to Google Sheets
                        sheet.append_rows(sheet_data)
                        print(f"✅ Appended {len(questions)} rows to Google Sheets")
                        
                        total_questions_generated += len(questions)
                        consecutive_errors = 0
                        
                        # Update progress only on success
                        update_current_page(next_page)
                        
                    except Exception as db_error:
                        print(f"❌ Database/Sheet Error: {db_error}")
                        consecutive_errors += 1
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                            print("🚨 Too many consecutive DB/Sheet errors. Stopping execution.")
                            break
                        # Do NOT update page number on failure – we'll retry same chunk later
                        print("⚠️ Will retry the same chunk after pause.")
                else:
                    print(f"⚠️ No questions generated for pages {current_page}-{next_page}. Advancing to next chunk.")
                    update_current_page(next_page)
            else:
                print(f"⚠️ Insufficient text on pages {current_page}-{next_page} (length {len(text_chunk.strip())} chars). Skipping chunk.")
                update_current_page(next_page)
            
            # Rate limiting pause (protect API tokens)
            print(f"⏳ Pausing for 2 minutes to avoid rate limits... (Total questions so far: {total_questions_generated})")
            time.sleep(120)
            
        except KeyboardInterrupt:
            print("\n⛔ Process interrupted by user. Progress saved.")
            break
        except Exception as loop_error:
            print(f"❌ Unexpected error in main loop: {loop_error}")
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print("🚨 Too many consecutive errors. Stopping execution.")
                break
            print("⚠️ Waiting 30 seconds before retry...")
            time.sleep(30)
    
    # Final summary
    print("\n" + "="*70)
    print(f"📊 FINAL STATISTICS: {total_questions_generated} questions generated and stored.")
    print("="*70)

# ==========================================
# 7. ENTRY POINT
# ==========================================
if __name__ == "__main__":
    main()
