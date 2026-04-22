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
        print(f"📌 Progress updated to page {page_num}")
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
        print(f"📖 Reading pages {start_page} to {actual_end} (total pages: {total_pages})")
        for i in range(start_page, actual_end):
            try:
                page_text = reader.pages[i].extract_text()
                if page_text:
                    text += page_text + "\n"
            except Exception as e:
                print(f"⚠️ Page {i} error: {e}")
                continue
    except Exception as e:
        print(f"❌ PDF error: {e}")
    return text if text.strip() else ""

def generate_questions(text_chunk, key_attempt=0, model_attempt=0):
    """Full AI brain + 3-model fallback + 9-key rotation"""
    try:
        current_key = get_active_gemini_key(key_attempt)
        ai_client = genai.Client(api_key=current_key)
        
        # Full prompt with 21 examples
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
        
        models = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-flash']
        for idx in range(model_attempt, len(models)):
            model = models[idx]
            try:
                print(f"🤖 Trying {model} (key {key_attempt})")
                response = ai_client.models.generate_content(model=model, contents=prompt)
                clean = re.sub(r'```json\n|\n```|```', '', response.text).strip()
                questions = json.loads(clean)
                if not isinstance(questions, list):
                    questions = [questions]
                print(f"✅ Generated {len(questions)} questions using {model}")
                return questions
            except Exception as e:
                print(f"⚠️ {model} failed: {e}")
                continue
        
        # All models failed with this key → try next key
        if key_attempt < len(GEMINI_KEYS) - 1:
            print(f"🔄 Switching to next API key (attempt {key_attempt+1})")
            time.sleep(2)
            return generate_questions(text_chunk, key_attempt+1, 0)
        else:
            print("🚨 All keys exhausted. Waiting 30 minutes...")
            time.sleep(1800)
            return generate_questions(text_chunk, 0, 0)
            
    except Exception as e:
        print(f"❌ Generation error: {e}")
        if key_attempt < len(GEMINI_KEYS) - 1:
            return generate_questions(text_chunk, key_attempt+1, 0)
        else:
            time.sleep(1800)
            return generate_questions(text_chunk, 0, 0)

# ==========================================
# 3. MAIN FUNCTION (GUARANTEED LOOP)
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    pdf_filename = "book.pdf"
    
    # Download PDF if missing
    if not os.path.exists(pdf_filename):
        print("📥 Downloading book from Google Drive...")
        try:
            url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
            gdown.download(url, pdf_filename, quiet=False)
            if os.path.exists(pdf_filename):
                print(f"✅ Downloaded: {os.path.getsize(pdf_filename)} bytes")
            else:
                raise Exception("Download failed")
        except Exception as e:
            print(f"❌ Download error: {e}")
            return
    else:
        print(f"✅ PDF already exists: {pdf_filename}")
    
    # ========== CRITICAL: PROCESSING LOOP ==========
    print("\n" + "="*60)
    print("📖 STARTING PAGE-BY-PAGE PROCESSING LOOP")
    print("="*60 + "\n")
    
    page_count = 0
    error_count = 0
    MAX_ERRORS = 5
    
    while True:
        try:
            current_page = get_current_page()
            next_page = current_page + 5
            print(f"\n🔍 Processing pages {current_page} to {next_page}")
            
            text_chunk = extract_pdf_text(current_page, next_page, pdf_filename)
            
            if text_chunk is None:
                print("🏁 Reached end of PDF. Mission complete!")
                break
            
            if len(text_chunk.strip()) > 150:
                print(f"🧠 Generating questions for pages {current_page}-{next_page} (text length: {len(text_chunk)})")
                questions = generate_questions(text_chunk)
                
                if questions and len(questions) > 0:
                    try:
                        # MongoDB
                        questions_collection.insert_many(questions)
                        # Google Sheets
                        rows = []
                        for q in questions:
                            rows.append([
                                q.get("section", "General"),
                                q.get("question", ""),
                                q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                                q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                            ])
                        sheet.append_rows(rows)
                        print(f"✅ Saved {len(questions)} questions to MongoDB & Sheets")
                        page_count += len(questions)
                        error_count = 0
                        update_current_page(next_page)
                    except Exception as e:
                        print(f"❌ Save error: {e}")
                        error_count += 1
                else:
                    print(f"⚠️ No questions generated, advancing to next chunk")
                    update_current_page(next_page)
            else:
                print(f"⚠️ Insufficient text (length {len(text_chunk.strip())}), skipping chunk")
                update_current_page(next_page)
            
            print(f"⏳ Waiting 2 minutes... (Total questions so far: {page_count})")
            time.sleep(120)
            
        except KeyboardInterrupt:
            print("\n⛔ Interrupted by user")
            break
        except Exception as e:
            print(f"❌ Loop error: {e}")
            error_count += 1
            if error_count >= MAX_ERRORS:
                print("🚨 Too many errors, stopping")
                break
            time.sleep(30)
    
    print(f"\n📊 FINAL: {page_count} questions generated and stored.")

if __name__ == "__main__":
    main()
