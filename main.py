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

# Force flush prints (so Render logs show everything)
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# CONFIGURATION
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
questions_collection = db['questions_db']
print("✅ MongoDB Connection: SUCCESS")

# Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1
print("✅ Google Sheets Connection: SUCCESS")

# ==========================================
# FUNCTIONS
# ==========================================
def get_current_page():
    try:
        tracker = progress_collection.find_one({"_id": "pdf_tracker"})
        return tracker.get("current_page", 0) if tracker else 0
    except:
        return 0

def update_current_page(page_num):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)
    print(f"📌 Progress: page {page_num}")

def get_gemini_key(attempt):
    return GEMINI_KEYS[attempt % len(GEMINI_KEYS)].strip()

def extract_pdf_text(start, end, path="book.pdf"):
    if not os.path.exists(path):
        return ""
    reader = pypdf.PdfReader(path)
    total = len(reader.pages)
    if start >= total:
        return None
    end = min(end, total)
    print(f"📖 Reading pages {start} to {end} (total {total})")
    text = ""
    for i in range(start, end):
        try:
            text += reader.pages[i].extract_text() + "\n"
        except:
            continue
    return text.strip() if text.strip() else ""

def generate_questions(chunk, key_attempt=0, model_attempt=0):
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
{chunk[:20000]}"""

    models = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-flash']
    for idx in range(model_attempt, len(models)):
        try:
            client = genai.Client(api_key=get_gemini_key(key_attempt))
            resp = client.models.generate_content(model=models[idx], contents=prompt)
            raw = resp.text
            clean = re.sub(r'```json\n|\n```|```', '', raw).strip()
            qs = json.loads(clean)
            if not isinstance(qs, list):
                qs = [qs]
            print(f"✅ Generated {len(qs)} questions using {models[idx]}")
            return qs
        except Exception as e:
            print(f"⚠️ {models[idx]} failed: {e}")
            continue
    if key_attempt < len(GEMINI_KEYS)-1:
        return generate_questions(chunk, key_attempt+1, 0)
    else:
        time.sleep(1800)
        return generate_questions(chunk, 0, 0)

# ==========================================
# MAIN FUNCTION (SAME WORKING STRUCTURE)
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    pdf = "book.pdf"
    
    # ---------- DOWNLOAD ----------
    if not os.path.exists(pdf):
        print("📥 Downloading book from Google Drive...")
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
        gdown.download(url, pdf, quiet=False)
        # Force flush after download
        sys.stdout.flush()
        time.sleep(1)  # Give time for file system
        if os.path.exists(pdf):
            size = os.path.getsize(pdf)
            print(f"✅ Book Downloaded Successfully. Size: {size} bytes")
            # Write a marker file to prove we reached here
            with open("download_done.marker", "w") as f:
                f.write("done")
        else:
            print("❌ Download failed. Exiting.")
            return
    else:
        print(f"✅ PDF already exists: {pdf} (size: {os.path.getsize(pdf)} bytes)")
    
    # ---------- PROCESSING LOOP ----------
    print("\n" + "="*60)
    print("📖 STARTING PAGE-BY-PAGE PROCESSING LOOP")
    print("="*60 + "\n")
    
    total_q = 0
    errors = 0
    while True:
        try:
            page = get_current_page()
            next_page = page + 5
            print(f"\n🔍 Processing pages {page} to {next_page}")
            text = extract_pdf_text(page, next_page, pdf)
            if text is None:
                print("🏁 End of PDF reached. Mission complete!")
                break
            if len(text) > 150:
                print(f"🧠 Generating questions (text length: {len(text)})")
                qs = generate_questions(text)
                if qs:
                    questions_collection.insert_many(qs)
                    rows = []
                    for q in qs:
                        rows.append([
                            q.get("section", "General"),
                            q.get("question", ""),
                            q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                            q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                        ])
                    sheet.append_rows(rows)
                    total_q += len(qs)
                    print(f"✅ Saved {len(qs)} questions (total {total_q})")
                    errors = 0
                    update_current_page(next_page)
                else:
                    update_current_page(next_page)
            else:
                print("⚠️ Not enough text, skipping")
                update_current_page(next_page)
            print("⏳ Sleeping 120 seconds...")
            time.sleep(120)
        except Exception as e:
            print(f"❌ Loop error: {e}")
            errors += 1
            if errors > 5:
                print("Too many errors, stopping")
                break
            time.sleep(30)
    print(f"\n📊 FINAL: {total_q} questions generated.")

if __name__ == "__main__":
    main()
