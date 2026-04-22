import os
import sys
import json
import time
import re
from datetime import datetime, timedelta
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
from google import genai
import pypdf
import gdown
from keep_alive import keep_alive

sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# 1. CONFIGURATION
# ==========================================
OPENROUTER_KEYS = os.getenv("OPENROUTER_KEYS", "").split(",")
GEMINI_KEYS = os.getenv("GEMINI_KEYS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
SHEET_ID = "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"
DRIVE_FILE_ID = "1dzPl2G-vVjK7zSMCWAyq34uMrX-RamiS"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

OPENROUTER_MODEL = "openrouter/free"
OPENROUTER_TEMPERATURE = 0.4

GEMINI_MODELS = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro"
]
GEMINI_TEMPERATURE = 0.4

if not GEMINI_KEYS or GEMINI_KEYS == ['']:
    raise ValueError("❌ GEMINI_KEYS not set")
if not MONGO_URI:
    raise ValueError("❌ MONGO_URI not set")
if not SERVICE_ACCOUNT_JSON:
    raise ValueError("❌ SERVICE_ACCOUNT_JSON not set")

OPENROUTER_KEYS = [k.strip() for k in OPENROUTER_KEYS if k.strip()]
GEMINI_KEYS = [k.strip() for k in GEMINI_KEYS if k.strip()]

# MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['agri_data_bank']
progress_collection = db['process_tracker']
config_collection = db['config']
print("✅ MongoDB Connection: SUCCESS")

# Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1
print("✅ Google Sheets Connection: SUCCESS")

# ==========================================
# 2. RESET (ONLY ONCE – MONGO FLAG)
# ==========================================
def is_reset_done():
    doc = config_collection.find_one({"_id": "reset_flag"})
    return doc.get("done", False) if doc else False

def mark_reset_done():
    config_collection.update_one({"_id": "reset_flag"}, {"$set": {"done": True}}, upsert=True)

def get_current_page():
    try:
        tracker = progress_collection.find_one({"_id": "pdf_tracker"})
        return tracker.get("current_page", 0) if tracker else 0
    except:
        return 0

def update_current_page(page_num):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)
    print(f"📌 Page tracker updated to {page_num}")

def reset_and_start_fresh():
    print("🔄 Resetting all data – starting fresh from page 1...")
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": 0}}, upsert=True)
    if 'questions_db' in db.list_collection_names():
        db['questions_db'].drop()
    sheet.clear()
    sheet.append_row(["Section", "Question", "Option1", "Option2", "Option3", "Option4", "Option5", "Answer"])
    print("✅ Reset complete. Starting from page 0.")
    mark_reset_done()

# ==========================================
# 3. WAIT UNTIL 5:30 AM IST
# ==========================================
def wait_until_5_30_am_ist():
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    target_ist = now_ist.replace(hour=5, minute=30, second=0, microsecond=0)
    if now_ist >= target_ist:
        target_ist += timedelta(days=1)
    wait_seconds = (target_ist - now_ist).total_seconds()
    print(f"⏰ Waiting until {target_ist.strftime('%Y-%m-%d %H:%M:%S')} IST ({wait_seconds/3600:.1f} hours)")
    time.sleep(wait_seconds)

# ==========================================
# 4. PDF EXTRACTION (3 PAGES)
# ==========================================
def extract_pdf_text(start_page, end_page, pdf_path="book.pdf"):
    if not os.path.exists(pdf_path):
        return ""
    reader = pypdf.PdfReader(pdf_path)
    total_pages = len(reader.pages)
    if start_page >= total_pages:
        return None
    actual_end = min(end_page, total_pages)
    print(f"📖 Reading pages {start_page} to {actual_end} (total {total_pages})")
    text = ""
    for i in range(start_page, actual_end):
        try:
            page_text = reader.pages[i].extract_text()
            if page_text:
                text += page_text + "\n"
        except:
            continue
    return text.strip() if text.strip() else ""

# ==========================================
# 5. OPENROUTER API CALL
# ==========================================
def call_openrouter(api_key, prompt):
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": OPENROUTER_TEMPERATURE,
        "max_tokens": 2000
    }
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    elif response.status_code == 402:
        raise Exception("INSUFFICIENT_CREDITS")
    else:
        raise Exception(f"OpenRouter error {response.status_code}: {response.text[:200]}")

# ==========================================
# 6. PROMPT – EXACTLY MATCHING YOUR INITIAL EXAMPLES
# ==========================================
def build_prompt(text_chunk):
    truncated = text_chunk[:6000]
    return f"""You are a professional agriculture exam question setter for UPSSSC AGTA and IBPS AFO (Mains level).
Based on the provided text, generate between 15 and 20 high‑quality conceptual questions.

CRITICAL RULES:
1. Level: MODERATE (conceptual and professional).
2. Questions MUST be 2 to 3 lines long (exactly like the examples below). DO NOT use phrases like "According to the text".
3. Return ONLY a valid JSON list. No code blocks, no markdown, no text explanations.
4. Provide exactly 5 options (opt1 to opt5). Options should be meaningful and exam‑oriented (similar to the examples).
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
{truncated}
"""

# ==========================================
# 7. HELPER: PARSE QUESTIONS FROM RESPONSE
# ==========================================
def parse_questions(response_text):
    """Parse JSON response and ensure it's a list of dicts with required fields."""
    # Remove markdown code blocks
    clean = re.sub(r'```json\n|\n```|```', '', response_text).strip()
    # Find JSON array
    json_match = re.search(r'\[[\s\S]*\]', clean)
    if not json_match:
        raise ValueError("No JSON array found in response")
    try:
        questions = json.loads(json_match.group(0))
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decode error: {e}")
    if not isinstance(questions, list):
        questions = [questions]
    # Validate each question
    valid_questions = []
    for q in questions:
        if not isinstance(q, dict):
            continue
        required = ['section', 'question', 'opt1', 'opt2', 'opt3', 'opt4', 'opt5', 'answer']
        if all(k in q for k in required):
            valid_questions.append(q)
        else:
            print(f"⚠️ Skipping malformed question: {q}")
    if len(valid_questions) < 15:
        raise ValueError(f"Only {len(valid_questions)} valid questions (need 15)")
    return valid_questions[:20]

# ==========================================
# 8. GENERATE QUESTIONS (OPENROUTER + GEMINI)
# ==========================================
def generate_questions(text_chunk):
    prompt = build_prompt(text_chunk)
    total_attempts = 0
    max_attempts = (len(OPENROUTER_KEYS) + len(GEMINI_KEYS) * len(GEMINI_MODELS))

    # LAYER 1: OPENROUTER
    if OPENROUTER_KEYS:
        print(f"🌐 OpenRouter layer: {len(OPENROUTER_KEYS)} keys with auto model (temp={OPENROUTER_TEMPERATURE})")
        for key_idx, api_key in enumerate(OPENROUTER_KEYS):
            total_attempts += 1
            print(f"🌐 Attempt {total_attempts}/{max_attempts}: OpenRouter key {key_idx}")
            try:
                response_text = call_openrouter(api_key, prompt)
                questions = parse_questions(response_text)
                print(f"✅ Generated {len(questions)} questions using OpenRouter (auto model)")
                return questions
            except Exception as e:
                err = str(e)
                print(f"⚠️ OpenRouter key {key_idx} failed: {err[:150]}")
                if "INSUFFICIENT_CREDITS" in err or "402" in err:
                    print("⏳ Insufficient credits – moving to next key (short wait)")
                    time.sleep(5)
                    continue
                if "JSON" in err or "no JSON array" in err.lower():
                    print("⏳ JSON parse error – moving to next key")
                    time.sleep(5)
                    continue
                print("⏳ Waiting 60 seconds before next key...")
                time.sleep(60)
                continue

    # LAYER 2: GEMINI
    print(f"🤖 Gemini layer: {len(GEMINI_KEYS)} keys × {len(GEMINI_MODELS)} models = {len(GEMINI_KEYS)*len(GEMINI_MODELS)} attempts (temp={GEMINI_TEMPERATURE})")
    for key_idx, api_key in enumerate(GEMINI_KEYS):
        for model in GEMINI_MODELS:
            total_attempts += 1
            print(f"🤖 Attempt {total_attempts}/{max_attempts}: Gemini key {key_idx}, model {model}")
            try:
                gemini_client = genai.Client(api_key=api_key)
                response = gemini_client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config={
                        "temperature": GEMINI_TEMPERATURE,
                        "max_output_tokens": 2000
                    }
                )
                raw = response.text
                questions = parse_questions(raw)
                print(f"✅ Generated {len(questions)} questions using Gemini/{model}")
                return questions
            except Exception as e:
                err = str(e)
                print(f"⚠️ Gemini {model} failed: {err[:150]}")
                if "429" in err or "503" in err:
                    print("⏳ Quota/overload – waiting 60 seconds")
                    time.sleep(60)
                    continue
                if "404" in err:
                    print("⏳ Model not found – moving to next model")
                    time.sleep(5)
                    continue
                if "JSON" in err or "no JSON array" in err.lower():
                    print("⏳ JSON parse error – moving to next model")
                    time.sleep(5)
                    continue
                print("⏳ Waiting 10 seconds")
                time.sleep(10)
                continue

    # ALL ATTEMPTS EXHAUSTED
    print(f"🚨 All {max_attempts} attempts exhausted. Waiting 1 hour...")
    time.sleep(3600)
    try:
        test_client = genai.Client(api_key=GEMINI_KEYS[0])
        test_client.models.generate_content(model=GEMINI_MODELS[0], contents="test")
    except Exception as test_err:
        if "429" in str(test_err):
            print("⚠️ Quota still exhausted. Waiting until 5:30 AM IST.")
            wait_until_5_30_am_ist()
    return generate_questions(text_chunk)

# ==========================================
# 9. MAIN LOOP
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    if not is_reset_done():
        reset_and_start_fresh()
    else:
        print(f"✅ Reset already performed. Resuming from page {get_current_page()} (no reset)")
    
    pdf = "book.pdf"
    if not os.path.exists(pdf):
        print("📥 Downloading book...")
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
        gdown.download(url, pdf, quiet=False)
        if os.path.exists(pdf):
            print(f"✅ Downloaded: {os.path.getsize(pdf)} bytes")
        else:
            print("❌ Download failed.")
            return
    else:
        print(f"✅ PDF exists: {pdf}")
    
    print("\n" + "="*60)
    print("📖 PROCESSING (3 pages/chunk, 15–20 questions)")
    print(f"🌐 OpenRouter: {len(OPENROUTER_KEYS)} keys with auto model (temp={OPENROUTER_TEMPERATURE})")
    print(f"🤖 Gemini: {len(GEMINI_KEYS)} keys × {len(GEMINI_MODELS)} models = {len(GEMINI_KEYS)*len(GEMINI_MODELS)} attempts (temp={GEMINI_TEMPERATURE})")
    print(f"🔁 Total attempts per chunk: {len(OPENROUTER_KEYS) + len(GEMINI_KEYS)*len(GEMINI_MODELS)}")
    print("⏱️ Gaps: 5s for credits/parse/404 errors, 60s for quota/other errors")
    print("🚨 After all fails: wait 1h, then if still 429 wait until 5:30 AM IST")
    print("="*60 + "\n")
    
    total_q = 0
    errors = 0
    
    while True:
        try:
            page = get_current_page()
            next_page = page + 3
            print(f"\n🔍 Chunk: pages {page} to {next_page-1}")
            text = extract_pdf_text(page, next_page, pdf)
            if text is None:
                print("🏁 End of PDF.")
                break
            if len(text) < 150:
                print(f"⚠️ Low text ({len(text)} chars). Skipping.")
                update_current_page(next_page)
                continue
            
            print(f"🧠 Generating 15–20 questions ({len(text)} chars)")
            questions = generate_questions(text)
            if not questions:
                print("⚠️ No questions. Advancing.")
                update_current_page(next_page)
                continue
            
            rows = []
            for q in questions:
                rows.append([
                    q.get("section", "General"),
                    q.get("question", ""),
                    q.get("opt1", ""), q.get("opt2", ""), q.get("opt3", ""),
                    q.get("opt4", ""), q.get("opt5", ""), q.get("answer", "")
                ])
            sheet.append_rows(rows, value_input_option="RAW")
            total_q += len(questions)
            print(f"✅ Appended {len(questions)} questions (total {total_q})")
            update_current_page(next_page)
            errors = 0
            print("⏳ Success gap: 30 seconds")
            time.sleep(30)
        except Exception as e:
            print(f"❌ Loop error: {e}")
            errors += 1
            if errors > 5:
                break
            time.sleep(60)
    
    print(f"\n📊 FINAL: {total_q} questions in Google Sheets.")

if __name__ == "__main__":
    main()
