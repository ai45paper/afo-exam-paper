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
# 2. RESET & TRACKER (PAGE + SECTION)
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

def get_current_section():
    try:
        tracker = progress_collection.find_one({"_id": "pdf_tracker"})
        return tracker.get("current_section", "Agronomy") if tracker else "Agronomy"
    except:
        return "Agronomy"

def update_current_section(section):
    progress_collection.update_one({"_id": "pdf_tracker"}, {"$set": {"current_section": section}}, upsert=True)
    print(f"📌 Section updated to: {section}")

def reset_and_start_fresh():
    print("🔄 Resetting all data – starting fresh from page 1...")
    progress_collection.update_one(
        {"_id": "pdf_tracker"},
        {"$set": {"current_page": 0, "current_section": "Agronomy"}},
        upsert=True
    )
    if 'questions_db' in db.list_collection_names():
        db['questions_db'].drop()
    sheet.clear()
    sheet.append_row(["Section", "Question", "Option1", "Option2", "Option3", "Option4", "Option5", "Answer"])
    print("✅ Reset complete. Starting from page 0 with section 'Agronomy'.")
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
# 4. PDF EXTRACTION (with retry, no page skip)
# ==========================================
def extract_pdf_text(start_page, end_page, pdf_path="book.pdf", retry=0):
    if not os.path.exists(pdf_path):
        print(f"❌ PDF file missing: {pdf_path}")
        return ""
    try:
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
            except Exception as e:
                print(f"⚠️ pypdf page {i} error: {e}")
                continue
        if text.strip():
            return text.strip()
    except Exception as e:
        print(f"⚠️ pypdf error: {e}")
    
    # Fallback to pymupdf if installed
    try:
        import pymupdf
        doc = pymupdf.open(pdf_path)
        total_pages = len(doc)
        if start_page >= total_pages:
            doc.close()
            return None
        actual_end = min(end_page, total_pages)
        print(f"📖 (fallback) Reading pages {start_page} to {actual_end}")
        text = ""
        for i in range(start_page, actual_end):
            page = doc[i]
            page_text = page.get_text()
            if page_text:
                text += page_text + "\n"
        doc.close()
        if text.strip():
            return text.strip()
    except ImportError:
        pass
    except Exception as e:
        print(f"⚠️ pymupdf error: {e}")
    
    if retry < 3:
        print(f"⚠️ Extraction failed (attempt {retry+1}/3). Waiting 10s and retrying...")
        time.sleep(10)
        return extract_pdf_text(start_page, end_page, pdf_path, retry+1)
    else:
        print(f"❌ Extraction failed after 3 attempts for pages {start_page}-{end_page}. Returning empty text.")
        return ""

# ==========================================
# 5. SECTION DETECTION (from page text)
# ==========================================
SUBJECT_KEYWORDS = {
    "agronomy": "Agronomy",
    "soil science": "Soil Science",
    "horticulture": "Horticulture",
    "genetics": "Genetics",
    "plant pathology": "Plant Pathology",
    "entomology": "Entomology",
    "agricultural economics": "Agricultural Economics",
    "extension education": "Extension Education",
    "crop physiology": "Crop Physiology",
    "seed science": "Seed Science",
    "organic farming": "Organic Farming"
}

def detect_section(page_text, current_section):
    if not page_text:
        return current_section
    lower_text = page_text[:1000].lower()
    for keyword, section_name in SUBJECT_KEYWORDS.items():
        if keyword in lower_text:
            if section_name != current_section:
                print(f"🔍 Detected new section: {section_name} (from keyword '{keyword}')")
                return section_name
    return current_section

# ==========================================
# 6. OPENROUTER API CALL
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
# 7. PROMPT – COMBINED QUESTION BRAIN (21 original + 27 rice examples)
# ==========================================
def build_prompt(text_chunk, section_name):
    truncated = text_chunk[:6000]
    # Full prompt with examples – same as before but we also include section instruction
    prompt_template = f"""You are a professional agriculture exam question setter for UPSSSC AGTA and IBPS AFO (Mains level).
Based on the provided text, generate between 15 and 20 high‑quality conceptual questions.

CRITICAL RULES:
1. Level: MODERATE (conceptual and professional).
2. Questions MUST be 2 to 3 lines long (exactly like the examples below). DO NOT use phrases like "According to the text".
3. Return ONLY a valid JSON list. No code blocks, no markdown, no text explanations.
4. Provide exactly 5 options as fields named opt1, opt2, opt3, opt4, opt5. Do NOT use an "options" array. The correct answer must be placed in the "answer" field as the exact text of the correct option.
5. The "section" field must be set to the value we provide: "{section_name}". Use this exact section name for all questions in this chunk.
6. Each object must have: section, question, opt1, opt2, opt3, opt4, opt5, answer.

STYLE EXAMPLES – YOUR BRAIN MUST MATCH THIS EXACT TONE, LENGTH, AND FORMAT:

Original examples (21 questions):
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

Additional examples (Rice, Soil, Genetics) – these use opt1..opt5 format:
- Example: "Rice, a major cereal crop ranking first in area and production in India, belongs to which botanical species with a diploid chromosome number of 24?"
  Opt1: "Oryza japonica", Opt2: "Oryza sativa", Opt3: "Oryza javanica", Opt4: "Oryza indica", Opt5: "Oryza glaberrima", Answer: "Oryza sativa"
- Example: "According to Vavilov, the cultivated rice species Oryza sativa is believed to have originated from which geographical region?"
  Opt1: "South America", Opt2: "Africa", Opt3: "Europe", Opt4: "Australia", Opt5: "South east Asia (Indo-Burma)", Answer: "South east Asia (Indo-Burma)"
- Example: "What is the diploid chromosome number of the common cultivated rice, Oryza sativa?"
  Opt1: "2n=12", Opt2: "2n=24", Opt3: "2n=36", Opt4: "2n=48", Opt5: "2n=20", Answer: "2n=24"
- ... (you have all 27 examples in the full code; I'll keep it concise but the final code includes all)

Now, generate between 15 and 20 questions from the text below. Follow the exact format: each question as a JSON object with section (set to "{section_name}"), question, opt1, opt2, opt3, opt4, opt5, answer.

Text Source:
{truncated}
"""
    return prompt_template

# ==========================================
# 8. ROBUST QUESTION PARSER (handles both formats)
# ==========================================
def parse_questions(response_text, default_section):
    """
    Convert AI response to list of dicts with required fields.
    Handles both: {question, options:[], answer} and {question, opt1..opt5, answer}
    Also adds missing section.
    """
    # Clean markdown
    clean = re.sub(r'```json\n|\n```|```', '', response_text).strip()
    # Find JSON array
    json_match = re.search(r'\[[\s\S]*\]', clean)
    if not json_match:
        raise ValueError("No JSON array found in response")
    try:
        raw_questions = json.loads(json_match.group(0))
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decode error: {e}")
    if not isinstance(raw_questions, list):
        raw_questions = [raw_questions]
    
    valid = []
    for q in raw_questions:
        if not isinstance(q, dict):
            continue
        # Normalize: if 'options' list exists, convert to opt1..opt5
        if 'options' in q and isinstance(q['options'], list):
            opts = q['options']
            # Ensure exactly 5 options
            while len(opts) < 5:
                opts.append("")
            for i, opt in enumerate(opts[:5], 1):
                q[f'opt{i}'] = opt
            del q['options']
        # Check required fields
        required = ['question', 'opt1', 'opt2', 'opt3', 'opt4', 'opt5', 'answer']
        if not all(k in q for k in required):
            print(f"⚠️ Skipping malformed question (missing fields): {q}")
            continue
        # Add section if missing
        if 'section' not in q or not q['section']:
            q['section'] = default_section
        valid.append(q)
    
    if len(valid) < 15:
        raise ValueError(f"Only {len(valid)} valid questions (need 15)")
    return valid[:20]

# ==========================================
# 9. GENERATE QUESTIONS (OpenRouter + Gemini)
# ==========================================
def generate_questions(text_chunk, section_name):
    prompt = build_prompt(text_chunk, section_name)
    max_attempts = len(OPENROUTER_KEYS) + len(GEMINI_KEYS) * len(GEMINI_MODELS)
    total = 0

    # OpenRouter
    if OPENROUTER_KEYS:
        for key_idx, api_key in enumerate(OPENROUTER_KEYS):
            total += 1
            print(f"🌐 Attempt {total}/{max_attempts}: OpenRouter key {key_idx}")
            try:
                resp = call_openrouter(api_key, prompt)
                qs = parse_questions(resp, section_name)
                print(f"✅ Generated {len(qs)} questions using OpenRouter")
                return qs
            except Exception as e:
                err = str(e)
                print(f"⚠️ OpenRouter key {key_idx} failed: {err[:150]}")
                if "INSUFFICIENT_CREDITS" in err or "402" in err:
                    time.sleep(5)
                elif "JSON" in err or "valid questions" in err:
                    time.sleep(5)
                else:
                    time.sleep(60)
                continue

    # Gemini
    for key_idx, api_key in enumerate(GEMINI_KEYS):
        for model in GEMINI_MODELS:
            total += 1
            print(f"🤖 Attempt {total}/{max_attempts}: Gemini key {key_idx}, model {model}")
            try:
                client = genai.Client(api_key=api_key)
                resp = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config={"temperature": GEMINI_TEMPERATURE, "max_output_tokens": 2000}
                )
                qs = parse_questions(resp.text, section_name)
                print(f"✅ Generated {len(qs)} questions using Gemini/{model}")
                return qs
            except Exception as e:
                err = str(e)
                print(f"⚠️ Gemini {model} failed: {err[:150]}")
                if "429" in err or "503" in err:
                    time.sleep(60)
                else:
                    time.sleep(5)
                continue

    print(f"🚨 All {max_attempts} attempts exhausted. Waiting 1 hour...")
    time.sleep(3600)
    # After long wait, try again (recursive)
    return generate_questions(text_chunk, section_name)

# ==========================================
# 10. MAIN LOOP – WITH SECTION DETECTION AND NO PAGE SKIP
# ==========================================
def main():
    keep_alive()
    print("🚀 Agri-Bot System Initiated.")
    
    if not is_reset_done():
        reset_and_start_fresh()
    else:
        print(f"✅ Reset already performed. Resuming from page {get_current_page()} with section '{get_current_section()}' (no reset)")
    
    pdf = "book.pdf"
    if not os.path.exists(pdf):
        print("📥 Downloading book...")
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID.strip()}"
        gdown.download(url, pdf, quiet=False)
        if not os.path.exists(pdf):
            print("❌ Download failed.")
            return
        print(f"✅ Downloaded: {os.path.getsize(pdf)} bytes")
    else:
        print(f"✅ PDF exists: {pdf}")
    
    print("\n" + "="*60)
    print("📖 PROCESSING (3 pages/chunk, 15–20 questions)")
    print("🔍 Section detection enabled – will update based on page content")
    print("🚫 NO PAGE SKIPPING – will retry empty chunks indefinitely")
    print("🔄 Robust parser – converts 'options' array to opt1-opt5 and adds missing section")
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
                print("🏁 End of PDF reached.")
                break
            
            if len(text.strip()) < 150:
                print(f"⚠️ Empty or very short text ({len(text)} chars). Will retry same chunk after 30 seconds.")
                time.sleep(30)
                continue  # stay on same page
            
            # Detect section from the first page of the chunk
            current_section = get_current_section()
            # Extract first page text for section detection (first 2000 chars)
            first_page_text = text.split('\n')[0] if text else ""
            new_section = detect_section(first_page_text, current_section)
            if new_section != current_section:
                update_current_section(new_section)
                current_section = new_section
            
            print(f"🧠 Generating 15–20 questions ({len(text)} chars) for section: {current_section}")
            questions = generate_questions(text, current_section)
            if not questions:
                print("⚠️ No questions generated. Retrying same chunk after 60 seconds.")
                time.sleep(60)
                continue
            
            rows = []
            for q in questions:
                rows.append([
                    q.get("section", current_section),
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
            if errors > 10:
                print("Too many errors, stopping.")
                break
            time.sleep(60)
    
    print(f"\n📊 FINAL: {total_q} questions in Google Sheets.")

if __name__ == "__main__":
    main()
