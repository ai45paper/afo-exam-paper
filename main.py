import os
import sys
import json
import time
import re
import gc
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
import fitz  # PyMuPDF
import gdown
from flask import Flask
from threading import Thread
import google.generativeai as genai
import anthropic

# Optional OCR imports – will be used only if available
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("⚠️ pdf2image or pytesseract not installed. OCR fallback disabled.")

# ==========================================
# 1. ENVIRONMENT VARIABLES & CONFIG
# ==========================================
CLAUDE_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
NVIDIA_KEY = os.getenv("NVIDIA_API_KEY", "").strip()
OPENROUTER_KEYS = [k.strip() for k in os.getenv("OPENROUTER_KEYS", "").split(",") if k.strip()]
GEMINI_KEYS = [k.strip() for k in os.getenv("GEMINI_KEYS", "").split(",") if k.strip()]

MONGO_URI = os.getenv("MONGO_URI", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
DRIVE_FILE_ID = os.getenv("DRIVE_FILE_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "")

# Optional manual start page (1‑based index in PDF, but code uses 0‑based internally)
MANUAL_START_PAGE = int(os.getenv("MANUAL_START_PAGE", "969")) - 1   # convert to 0‑based

# Poppler path for pdf2image (adjust if needed)
POPPLER_PATH = os.getenv("POPPLER_PATH", "/usr/bin")

# Validate mandatory variables
if not MONGO_URI or not SHEET_ID or not DRIVE_FILE_ID or not SERVICE_ACCOUNT_JSON:
    print("❌ Missing required environment variables. Exiting.")
    sys.exit(1)

# Section ranges (1‑based page numbers)
SECTION_RANGES = [
    (1, 75, "Agronomy"), (76, 242, "Horticulture"), (243, 308, "Entomology"),
    (309, 389, "Fisheries"), (390, 517, "Animal Husbandry"), (518, 557, "Plant Pathology"),
    (558, 585, "Agricultural Economics"), (586, 704, "General Agriculture"),
    (705, 727, "Seed Technology"), (728, 759, "Weed Science"), (760, 771, "Apiculture"),
    (772, 803, "Forestry"), (804, 839, "Meteorology"), (840, 860, "Genetics and Breeding"),
    (861, 931, "Agricultural Engineering"), (932, 941, "Extension Education"),
    (942, 946, "Mushroom Cultivation"), (947, 964, "Sericulture"), (965, 966, "Lac Culture"),
    (967, 1075, "Soil Science")
]

# ==========================================
# 2. DATABASE & GOOGLE SHEETS INIT
# ==========================================
print("🔄 Connecting to MongoDB...")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo_client['agri_data_bank']
tracker_col = db['process_tracker']

print("🔄 Connecting to Google Sheets...")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gsheet_client = gspread.authorize(creds)
sheet = gsheet_client.open_by_key(SHEET_ID).sheet1

# ==========================================
# 3. TRACKER & APPEND‑ONLY LOGIC
# ==========================================
def init_tracker_and_sheet():
    """Get the last processed page index (0‑based) from MongoDB, apply manual override."""
    tracker = tracker_col.find_one({"_id": "pdf_tracker"})
    last_page = tracker.get("current_page", 0) if tracker else 0

    if last_page < MANUAL_START_PAGE:
        last_page = MANUAL_START_PAGE
        update_tracker(last_page)
        print(f"🚀 MANUAL OVERRIDE: Starting from page {last_page + 1}")
    else:
        print(f"✅ Resuming from page {last_page + 1} (MongoDB tracked)")

    # No sheet deletion – we always append new data
    print("📝 Google Sheet: existing data is safe. New MCQs will be appended.")
    return last_page

def update_tracker(page_num):
    """Save the current page index (0‑based) to MongoDB."""
    tracker_col.update_one({"_id": "pdf_tracker"}, {"$set": {"current_page": page_num}}, upsert=True)

def get_section(page_idx):
    """Return section name based on 1‑based human page number."""
    human_page = page_idx + 1
    for start, end, name in SECTION_RANGES:
        if start <= human_page <= end:
            return name
    return "General Agriculture"

# ==========================================
# 4. TEXT EXTRACTION (with OCR fallback)
# ==========================================
def extract_text_with_ocr(doc, pdf_path, page_index):
    """Extract text from PDF page; if too short, use OCR (if available)."""
    page = doc.load_page(page_index)
    text = page.get_text()
    if text and len(text.strip()) > 100:
        return text

    if not OCR_AVAILABLE:
        return ""

    print(f"🔍 OCR activated for page {page_index + 1}")
    try:
        images = convert_from_path(
            pdf_path,
            first_page=page_index + 1,
            last_page=page_index + 1,
            dpi=200,
            poppler_path=POPPLER_PATH
        )
        ocr_text = ""
        for img in images:
            # Convert to grayscale for better OCR
            ocr_text += pytesseract.image_to_string(img.convert("L"), lang="eng", config="--oem 3 --psm 6")
        return ocr_text
    except Exception as e:
        print(f"⚠️ OCR failed: {e}")
        return ""

# ==========================================
# 5. PROMPT FOR QUESTION GENERATION
# ==========================================
def build_afo_prompt(text, section):
    examples = """
REFERENCE QUESTION STYLE (Follow exactly):

The excretory organ of silkworm which is located at the junction of the midgut and hindgut is known as
Options: Proboscis | Malpighian tubule | Nephridia | Green glands | None
Answer: Malpighian tubule

Type of silviculture system which can regenerate through seeds and where the majority have a long life is
Options: Pollarding | High forest | Coppicing | Forking | None
Answer: High forest

The process of removing the green colouring (known as chlorophyll) from the skin of citrus fruit by introducing measured amounts of ethylene gas is known as
Options: Ripening | Degreening | Physiological maturity | Denavelling | Dehusking
Answer: Degreening
"""
    return f"""You are Satyam Sir, an expert agriculture mentor setting a mock paper for the AGTA 2026 and IBPS AFO Mains batches.

YOUR TASK: Generate exactly 10 high-quality multiple-choice questions from the provided text.

STRICT RULES:
1. Language: 100% STRICTLY ENGLISH ONLY. No Hindi.
2. Difficulty: Moderate level. Keep them engaging.
3. Options: Exactly 5 options per question.
4. Answer Match: The text in the 'answer' field MUST exactly match the text of one of the 5 options.
5. Question Length: 20 to 35 words.
6. Explanation: Max 20 words.
7. Format: Return ONLY a raw JSON array. DO NOT use markdown code blocks (like ```json). No introductory or concluding text.

Topic: {section}

{examples}

EXPECTED JSON SCHEMA:
[
  {{
    "section": "{section}",
    "question": "Question text here strictly in English...",
    "opt1": "First option text",
    "opt2": "Second option text",
    "opt3": "Third option text",
    "opt4": "Fourth option text",
    "opt5": "Fifth option text",
    "answer": "Exact text of the correct option",
    "explanation": "Short conceptual explanation strictly in English."
  }}
]

Content:
{text[:5000]}
"""

# ==========================================
# 6. JSON PARSING AND CLEANING
# ==========================================
def extract_and_clean_json(raw_text):
    """Extract JSON array from AI response, clean option prefixes, validate answer."""
    if not raw_text:
        return None

    # Remove markdown code blocks
    clean_text = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw_text.strip(), flags=re.MULTILINE)

    # Find first valid JSON array
    match = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', clean_text)
    if not match:
        return None

    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None

    # Keep only first 10 items
    data = data[:10]
    cleaned_data = []

    def clean_option(opt):
        """Remove common prefixes like 'Option 1:', '1.', 'A)' etc."""
        opt_str = str(opt).strip()
        opt_str = re.sub(r'^(Option\s*\d+\s*:|^\d+\.\s*|^[a-eA-E]\)\s*)', '', opt_str, flags=re.IGNORECASE)
        return opt_str.strip()

    for item in data:
        question = str(item.get("question", "")).strip()
        opt1 = clean_option(item.get("opt1", ""))
        opt2 = clean_option(item.get("opt2", ""))
        opt3 = clean_option(item.get("opt3", ""))
        opt4 = clean_option(item.get("opt4", ""))
        opt5 = clean_option(item.get("opt5", ""))
        answer = clean_option(item.get("answer", ""))
        explanation = str(item.get("explanation", "")).strip()

        if not question or not answer:
            continue

        # Validate that answer exactly matches one of the cleaned options (case‑insensitive)
        valid_options = [opt1, opt2, opt3, opt4, opt5]
        if answer.lower() not in [o.lower() for o in valid_options]:
            continue

        cleaned_data.append({
            "section": str(item.get("section", "")).strip() or "General Agriculture",
            "question": question,
            "opt1": opt1,
            "opt2": opt2,
            "opt3": opt3,
            "opt4": opt4,
            "opt5": opt5,
            "answer": answer,
            "explanation": explanation
        })

    return cleaned_data if cleaned_data else None

# ==========================================
# 7. AI MODEL CALLS WITH FALLBACK
# ==========================================
def call_claude(prompt):
    if not CLAUDE_KEY:
        return None
    try:
        print("🤖 Claude: claude-opus-4-20250805")
        client = anthropic.Anthropic(api_key=CLAUDE_KEY)
        response = client.messages.create(
            model="claude-opus-4-20250805",
            max_tokens=2500,
            temperature=0.4,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        print(f"⚠️ Claude error: {e}")
        return None

def call_openrouter(prompt):
    if not OPENROUTER_KEYS:
        return None
    models = [
        "openrouter/auto",
        "meta-llama/llama-3.1-70b-instruct",
        "anthropic/claude-3.5-sonnet",
        "mistralai/mixtral-8x7b-instruct"
    ]
    url = "https://openrouter.ai/api/v1/chat/completions"
    for model in models:
        for key in OPENROUTER_KEYS:
            try:
                print(f"🔄 OpenRouter: {model}")
                resp = requests.post(
                    url,
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={"model": model, "messages": [{"role": "user", "content": prompt}]},
                    timeout=60
                )
                if resp.status_code == 200:
                    return resp.json()["choices"][0]["message"]["content"]
                if resp.status_code == 429:
                    time.sleep(5)
            except Exception:
                continue
    return None

def call_nvidia(prompt):
    if not NVIDIA_KEY:
        return None
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    models = ["meta/llama3-70b-instruct", "nvidia/nemotron-4-340b-instruct"]
    for model in models:
        try:
            print(f"🧠 NVIDIA: {model}")
            resp = requests.post(
                url,
                headers={"Authorization": f"Bearer {NVIDIA_KEY}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}]},
                timeout=60
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
        except Exception:
            continue
    return None

def call_gemini(prompt):
    if not GEMINI_KEYS:
        return None
    models = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for key in GEMINI_KEYS:
        try:
            genai.configure(api_key=key)
            for model_name in models:
                try:
                    print(f"🤖 Gemini: {model_name}")
                    model = genai.GenerativeModel(model_name)
                    response = model.generate_content(
                        prompt,
                        generation_config={"response_mime_type": "application/json"}
                    )
                    if response and response.text:
                        return response.text
                except Exception as e:
                    if "429" in str(e) or "quota" in str(e).lower():
                        print("⏸️ Quota exhausted, trying next key...")
                        break
                    continue
        except Exception:
            continue
    return None

def generate_questions(text, section):
    """Try all AI providers in order until valid JSON is obtained."""
    prompt = build_afo_prompt(text, section)
    for func in [call_claude, call_openrouter, call_nvidia, call_gemini]:
        raw_response = func(prompt)
        if raw_response:
            cleaned = extract_and_clean_json(raw_response)
            if cleaned:
                return cleaned
    return None

# ==========================================
# 8. MAIN WORKFLOW (PDF PROCESSING)
# ==========================================
def main_workflow():
    pdf_path = "book.pdf"
    print("▶️ ENGINE STARTING...")

    # ---- Download PDF if not already present ----
    if not os.path.exists(pdf_path):
        print("📥 Downloading PDF from Google Drive (using gdown)...")
        try:
            # gdown handles large files and confirmation tokens automatically
            gdown.download(id=DRIVE_FILE_ID, output=pdf_path, quiet=False)
            if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) == 0:
                raise Exception("Downloaded file is empty or missing.")
            print("✅ PDF downloaded successfully.")
            gc.collect()
            time.sleep(2)
        except Exception as e:
            print(f"❌ PDF download failed: {e}")
            return

    # ---- Get total pages ----
    try:
        with fitz.open(pdf_path) as doc:
            total_pages = doc.page_count
        print(f"📄 Total pages in PDF: {total_pages}")
    except Exception as e:
        print(f"❌ Cannot read PDF: {e}")
        return

    curr_page = init_tracker_and_sheet()
    if curr_page >= total_pages:
        print("✅ Processing already completed. Exiting.")
        return

    buffer = []   # stores rows to be written to Google Sheets

    # ---- Process 2 pages at a time ----
    while curr_page < total_pages:
        next_page = min(curr_page + 2, total_pages)
        section = get_section(curr_page)

        try:
            print(f"\n📖 Pages {curr_page+1}-{next_page} | {section}")

            # Extract text from current batch
            combined_text = ""
            with fitz.open(pdf_path) as doc:
                for i in range(curr_page, next_page):
                    page_text = extract_text_with_ocr(doc, pdf_path, i)
                    if page_text:
                        combined_text += page_text + "\n"

            if len(combined_text.strip()) > 50:
                questions = generate_questions(combined_text, section)
                if questions:
                    for q in questions:
                        buffer.append([
                            q.get("section", section),
                            q.get("question", ""),
                            q.get("opt1", ""),
                            q.get("opt2", ""),
                            q.get("opt3", ""),
                            q.get("opt4", ""),
                            q.get("opt5", ""),
                            q.get("answer", ""),
                            q.get("explanation", "")
                        ])

                    # Write to Sheets when buffer reaches 50 rows
                    if len(buffer) >= 50:
                        sheet.append_rows(buffer, value_input_option="RAW")
                        print(f"✅ Saved batch of {len(buffer)} MCQs to Google Sheets.")
                        buffer = []
                else:
                    print("⚠️ No valid questions generated for this batch.")
            else:
                print(f"⚠️ Insufficient text on pages {curr_page+1}-{next_page} (length {len(combined_text)}).")

            # Update tracker and move forward
            update_tracker(next_page)
            curr_page = next_page

            # Cleanup and delay to avoid rate limits
            gc.collect()
            time.sleep(8)

        except Exception as e:
            print(f"❌ Error processing pages {curr_page+1}-{next_page}: {e}")
            # Move past the problematic batch to avoid infinite loop
            update_tracker(next_page)
            curr_page = next_page
            time.sleep(10)

    # Flush any remaining questions
    if buffer:
        sheet.append_rows(buffer, value_input_option="RAW")
        print(f"✅ Final batch saved: {len(buffer)} MCQs.")

    print("🎉 All pages processed successfully!")

# ==========================================
# 9. FLASK SERVER FOR HEALTH CHECKS
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "AGTA 2026 Engine is running."

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    # Start the PDF processing in a background thread
    thread = Thread(target=main_workflow, daemon=True)
    thread.start()
    # Run Flask web server (required for platforms like Render)
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
