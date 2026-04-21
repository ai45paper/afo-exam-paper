import os
from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "Agri-Data-Bank Bot is Awake and Running!"

def run():
    # Render खुद एक PORT असाइन करता है (आमतौर पर 10000), 
    # अगर वह नहीं मिलता है, तो बैकअप के तौर पर 8080 यूज़ करेगा।
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.start()
