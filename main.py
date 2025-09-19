import os
import requests
import threading
import telebot
import time
from flask import Flask
from collections import defaultdict

# --- FLASK APP CHO HEALTH CHECK ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return {'status': 'Bot is running', 'timestamp': time.time()}

# --- PHẦN 1: TẢI CÁC BIẾN MÔI TRƯỜNG ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
IMAGE_WEBHOOK_URL = os.getenv("IMAGE_WEBHOOK")
VIDEO_WEBHOOK_URL = os.getenv("VIDEO_WEBHOOK")
DOCS_WEBHOOK_URL = os.getenv("DOCS_WEBHOOK")

if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN environment variable is required")

bot = telebot.TeleBot(TELEGRAM_TOKEN)
user_buffers = defaultdict(list)
buffer_timers = {}

# --- PHẦN 2: HÀM GỌI PIPEDREAM TRONG NỀN ---
def call_pipedream_in_background(target_webhook, payload, user_id=None, file_count=1):
    """Gọi Pipedream trong luồng riêng"""
    print(f"Bắt đầu gọi Pipedream trong nền tới: {target_webhook}")
    try:
        response = requests.post(target_webhook, json=payload, timeout=15)
        response.raise_for_status()
        print(f"Gọi Pipedream thành công. Status: {response.status_code}")

        if user_id and file_count == 1:
            bot.send_message(user_id, "✅ File đã được xử lý thành công!")

    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi gọi Pipedream: {e}")
        if user_id:
            bot.send_message(user_id, "❌ Có lỗi xảy ra khi xử lý file")

# --- PHẦN 3: XỬ LÝ BATCH ---
def process_batch(user_id):
    """Xử lý batch files của user"""
    if user_id not in user_buffers:
        return

    files = user_buffers[user_id].copy()
    user_buffers[user_id].clear()

    if not files:
        return

    print(f"Processing batch of {len(files)} files for user {user_id}")
    bot.send_message(user_id, f"📦 Đang xử lý {len(files)} files...")

    for message in files:
        target_webhook = None
        if message.photo:
            target_webhook = IMAGE_WEBHOOK_URL
        elif message.video:
            target_webhook = VIDEO_WEBHOOK_URL
        elif message.document:
            target_webhook = DOCS_WEBHOOK_URL

        if target_webhook:
            thread = threading.Thread(
                target=call_pipedream_in_background,
                args=(target_webhook, message.json, user_id, len(files))
            )
            thread.daemon = True
            thread.start()

# --- PHẦN 4: HANDLER FILE ---
@bot.message_handler(content_types=['photo', 'video', 'document'])
def handle_file_with_batching(message):
    user_id = message.from_user.id
    target_webhook = None

    if message.photo and IMAGE_WEBHOOK_URL:
        target_webhook = IMAGE_WEBHOOK_URL
    elif message.video and VIDEO_WEBHOOK_URL:
        target_webhook = VIDEO_WEBHOOK_URL
    elif message.document and DOCS_WEBHOOK_URL:
        target_webhook = DOCS_WEBHOOK_URL

    if not target_webhook:
        bot.reply_to(message, "❌ File này chưa được hỗ trợ hoặc chưa cấu hình webhook.")
        return

    user_buffers[user_id].append(message)

    if user_id in buffer_timers:
        buffer_timers[user_id].cancel()

    timer = threading.Timer(3.0, lambda: process_batch(user_id))
    buffer_timers[user_id] = timer
    timer.start()

# --- COMMANDS CƠ BẢN ---
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    welcome_text = """
🤖 **File Manager Bot**

Gửi file để tôi tự động phân loại và lưu trữ:
📷 Ảnh → Image Pipeline
🎥 Video → Video Pipeline  
📄 Documents → Document Pipeline

**Commands:**
/start - Hiển thị hướng dẫn
/help - Hướng dẫn sử dụng
/status - Kiểm tra trạng thái bot

Bot sẽ tự động group các files liên tục để tránh spam thông báo ✨
    """
    bot.reply_to(message, welcome_text, parse_mode='Markdown')

@bot.message_handler(commands=['status'])
def check_status(message):
    status_text = f"""
📊 **Bot Status**

🔗 **Webhooks configured:**
📷 Images: {'✅' if IMAGE_WEBHOOK_URL else '❌'}
🎥 Videos: {'✅' if VIDEO_WEBHOOK_URL else '❌'} 
📄 Documents: {'✅' if DOCS_WEBHOOK_URL else '❌'}

⚡ Bot is running and ready to process files!
    """
    bot.reply_to(message, status_text, parse_mode='Markdown')

# --- PHẦN 5: KHỞI CHẠY BOT ---
if __name__ == "__main__":
    print("🚀 Bot đang khởi động...")
    print(f"📷 Image webhook: {'Yes' if IMAGE_WEBHOOK_URL else 'No'}")
    print(f"🎥 Video webhook: {'Yes' if VIDEO_WEBHOOK_URL else 'No'}")
    print(f"📄 Docs webhook: {'Yes' if DOCS_WEBHOOK_URL else 'No'}")

    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000))))
    flask_thread.daemon = True
    flask_thread.start()

    print("✅ Bot ready! Starting polling...")
    bot.polling(none_stop=True, interval=0, timeout=20)
