import os
import requests
import threading
import telebot
import time
import json
from flask import Flask, jsonify
from collections import defaultdict
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FLASK APP FOR HEALTH CHECK ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return jsonify({
        'status': 'Bot is running', 
        'timestamp': time.time(),
        'webhooks': {
            'image': bool(os.getenv("IMAGE_WEBHOOK")),
            'video': bool(os.getenv("VIDEO_WEBHOOK")),
            'docs': bool(os.getenv("DOCS_WEBHOOK"))
        }
    })

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'})

# --- ENVIRONMENT VARIABLES ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
IMAGE_WEBHOOK_URL = os.getenv("IMAGE_WEBHOOK")
VIDEO_WEBHOOK_URL = os.getenv("VIDEO_WEBHOOK")
DOCS_WEBHOOK_URL = os.getenv("DOCS_WEBHOOK")

# Configuration
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "5"))  # seconds
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "10"))  # files
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))  # seconds

if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN environment variable is required")

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# Thread-safe storage for batching
class BatchManager:
    def __init__(self):
        self.user_buffers: Dict[int, List] = defaultdict(list)
        self.buffer_timers: Dict[int, threading.Timer] = {}
        self.lock = threading.Lock()
    
    def add_file(self, user_id: int, message):
        with self.lock:
            self.user_buffers[user_id].append(message)
            
            # Cancel existing timer
            if user_id in self.buffer_timers:
                self.buffer_timers[user_id].cancel()
            
            # Check if we hit max batch size
            if len(self.user_buffers[user_id]) >= MAX_BATCH_SIZE:
                self._process_batch_now(user_id)
            else:
                # Set new timer
                timer = threading.Timer(BATCH_TIMEOUT, lambda: self._process_batch_now(user_id))
                self.buffer_timers[user_id] = timer
                timer.start()
    
    def _process_batch_now(self, user_id: int):
        with self.lock:
            if user_id in self.buffer_timers:
                self.buffer_timers[user_id].cancel()
                del self.buffer_timers[user_id]
            
            files = self.user_buffers[user_id].copy()
            self.user_buffers[user_id].clear()
        
        if files:
            self._process_batch(user_id, files)
    
    def _process_batch(self, user_id: int, files: List):
        """Process batch of files"""
        try:
            logger.info(f"Processing batch of {len(files)} files for user {user_id}")
            
            if len(files) > 1:
                bot.send_message(user_id, f"📦 Đang xử lý {len(files)} files...")
            
            # Group files by type
            file_groups = {
                'images': [],
                'videos': [],
                'documents': []
            }
            
            for message in files:
                if message.photo and IMAGE_WEBHOOK_URL:
                    file_groups['images'].append(message)
                elif message.video and VIDEO_WEBHOOK_URL:
                    file_groups['videos'].append(message)
                elif message.document and DOCS_WEBHOOK_URL:
                    file_groups['documents'].append(message)
            
            # Process each group
            for file_type, file_list in file_groups.items():
                if file_list:
                    self._process_file_group(user_id, file_type, file_list)
                    
        except Exception as e:
            logger.error(f"Error processing batch for user {user_id}: {e}")
            bot.send_message(user_id, "❌ Có lỗi xảy ra khi xử lý files")
    
    def _process_file_group(self, user_id: int, file_type: str, files: List):
        """Process a group of files of the same type"""
        webhook_urls = {
            'images': IMAGE_WEBHOOK_URL,
            'videos': VIDEO_WEBHOOK_URL,
            'documents': DOCS_WEBHOOK_URL
        }
        
        webhook_url = webhook_urls.get(file_type)
        if not webhook_url:
            return
        
        # Prepare batch payload
        batch_payload = {
            'user_id': user_id,
            'file_type': file_type,
            'batch_size': len(files),
            'timestamp': time.time(),
            'files': []
        }
        
        for message in files:
            try:
                file_data = self._extract_file_data(message)
                if file_data:
                    batch_payload['files'].append(file_data)
            except Exception as e:
                logger.error(f"Error extracting file data: {e}")
        
        if batch_payload['files']:
            # Call webhook in background
            thread = threading.Thread(
                target=self._call_webhook_background,
                args=(webhook_url, batch_payload, user_id, len(files))
            )
            thread.daemon = True
            thread.start()
    
    def _extract_file_data(self, message) -> Optional[dict]:
        """Extract file data from telegram message"""
        try:
            file_data = {
                'message_id': message.message_id,
                'date': message.date,
                'chat_id': message.chat.id,
                'user_id': message.from_user.id,
                'caption': message.caption,
            }
            
            if message.photo:
                # Get highest resolution photo
                photo = max(message.photo, key=lambda x: x.file_size or 0)
                file_info = bot.get_file(photo.file_id)
                file_data.update({
                    'file_id': photo.file_id,
                    'file_unique_id': photo.file_unique_id,
                    'file_size': photo.file_size,
                    'file_path': file_info.file_path,
                    'download_url': f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_info.file_path}"
                })
            
            elif message.video:
                file_info = bot.get_file(message.video.file_id)
                file_data.update({
                    'file_id': message.video.file_id,
                    'file_unique_id': message.video.file_unique_id,
                    'file_size': message.video.file_size,
                    'duration': message.video.duration,
                    'width': message.video.width,
                    'height': message.video.height,
                    'file_path': file_info.file_path,
                    'download_url': f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_info.file_path}"
                })
            
            elif message.document:
                file_info = bot.get_file(message.document.file_id)
                file_data.update({
                    'file_id': message.document.file_id,
                    'file_unique_id': message.document.file_unique_id,
                    'file_size': message.document.file_size,
                    'file_name': message.document.file_name,
                    'mime_type': message.document.mime_type,
                    'file_path': file_info.file_path,
                    'download_url': f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_info.file_path}"
                })
            
            return file_data
            
        except Exception as e:
            logger.error(f"Error extracting file data: {e}")
            return None
    
    def _call_webhook_background(self, webhook_url: str, payload: dict, user_id: int, file_count: int):
        """Call webhook in background thread"""
        try:
            logger.info(f"Calling webhook: {webhook_url}")
            response = requests.post(
                webhook_url, 
                json=payload, 
                timeout=REQUEST_TIMEOUT,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            logger.info(f"Webhook call successful. Status: {response.status_code}")
            
            if file_count == 1:
                bot.send_message(user_id, "✅ File đã được xử lý thành công!")
            else:
                bot.send_message(user_id, f"✅ Đã xử lý {file_count} files thành công!")
                
        except requests.exceptions.Timeout:
            logger.error("Webhook call timed out")
            bot.send_message(user_id, "⏰ Timeout khi xử lý files, vui lòng thử lại")
        except requests.exceptions.RequestException as e:
            logger.error(f"Webhook call failed: {e}")
            bot.send_message(user_id, "❌ Có lỗi xảy ra khi xử lý files")

# Initialize batch manager
batch_manager = BatchManager()

# --- FILE HANDLERS ---
@bot.message_handler(content_types=['photo', 'video', 'document'])
def handle_file(message):
    user_id = message.from_user.id
    
    # Check if we have appropriate webhook configured
    has_webhook = False
    if message.photo and IMAGE_WEBHOOK_URL:
        has_webhook = True
    elif message.video and VIDEO_WEBHOOK_URL:
        has_webhook = True
    elif message.document and DOCS_WEBHOOK_URL:
        has_webhook = True
    
    if not has_webhook:
        bot.reply_to(message, "❌ File này chưa được hỗ trợ hoặc chưa cấu hình webhook.")
        return
    
    # Add to batch
    batch_manager.add_file(user_id, message)

# --- BASIC COMMANDS ---
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    welcome_text = f"""
🤖 **File Manager Bot**

Gửi file để tôi tự động phân loại và lưu trữ:
📷 Ảnh → Image Pipeline {'✅' if IMAGE_WEBHOOK_URL else '❌'}
🎥 Video → Video Pipeline {'✅' if VIDEO_WEBHOOK_URL else '❌'}
📄 Documents → Document Pipeline {'✅' if DOCS_WEBHOOK_URL else '❌'}

**Commands:**
/start - Hiển thị hướng dẫn
/help - Hướng dẫn sử dụng  
/status - Kiểm tra trạng thái bot
/config - Xem cấu hình hiện tại

**Tính năng:**
• Batch processing: Gửi nhiều files liên tục sẽ được group lại
• Timeout: {BATCH_TIMEOUT}s hoặc tối đa {MAX_BATCH_SIZE} files
• Auto-retry và error handling

Bot sẽ tự động group các files để tránh spam thông báo ✨
    """
    bot.reply_to(message, welcome_text, parse_mode='Markdown')

@bot.message_handler(commands=['status'])
def check_status(message):
    active_batches = len(batch_manager.user_buffers)
    status_text = f"""
📊 **Bot Status**

🔗 **Webhooks:**
📷 Images: {'✅' if IMAGE_WEBHOOK_URL else '❌'}
🎥 Videos: {'✅' if VIDEO_WEBHOOK_URL else '❌'}
📄 Documents: {'✅' if DOCS_WEBHOOK_URL else '❌'}

⚙️ **Config:**
• Batch timeout: {BATCH_TIMEOUT}s
• Max batch size: {MAX_BATCH_SIZE} files
• Request timeout: {REQUEST_TIMEOUT}s

📦 **Active batches:** {active_batches}

⚡ Bot is running and ready!
    """
    bot.reply_to(message, status_text, parse_mode='Markdown')

@bot.message_handler(commands=['config'])
def show_config(message):
    config_text = f"""
⚙️ **Configuration**

**Environment Variables:**
• BATCH_TIMEOUT: {BATCH_TIMEOUT}s
• MAX_BATCH_SIZE: {MAX_BATCH_SIZE} files
• REQUEST_TIMEOUT: {REQUEST_TIMEOUT}s

**Webhooks:**
• IMAGE_WEBHOOK: {'Set' if IMAGE_WEBHOOK_URL else 'Not set'}
• VIDEO_WEBHOOK: {'Set' if VIDEO_WEBHOOK_URL else 'Not set'}
• DOCS_WEBHOOK: {'Set' if DOCS_WEBHOOK_URL else 'Not set'}

Để thay đổi, update environment variables và restart bot.
    """
    bot.reply_to(message, config_text, parse_mode='Markdown')

# --- ERROR HANDLER ---
@bot.message_handler(func=lambda message: True)
def handle_unknown(message):
    bot.reply_to(message, "🤔 Tôi chỉ xử lý files (ảnh, video, documents). Gửi /help để xem hướng dẫn!")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    logger.info("🚀 Bot starting up...")
    logger.info(f"📷 Image webhook: {'Configured' if IMAGE_WEBHOOK_URL else 'Not configured'}")
    logger.info(f"🎥 Video webhook: {'Configured' if VIDEO_WEBHOOK_URL else 'Not configured'}")
    logger.info(f"📄 Docs webhook: {'Configured' if DOCS_WEBHOOK_URL else 'Not configured'}")
    logger.info(f"⚙️ Batch timeout: {BATCH_TIMEOUT}s, Max size: {MAX_BATCH_SIZE}")
    
    # Start Flask app in background
    flask_thread = threading.Thread(
        target=lambda: app.run(
            host='0.0.0.0', 
            port=int(os.getenv('PORT', 5000)),
            debug=False
        )
    )
    flask_thread.daemon = True
    flask_thread.start()
    
    logger.info("✅ Bot ready! Starting polling...")
    
    # Start bot polling with error handling
    try:
        bot.polling(none_stop=True, interval=1, timeout=30)
    except Exception as e:
        logger.error(f"Bot polling error: {e}")
        raise
