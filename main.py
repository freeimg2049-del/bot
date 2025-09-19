import os
import requests
import threading
import telebot
import time
from flask import Flask
from collections import defaultdict
import sqlite3
from datetime import datetime, timedelta
import json

# --- FLASK APP CHO HEALTH CHECK ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return {'status': 'Bot is running', 'timestamp': time.time()}

# --- PHẦN 1: TẢI CÁC BIẾN MÔI TRƯỜNG ---
# Lấy Token của Bot Telegram từ biến môi trường
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
# Tải các webhook chuyên dụng từ biến môi trường
IMAGE_WEBHOOK_URL = os.getenv("IMAGE_WEBHOOK")
VIDEO_WEBHOOK_URL = os.getenv("VIDEO_WEBHOOK")
DOCS_WEBHOOK_URL = os.getenv("DOCS_WEBHOOK")

# Validation
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN environment variable is required")

# --- DATABASE SETUP ---
def init_database():
    """Khởi tạo database SQLite để lưu analytics"""
    conn = sqlite3.connect('bot_analytics.db')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS file_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            file_name TEXT,
            file_type TEXT,
            file_size INTEGER,
            pipeline_used TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'processing'
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            total_files INTEGER DEFAULT 0,
            total_images INTEGER DEFAULT 0,
            total_videos INTEGER DEFAULT 0,
            total_documents INTEGER DEFAULT 0,
            total_size INTEGER DEFAULT 0,
            first_upload TIMESTAMP,
            last_upload TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def log_file_upload(user_id, username, file_name, file_type, file_size, pipeline):
    """Ghi log khi upload file"""
    conn = sqlite3.connect('bot_analytics.db')
    
    # Insert file log
    conn.execute('''
        INSERT INTO file_logs (user_id, username, file_name, file_type, file_size, pipeline_used)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (user_id, username, file_name, file_type, file_size, pipeline))
    
    # Update user stats
    conn.execute('''
        INSERT OR REPLACE INTO user_stats 
        (user_id, username, total_files, total_images, total_videos, total_documents, total_size, first_upload, last_upload)
        VALUES (
            ?,
            ?,
            COALESCE((SELECT total_files FROM user_stats WHERE user_id = ?), 0) + 1,
            COALESCE((SELECT total_images FROM user_stats WHERE user_id = ?), 0) + ?,
            COALESCE((SELECT total_videos FROM user_stats WHERE user_id = ?), 0) + ?,
            COALESCE((SELECT total_documents FROM user_stats WHERE user_id = ?), 0) + ?,
            COALESCE((SELECT total_size FROM user_stats WHERE user_id = ?), 0) + ?,
            COALESCE((SELECT first_upload FROM user_stats WHERE user_id = ?), ?),
            ?
        )
    ''', (
        user_id, username, user_id, user_id,
        1 if file_type == 'image' else 0,
        user_id,
        1 if file_type == 'video' else 0,
        user_id,
        1 if file_type == 'document' else 0,
        user_id, file_size,
        user_id, datetime.now(),
        datetime.now()
    ))
    
    conn.commit()
    conn.close()

def get_user_dashboard(user_id):
    """Lấy dashboard data cho user"""
    conn = sqlite3.connect('bot_analytics.db')
    cursor = conn.cursor()
    
    # Get user stats
    cursor.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
    stats = cursor.fetchone()
    
    if not stats:
        return None
    
    # Get recent files (last 10)
    cursor.execute('''
        SELECT file_name, file_type, processed_at, status 
        FROM file_logs 
        WHERE user_id = ? 
        ORDER BY processed_at DESC 
        LIMIT 10
    ''', (user_id,))
    recent_files = cursor.fetchall()
    
    # Get today's stats
    today = datetime.now().date()
    cursor.execute('''
        SELECT COUNT(*), SUM(file_size) 
        FROM file_logs 
        WHERE user_id = ? AND DATE(processed_at) = ?
    ''', (user_id, today))
    today_stats = cursor.fetchone()
    
    # Get this week's stats
    week_ago = datetime.now() - timedelta(days=7)
    cursor.execute('''
        SELECT COUNT(*), SUM(file_size) 
        FROM file_logs 
        WHERE user_id = ? AND processed_at >= ?
    ''', (user_id, week_ago))
    week_stats = cursor.fetchone()
    
    conn.close()
    
    return {
        'total': {
            'files': stats[2],
            'images': stats[3], 
            'videos': stats[4],
            'documents': stats[5],
            'size': stats[6],
            'first_upload': stats[7],
            'last_upload': stats[8]
        },
        'today': {
            'files': today_stats[0] or 0,
            'size': today_stats[1] or 0
        },
        'week': {
            'files': week_stats[0] or 0,
            'size': week_stats[1] or 0
        },
        'recent_files': recent_files
    }

# Initialize database
init_database()

# Khởi tạo bot
bot = telebot.TeleBot(TELEGRAM_TOKEN)
user_buffers = defaultdict(list)
buffer_timers = {}

# --- BATCH PROCESSING VARIABLES ---
# --- PHẦN 2: HÀM GỌI PIPEDREAM TRONG NỀN ---
def call_pipedream_in_background(target_webhook, payload, user_id=None, file_count=1, file_info=None):
    """
    Hàm này chạy trong một luồng riêng để gọi Pipedream.
    Nó không ảnh hưởng đến luồng chính đang trả lời Telegram.
    """
    print(f"Bắt đầu gọi Pipedream trong nền tới: {target_webhook}")
    try:
        # Gửi yêu cầu POST với dữ liệu JSON, đặt timeout 15 giây để tránh treo
        response = requests.post(target_webhook, json=payload, timeout=15)
        response.raise_for_status()
        print(f"Gọi Pipedream trong nền thành công. Status: {response.status_code}")
        
        # Update database status to success
        if file_info:
            conn = sqlite3.connect('bot_analytics.db')
            conn.execute('''
                UPDATE file_logs 
                SET status = 'success' 
                WHERE user_id = ? AND file_name = ? AND processed_at = (
                    SELECT MAX(processed_at) FROM file_logs 
                    WHERE user_id = ? AND file_name = ?
                )
            ''', (user_id, file_info['name'], user_id, file_info['name']))
            conn.commit()
            conn.close()
        
        # Optional: Notify completion for single files
        if user_id and file_count == 1:
            try:
                bot.send_message(user_id, "✅ File đã được xử lý thành công!")
            except:
                pass  # Ignore notification errors
                
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi gọi Pipedream trong nền: {e}")
        
        # Update database status to failed
        if file_info:
            conn = sqlite3.connect('bot_analytics.db')
            conn.execute('''
                UPDATE file_logs 
                SET status = 'failed' 
                WHERE user_id = ? AND file_name = ? AND processed_at = (
                    SELECT MAX(processed_at) FROM file_logs 
                    WHERE user_id = ? AND file_name = ?
                )
            ''', (user_id, file_info['name'], user_id, file_info['name']))
            conn.commit()
            conn.close()
            
        if user_id:
            try:
                bot.send_message(user_id, "❌ Có lỗi xảy ra khi xử lý file")
            except:
                pass

# --- BATCH PROCESSING FUNCTION ---
def process_batch(user_id):
    """Xử lý batch files của user"""
    if user_id not in user_buffers:
        return
        
    files = user_buffers[user_id].copy()
    user_buffers[user_id].clear()
    
    if not files:
        return
    
    print(f"Processing batch of {len(files)} files for user {user_id}")
    
    # Notify user about batch
    try:
        bot.send_message(user_id, f"📦 Đang xử lý {len(files)} files...")
    except:
        pass
    
    # Process each file
    for message in files:
        target_webhook = None
        file_type = ""
        pipeline = ""
        
        # Get file info
        file_info = {}
        if message.photo:
            target_webhook = IMAGE_WEBHOOK_URL
            file_type = "image"
            pipeline = "images"
            # Get largest photo size
            photo = max(message.photo, key=lambda x: x.file_size)
            file_info = {
                'name': f"photo_{photo.file_id}.jpg",
                'size': photo.file_size or 0
            }
        elif message.video:
            target_webhook = VIDEO_WEBHOOK_URL
            file_type = "video" 
            pipeline = "videos"
            file_info = {
                'name': message.video.file_name or f"video_{message.video.file_id}",
                'size': message.video.file_size or 0
            }
        elif message.document:
            target_webhook = DOCS_WEBHOOK_URL
            file_type = "document"
            pipeline = "documents"
            file_info = {
                'name': message.document.file_name or f"document_{message.document.file_id}",
                'size': message.document.file_size or 0
            }
            
        if target_webhook and file_info:
            # Log to database
            username = message.from_user.username or message.from_user.first_name or "Unknown"
            log_file_upload(user_id, username, file_info['name'], file_type, file_info['size'], pipeline)
            
            # Process in background thread
            thread = threading.Thread(
                target=call_pipedream_in_background, 
                args=(target_webhook, message.json, user_id, len(files), file_info)
            )
            thread.daemon = True
            thread.start()

# --- PHẦN 3: HÀM XỬ LÝ CHÍNH KHI NHẬN FILE ---
@bot.message_handler(content_types=['photo', 'video', 'document'])
def handle_file_with_batching(message):
    """
    Xử lý file với batching để tránh spam notifications
    """
    user_id = message.from_user.id
    
    # Validate webhooks
    target_webhook = None
    if message.photo and IMAGE_WEBHOOK_URL:
        target_webhook = IMAGE_WEBHOOK_URL
    elif message.video and VIDEO_WEBHOOK_URL:
        target_webhook = VIDEO_WEBHOOK_URL
    elif message.document and DOCS_WEBHOOK_URL:
        target_webhook = DOCS_WEBHOOK_URL
    
    if not target_webhook:
        bot.reply_to(message, "❌ Loại file này không được hỗ trợ hoặc chưa cấu hình webhook.")
        return
    
    # Add to user buffer
    user_buffers[user_id].append(message)
    
    # Cancel existing timer
    if user_id in buffer_timers:
        buffer_timers[user_id].cancel()
    
    # Set new timer (3 seconds delay)
    timer = threading.Timer(3.0, lambda: process_batch(user_id))
    buffer_timers[user_id] = timer
    timer.start()

# --- DASHBOARD COMMANDS ---
@bot.message_handler(commands=['dashboard', 'stats'])
def show_dashboard(message):
    """Hiển thị dashboard analytics cho user"""
    user_id = message.from_user.id
    dashboard_data = get_user_dashboard(user_id)
    
    if not dashboard_data:
        bot.reply_to(message, "📊 Chưa có dữ liệu. Hãy upload file đầu tiên!")
        return
    
    # Format file size
    def format_size(size_bytes):
        if size_bytes == 0:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    
    total = dashboard_data['total']
    today = dashboard_data['today']
    week = dashboard_data['week']
    
    # Create dashboard text
    dashboard_text = f"""
📊 **YOUR DASHBOARD**

**📈 TỔNG QUAN**
📁 Tổng files: **{total['files']}**
📷 Ảnh: {total['images']} files
🎥 Video: {total['videos']} files  
📄 Documents: {total['documents']} files
💾 Tổng dung lượng: **{format_size(total['size'])}**

**📅 HÔM NAY**
📤 Upload: {today['files']} files ({format_size(today['size'])})

**📊 TUẦN NÀY** 
📈 Upload: {week['files']} files ({format_size(week['size'])})

**⏰ THỜI GIAN**
🚀 Lần đầu: {total['first_upload'][:16] if total['first_upload'] else 'N/A'}
🕒 Gần nhất: {total['last_upload'][:16] if total['last_upload'] else 'N/A'}

Sử dụng /recent để xem files gần đây
Sử dụng /search <tên file> để tìm kiếm
    """
    
    bot.reply_to(message, dashboard_text, parse_mode='Markdown')

@bot.message_handler(commands=['recent'])
def show_recent_files(message):
    """Hiển thị files gần đây"""
    user_id = message.from_user.id
    dashboard_data = get_user_dashboard(user_id)
    
    if not dashboard_data or not dashboard_data['recent_files']:
        bot.reply_to(message, "📝 Không có files gần đây")
        return
    
    recent_text = "📝 **FILES GẦN ĐÂY** (10 files mới nhất)\n\n"
    
    for i, (file_name, file_type, processed_at, status) in enumerate(dashboard_data['recent_files'], 1):
        status_emoji = "✅" if status == "success" else "❌" if status == "failed" else "⏳"
        type_emoji = {"image": "📷", "video": "🎥", "document": "📄"}.get(file_type, "📁")
        
        # Shorten long filenames
        display_name = file_name[:30] + "..." if len(file_name) > 30 else file_name
        time_str = processed_at[:16]  # YYYY-MM-DD HH:MM
        
        recent_text += f"{i}. {type_emoji} {display_name}\n"
        recent_text += f"   {status_emoji} {time_str}\n\n"
    
    bot.reply_to(message, recent_text, parse_mode='Markdown')

@bot.message_handler(commands=['search'])
def search_files(message):
    """Tìm kiếm files theo tên"""
    user_id = message.from_user.id
    
    # Get search query
    command_parts = message.text.split(' ', 1)
    if len(command_parts) < 2:
        bot.reply_to(message, "💡 Sử dụng: /search <tên file>")
        return
    
    search_query = command_parts[1].strip()
    
    # Search in database
    conn = sqlite3.connect('bot_analytics.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT file_name, file_type, processed_at, status 
        FROM file_logs 
        WHERE user_id = ? AND file_name LIKE ? 
        ORDER BY processed_at DESC 
        LIMIT 20
    ''', (user_id, f'%{search_query}%'))
    
    results = cursor.fetchall()
    conn.close()
    
    if not results:
        bot.reply_to(message, f"🔍 Không tìm thấy file nào chứa '{search_query}'")
        return
    
    search_text = f"🔍 **KẾT QUẢ TÌM KIẾM** '{search_query}'\n\n"
    
    for i, (file_name, file_type, processed_at, status) in enumerate(results, 1):
        status_emoji = "✅" if status == "success" else "❌" if status == "failed" else "⏳"
        type_emoji = {"image": "📷", "video": "🎥", "document": "📄"}.get(file_type, "📁")
        
        display_name = file_name[:40] + "..." if len(file_name) > 40 else file_name
        time_str = processed_at[:16]
        
        search_text += f"{i}. {type_emoji} {display_name}\n"
        search_text += f"   {status_emoji} {time_str}\n\n"
    
    search_text += f"\nTìm thấy {len(results)} file(s)"
    
    bot.reply_to(message, search_text, parse_mode='Markdown')
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    welcome_text = """
🤖 **File Manager Bot**

Gửi file để tôi tự động phân loại và lưu trữ:
📷 Ảnh → Image Pipeline
🎥 Video → Video Pipeline  
📄 Documents → Document Pipeline

**Commands:**
/start - Hiển thị tin nhắn này
/help - Hướng dẫn sử dụng
/status - Kiểm tra trạng thái bot

Bot sẽ tự động group các files liên tục để tránh spam thông báo! ✨
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

# --- ERROR HANDLER ---
def error_handler(message):
    print(f"Error in message handling: {message}")

# --- PHẦN 4: KHỞI CHẠY BOT ---
if __name__ == "__main__":
    print("🚀 Bot đang khởi động...")
    print(f"✅ Token configured: {'Yes' if TELEGRAM_TOKEN else 'No'}")
    print(f"📷 Image webhook: {'Yes' if IMAGE_WEBHOOK_URL else 'No'}")
    print(f"🎥 Video webhook: {'Yes' if VIDEO_WEBHOOK_URL else 'No'}")
    print(f"📄 Docs webhook: {'Yes' if DOCS_WEBHOOK_URL else 'No'}")
    
    # Start Flask in separate thread for health checks
    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000))))
    flask_thread.daemon = True
    flask_thread.start()
    
    print("✅ Bot ready! Starting polling...")
    
    # Start bot with error handling
    try:
        bot.polling(none_stop=True, interval=0, timeout=20)
    except Exception as e:
        print(f"❌ Bot error: {e}")
        time.sleep(15)
        # Restart polling
        bot.polling(none_stop=True, interval=0, timeout=20)