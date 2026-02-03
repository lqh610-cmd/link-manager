import os
import sqlite3
import requests
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time
from flask import Flask, request, jsonify, render_template, g
import webview
from html import escape as html_escape
import socket
from urllib.parse import urlparse
import atexit
import tempfile

# =====================================================
# APP + PATH
# =====================================================
app = Flask(__name__,
            template_folder='templates',
            static_folder='static')

import sys

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.path.join(BASE_DIR, 'links.db')


# =====================================================
# DATABASE INITIALIZATION
# =====================================================
def init_db():
    """Khởi tạo database"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = 10000")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA foreign_keys = ON")

    conn.executescript("""
    CREATE TABLE IF NOT EXISTS contents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id INTEGER,
        url TEXT,
        status TEXT DEFAULT 'unknown',
        last_checked DATETIME,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(content_id) REFERENCES contents(id) ON DELETE CASCADE,
        UNIQUE(content_id, url) ON CONFLICT REPLACE
    );

    CREATE INDEX IF NOT EXISTS idx_content_id ON links(content_id);
    CREATE INDEX IF NOT EXISTS idx_url_status ON links(url, status);
    CREATE INDEX IF NOT EXISTS idx_contents_updated ON contents(updated_at DESC);
    """)

    conn.close()


# =====================================================
# DATABASE CONNECTION
# =====================================================
def get_db():
    """Lấy connection từ pool"""
    if not hasattr(g, 'db'):
        g.db = sqlite3.connect(DB_PATH, check_same_thread=False)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(error):
    """Đóng connection khi kết thúc request"""
    if hasattr(g, 'db'):
        g.db.close()


# =====================================================
# URL NORMALIZATION
# =====================================================
def normalize_url(url: str) -> str:
    """Chuẩn hóa URL trước khi check"""
    url = url.strip()
    if not url:
        return ""

    # Loại bỏ các protocol vô nghĩa
    if url.startswith(("javascript:", "file:", "about:", "mailto:", "tel:")):
        return ""

    # Thêm https:// nếu không có protocol
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    return url


def domain_resolves(url: str) -> bool:
    """Kiểm tra domain có resolve được không (siêu nhanh)"""
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        socket.gethostbyname(host)
        return True
    except:
        return False


# =====================================================
# OPTIMIZED CHECK LINK - SIÊU NHANH
# =====================================================
_session = None
_session_lock = threading.Lock()


def get_session():
    """Tạo session tái sử dụng cho requests với thread safety"""
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=50,  # Tăng số kết nối
                    pool_maxsize=50,  # Tăng kích thước pool
                    max_retries=1,  # Chỉ retry 1 lần
                    pool_block=False
                )
                _session.mount('http://', adapter)
                _session.mount('https://', adapter)
                _session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive"
                })
    return _session


def check_one_url(url: str) -> str:
    """Kiểm tra URL SIÊU NHANH - BỎ HEAD, DÙNG GET nhẹ"""
    # Loại bỏ sớm các URL vô nghĩa
    if not url or url.startswith(("javascript:", "file:", "about:", "mailto:", "tel:")):
        return "dead"

    # Kiểm tra domain có resolve được không
    if not domain_resolves(url):
        return "dead"

    session = get_session()

    try:
        # DÙNG GET với timeout cực ngắn và stream=True để cắt sớm
        response = session.get(
            url,
            timeout=(1.0, 1.5),  # Timeout cực ngắn
            allow_redirects=True,
            stream=True  # Quan trọng: không tải content
        )

        # Đóng response ngay lập tức
        response.close()

        # Chỉ cần status code
        if 200 <= response.status_code < 400:
            return "alive"
        return "dead"

    except requests.exceptions.Timeout:
        return "dead"
    except requests.exceptions.ConnectionError:
        return "dead"
    except requests.exceptions.RequestException:
        return "dead"
    except Exception:
        return "dead"


# =====================================================
# API ENDPOINTS
# =====================================================
@app.get("/api/health")
def health_check():
    """Endpoint kiểm tra sức khỏe API"""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


@app.post("/api/check-links")
def api_check_links():
    """Kiểm tra link với batch processing - TỐI ƯU TỐC ĐỘ"""
    data = request.json or {}
    links = data.get("links", [])

    if not links:
        return jsonify([])

    # Tăng worker lên cao (I/O bound nên OK)
    max_workers = min(20, len(links))

    urls = []
    link_ids = []

    for link in links:
        raw_url = link.get('url', '').strip()
        url = normalize_url(raw_url)  # Chuẩn hóa URL trước
        if url:
            urls.append(url)
            link_id = link.get('id')
            link_ids.append(link_id if link_id and link_id > 0 else None)

    # Batch check với nhiều worker
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        statuses = list(executor.map(check_one_url, urls))

    now = datetime.now().isoformat()
    results = []
    db = get_db()

    try:
        db.execute("BEGIN")

        for url, link_id, status in zip(urls, link_ids, statuses):
            result_item = {"url": url, "status": status}

            if link_id:
                try:
                    cursor = db.execute(
                        "SELECT id FROM links WHERE id = ?",
                        (int(link_id),)
                    )
                    if cursor.fetchone():
                        db.execute(
                            "UPDATE links SET status = ?, last_checked = ? WHERE id = ?",
                            (status, now, int(link_id))
                        )
                        result_item["id"] = int(link_id)
                    else:
                        result_item["id"] = None
                except Exception as e:
                    app.logger.warning(f"ID {link_id} không tồn tại: {e}")
                    result_item["id"] = None

            results.append(result_item)

        db.commit()

    except Exception as e:
        db.rollback()
        app.logger.error(f"Lỗi khi update link status: {e}")

        for url, status in zip(urls, statuses):
            results.append({"url": url, "status": status})

        return jsonify(results)

    return jsonify(results)


# =====================================================
# API XÓA NỘI DUNG
# =====================================================
@app.delete("/api/delete-content/<int:content_id>")
def delete_content(content_id):
    """Xóa nội dung và tất cả link liên quan"""
    if not content_id or content_id <= 0:
        return jsonify({"error": "Invalid content ID"}), 400

    db = get_db()
    try:
        db.execute("BEGIN")

        # Xóa tất cả link trước (do foreign key constraint)
        db.execute("DELETE FROM links WHERE content_id = ?", (content_id,))

        # Xóa nội dung
        cursor = db.execute("DELETE FROM contents WHERE id = ?", (content_id,))

        if cursor.rowcount == 0:
            db.rollback()
            return jsonify({"error": "Content not found"}), 404

        db.commit()
        return jsonify({"success": True, "deleted_id": content_id})

    except Exception as e:
        db.rollback()
        app.logger.error(f"Lỗi khi xóa nội dung {content_id}: {e}")
        return jsonify({"error": str(e)}), 500


# =====================================================
# API LƯU TỪNG HÀNG
# =====================================================
@app.post("/api/save-row")
def save_row():
    """Lưu một hàng cụ thể"""
    item = request.json or {}
    if not item:
        return jsonify({"error": "No data provided"}), 400

    content_id = item.get("id")
    description = item.get("description", "").strip()
    urls = item.get("urls", [])

    description = html_escape(description)
    cleaned_urls = []
    for url_data in urls:
        url = normalize_url(url_data.get("url", ""))  # Chuẩn hóa khi lưu
        status = url_data.get("status", "unknown")
        url_id = url_data.get("id")
        cleaned_urls.append({
            "id": url_id,
            "url": html_escape(url) if url else "",
            "status": status
        })

    db = get_db()
    try:
        db.execute("BEGIN")

        if not content_id or content_id < 0:
            cursor = db.execute(
                "INSERT INTO contents(description) VALUES (?)",
                (description,)
            )
            content_id = cursor.lastrowid
        else:
            db.execute("""
                UPDATE contents 
                SET description = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (description, content_id))
            db.execute("DELETE FROM links WHERE content_id = ?", (content_id,))

        for url_data in cleaned_urls:
            url = url_data.get("url", "").strip()
            status = url_data.get("status", "unknown")

            if not url:
                continue

            cursor = db.execute("""
                INSERT INTO links (content_id, url, status)
                VALUES (?, ?, ?)
            """, (content_id, url, status))
            url_data["id"] = cursor.lastrowid

        db.commit()

        cursor = db.execute("""
            SELECT c.id as content_id, c.description, 
                   l.id as link_id, l.url, l.status
            FROM contents c
            LEFT JOIN links l ON c.id = l.content_id
            WHERE c.id = ?
            ORDER BY l.id
        """, (content_id,))

        rows = cursor.fetchall()
        result = {
            "id": content_id,
            "description": description,
            "urls": []
        }

        for row in rows:
            if row['link_id'] is not None:
                result["urls"].append({
                    "id": row['link_id'],
                    "url": row['url'],
                    "status": row['status'] or "unknown"
                })

        return jsonify(result)

    except Exception as e:
        db.rollback()
        app.logger.error(f"Lỗi khi lưu hàng: {e}")
        return jsonify({"error": str(e)}), 500


# =====================================================
# LOAD DATA API
# =====================================================
def load_data_from_db():
    """Tải dữ liệu từ database riêng biệt"""
    db = get_db()

    query = """
    SELECT 
        c.id as content_id,
        c.description,
        l.id as link_id,
        l.url,
        l.status,
        l.last_checked
    FROM contents c
    LEFT JOIN links l ON c.id = l.content_id
    ORDER BY c.id, l.id
    """

    rows = db.execute(query).fetchall()

    result_map = {}

    for row in rows:
        content_id = row['content_id']

        if content_id not in result_map:
            result_map[content_id] = {
                "id": content_id,
                "description": html_escape(row['description'] or "") if row['description'] else "",
                "urls": []
            }

        if row['link_id'] is not None and row['url'] is not None:
            result_map[content_id]["urls"].append({
                "id": row['link_id'],
                "url": html_escape(row['url']),
                "status": row['status'] or "unknown"
            })

    result = list(result_map.values())
    result.sort(key=lambda x: x["id"])

    return result


@app.get("/api/data")
def load_data_api():
    """API endpoint tải dữ liệu"""
    try:
        result = load_data_from_db()
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"Lỗi khi tải dữ liệu: {e}")
        return jsonify({"error": str(e)}), 500


# =====================================================
# FRONTEND
# =====================================================
@app.get("/")
def index():
    return render_template("index.html")


# =====================================================
# CLEANUP FUNCTIONS
# =====================================================
_window = None


def cleanup_webview():
    """Dọn dẹp webview resources khi thoát"""
    global _window
    if _window:
        try:
            # Force close webview
            if hasattr(_window, 'destroy'):
                _window.destroy()
        except:
            pass

    # Cố gắng xóa thư mục tạm nếu cần
    try:
        temp_dir = os.path.join(tempfile.gettempdir(), 'pywebview')
        if os.path.exists(temp_dir):
            for root, dirs, files in os.walk(temp_dir, topdown=False):
                for name in files:
                    try:
                        os.chmod(os.path.join(root, name), 0o777)
                        os.remove(os.path.join(root, name))
                    except:
                        pass
                for name in dirs:
                    try:
                        os.rmdir(os.path.join(root, name))
                    except:
                        pass
    except:
        pass


# Đăng ký cleanup
atexit.register(cleanup_webview)


# =====================================================
# RUN APP - TỐI ƯU KHỞI ĐỘNG
# =====================================================
def start_flask():
    """Khởi động Flask server"""
    # Khởi tạo DB trước khi app chạy
    init_db()

    # Tạo session sớm để giảm latency đầu tiên
    get_session()

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
        use_reloader=False,
        threaded=True
    )


def get_screen_size():
    """Lấy kích thước màn hình"""
    try:
        import tkinter as tk
        root = tk.Tk()
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        root.destroy()
        return screen_width, screen_height
    except:
        return 1920, 1080


if __name__ == "__main__":
    # Khởi động Flask trong thread riêng
    flask_thread = threading.Thread(target=start_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # Chờ ngắn để Flask khởi động
    time.sleep(1)

    # Lấy kích thước màn hình
    screen_width, screen_height = get_screen_size()

    # Mở cửa sổ full screen ngay từ đầu
    window_width = screen_width // 2
    window_height = screen_height // 2

    window_x = 0
    window_y = 0

    try:
        window = webview.create_window(
            title="Quản Lý Link",
            url="http://127.0.0.1:5000",
            width=window_width,
            height=window_height,
            x=window_x,
            y=window_y,
            resizable=True,
            min_size=(600, 400),
            background_color='#f5f7fa',

        )

        _window = window  # Lưu reference

        webview.start()
    except Exception as e:
        print(f"Lỗi khi khởi động window: {e}")
        print("Bạn có thể truy cập ứng dụng tại: http://127.0.0.1:5000")

    # Đảm bảo dọn dẹp khi thoát
    cleanup_webview()