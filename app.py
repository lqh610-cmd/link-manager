import os
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
import traceback
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

# =====================================================
# APP + PATH
# =====================================================
app = Flask(__name__,
            template_folder='templates',
            static_folder='static')

import sys

# Kiểm tra môi trường
IS_RENDER = 'RENDER' in os.environ
IS_LOCAL = not IS_RENDER

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# =====================================================
# DATABASE CONFIGURATION
# =====================================================
if IS_RENDER:
    # PostgreSQL trên Render
    DATABASE_URL = os.environ.get('DATABASE_URL')
else:
    # SQLite cho local development
    DATABASE_URL = f"sqlite:///{os.path.join(BASE_DIR, 'link_manager.db')}"

# Connection pool cho PostgreSQL
pg_pool = None


def get_db_connection():
    """Get database connection - supports both PostgreSQL and SQLite"""
    if IS_RENDER:
        # PostgreSQL
        global pg_pool
        if pg_pool is None:
            pg_pool = SimpleConnectionPool(1, 10, DATABASE_URL, sslmode='require')

        conn = pg_pool.getconn()
        conn.autocommit = False
        return conn
    else:
        # SQLite cho local
        import sqlite3
        conn = sqlite3.connect(os.path.join(BASE_DIR, 'link_manager.db'))
        conn.row_factory = sqlite3.Row
        return conn


def close_db_connection(conn):
    """Close database connection"""
    if IS_RENDER and pg_pool:
        pg_pool.putconn(conn)
    else:
        conn.close()


def init_db():
    """Initialize database tables"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if IS_RENDER:
            # PostgreSQL
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS collections
                           (
                               id
                               SERIAL
                               PRIMARY
                               KEY,
                               name
                               VARCHAR
                           (
                               255
                           ) UNIQUE NOT NULL,
                               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                               updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                               )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS links
                           (
                               id
                               SERIAL
                               PRIMARY
                               KEY,
                               collection_id
                               INTEGER
                               NOT
                               NULL
                               REFERENCES
                               collections
                           (
                               id
                           ) ON DELETE CASCADE,
                               url TEXT NOT NULL,
                               status VARCHAR
                           (
                               50
                           ) DEFAULT 'unknown',
                               last_checked TIMESTAMP,
                               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                               updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                               UNIQUE
                           (
                               collection_id,
                               url
                           )
                               )
                           ''')

            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_links_collection ON links(collection_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_links_url ON links(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_collections_name ON collections(name)')

        else:
            # SQLite
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS collections
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               name
                               TEXT
                               UNIQUE
                               NOT
                               NULL,
                               created_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               updated_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS links
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               collection_id
                               INTEGER
                               NOT
                               NULL,
                               url
                               TEXT
                               NOT
                               NULL,
                               status
                               TEXT
                               DEFAULT
                               'unknown',
                               last_checked
                               TIMESTAMP,
                               created_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               updated_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               FOREIGN
                               KEY
                           (
                               collection_id
                           ) REFERENCES collections
                           (
                               id
                           )
                               ON DELETE CASCADE,
                               UNIQUE
                           (
                               collection_id,
                               url
                           )
                               )
                           ''')

            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_links_collection ON links(collection_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_links_url ON links(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_collections_name ON collections(name)')

        conn.commit()
        print("✅ Database initialized successfully")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error initializing database: {e}")
        raise
    finally:
        close_db_connection(conn)


# =====================================================
# UTILITY FUNCTIONS
# =====================================================
def normalize_url(url: str) -> str:
    """Chuẩn hóa URL"""
    if not url:
        return ""

    url = url.strip()

    # Bỏ các protocol không phải http/https
    if url.startswith(("javascript:", "file:", "about:", "mailto:", "tel:")):
        return ""

    # Thêm https:// nếu thiếu
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    return url.rstrip('/')


def domain_resolves(url: str) -> bool:
    """Kiểm tra domain có resolve được không"""
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        socket.gethostbyname(host)
        return True
    except:
        return False


# =====================================================
# DATABASE OPERATIONS
# =====================================================
def get_or_create_collection(name):
    """Lấy hoặc tạo collection mới"""
    conn = get_db_connection()

    try:
        # Tìm collection theo tên
        if IS_RENDER:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM collections WHERE name = %s', (name,))
            collection = cursor.fetchone()
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM collections WHERE name = ?', (name,))
            row = cursor.fetchone()
            collection = dict(row) if row else None

        if collection:
            return collection

        # Tạo collection mới
        now = datetime.now().isoformat()
        if IS_RENDER:
            cursor.execute(
                'INSERT INTO collections (name, created_at, updated_at) VALUES (%s, %s, %s) RETURNING *',
                (name, now, now)
            )
            collection = cursor.fetchone()
        else:
            cursor.execute(
                'INSERT INTO collections (name, created_at, updated_at) VALUES (?, ?, ?)',
                (name, now, now)
            )
            collection_id = cursor.lastrowid
            cursor.execute('SELECT * FROM collections WHERE id = ?', (collection_id,))
            collection = dict(cursor.fetchone())

        conn.commit()
        return collection

    except Exception as e:
        conn.rollback()
        raise
    finally:
        close_db_connection(conn)


def get_collection_by_name(name):
    """Lấy collection theo tên"""
    conn = get_db_connection()

    try:
        if IS_RENDER:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM collections WHERE name = %s', (name,))
            result = cursor.fetchone()
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM collections WHERE name = ?', (name,))
            row = cursor.fetchone()
            result = dict(row) if row else None

        return result
    finally:
        close_db_connection(conn)


def get_all_collections():
    """Lấy tất cả collections"""
    conn = get_db_connection()

    try:
        if IS_RENDER:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM collections ORDER BY name')
            return cursor.fetchall()
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM collections ORDER BY name')
            return [dict(row) for row in cursor.fetchall()]
    finally:
        close_db_connection(conn)


def update_collection_name(old_name, new_name):
    """Đổi tên collection"""
    conn = get_db_connection()

    try:
        # Kiểm tra tên mới đã tồn tại chưa
        if IS_RENDER:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM collections WHERE name = %s', (new_name,))
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM collections WHERE name = ?', (new_name,))

        if cursor.fetchone():
            raise ValueError(f"Collection '{new_name}' đã tồn tại")

        # Cập nhật tên
        now = datetime.now().isoformat()
        if IS_RENDER:
            cursor.execute(
                'UPDATE collections SET name = %s, updated_at = %s WHERE name = %s',
                (new_name, now, old_name)
            )
        else:
            cursor.execute(
                'UPDATE collections SET name = ?, updated_at = ? WHERE name = ?',
                (new_name, now, old_name)
            )

        if cursor.rowcount == 0:
            raise ValueError(f"Collection '{old_name}' không tồn tại")

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        raise
    finally:
        close_db_connection(conn)


def delete_collection(name):
    """Xóa collection và tất cả links của nó"""
    conn = get_db_connection()

    try:
        # Tìm collection
        if IS_RENDER:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM collections WHERE name = %s', (name,))
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM collections WHERE name = ?', (name,))

        collection = cursor.fetchone()

        if not collection:
            raise ValueError(f"Collection '{name}' không tồn tại")

        # Xóa collection (links sẽ tự động xóa do CASCADE)
        if IS_RENDER:
            cursor.execute('DELETE FROM collections WHERE name = %s', (name,))
        else:
            cursor.execute('DELETE FROM collections WHERE name = ?', (name,))

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        raise
    finally:
        close_db_connection(conn)


def save_links_to_collection(collection_name, urls_data):
    """Lưu/làm mới links trong collection"""
    conn = get_db_connection()

    try:
        # Lấy hoặc tạo collection
        collection = get_or_create_collection(collection_name)
        collection_id = collection['id']

        # Lấy links hiện tại
        if IS_RENDER:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM links WHERE collection_id = %s', (collection_id,))
            existing_links = {row['url']: dict(row) for row in cursor.fetchall()}
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM links WHERE collection_id = ?', (collection_id,))
            existing_links = {row['url']: dict(row) for row in cursor.fetchall()}

        now = datetime.now().isoformat()

        # Xử lý từng URL
        for url_item in urls_data:
            url = normalize_url(url_item.get('url', ''))
            if not url:
                continue

            status = url_item.get('status', 'unknown')

            if url in existing_links:
                # Cập nhật link đã tồn tại
                if IS_RENDER:
                    cursor.execute(
                        '''UPDATE links
                           SET status       = %s,
                               updated_at   = %s,
                               last_checked = %s
                           WHERE collection_id = %s
                             AND url = %s''',
                        (status, now, now, collection_id, url)
                    )
                else:
                    cursor.execute(
                        '''UPDATE links
                           SET status       = ?,
                               updated_at   = ?,
                               last_checked = ?
                           WHERE collection_id = ?
                             AND url = ?''',
                        (status, now, now, collection_id, url)
                    )
            else:
                # Thêm link mới
                try:
                    if IS_RENDER:
                        cursor.execute(
                            '''INSERT INTO links
                                   (collection_id, url, status, created_at, updated_at, last_checked)
                               VALUES (%s, %s, %s, %s, %s, %s)''',
                            (collection_id, url, status, now, now, now)
                        )
                    else:
                        cursor.execute(
                            '''INSERT INTO links
                                   (collection_id, url, status, created_at, updated_at, last_checked)
                               VALUES (?, ?, ?, ?, ?, ?)''',
                            (collection_id, url, status, now, now, now)
                        )
                except Exception:
                    # URL đã tồn tại, bỏ qua
                    pass

        # Xác định URLs cần xóa
        new_urls = {normalize_url(u.get('url', '')) for u in urls_data if normalize_url(u.get('url', ''))}
        urls_to_delete = [url for url in existing_links if url not in new_urls]

        if urls_to_delete:
            if IS_RENDER:
                placeholders = ','.join(['%s'] * len(urls_to_delete))
                cursor.execute(
                    f'DELETE FROM links WHERE collection_id = %s AND url IN ({placeholders})',
                    (collection_id, *urls_to_delete)
                )
            else:
                placeholders = ','.join(['?'] * len(urls_to_delete))
                cursor.execute(
                    f'DELETE FROM links WHERE collection_id = ? AND url IN ({placeholders})',
                    (collection_id, *urls_to_delete)
                )

        conn.commit()

        # Lấy danh sách links hiện tại
        if IS_RENDER:
            cursor.execute(
                'SELECT id, url, status FROM links WHERE collection_id = %s ORDER BY created_at',
                (collection_id,)
            )
            links = [{'id': row['id'], 'url': row['url'], 'status': row['status']}
                     for row in cursor.fetchall()]
        else:
            cursor.execute(
                'SELECT id, url, status FROM links WHERE collection_id = ? ORDER BY created_at',
                (collection_id,)
            )
            links = [{'id': row[0], 'url': row[1], 'status': row[2]}
                     for row in cursor.fetchall()]

        return {
            'collection': collection_name,
            'urls': links
        }

    except Exception as e:
        conn.rollback()
        raise
    finally:
        close_db_connection(conn)


def get_links_in_collection(collection_name):
    """Lấy tất cả links trong collection"""
    conn = get_db_connection()

    try:
        # Tìm collection
        if IS_RENDER:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM collections WHERE name = %s', (collection_name,))
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM collections WHERE name = ?', (collection_name,))

        collection = cursor.fetchone()

        if not collection:
            return []

        # Lấy links
        collection_id = collection[0]

        if IS_RENDER:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT id, url, status FROM links WHERE collection_id = %s ORDER BY created_at',
                (collection_id,)
            )
            return [{'id': row[0], 'url': row[1], 'status': row[2]}
                    for row in cursor.fetchall()]
        else:
            cursor.execute(
                'SELECT id, url, status FROM links WHERE collection_id = ? ORDER BY created_at',
                (collection_id,)
            )
            return [{'id': row[0], 'url': row[1], 'status': row[2]}
                    for row in cursor.fetchall()]

    finally:
        close_db_connection(conn)


def update_link_status(collection_name, link_id, status):
    """Cập nhật trạng thái link"""
    conn = get_db_connection()

    try:
        now = datetime.now().isoformat()

        if IS_RENDER:
            cursor = conn.cursor()
            cursor.execute('''
                           UPDATE links
                           SET status       = %s,
                               last_checked = %s,
                               updated_at   = %s
                           WHERE id = %s
                             AND collection_id = (SELECT id
                                                  FROM collections
                                                  WHERE name = %s)
                           ''', (status, now, now, link_id, collection_name))
        else:
            cursor = conn.cursor()
            cursor.execute('''
                           UPDATE links
                           SET status       = ?,
                               last_checked = ?,
                               updated_at   = ?
                           WHERE id = ?
                             AND collection_id = (SELECT id
                                                  FROM collections
                                                  WHERE name = ?)
                           ''', (status, now, now, link_id, collection_name))

        conn.commit()
        return cursor.rowcount > 0

    except Exception as e:
        conn.rollback()
        raise
    finally:
        close_db_connection(conn)


# =====================================================
# URL CHECKING FUNCTIONS
# =====================================================
_session = None
_session_lock = threading.Lock()


def get_session():
    """Tạo session tái sử dụng"""
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=50,
                    pool_maxsize=50,
                    max_retries=1,
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
    """Kiểm tra URL"""
    if not url or url.startswith(("javascript:", "file:", "about:", "mailto:", "tel:")):
        return "dead"

    if not domain_resolves(url):
        return "dead"

    session = get_session()

    try:
        response = session.get(
            url,
            timeout=(1.0, 1.5),
            allow_redirects=True,
            stream=True
        )
        response.close()

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
    try:
        db_type = "PostgreSQL" if IS_RENDER else "SQLite"
        return jsonify({
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "database": db_type,
            "environment": "Render" if IS_RENDER else "Local",
            "message": "API đang hoạt động"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }), 500


@app.post("/api/update-collection")
def update_collection_api():
    """Cập nhật collection"""
    try:
        data = request.json or {}
        collection_name = data.get("collection", "").strip()
        urls = data.get("urls", [])

        if not collection_name:
            return jsonify({"error": "Tên collection không được để trống"}), 400

        result = save_links_to_collection(collection_name, urls)
        return jsonify(result)

    except Exception as e:
        app.logger.error(f"Lỗi khi cập nhật collection: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.post("/api/rename-collection")
def rename_collection_api():
    """Đổi tên collection"""
    try:
        data = request.json or {}
        old_name = data.get("old_name", "").strip()
        new_name = data.get("new_name", "").strip()

        if not old_name or not new_name:
            return jsonify({"error": "Tên collection cũ và mới không được để trống"}), 400

        update_collection_name(old_name, new_name)

        return jsonify({
            "success": True,
            "old_name": old_name,
            "new_name": new_name
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 409
    except Exception as e:
        app.logger.error(f"Lỗi khi đổi tên collection: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.post("/api/check-links")
def api_check_links():
    """Kiểm tra link và lưu kết quả"""
    try:
        data = request.json or {}
        collection_name = data.get("collection", "")
        links = data.get("links", [])

        if not collection_name or not links:
            return jsonify([]), 400

        max_workers = min(20, len(links))
        urls_to_check = []
        link_data = []

        for link in links:
            raw_url = link.get('url', '').strip()
            url = normalize_url(raw_url)
            if url:
                urls_to_check.append(url)
                link_data.append({
                    'id': link.get('id'),
                    'url': url,
                    'original': link
                })

        # Batch check
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            statuses = list(executor.map(check_one_url, urls_to_check))

        results = []
        for data_item, status in zip(link_data, statuses):
            if data_item['id']:
                update_link_status(collection_name, data_item['id'], status)

            results.append({
                "url": data_item['url'],
                "status": status,
                "id": data_item['id']
            })

        return jsonify(results)

    except Exception as e:
        app.logger.error(f"Lỗi khi kiểm tra links: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.delete("/api/delete-collection/<string:collection_name>")
def delete_collection_api(collection_name):
    """Xóa toàn bộ collection"""
    try:
        delete_collection(collection_name)
        return jsonify({"success": True, "deleted_collection": collection_name})

    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        app.logger.error(f"Lỗi khi xóa collection {collection_name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.get("/api/data")
def load_data_api():
    """API endpoint tải dữ liệu từ database"""
    try:
        collections = get_all_collections()
        result = []

        for collection in collections:
            collection_name = collection['name']
            links = get_links_in_collection(collection_name)

            result.append({
                "collection": collection_name,
                "urls": links if links else []
            })

        return jsonify(result)

    except Exception as e:
        app.logger.error(f"Lỗi khi tải dữ liệu: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.post("/api/add-collection")
def add_collection_api():
    """Thêm collection mới"""
    try:
        data = request.json or {}
        collection_name = data.get("collection", "").strip()

        if not collection_name:
            collection_name = f"Collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Tạo collection (có thể trống)
        collection = get_or_create_collection(collection_name)

        # Xử lý URLs nếu có
        urls = data.get("urls", [])
        if urls:
            save_links_to_collection(collection_name, urls)
            links = get_links_in_collection(collection_name)
        else:
            links = []

        return jsonify({
            "success": True,
            "collection": collection_name,
            "urls": links
        })

    except Exception as e:
        app.logger.error(f"Lỗi khi thêm collection: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# =====================================================
# INITIALIZE DATA
# =====================================================
def initialize_sample_data():
    """Tạo dữ liệu mẫu nếu database trống"""
    try:
        collections = get_all_collections()

        if not collections:
            print("📝 Tạo dữ liệu mẫu...")

            sample_collections = [
                {
                    "name": "Các trang web hữu ích",
                    "links": [
                        {"url": "https://google.com", "status": "alive"},
                        {"url": "https://github.com", "status": "alive"}
                    ]
                },
                {
                    "name": "Học lập trình",
                    "links": [
                        {"url": "https://stackoverflow.com", "status": "alive"},
                        {"url": "https://w3schools.com", "status": "alive"}
                    ]
                }
            ]

            for collection_data in sample_collections:
                collection_name = collection_data["name"]
                save_links_to_collection(collection_name, collection_data["links"])

            print("✅ Đã tạo dữ liệu mẫu")
        else:
            print(f"✅ Đã có {len(collections)} collections trong database")
    except Exception as e:
        print(f"⚠️ Không thể tạo dữ liệu mẫu: {e}")


# =====================================================
# FRONTEND
# =====================================================
@app.get("/")
def index():
    return render_template("indexN.html")


# =====================================================
# CLEANUP FUNCTIONS
# =====================================================
_window = None


def cleanup_webview():
    """Dọn dẹp webview resources khi thoát"""
    global _window
    if _window:
        try:
            if hasattr(_window, 'destroy'):
                _window.destroy()
        except:
            pass

    try:
        temp_dir = os.path.join(tempfile.gettempdir(), 'pywebview')
        if os.path.exists(temp_dir):
            for root, dirs, files in os.walk(temp_dir, topdown=False):
                for name in files:
                    try:
                        os.remove(os.path.join(root, name))
                    except:
                        pass
    except:
        pass


atexit.register(cleanup_webview)


# =====================================================
# RUN APP
# =====================================================
def start_flask():
    """Khởi động Flask server"""
    # Khởi tạo database
    init_db()
    get_session()
    initialize_sample_data()

    if IS_RENDER:
        # Chạy trên Render
        port = int(os.environ.get('PORT', 10000))
        print("=" * 50)
        print("🚀 QUẢN LÝ LINK - PostgreSQL on Render")
        print("=" * 50)
        print(f"📊 Database: PostgreSQL")
        print(f"🌍 Environment: Render Production")
        print(f"🔗 Public URL sẽ được cung cấp sau khi deploy")
        print("=" * 50)

        app.run(host='0.0.0.0', port=port, debug=False)
    else:
        # Chạy local với webview - SỬA LẠI PHẦN NÀY
        print("=" * 50)
        print("🚀 QUẢN LÝ LINK - Local Development")
        print("=" * 50)
        print(f"📊 Database: SQLite")
        print(f"🏠 Environment: Local Development")
        print(f"🌐 Local URL: http://localhost:5000")  # Đổi thành localhost
        print("=" * 50)

        flask_thread = threading.Thread(target=lambda: app.run(
            host="localhost",  # ĐỔI TỪ 127.0.0.1 thành localhost
            port=5000,
            debug=False,
            use_reloader=False,
            threaded=True
        ))
        flask_thread.daemon = True
        flask_thread.start()

        time.sleep(3)

        screen_width, screen_height = 1400, 800
        try:
            import tkinter as tk
            root = tk.Tk()
            screen_width = root.winfo_screenwidth()
            screen_height = root.winfo_screenheight()
            root.destroy()
        except:
            pass

        window_width = min(1400, screen_width - 100)
        window_height = min(800, screen_height - 100)
        window_x = (screen_width - window_width) // 2
        window_y = (screen_height - window_height) // 2

        try:
            window = webview.create_window(
                title="Quản Lý Link - Local Database",
                url="http://localhost:5000",  # ĐỔI TỪ 127.0.0.1 thành localhost
                width=window_width,
                height=window_height,
                x=window_x,
                y=window_y,
                resizable=True,
                min_size=(800, 500),
                background_color='#f5f7fa',
            )

            _window = window
            webview.start()
        except Exception as e:
            print(f"⚠️ Lỗi khi khởi động window: {e}")
            print("🌐 Bạn có thể truy cập ứng dụng tại: http://localhost:5000")  # Đổi thành localhost

        cleanup_webview()


def start_render():
    """Start for Render deployment"""
    init_db()
    get_session()
    initialize_sample_data()

    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == "__main__":
    if IS_RENDER:
        start_render()
    else:
        start_flask()