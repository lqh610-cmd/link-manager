# 📊 Quản Lý Link - Link Manager

Ứng dụng quản lý và kiểm tra trạng thái link với giao diện desktop và web.

## 🌟 Tính năng
- Quản lý collection và URL
- Kiểm tra trạng thái link tự động
- Giao diện desktop với pywebview (local)
- Triển khai web trên Render.com
- Hỗ trợ PostgreSQL trên Render, SQLite local

## 🚀 Triển khai

### Local Development
```bash
# 1. Clone repository
git clone <repository-url>
cd link-manager

# 2. Tạo virtual environment
python -m venv venv

# 3. Kích hoạt virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 4. Cài đặt dependencies
pip install -r requirements.txt

# 5. Chạy ứng dụng
python app.py