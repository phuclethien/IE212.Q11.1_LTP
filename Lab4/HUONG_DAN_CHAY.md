# Hướng dẫn chạy

## Bước 1: Cài đặt thư viện

```bash
pip install -r requirements.txt
```

## Bước 2: Chạy Processing Server (Terminal 1)

```bash
python processing_server.py
```

## Bước 3: Chạy Camera Server (Terminal 2)

Mở terminal mới và chạy:

```bash
python camera_server.py
```

Cửa sổ camera sẽ hiện ra và bắt đầu streaming.

## Bước 4: Dừng chương trình

- Nhấn phím **Q** trong cửa sổ camera, hoặc
- Nhấn **Ctrl+C** trong terminal

## Xem kết quả

Các frame đã xóa nền được lưu trong thư mục: **`output_frames/`**

---
