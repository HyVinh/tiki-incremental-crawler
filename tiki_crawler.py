import csv
import io
import json
import os
import time
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- KHÔNG GIAN LƯU TRỮ VÀ ĐỊNH DANH THƯ MỤC DRIVE ---
STATE_FOLDER_ID = "1JFd0x0iWoahvx3OEttRCiirrw6r1VFSL"       # Thư mục gốc chứa crawl_state
PRODUCTS_FOLDER_ID = "1uoe_Z07g3Gx4JQJTBc48ft5_dvBSxNRQ"    # Thư mục Products
REVIEWS_FOLDER_ID = "1cceBNVwtpsnB2NwO1jlvSyKSA8IRI2F1"     # Thư mục Reviews

STATE_FILE = "crawl_state.json"
BATCH_SIZE = 3
REVIEWS_PER_PRODUCT_PAGE = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

def get_drive_service():
    """Đọc cấu hình mã hóa token.json truyền từ môi trường bí mật của GitHub Secrets"""
    token_env = os.environ.get("GOOGLE_DRIVE_TOKEN")
    if not token_env:
        raise ValueError(" Không tìm thấy biến môi trường GOOGLE_DRIVE_TOKEN!")
    
    token_info = json.loads(token_env)
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=creds)

def find_file_on_drive(service, filename, folder_id):
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def download_file_from_drive(service, filename, local_path, folder_id):
    file_id = find_file_on_drive(service, filename, folder_id)
    if not file_id: return False
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(local_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return True

def upload_or_update_to_drive(service, filename, local_path, folder_id, mime_type="text/csv"):
    file_id = find_file_on_drive(service, filename, folder_id)
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"   [Sync] Đã đồng bộ lên Drive: '{filename}'", flush=True)
    else:
        file_metadata = {'name': filename, 'parents': [folder_id]}
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"   [Sync] Đã tạo mới trên Drive: '{filename}'", flush=True)

def initialize_state(service, initial_categories):
    if download_file_from_drive(service, STATE_FILE, STATE_FILE, STATE_FOLDER_ID):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
        if "current_batch" not in state:
            state["current_batch"] = 1
        total = len(state['pending']) + len(state['completed'])
        print(f"[State] Đã tải trạng thái cũ. Đã xong {len(state['completed'])}/{total} danh mục.", flush=True)
    else:
        state = {"pending": [str(cid) for cid in initial_categories], "completed": [], "current_batch": 1}
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=4)
        upload_or_update_to_drive(service, STATE_FILE, STATE_FILE, STATE_FOLDER_ID, "application/json")
        print(f"[State] Khởi tạo hàng đợi mới gồm {len(initial_categories)} danh mục.", flush=True)
    return state

def run_incremental_crawl(initial_categories_list):
    service = get_drive_service()
    state = initialize_state(service, initial_categories_list)
    
    if not state["pending"]:
        print("[Hoàn thành] Không còn danh mục nào trong hàng đợi!", flush=True)
        return

    current_batch_idx = state.get("current_batch", 1)
    categories_to_run = state["pending"][:BATCH_SIZE]
    print(f"\n Khởi động Batch {current_batch_idx}. Xử lý: {categories_to_run}", flush=True)

    PRODUCT_FILE = f"tiki_products_batch_{current_batch_idx:02d}.csv"
    REVIEW_FILE = f"tiki_reviews_batch_{current_batch_idx:02d}.csv"

    f_prod = open(PRODUCT_FILE, mode="w", encoding="utf-8-sig", newline="")
    prod_writer = csv.DictWriter(f_prod, fieldnames=["ID Danh Mục", "ID Sản Phẩm", "Tên Sản Phẩm", "URL Sản Phẩm", "Thương Hiệu / Tác Giả", "Giá", "Đánh Giá TB"])
    prod_writer.writeheader()

    f_rev = open(REVIEW_FILE, mode="w", encoding="utf-8-sig", newline="")
    rev_writer = csv.DictWriter(f_rev, fieldnames=["ID Sản Phẩm", "Tên Sản Phẩm", "ID Đánh Giá", "User ID", "Người mua", "Vùng miền", "Số sao", "Nội dung bình luận", "Ngày đánh giá"])
    rev_writer.writeheader()

    for cat_id in categories_to_run:
        print(f"\n Danh mục ID: {cat_id}...", flush=True)
        page, product_count, max_products_per_cat = 1, 0, 500

        while product_count < max_products_per_cat:
            prod_url = f"https://tiki.vn/api/v2/products?category={cat_id}&page={page}&limit=40"
            try:
                res = requests.get(prod_url, headers=HEADERS, timeout=10)
                if res.status_code != 200: break
                products = res.json().get("data", [])
                if not products: break

                for p in products:
                    pid = str(p.get("id"))
                    p_name = p.get("name")
                    prod_writer.writerow({
                        "ID Danh Mục": cat_id, "ID Sản Phẩm": pid, "Tên Sản Phẩm": p_name,
                        "URL Sản Phẩm": f"https://tiki.vn/{p.get('url_path')}",
                        "Thương Hiệu / Tác Giả": p.get("brand_name", "Không rõ"), "Giá": p.get("price"),
                        "Đánh Giá TB": p.get("rating_average")
                    })
                    product_count += 1

                    seen_buyers = set()
                    for r_page in range(1, REVIEWS_PER_PRODUCT_PAGE + 1):
                        rev_url = f"https://tiki.vn/api/v2/reviews?limit=20&page={r_page}&product_id={pid}"
                        try:
                            r_res = requests.get(rev_url, headers=HEADERS, timeout=10)
                            if r_res.status_code == 200:
                                reviews = r_res.json().get("data", [])
                                if not reviews: break
                                for r in reviews:
                                    buyer = r.get("created_by", {}).get("full_name", "").strip()
                                    if not buyer or buyer in seen_buyers: continue
                                    seen_buyers.add(buyer)
                                    user_obj = r.get("created_by", {})
                                    rev_writer.writerow({
                                        "ID Sản Phẩm": pid, 
                                        "Tên Sản Phẩm": p_name, 
                                        "ID Đánh Giá": r.get("id"),
                                        "User ID": user_obj.get("id", "Không rõ"),            # LẤY THÊM ID USER
                                        "Người mua": buyer, 
                                        "Vùng miền": user_obj.get("region", "Không rõ"),     # LẤY THÊM VÙNG MIỀN
                                        "Số sao": r.get("rating"), 
                                        "Nội dung bình luận": r.get("content"),
                                        "Ngày đánh giá": r.get("timeline", {}).get("review_created_date")
                                    })
                        except: break
                        time.sleep(0.4)
                print(f"   -> Hoàn tất trang {page}. Lũy kế: {product_count} SP.", flush=True)
                page += 1
                time.sleep(0.8)
            except Exception as e:
                print(f" Lỗi kết nối danh mục {cat_id} trang {page}: {e}", flush=True)
                break

        state["pending"].remove(cat_id)
        state["completed"].append(cat_id)

    f_prod.close()
    f_rev.close()
    state["current_batch"] = current_batch_idx + 1

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=4)

    print(f"\n Đang đẩy tệp phân mảnh ca số {current_batch_idx} lên Google Drive...", flush=True)

    service = get_drive_service()
    
    upload_or_update_to_drive(service, STATE_FILE, STATE_FILE, STATE_FOLDER_ID, "application/json")
    upload_or_update_to_drive(service, PRODUCT_FILE, PRODUCT_FILE, PRODUCTS_FOLDER_ID)
    upload_or_update_to_drive(service, REVIEW_FILE, REVIEW_FILE, REVIEWS_FOLDER_ID)
    print("🏁 [Thành công] Toàn bộ tiến trình lịch sử đã được bảo lưu!", flush=True)

if __name__ == "__main__":
    DANH_SACH_DANH_MUC_GOCO = [
        '1882', '8594', '2549', '1520', '1883', '8322', '1975', '1789', '1815', '9',
        '4384', '17166', '1846', '1686', '4221', '1703', '1801', '27498', '44792', '8371',
        '6000', '11312', '27616', '15078', '1795', '11603', '5250', '11601', '2640', '8339',
        '2551', '10418', '10416', '6568', '10570', '44824', '54276', '54290', '54302', '54316',
        '54330', '54344', '54362', '54384', '54398', '54412', '54430', '54438', '54452', '54466',
        '54474', '54500', '8061', '1796', '28856', '1794', '1582', '2306', '2322', '1584',
        '1592', '2307', '5873', '1591', '1594', '1595', '8142', '8161', '5874', '1884',
        '1946', '1698', '27600', '1702', '1508', '5404', '6179', '49372', '49384', '27562',
        '27548', '10382', '27570', '49516', '4546', '16004', '49532', '49524', '8355', '27604',
        '4550', '1192', '4551', '8357', '1008', '5340', '1581', '27572', '5342', '5341',
        '8338', '8337', '4558', '4561', '4560', '4559', '8352', '5337', '49650', '27608',
        '8387', '27612', '6526', '8370', '27550', '8374', '1778', '27542', '11375', '8129',
        '12884', '2663', '8060', '8093', '8095', '1973', '1951', '1974', '2223', '2150',
        '2015', '1966', '1954', '8313', '23054', '10068', '18852', '5451', '22998', '15074',
        '4422', '53620', '4421', '53562', '24024', '53582', '11347', '21268', '21442', '21346',
        '21382', '21356', '20908', '21298', '21166', '20824', '20766', '21134', '21074', '25036',
        '21496', '21054', '8214', '28670', '8215', '28432', '8039', '2667', '11332', '11319',
        '11322', '11334', '11313', '11327', '11326', '13744', '24832', '6070', '8431', '8597',
        '8595', '8435', '17208', '316', '7741', '18328', '320', '26568', '8074', '2328',
        '3865', '3862', '3868', '5015', '3866', '3864', '3863', '3869', '6141', '6140',
        '8428', '6826', '24306', '24128', '24002', '24294', '8413', '4227', '8411', '23120',
        '6827', '10803', '24258', '1818', '4077', '28814', '28834', '1840', '28794', '2757',
        '28822', '8047', '28806', '24088', '1996'
    ]
    run_incremental_crawl(DANH_SACH_DANH_MUC_GOCO)
