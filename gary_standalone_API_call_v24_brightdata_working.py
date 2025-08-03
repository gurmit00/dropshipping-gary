import os
import requests
import csv
import time
import re
import json
import uuid
import logging
import threading
import queue
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, unquote
import mysql.connector
from mysql.connector import errorcode, errors as mysql_errors
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIG ===
DB_CONFIG = {
    "host": "50.87.172.208",
    "port": 3306,
    "user": "obaqucmy_upwork",
    "password": "Letmein123!",
    "database": "obaqucmy_fashion",
}

INPUT_FILE = "input_urls.csv"
PAGES_TO_FETCH = 5
DETAIL_WORKERS = 4
DETAIL_BATCH_COMMIT_SIZE = 100
DETAIL_QUEUE_MAXSIZE = 1000
SENTINEL = object()

# === BRIGHT DATA CONFIG (INDIAZONE) ===
BRIGHTDATA_USERNAME = "hl_03f9c8e5"
BRIGHTDATA_ZONE = "indiazone"
BRIGHTDATA_PASSWORD = "s335a3ff8pmk"
BRIGHTDATA_HOST = "brd.superproxy.io"
BRIGHTDATA_PORT = 33335

def get_brightdata_proxies():
    proxy_user = f"brd-customer-{BRIGHTDATA_USERNAME}-zone-{BRIGHTDATA_ZONE}"
    proxy_pass = BRIGHTDATA_PASSWORD
    proxy_url = f"http://{proxy_user}:{proxy_pass}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"
    return {"http": proxy_url, "https": proxy_url}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

def make_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=["GET", "POST", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

HTTP = make_session()

def default_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nykaafashion.com/",
        "Origin": "https://www.nykaafashion.com",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Connection": "keep-alive",
    }

def get_mysql_connection(retries=3, backoff=2):
    for attempt in range(1, retries + 1):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            logging.info("✅ Connected to MySQL on attempt %s", attempt)
            return conn
        except mysql_errors.Error as e:
            logging.warning("MySQL connection attempt %s failed: %s", attempt, e)
            if attempt < retries:
                time.sleep(backoff ** attempt)
    logging.critical("Failed to connect to MySQL after %s attempts", retries)
    return None

def normalize_mysql_value(v):
    if v is None:
        return None
    if isinstance(v, list):
        if all(isinstance(i, (str, int, float, bool, type(None))) for i in v):
            return ",".join("" if i is None else str(i) for i in v)
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    return v

def adaptive_get(url, headers=None, timeout=30):
    proxies = get_brightdata_proxies()
    for attempt in range(1, 4):
        try:
            resp = HTTP.get(url, headers=headers or default_headers(), proxies=proxies, timeout=timeout, verify=False)
            if resp.status_code == 200:
                return resp
            else:
                logging.warning("Bright Data proxy status %s for %s; snippet: %.200s", resp.status_code, url, resp.text)
        except Exception:
            logging.exception("Exception during Bright Data proxy fetch for %s (attempt %s)", url, attempt)
        time.sleep(0.7 * attempt)
    return None

def parse_listing_url(listing_url):
    parsed = urlparse(listing_url.strip())
    m = re.search(r'/c/(\d+)', parsed.path)
    category = m.group(1) if m else None

    brand = None
    sort = None
    price_range = None
    gender = None
    gender_code = None

    qs = parse_qs(parsed.query)
    f_raw_list = qs.get("f", [])
    if f_raw_list:
        f_decoded = unquote(f_raw_list[0])
        parts = f_decoded.split(";")
        for part in parts:
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            if k == "brand_filter":
                brand = v.rstrip("_")
            elif k == "sort":
                sort = v
            elif k == "price_filter":
                price_range = v
            elif k == "gender_filter":
                gender_code = v.rstrip("_")
                gender_map = {
                    "5199": "boys",
                    "5198": "girls",
                }
                gender = gender_map.get(gender_code)
    return category, brand, sort, price_range, gender, gender_code

def scrape_products(category, brand, sort=None, price_range=None, pages=PAGES_TO_FETCH):
    all_products = []
    for page in range(1, pages + 1):
        logging.info("[Step] Fetching products list: Category=%s, Brand=%s, Sort=%s, Price=%s, Page=%s",
                     category, brand, sort, price_range, page)
        base_url = (
            f"https://www.nykaafashion.com/rest/appapi/V2/categories/products"
            f"?categoryId={category}&PageSize=36"
        )
        params = []
        params.append(f"sort={sort or 'bestseller'}")
        if brand:
            params.append(f"brand_filter={brand}")
        if price_range:
            params.append(f"price_filter={price_range}")
        params.append(f"currentPage={page}")
        params.append("filter_format=v2")
        params.append("currency=INR")
        params.append("country_code=IN")
        params.append("apiVersion=5")
        params.append("deviceType=MSITE")
        params.append("device_os=mweb_mac")
        url = base_url + "&" + "&".join(params)

        for attempt in range(1, 4):
            try:
                response = adaptive_get(url, headers=default_headers(), timeout=30)
                if not response:
                    logging.warning("No response for listing URL %s attempt %s", url, attempt)
                    time.sleep(0.5 * attempt)
                    continue
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except ValueError as ve:
                        logging.error("JSON parse failed for listing URL: %s; error: %s", url, ve)
                        break
                    products = data.get("response", {}).get("products", [])
                    logging.info("Found %s products for Category=%s Brand=%s Page=%s", len(products), category, brand, page)
                    if not products:
                        # Stop further paging if zero found
                        return all_products
                    all_products.extend(products)
                    break
                else:
                    logging.warning("Unexpected status %s for %s (attempt %s)", response.status_code, url, attempt)
                    time.sleep(0.5 * attempt)
            except Exception:
                logging.exception("Exception fetching listing Category=%s Brand=%s Page=%s attempt %s", category, brand, page, attempt)
                time.sleep(0.5 * attempt)
        time.sleep(0.2)
    return all_products

def fetch_product_detail(product_id, max_attempts=3):
    url = (
        f"https://www.nykaafashion.com/rest/appapi/V3/products/id/{product_id}"
        f"?currency=INR&country_code=IN&size_data=true&platform=MSITE"
    )
    for attempt in range(1, max_attempts + 1):
        try:
            response = adaptive_get(url, headers=default_headers(), timeout=30)
            if not response:
                logging.warning("No response for detail fetch %s attempt %s", product_id, attempt)
                time.sleep(0.4 * attempt)
                continue
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    logging.error("JSON decode failed for product %s on attempt %s", product_id, attempt)
                    return None
            else:
                logging.warning("Unexpected status %s fetching detail %s (attempt %s)", response.status_code, product_id, attempt)
                time.sleep(0.4 * attempt)
        except Exception:
            logging.exception("Exception fetching product detail %s attempt %s", product_id, attempt)
            time.sleep(0.4 * attempt)
    logging.error("Giving up on product detail %s after %s attempts", product_id, max_attempts)
    return None

def extract_sizes(product):
    sizes = []
    seen = set()
    for opt in product.get("sizeOptions", {}).get("options", []):
        size_label = opt.get("sizeName") or opt.get("name")
        if not size_label:
            continue
        size_label = size_label.strip()
        in_stock = None
        if "isOutOfStock" in opt:
            in_stock = (opt.get("isOutOfStock") == 0)
        elif opt.get("in_stock") is not None:
            in_stock = bool(opt.get("in_stock"))
        repr_size = f"{size_label}({'in' if in_stock else 'oos'})" if in_stock is not None else size_label
        if repr_size not in seen:
            seen.add(repr_size)
            sizes.append(repr_size)
    parent_size = product.get("sizeName")
    if parent_size:
        parent_size = parent_size.strip()
        in_stock_parent = not bool(product.get("isOutOfStock"))
        repr_parent = f"{parent_size}({'in' if in_stock_parent else 'oos'})"
        if repr_parent not in seen:
            sizes.append(repr_parent)
    return ", ".join(sorted(sizes, key=lambda x: x.lower()))

def extract_images(product, max_images=5):
    images = []
    for media in product.get("productMedia", [])[:max_images]:
        url = media.get("url")
        if url:
            images.append(url)
    return ", ".join(images)

def extract_category_url(product):
    bc = product.get("breadcrumbs", [])
    filtered = [b for b in bc if b.get("value") and b.get("value") != "/"]
    if filtered:
        val = filtered[-1].get("value", "")
        if val.startswith("http"):
            return val
        return "https://www.nykaafashion.com" + val
    return ""

def extract_essential_details(detail_json):
    product = detail_json.get("response", {}).get("product", {})
    if not product:
        return None
    return {
        "id": product.get("id"),
        "category_url": extract_category_url(product),
        "product_url": product.get("meta_data", {}).get("productUrl") or (
            "https://www.nykaafashion.com" + product.get("action_url", "")
        ),
        "name": f"{product.get('title','').strip()} {product.get('subTitle','').strip()}".strip(),
        "sku": product.get("sku"),
        "price": product.get("price"),
        "discounted_price": product.get("discountedPrice"),
        "sizes_available": extract_sizes(product),
        "main_image_urls": extract_images(product),
        "max_allowed_qty": product.get("max_allowed_qty"),
        "is_out_of_stock": product.get("isOutOfStock"),
        "color_name": product.get("color", {}).get("name", ""),
        "category_name": product.get("categoryName"),
    }

def insert_basic_products(scrape_run_id, products, cursor, db):
    basic_fields = ["id", "sku", "price", "discountedPrice", "discount", "type", "imageUrl", "categoryId", "title"]
    insert_basic_fields = ["scrape_run_id"] + basic_fields
    placeholders = ", ".join(["%s"] * len(insert_basic_fields))
    insert_sql = f"INSERT INTO products_from_category ({', '.join(insert_basic_fields)}) VALUES ({placeholders})"
    for p in products:
        vals = [scrape_run_id] + [normalize_mysql_value(p.get(f)) for f in basic_fields]
        success = False
        for attempt in range(1, 3):
            try:
                cursor.execute(insert_sql, vals)
                success = True
                break
            except mysql_errors.OperationalError as oe:
                logging.warning("Basic product insert OperationalError attempt %s for product %s: %s", attempt, p.get("id"), oe)
                if attempt < 2:
                    time.sleep(1 * attempt)
                else:
                    logging.exception("Giving up basic product insert for %s", p.get("id"))
            except Exception:
                logging.exception("Failed to insert basic product %s", p.get("id"))
                break
        if not success:
            logging.error("🛑 Could not insert basic product %s after retries", p.get("id"))
    try:
        db.commit()
    except Exception:
        logging.exception("Failed to commit basic product inserts")

def detail_writer_thread(
    scrape_run_id,
    detail_queue,
    detail_fields,
    insert_detail_sql,
    writer_done_event,
    detail_stats,
    detail_stats_lock,
):
    db = get_mysql_connection()
    if not db:
        logging.critical("Detail writer cannot get DB connection, exiting.")
        writer_done_event.set()
        return
    cursor = db.cursor()
    buffer_count = 0
    try:
        while True:
            item = detail_queue.get()
            if item is SENTINEL:
                break
            vals = [scrape_run_id] + [normalize_mysql_value(item.get(f)) for f in detail_fields]
            inserted = False
            for attempt in range(1, 5):
                try:
                    cursor.execute(insert_detail_sql, vals)
                    inserted = True
                    break
                except (mysql_errors.InterfaceError, mysql_errors.OperationalError) as e:
                    logging.error("MySQL connection error on detail insert for %s (attempt %s): %s", item.get("id"), attempt, e)
                    try:
                        if cursor: cursor.close()
                        if db: db.close()
                    except: pass
                    db = get_mysql_connection()
                    if not db:
                        time.sleep(2)
                        continue
                    cursor = db.cursor()
                    time.sleep(2 * attempt)
                except Exception as e:
                    logging.exception("Failed to insert detailed product %s (other error)", item.get("id"))
                    break
            if not inserted:
                logging.error("🛑 Failed to insert detailed product %s after retries", item.get("id"))
            else:
                with detail_stats_lock:
                    detail_stats["inserted"] += 1
                buffer_count += 1
            if buffer_count >= DETAIL_BATCH_COMMIT_SIZE:
                try:
                    db.commit()
                    logging.info("Committed batch of %s detailed inserts", buffer_count)
                except Exception:
                    logging.exception("Failed commit in writer after batch")
                buffer_count = 0
    finally:
        if buffer_count:
            try:
                db.commit()
                logging.info("Final commit of %s pending detailed inserts", buffer_count)
            except Exception:
                logging.exception("Failed final commit in writer")
        try:
            if cursor: cursor.close()
            if db: db.close()
        except: pass
        writer_done_event.set()
        logging.info("Detail writer exiting.")

def update_scrape_run_progress(scrape_run_id, input_urls, total_listing_count):
    db = get_mysql_connection()
    if not db:
        logging.error("Cannot update progress for scrape_runs %s: connection failed", scrape_run_id)
        return
    cur = db.cursor()
    try:
        cur.execute("UPDATE scrape_runs SET listing_urls=%s, product_count=%s WHERE scrape_run_id=%s",
                    (",".join(input_urls), total_listing_count, scrape_run_id))
        db.commit()
    except Exception:
        logging.exception("Failed to update scrape_runs progress for %s", scrape_run_id)
    finally:
        cur.close()
        db.close()

def update_scrape_run_final(scrape_run_id, input_urls, total_listing_count, details_count):
    db = get_mysql_connection()
    if not db:
        logging.error("Cannot update final metadata for scrape_runs %s: connection failed", scrape_run_id)
        return
    cur = db.cursor()
    try:
        cur.execute(
            "UPDATE scrape_runs SET listing_urls=%s, product_count=%s, details_count=%s, finished_at=NOW() WHERE scrape_run_id=%s",
            (",".join(input_urls), total_listing_count, details_count, scrape_run_id),
        )
        db.commit()
        logging.info("✅ scrape_runs metadata updated successfully for %s", scrape_run_id)
    except Exception:
        logging.exception("Failed to update scrape_runs final metadata for %s", scrape_run_id)
    finally:
        cur.close()
        db.close()

def process_category_url(category_url):
    main_db = get_mysql_connection()
    if not main_db:
        logging.error("Could not connect to MySQL for URL: %s", category_url)
        return
    main_cursor = main_db.cursor()
    scrape_run_id = str(uuid.uuid4())
    listing_gender_map = {}

    try:
        main_cursor.execute(
            "INSERT INTO scrape_runs (scrape_run_id, listing_urls, product_count) VALUES (%s,%s,%s)",
            (scrape_run_id, category_url, 0),
        )
        main_db.commit()
    except Exception:
        logging.exception("Failed to create initial scrape_runs entry for URL: %s", category_url)
        return

    detail_queue = queue.Queue(maxsize=DETAIL_QUEUE_MAXSIZE)
    detail_fields = [
        "id", "category_url", "product_url", "name", "sku", "price", "discounted_price",
        "sizes_available", "main_image_urls", "max_allowed_qty", "is_out_of_stock",
        "color_name", "category_name", "gender", "gender_code",
    ]
    insert_detail_sql = f"""
        INSERT INTO product_details ({', '.join(['scrape_run_id'] + detail_fields)})
        VALUES ({', '.join(['%s'] * (1 + len(detail_fields)))})
        ON DUPLICATE KEY UPDATE
            category_url=VALUES(category_url), product_url=VALUES(product_url),
            name=VALUES(name), sku=VALUES(sku), price=VALUES(price),
            discounted_price=VALUES(discounted_price), sizes_available=VALUES(sizes_available),
            main_image_urls=VALUES(main_image_urls), max_allowed_qty=VALUES(max_allowed_qty),
            is_out_of_stock=VALUES(is_out_of_stock), color_name=VALUES(color_name),
            category_name=VALUES(category_name), gender=VALUES(gender), gender_code=VALUES(gender_code)
    """
    writer_done_event = threading.Event()
    detail_stats = {"inserted": 0}
    detail_stats_lock = threading.Lock()
    writer_thread = threading.Thread(
        target=detail_writer_thread,
        args=(
            scrape_run_id, detail_queue, detail_fields, insert_detail_sql,
            writer_done_event, detail_stats, detail_stats_lock
        ),
        daemon=True,
    )
    writer_thread.start()

    all_listing_products = []
    successful_detail_ids = set()
    fetch_failures, extract_failures = [], []
    success_lock = threading.Lock()

    try:
        category, brand, sort, price_range, gender, gender_code = parse_listing_url(category_url)
        if not category:
            logging.warning("Skipping URL with no category parsed: %s", category_url)
            return
        listing_products = scrape_products(category, brand, sort=sort, price_range=price_range)
        if not listing_products:
            logging.info("No products found for category URL %s, skipping details.", category_url)
            update_scrape_run_final(scrape_run_id, [category_url], 0, 0)
            main_cursor.close()
            main_db.close()
            return
        for p in listing_products:
            p['gender'] = gender
            p['gender_code'] = gender_code
            pid = p.get("id")
            if pid:
                listing_gender_map[pid] = {"gender": gender, "gender_code": gender_code}
        all_listing_products.extend(listing_products)
        insert_basic_products(scrape_run_id, listing_products, main_cursor, main_db)
        update_scrape_run_progress(scrape_run_id, [category_url], len(all_listing_products))
    except Exception:
        logging.exception("Failed while processing URL: %s", category_url)
        return

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        future_to_id = {}
        for p in all_listing_products:
            pid = p.get("id")
            if not pid: continue
            future = executor.submit(fetch_product_detail, pid, 3)
            future_to_id[future] = pid

        for future in as_completed(future_to_id):
            prod_id = future_to_id[future]
            detail_json = future.result()
            if not detail_json:
                fetch_failures.append(prod_id)
                continue
            detail = extract_essential_details(detail_json)
            if not detail:
                extract_failures.append(prod_id)
                continue
            listing_attrs = listing_gender_map.get(prod_id, {})
            detail['gender'] = listing_attrs.get("gender")
            detail['gender_code'] = listing_attrs.get("gender_code")
            with success_lock:
                successful_detail_ids.add(prod_id)
            try:
                detail_queue.put(detail, timeout=5)
            except queue.Full:
                logging.warning("Detail queue full, dropping product %s", prod_id)

    detail_queue.put(SENTINEL)
    writer_done_event.wait(timeout=300)

    with detail_stats_lock:
        inserted_details = detail_stats["inserted"]
    update_scrape_run_final(scrape_run_id, [category_url], len(all_listing_products), inserted_details)

    main_cursor.close()
    main_db.close()
    logging.info("✅ Finished URL: %s | Products: %d | Details: %d", category_url, len(all_listing_products), inserted_details)
    if fetch_failures:
        logging.warning("Detail fetch failures for %s: %s", category_url, fetch_failures[:10])
    if extract_failures:
        logging.warning("Detail extraction failures for %s: %s", category_url, extract_failures[:10])

def main():
    logging.info("▶︎ Starting category-by-category scrape")
    if not os.path.isfile(INPUT_FILE):
        logging.error("Input file '%s' not found in cwd %s", INPUT_FILE, os.getcwd())
        return

    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        urls = [row.get("url", "").strip() for row in reader if row.get("url", "").strip()]
    for url in urls:
        process_category_url(url)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
