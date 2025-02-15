import time
import uuid
import re
import json
import multiprocessing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from kafka import KafkaProducer
from pymongo import MongoClient

# Kafka 관련 설정
KAFKA_BROKER = "192.168.56.125:9092"
KAFKA_TOPIC = "review-topic" 

def setup_driver():
    """Linux 환경에서 Headless Chrome 드라이버 설정 (Windows UA 사용)"""
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')  # GUI 없이 실행
    options.add_argument('--disable-gpu')
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/91.0.4472.124 Safari/537.36'
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def scroll_down(driver, count=3):
    """페이지 하단까지 스크롤하여 추가 리뷰 로딩 (lazy load)"""
    for i in range(count):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        print(f"[-] 리뷰 페이지 스크롤 {i+1}회 완료")

def clean_text(text):
    """특수문자 및 줄바꿈 제거"""
    return re.sub(r'[^0-9a-zA-Z가-힣\s]', '', text).strip()

def extract_reviews(driver, product_uid, category_name, max_pages=5, max_reviews=50):
    """
    한 제품 페이지에서 리뷰 탭을 클릭한 후, 최대 max_pages 페이지 또는 max_reviews 리뷰까지 크롤링하여
    리뷰 데이터를 리스트에 저장합니다.
    """
    reviews_data = []
    page = 1
    review_count = 0

    try:
        # 상품평 탭 클릭 (XPath는 쿠팡 사이트 구조에 맞게 수정 필요)
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='btfTab']/ul[1]/li[2]"))
        ).click()
        time.sleep(2)
    except Exception as e:
        print(f"[X] 상품평 탭 클릭 실패: {e}")
        return reviews_data

    while page <= max_pages and review_count < max_reviews:
        print(f"[*] {product_uid} - {category_name} - 리뷰 페이지 {page} 크롤링 중...")
        scroll_down(driver, count=3)
        try:
            # 리뷰 리스트 요소 (XPath는 쿠팡 리뷰 영역에 따라 수정 필요)
            reviews = driver.find_elements(By.XPATH, "//*[@id='btfTab']/ul[2]/li[2]/div/div[6]/section[4]/article/div[4]/div")
            if not reviews:
                print("[X] 리뷰 요소를 찾을 수 없음! 리뷰 페이지를 종료합니다.")
                break
        except Exception as e:
            print(f"[X] 리뷰 요소 찾기 실패: {e}")
            break

        for review in reviews:
            if review_count >= max_reviews:
                break
            review_text = clean_text(review.text.strip())
            if review_text:
                reviews_data.append({
                    "reviewuid": str(uuid.uuid4()),
                    "review": review_text
                })
                review_count += 1

        print(f"[✓] 현재 페이지 누적 리뷰 수: {review_count}")

        if page < max_pages and review_count < max_reviews:
            try:
                # 다음 페이지 버튼 클릭 (XPath는 쿠팡 리뷰 페이지 구조에 맞게 수정)
                next_page_btn = driver.find_element(By.XPATH, "//*[@id='btfTab']/ul[2]/li[2]/div/div[6]/section[4]/div[3]/button[2]")
                driver.execute_script("arguments[0].scrollIntoView(true);", next_page_btn)
                time.sleep(2)
                driver.execute_script("arguments[0].click();", next_page_btn)
                time.sleep(3)
                page += 1
            except Exception as e:
                print(f"[X] 다음 리뷰 페이지로 이동 실패: {e}")
                break
        else:
            break

    return reviews_data

def process_product(product):
    """
    하나의 제품에 대해 리뷰를 크롤링하여 Kafka 토픽으로 전송합니다.
    """
    driver = setup_driver()
    # 페이지 로드 타임아웃 설정 (30초)
    driver.set_page_load_timeout(30)
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )

    product_uid = product.get("uid")
    product_name = product.get("name")
    product_link = product.get("link")
    if not product_link:
        driver.quit()
        producer.close()
        return None

    print(f"\n[→] {product_name} 리뷰 크롤링 시작...")
    try:
        driver.get(product_link)
        time.sleep(3)
        reviews = extract_reviews(driver, product_uid, product.get("category", "Unknown"), max_pages=5, max_reviews=50)
        review_doc = {
            "product_uid": product_uid,
            "product_name": product_name,
            "reviews": reviews  # 리뷰가 없으면 빈 리스트 []
        }
        # Kafka 토픽으로 데이터 전송
        producer.send(KAFKA_TOPIC, review_doc)
        producer.flush()  # 전송 대기
        print(f"[✓] 리뷰 Kafka 전송 완료: {product_name} - 리뷰 수: {len(reviews)}")
    except Exception as e:
        print(f"[X] 리뷰 크롤링 오류 ({product_name}): {e}")
        review_doc = None

    driver.quit()
    producer.close()
    return review_doc

def crawl_reviews_for_products_mp():
    """
    MongoDB의 'products' 컬렉션에서 site가 'coupang'인 제품 데이터를 읽어와,
    각 제품의 리뷰를 멀티프로세싱으로 크롤링하여 Kafka 토픽으로 전송합니다.
    """
    MONGO_URI = "mongodb://192.168.1.245:27017"
    client = MongoClient(MONGO_URI, replicaset="alpha-mongo")
    db = client["product_db"]
    products_coll = db["products"]

    products = list(products_coll.find({"site": "coupang"}))
    print(f"총 {len(products)}개의 쿠팡 제품 데이터가 있습니다.")

    num_processes = min(8, len(products))
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(process_product, products)

    print("\n[✓] 모든 제품의 리뷰 크롤링 및 Kafka 전송 완료!")
    return results

if __name__ == "__main__":
    crawl_reviews_for_products_mp()

