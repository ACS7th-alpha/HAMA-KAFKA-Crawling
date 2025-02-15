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
    """Headless Chrome 설정 (Linux 환경)"""
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
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

def clean_text(text):
    """특수문자 및 줄바꿈 제거"""
    return re.sub(r'[^0-9a-zA-Z가-힣\s]', '', text).strip()

def click_review_button(driver):
    """리뷰 버튼 클릭 (스크롤 후 JavaScript 클릭)"""
    try:
        review_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div/div[2]/div[1]/div[2]/div[3]/div[1]/div[3]/div[1]/div/div/a[2]/span/span[1]")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", review_button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", review_button)
        time.sleep(2)
        print("[✓] 리뷰 버튼 클릭 성공")
        return True
    except Exception as e:
        print(f"[X] 리뷰 버튼 클릭 실패: {e}")
        return False

def extract_reviews(driver, product_uid, category_name, max_reviews=8):
    """i-mom 리뷰 크롤링 (최대 8개)"""
    reviews_data = []

    # 리뷰 버튼 클릭
    if not click_review_button(driver):
        return reviews_data

    try:
        # 리뷰 리스트 찾기 (최대 8개)
        reviews = driver.find_elements(
            By.XPATH,
            "/html/body/div[1]/div[2]/div[1]/div[1]/div/div/div/div[2]/ul[2]/li[2]/ul/li/div[3]/span"
        )
        print(f"[✓] 리뷰 {len(reviews)}개 찾음")

        for review in reviews[:max_reviews]:  # 최대 8개까지만 수집
            review_text = clean_text(review.text.strip())
            if review_text:
                reviews_data.append({
                    "reviewuid": str(uuid.uuid4()),
                    "review": review_text
                })

    except Exception as e:
        print(f"[X] 리뷰 데이터 크롤링 오류: {e}")

    return reviews_data

def process_product(product):
    """
    하나의 제품에 대해 리뷰를 크롤링하여 Kafka 토픽으로 전송합니다.
    """
    driver = setup_driver()
    # Kafka Producer 생성 (JSON 직렬화)
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

        # 리뷰 가져오기
        reviews = extract_reviews(driver, product_uid, product.get("category", "Unknown"), max_reviews=8)
        review_doc = {
            "product_uid": product_uid,
            "product_name": product_name,
            "reviews": reviews  # 리뷰가 없으면 빈 리스트 []
        }
        # Kafka 토픽으로 데이터 전송
        producer.send(KAFKA_TOPIC, review_doc)
        producer.flush()
        print(f"[✓] 리뷰 Kafka 전송 완료: {product_name} - 리뷰 수: {len(reviews)}")
    except Exception as e:
        print(f"[X] 리뷰 크롤링 오류 ({product_name}): {e}")
        review_doc = None

    driver.quit()
    producer.close()
    return review_doc

def crawl_reviews_for_products_mp():
    """
    MongoDB의 'products' 컬렉션에서 site가 'i-mom'인 제품 데이터를 읽어와,
    각 제품의 리뷰를 멀티프로세싱으로 크롤링하여 Kafka 토픽으로 전송합니다.
    """
    MONGO_URI = "mongodb://192.168.1.245:27017"
    client = MongoClient(MONGO_URI, replicaset="alpha-mongo")
    db = client["product_db"]
    products_coll = db["products"]

    # 'i-mom' 제품 가져오기
    products = list(products_coll.find({"site": "i-mom"}))
    print(f"총 {len(products)}개의 i-mom 제품 데이터가 있습니다.")

    if not products:
        print("[!] 크롤링할 제품이 없습니다. 종료합니다.")
        return

    num_processes = max(1, min(4, len(products)))
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(process_product, products)

    print("\n[✓] 모든 제품의 리뷰 크롤링 및 Kafka 전송 완료!")
    return results

if __name__ == "__main__":
    crawl_reviews_for_products_mp()

