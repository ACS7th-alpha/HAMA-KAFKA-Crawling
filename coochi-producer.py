import time
import uuid
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from kafka import KafkaProducer

# ✅ Kafka 설정
KAFKA_BROKER = "192.168.56.125:9092"
KAFKA_TOPIC = "product-topic"

# ✅ Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"  # 메시지 손실 방지
)

def setup_driver():
    """ 셀레니움 드라이버 설정 """
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=options)

def send_to_kafka(data):
    """ Kafka로 데이터 전송 """
    try:
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
        print(f"[✓] Kafka로 전송 완료: {data['name']}")
    except Exception as e:
        print(f"[X] Kafka 전송 실패: {e}")

def parse_products(driver, category_name):
    """ 상품 정보를 크롤링 후 Kafka에 전송 """
    products = driver.find_elements(By.CLASS_NAME, "prdList__item")

    for product in products:
        try:
            item = {"site": "coochi", "category": category_name}

            # 링크 가져오기
            try:
                ahref = product.find_element(By.TAG_NAME, "a")
                item['link'] = ahref.get_attribute('href')
            except:
                item['link'] = "링크 없음"

            # UUID 생성 (Kafka에서 유니크한 식별자 역할)
            item["uid"] = str(uuid.uuid4())

            # 제품명
            try:
                name_element = product.find_element(By.CLASS_NAME, "name")
                item['name'] = name_element.text.strip()
            except:
                item['name'] = "제품명 없음"

            # 브랜드
            try:
                brand_element = product.find_element(By.CLASS_NAME, "prd_brand")
                item['brand'] = brand_element.text.strip()
            except:
                item['brand'] = "브랜드 없음"

            # 가격
            try:
                price_elements = product.find_elements(By.TAG_NAME, "span")
                item['sale_price'] = "가격 없음"
                for elem in price_elements:
                    text = elem.text.strip()
                    if text and "원" in text:
                        item['sale_price'] = text
                        break
            except:
                item['sale_price'] = "가격 없음"

            # 이미지
            try:
                img_tag = product.find_element(By.TAG_NAME, "img")
                item['img'] = img_tag.get_attribute("src")
            except:
                item['img'] = "이미지 정보 없음"

            # ✅ Kafka로 데이터 전송
            send_to_kafka(item)

        except Exception as e:
            print("[X] 제품 정보 추출 중 오류:", str(e))
            continue

def crawl_category_by_link(category_name, category_url):
    """ 카테고리 크롤링 """
    driver = setup_driver()
    try:
        driver.get(category_url)
        time.sleep(3)
    except:
        driver.quit()
        return

    page_num = 1

    while True:
        print(f"\n[-] '{category_name}' - 페이지 {page_num} 크롤링 중...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "prdList__item"))
            )
        except:
            break

        parse_products(driver, category_name)

        try:
            next_btn = driver.find_element(By.XPATH, f"//a[@class='other' and text()='{page_num + 1}']")
            driver.execute_script("arguments[0].click();", next_btn)
            page_num += 1
            time.sleep(2)
        except:
            break

    driver.quit()

def run_scraper():
    """ 크롤링 실행 """
    categories = [
        ("생활_위생용품", "https://coochi.co.kr/category/출산외출용품/210/"),
        ("기저귀_물티슈", "https://coochi.co.kr/category/기저귀위생용품/211/"),
        ("패션의류_잡화", "https://coochi.co.kr/category/가방패션잡화/212/"),
        ("수유_이유용품", "https://coochi.co.kr/category/이유용품/3082/"),
        ("수유_이유용품", "https://coochi.co.kr/category/수유용품/196/")
    ]

    for cat_name, url in categories:
        print(f"\n=== '{cat_name}' 크롤링 시작 ===")
        crawl_category_by_link(cat_name, url)

    print("\n[✓] 모든 크롤링 완료!")

if __name__ == "__main__":
    run_scraper()
