#!/usr/bin/env python3
import time
import sys
import os
import uuid
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from kafka import KafkaProducer
import json

# 출력 인코딩 설정 (Linux에서도 UTF-8)
sys.stdout.reconfigure(encoding='utf-8')

# 쿠팡 카테고리 URL 설정
CATEGORIES = {
    "기저귀_물티슈": [
        "https://www.coupang.com/np/categories/485952",
        "https://www.coupang.com/np/categories/485979"
    ],
    "생활_위생용품": ["https://www.coupang.com/np/categories/221945"],
    "수유_이유용품": [
        "https://www.coupang.com/np/categories/334841",
        "https://www.coupang.com/np/categories/221943"
    ],
    "스킨케어_화장품": ["https://www.coupang.com/np/categories/221944"],
    "식품": ["https://www.coupang.com/np/categories/221939"],
    "완구용품": ["https://www.coupang.com/np/categories/349657"],
    "침구류": ["https://www.coupang.com/np/categories/221942"],
    "패션의류_잡화": ["https://www.coupang.com/np/categories/508565"]
}

# Kafka 관련 설정
KAFKA_BROKER = "192.168.56.125:9092"
KAFKA_TOPIC = "product-topic"

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"  # 메시지 손실 방지
)

def setup_driver():
    """Linux 환경에서 Headless Chrome 드라이버 설정 (Windows UA 사용)"""
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')  # GUI 없이 실행
    options.add_argument('--disable-gpu')
    # Windows user agent (쿠팡이 Windows UA를 선호함)
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
    """페이지 하단까지 스크롤하여 추가 제품 로딩 (lazy load)"""
    for i in range(count):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        print(f"[-] 스크롤 {i+1}회 완료")

def clean_text(text):
    """특수문자 및 줄바꿈 제거"""
    return re.sub(r'[^0-9a-zA-Z가-힣\s]', '', text).strip()

def generate_uid(name):
    """새 UID 생성"""
    return str(uuid.uuid4())

def get_existing_links():
    """기존 제품 링크 목록을 반환 (임시로 빈 set 반환)"""
    return set()

def send_to_kafka(data):
    """Kafka로 데이터 전송"""
    try:
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
        print(f"[✓] Kafka로 전송 완료: {data['name']}")
    except Exception as e:
        print(f"[X] Kafka 전송 실패: {e}")

def parse_products(driver, category_name, existing_links):
    """제품 정보를 추출하여 Kafka로 전송"""
    product_list_xpath = "//ul[@id='productList']/li"
    scroll_down(driver, count=3)
    try:
        products = driver.find_elements(By.XPATH, product_list_xpath)
    except Exception as e:
        print("[X] 제품 리스트 찾기 실패:", e)
        return []
    
    results = []
    for product in products:
        try:
            item = {"site": "coupang", "category": category_name}
            try:
                a_tag = product.find_element(By.TAG_NAME, "a")
                link = a_tag.get_attribute("href")
                item["link"] = link
            except:
                item["link"] = "링크 없음"
            
            if item["link"] in existing_links:
                print(f"[!] 이미 존재하는 제품: {item['link']} → 스킵")
                continue
                
            item["uid"] = generate_uid(item["name"] if "name" in item else "")
            try:
                name_elem = product.find_element(By.XPATH, ".//a/dl/dd/div[2]")
                name_text = name_elem.text.strip()
                item["name"] = clean_text(name_text)
                brand_text = clean_text(name_text.split()[0])
                item["brand"] = brand_text if brand_text else "브랜드 없음"
            except:
                item["name"] = "제품명 없음"
                item["brand"] = "브랜드 없음"
            try:
                sale_price_elem = product.find_element(By.XPATH, ".//a/dl/dd/div[3]/div[1]/div[1]/em/strong")
                item["sale_price"] = sale_price_elem.text.strip() + "원"
            except:
                item["sale_price"] = "할인가 없음"
            try:
                img_elem = product.find_element(By.XPATH, ".//a/dl/dt/img")
                item["img"] = img_elem.get_attribute("src")
            except:
                item["img"] = "이미지 정보 없음"

            # MongoDB 저장 대신 Kafka로 전송
            send_to_kafka(item)
            existing_links.add(item["link"])
            results.append(item)
        except Exception as e:
            print("[X] 제품 정보 추출 중 오류:", e)
            continue
    return results

def click_next_page(driver, page, category_name):
    """
    페이지 이동 처리 함수 (쿠팡)
    - page 1~5: 페이지 번호 버튼 클릭
    - page 6 이상: '다음 페이지' 버튼 클릭 (XPath 고정)
    """
    try:
        if page < 6:
            xpath = f"//*[@id='product-list-paging']/div/a[{page+2}]"
        else:
            xpath = "/html/body/div[3]/section/form/div/div/div[1]/div[2]/div[4]/div/a[11]"
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        driver.execute_script("arguments[0].click();", next_button)
        print(f"[→] 다음 페이지 버튼 클릭 (페이지 {page} → {page+1})")
        time.sleep(3)
        return True
    except Exception as e:
        print("[X] 다음 페이지로 이동 실패:", e)
        return False

def crawl_coupang_category(category_name, category_urls, existing_uids, existing_links):
    """
    쿠팡 카테고리 크롤링 함수
    - 각 카테고리 URL마다 최대 5페이지까지만 순회하며 제품을 추출
    - 페이지 번호가 1~5는 번호 버튼 클릭, 6페이지 이상이면 '다음 페이지' 버튼 클릭
    - 결과는 Kafka로 전송됨
    """
    driver = setup_driver()
    results = []
    max_pages = 5  # 5페이지까지만 크롤링
    for url in category_urls:
        driver.get(url)
        time.sleep(3)
        page = 1
        while page <= max_pages:
            print(f"\n[*] 쿠팡 {category_name} - 페이지 {page} 크롤링 중...")
            scroll_down(driver, count=3)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, "//ul[@id='productList']/li"))
                )
                print("[✓] 제품 리스트 로딩 완료")
            except Exception as e:
                print("[X] 제품 리스트 로딩 실패:", e)
                break
            try:
                products = driver.find_elements(By.XPATH, "//ul[@id='productList']/li")
                print(f"[*] 현재 페이지에 {len(products)}개의 상품 발견")
            except Exception as e:
                print("[X] 제품 리스트를 찾을 수 없음:", e)
                break
            if not products:
                print("[X] 제품 정보가 없습니다.")
                break
            data = parse_products(driver, category_name, existing_links)
            results.extend(data)
            if page == max_pages:
                print("[✓] 지정된 최대 페이지 도달")
                break
            if not click_next_page(driver, page, category_name):
                break
            page += 1
        time.sleep(2)
    driver.quit()
    return results

def run_scraper():
    """전체 카테고리에 대해 크롤링을 실행 (Kafka로 전송)"""
    existing_links = get_existing_links()
    for category_name, category_urls in CATEGORIES.items():
        print(f"\n=== '{category_name}' 크롤링 시작 ===")
        crawl_coupang_category(category_name, category_urls, {}, existing_links)
    print("\n[✓] 쿠팡 전체 카테고리 크롤링 완료!")

if __name__ == "__main__":
    run_scraper()
