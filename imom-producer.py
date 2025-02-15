import time
import json
import sys
import uuid
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from kafka import KafkaProducer
from webdriver_manager.chrome import ChromeDriverManager

# ✅ 리눅스 환경에서 UTF-8 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

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
    """셀레니움 드라이버 설정"""
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scroll_down(driver, count=3):
    """ 여러 번 스크롤을 내려 추가 제품 로딩 """
    for i in range(count):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        print(f"[-] 스크롤 {i + 1}회 완료")

def send_to_kafka(data):
    """ Kafka로 데이터 전송 """
    try:
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
        print(f"[✓] Kafka로 전송 완료: {data['name']}")
    except Exception as e:
        print(f"[X] Kafka 전송 실패: {e}")

def crawl_category(category_name, category_xpath):
    """ 특정 카테고리에서 제품 정보 크롤링 후 Kafka 전송 """
    driver = setup_driver()
    driver.get("https://i-mom.co.kr")

    # 햄버거 버튼 클릭
    try:
        menu_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='#category']"))
        )
        driver.execute_script("arguments[0].click();", menu_button)
        time.sleep(2)
        print(f"[✓] 햄버거 버튼 클릭 성공 ({category_name})")
    except:
        driver.quit()
        return

    # 카테고리 클릭
    try:
        category_element = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, category_xpath))
        )
        driver.execute_script("arguments[0].click();", category_element)
        time.sleep(3)
        print(f"[✓] {category_name} 카테고리 클릭 성공")
    except:
        driver.quit()
        return

    page = 1
    last_page = None

    while True:
        print(f"\n[-] {category_name} - 페이지 {page} 크롤링 중...")
        scroll_down(driver, count=3)

        # 상품 리스트 로딩 대기
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//li[contains(@class, 'goods_list_style3')]"))
            )
            print("[✓] 상품 리스트 로딩 완료")
        except:
            break

        # 상품 요소들 찾기
        products = driver.find_elements(By.XPATH, "//li[contains(@class, 'goods_list_style3')]")
        if not products:
            break

        for product in products:
            try:
                item = {"site": "i-mom", "category": category_name}  # ✅ 사이트 구분 필드 추가

                # 링크
                try:
                    ahref = product.find_element(By.TAG_NAME, "a")
                    item['link'] = ahref.get_attribute('href')
                except:
                    item['link'] = "링크 없음"

                # UUID 생성 (Kafka에서 유니크한 식별자 역할)
                item["uid"] = str(uuid.uuid4())

                # 제품명 및 브랜드 추출
                try:
                    name_text = product.find_element(By.XPATH, ".//span[contains(@class, 'name')]").text.strip()
                    item['name'] = name_text
                    item['brand'] = re.sub(r'[^0-9a-zA-Z가-힣\s]', '', name_text.split()[0]) if name_text else "브랜드 없음"
                except:
                    item['name'] = "제품명 없음"
                    item['brand'] = "브랜드 없음"

                # 가격
                try:
                    item['sale_price'] = product.find_element(By.XPATH, ".//span[contains(@class, 'sale_price')]").text.strip()
                except:
                    item['sale_price'] = "할인가 없음"

                # 이미지
                try:
                    img_tag = product.find_element(By.TAG_NAME, "img")
                    item['img'] = img_tag.get_attribute("src")
                except:
                    item['img'] = "이미지 정보 없음"

                # ✅ Kafka로 전송
                send_to_kafka(item)

            except Exception as e:
                print("[X] 제품 정보 추출 중 오류:", str(e))
                continue

        # 다음 페이지 이동
        if last_page is None:
            try:
                last_page_element = driver.find_element(By.XPATH, "//a[@class='last']")
                last_page = int(last_page_element.get_attribute("href").split("goodsSearchPage(")[1].split(")")[0])
            except:
                break

        if page >= last_page:
            break

        try:
            next_page_num = page + 1
            driver.execute_script(f"goodsSearchPage({next_page_num})")
            WebDriverWait(driver, 10).until(EC.staleness_of(products[0]))
            time.sleep(3)
            page += 1
        except:
            break

    driver.quit()

def run_scraper():
    """ 크롤링 실행 (Kafka 전송) """
    categories = {
        "기저귀_물티슈": "//a[contains(text(), '기저귀/물티슈')]",
        "생활_위생용품": "//a[contains(text(), '생활/위생용품')]",
        "스킨케어_화장품": "//a[contains(text(), '스킨케어/화장품')]",
        "수유_이유용품": "//a[contains(text(), '수유/이유용품')]",
        "패션의류_잡화": "//a[contains(text(), '패션의류/잡화')]",
        "침구류": "//a[contains(text(), '침구류')]",
        "완구용품": "//a[contains(text(), '발육/외출/완구용품')]",
        "식품": "//a[contains(text(), '식품')]"
    }

    for category_name, category_xpath in categories.items():
        print(f"\n=== '{category_name}' 크롤링 시작 ===")
        crawl_category(category_name, category_xpath)

    print("\n[✓] 모든 크롤링 완료!")

if __name__ == "__main__":
    run_scraper()

