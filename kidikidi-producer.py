import time
import uuid
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from kafka import KafkaProducer
import json

# Kafka 설정
KAFKA_BROKER = "192.168.56.125:9092"
KAFKA_TOPIC = "product-topic"

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"  # 메시지 손실 방지
)

# 도메인 (상대경로 링크가 있을 경우 전체 URL을 만들기 위한 접두사)
BASE_DOMAIN = "https://kidikidi.elandmall.co.kr"

def setup_driver():
    """셀레니움 드라이버 설정 (headless 모드)"""
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')  # Linux 환경에서 GUI 없이 실행
    options.add_argument('--disable-gpu')
    options.add_argument('--remote-debugging-port=9222')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    return webdriver.Chrome(options=options)

def get_existing_links():
    """이미 처리된 링크를 메모리에 저장"""
    return set()

def get_full_link(link):
    """링크가 상대경로라면 BASE_DOMAIN을 붙여 전체 URL로 변환"""
    if link and link.startswith("/"):
        return BASE_DOMAIN + link
    return link

def parse_products(driver, mapped_category, existing_links):
    """
    상품 목록(li 요소)을 순회하며 제품 정보를 추출하여 Kafka로 전송
    """
    results = []
    product_list_xpath = "//section/div/div/div/div[2]/ul/li"

    # 페이지 하단으로 스크롤하여 lazy load 유도
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.8);")
    time.sleep(2)

    products = driver.find_elements(By.XPATH, product_list_xpath)
    if not products:
        print("   [*] 상품 목록을 찾을 수 없습니다.")
        return results

    for product in products:
        try:
            item = OrderedDict()
            item["site"] = "kidikidi"
            item["category"] = mapped_category

            # 1. 제품 링크 및 이미지 추출
            try:
                a_tag = product.find_element(By.XPATH, ".//div/div/div[1]/a")
                # 우선 data-href 속성을 확인
                link = a_tag.get_attribute("data-href")
                if not link or link.strip() == "":
                    link = a_tag.get_attribute("href")
                link = get_full_link(link)
                item["link"] = link

                try:
                    img_tag = a_tag.find_element(By.TAG_NAME, "img")
                    driver.execute_script("arguments[0].scrollIntoView(true);", img_tag)
                    time.sleep(0.5)
                    src = img_tag.get_attribute("src")
                    if "blank.gif" in src:
                        src_alt = img_tag.get_attribute("data-src") or img_tag.get_attribute("data-original")
                        if src_alt and src_alt.strip() != "":
                            src = src_alt
                        else:
                            driver.execute_script("arguments[0].scrollIntoView(true);", img_tag)
                            time.sleep(1)
                            src = img_tag.get_attribute("src")
                    item["img"] = src if src and src.strip() != "" else "이미지 정보 없음"
                except Exception as e_img:
                    item["img"] = "이미지 정보 없음"
            except Exception as e_link:
                item["link"] = "링크 없음"
                item["img"] = "이미지 정보 없음"

            # 중복 체크 (링크 기준)
            if item["link"] in existing_links:
                print(f"   [!] 이미 존재하는 제품: {item['link']} → 스킵")
                continue

            item["uid"] = str(uuid.uuid4())

            try:
                brand_element = product.find_element(By.XPATH, ".//div/div/div[2]/div[1]/a/strong")
                item["brand"] = brand_element.text.strip()
            except Exception as e_brand:
                item["brand"] = "브랜드 없음"

            try:
                name_element = product.find_element(By.XPATH, ".//div/div/div[2]/div[1]/a/p")
                item["name"] = name_element.text.strip()
            except Exception as e_name:
                item["name"] = "제품명 없음"

            try:
                price_element = product.find_element(By.XPATH, ".//div/div/div[2]/div[2]/div[1]/span/em")
                item["sale_price"] = price_element.text.strip()
            except Exception as e_price:
                item["sale_price"] = "가격 정보 없음"

            # Kafka 저장 대신 Kafka로 전송
            producer.send(KAFKA_TOPIC, item)
            existing_links.add(item["link"])
            results.append(item)
            print(f"   [+] Kafka 전송: {item['name']} ({item['link']})")
        except Exception as e:
            print("[X] 제품 정보 추출 중 오류:", str(e))
            continue

    return results

def crawl_category(category_display, category_xpath, mapped_category, existing_links):
    """
    메인 페이지에서 해당 카테고리 아이콘을 클릭한 후
    상품 목록 및 페이지네이션을 순회하며 상품 정보를 추출
    """
    driver = setup_driver()
    main_url = (
        "https://kidikidi.elandmall.co.kr/c/ctggrp?"
        "R11302001_srchOutcome_paging=1&dispCategoryGroupNo=400002&"
        "pageId=1739003492905&preCornerNo=R11300001_gnbMenu"
    )
    driver.get(main_url)
    time.sleep(3)

    try:
        category_icon = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, category_xpath))
        )
        driver.execute_script("arguments[0].click();", category_icon)
        time.sleep(3)
        print(f"[*] '{category_display}' 카테고리 선택됨 → 매핑 카테고리: {mapped_category}")
    except Exception as e:
        print(f"[X] 카테고리 클릭 오류 ({category_display}):", e)
        driver.quit()
        return

    page_num = 1
    prev_page_links = set()  # 이전 페이지의 제품 링크 집합

    while True:
        print(f"\n[-] '{mapped_category}' - 페이지 {page_num} 크롤링 중...")

        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.XPATH, "//section/div/div/div/div[2]/ul/li"))
            )
        except Exception as e:
            print("   [X] 상품 목록 로딩 실패:", e)
            break

        # 현재 페이지의 제품 링크 집합 추출
        current_page_links = set()
        try:
            product_elements = driver.find_elements(By.XPATH, "//section/div/div/div/div[2]/ul/li")
            for product in product_elements:
                try:
                    a_tag = product.find_element(By.XPATH, ".//div/div/div[1]/a")
                    link = a_tag.get_attribute("data-href")
                    if not link or link.strip() == "":
                        link = a_tag.get_attribute("href")
                    link = get_full_link(link)
                    current_page_links.add(link)
                except Exception:
                    continue
        except Exception as e:
            print("   [X] 제품 링크 추출 실패:", e)

        # 마지막 페이지 판단: 이전 페이지와 제품 링크 집합이 동일하면 새 제품 없음
        if prev_page_links and current_page_links == prev_page_links:
            print("   [*] 새로운 페이지 없음 (마지막 페이지 도달) → 종료")
            break

        prev_page_links = current_page_links

        # 제품 정보 추출 및 저장
        parse_products(driver, mapped_category, existing_links)

        # 페이지네이션 처리
        try:
            pagination_container_xpath = "/html/body/div[1]/div[2]/div[2]/div[4]/section/div/div/div/div[2]/div/div"
            pagination_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, pagination_container_xpath))
            )
            anchors = pagination_container.find_elements(By.TAG_NAME, "a")
            # 디버깅: 앵커 텍스트 출력
            anchor_texts = [a.text.strip() for a in anchors]
            print("   [*] 페이지네이션 앵커 텍스트:", anchor_texts)

            # 우선 숫자만 있는 앵커들을 필터링
            numeric_anchors = [a for a in anchors if a.text.strip().isdigit()]
            visible_pages = sorted([int(a.text.strip()) for a in numeric_anchors])
            # 현재 페이지가 visible_pages 내에 있다면, 그 다음 번호를 클릭
            next_btn = None
            if visible_pages:
                max_visible = visible_pages[-1]
                if page_num < max_visible:
                    next_page_str = str(page_num + 1)
                    for a in numeric_anchors:
                        if a.text.strip() == next_page_str:
                            next_btn = a
                            break
                elif page_num == max_visible:
                    # 현재 페이지가 최대 번호라면, 만약 non-numeric 앵커(예:"다음" 버튼)가 있으면 클릭
                    non_numeric = [a for a in anchors if not a.text.strip().isdigit()]
                    if non_numeric:
                        next_btn = non_numeric[0]
            else:
                # 숫자 앵커가 없으면, 만약 non-numeric 앵커가 있으면 그것을 사용
                non_numeric = [a for a in anchors if not a.text.strip().isdigit()]
                if non_numeric:
                    next_btn = non_numeric[0]

            if next_btn:
                driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", next_btn)
                page_num += 1
                time.sleep(3)
            else:
                print("   [*] 다음 페이지 버튼 없음 → 종료")
                break
        except Exception as e:
            print("   [*] 다음 페이지 버튼 클릭 실패 또는 더 이상 페이지가 없습니다.", e)
            break

    driver.quit()

def run_scraper():
    """전체 카테고리에 대해 크롤링을 실행 (중복 제외 후 Kafka로 전송)"""
    existing_links = get_existing_links()

    categories = [
        ("기저귀",       "/html/body/div[1]/div[2]/div[2]/div[3]/section/div/div[2]/div[1]/div/div[1]/div/div[2]/div/a/i", "기저귀_물티슈"),
        ("분유",         "/html/body/div[1]/div[2]/div[2]/div[3]/section/div/div[2]/div[1]/div/div[1]/div/div[3]/div/a/i", "수유_이유용품"),
        ("출산/육아용품", "/html/body/div[1]/div[2]/div[2]/div[3]/section/div/div[2]/div[1]/div/div[1]/div/div[4]/div/a/i", "생활_위생용품"),
        ("유아식기",     "/html/body/div[1]/div[2]/div[2]/div[3]/section/div/div[2]/div[1]/div/div[1]/div/div[5]/div/a/i", "생활_위생용품"),
        ("욕실용품",     "/html/body/div[1]/div[2]/div[2]/div[3]/section/div/div[2]/div[1]/div/div[1]/div/div[6]/div/a/i", "스킨케어_화장품"),
        ("베이비토이",   "/html/body/div[1]/div[2]/div[2]/div[3]/section/div/div[2]/div[1]/div/div[1]/div/div[9]/div/a/i", "완구용품"),
        ("인테리어/가구", "/html/body/div[1]/div[2]/div[2]/div[3]/section/div/div[2]/div[1]/div/div[1]/div/div[8]/div/a/i", "침구류")
    ]

    for display, xpath, mapped in categories:
        print(f"\n=== '{display}' 크롤링 시작 ===")
        crawl_category(display, xpath, mapped, existing_links)

    print("\n[✓] 모든 크롤링 완료!")

if __name__ == "__main__":
    try:
        run_scraper()
    finally:
        # 프로그램 종료 시 Kafka Producer 종료
        producer.close()

