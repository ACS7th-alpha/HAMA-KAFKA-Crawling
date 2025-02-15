import json
import logging
import threading
import time
import uuid
import re
import os
from flask import Flask, jsonify
from kafka import KafkaConsumer
from pymongo import MongoClient
import boto3
from boto3.session import Config

# --------------------------------------------------
# 설정: Kafka, MongoDB, Flask
# --------------------------------------------------

# Kafka 관련 설정
KAFKA_BROKER = "192.168.56.125:9092"
KAFKA_TOPIC = "review-topic"

# MongoDB 관련 설정
MONGO_URI = "mongodb://192.168.1.245:27017"
client = MongoClient(MONGO_URI, replicaset="alpha-mongo")
db = client["product_db"]

# 별도의 컬렉션 설정
review_analyze_collection = db["product_analyze"]

# Flask 애플리케이션 생성
app = Flask(__name__)

# 전역 변수: 처리된 메시지 수, Consumer 종료 플래그
messages_processed = 0
consumer_running = True

# --------------------------------------------------
# AWS 자격 증명 설정
# --------------------------------------------------
def load_aws_credentials():
    """AWS 자격 증명을 CSV 파일에서 로드합니다."""
    try:
        with open('/home/kevin/aws/hama-reviewbedrock_accessKeys.csv', 'r') as file:
            next(file)  # 헤더 행 건너뛰기
            credentials = next(file).strip().split(',')
            os.environ['AWS_ACCESS_KEY_ID'] = credentials[0]
            os.environ['AWS_SECRET_ACCESS_KEY'] = credentials[1]
            os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    except Exception as e:
        logging.error("AWS 자격 증명 로드 실패: %s", e)
        raise

# AWS 자격 증명 로드
load_aws_credentials()

# --------------------------------------------------
# AWS 클라이언트 설정
# --------------------------------------------------
# Comprehend는 서울 리전(ap-northeast-2)
comprehend_client = boto3.client('comprehend', region_name='ap-northeast-2')
# Bedrock은 버지니아 북부(us-east-1)
bedrock_client = boto3.client(
    'bedrock-runtime',
    region_name='us-east-1',
    config=Config(retries=dict(max_attempts=3))
)

def invoke_bedrock_model(prompt):
    """
    Amazon Bedrock Nova Pro 모델 API를 호출하여 요약문을 생성합니다.
    요청 본문에서 "messages"의 "content" 값은 문자열 배열로 전달해야 합니다.
    """
    request_body = {
        "inferenceConfig": {
            "max_new_tokens": 1500
        },
        "messages": [
            {
                "role": "user",
                "content": [ {"text": prompt} ]
            }
        ]
    }
    try:
        response = bedrock_client.invoke_model(
            modelId="amazon.nova-pro-v1:0",
            contentType="application/json",
            accept="application/json",
            body=json.dumps(request_body)
        )
        # 응답 본문 읽기 (응답 키는 "Body" 또는 "body"가 있을 수 있음)
        if "Body" in response:
            response_body = response["Body"].read().decode("utf-8")
        else:
            response_body = response["body"].read().decode("utf-8")
        logging.info("Full response body: %s", response_body)
        result = json.loads(response_body)
        # 응답 구조에 맞게 수정
        summary = result.get("output", {}).get("message", {}).get("content", [{}])[0].get("text", "")
        logging.info("Nova Pro summary generated: %s", summary)
        return summary
    except Exception as e:
        logging.error("Amazon Bedrock Nova Pro error: %s", e)
        return None

def generate_review_summary_nova_pro(reviews):
    """
    전체 리뷰 텍스트를 결합하되 최대 길이를 제한한 후,
    Amazon Bedrock Nova Pro 모델 API를 호출하여 요약문을 생성합니다.
    """
    # 각 리뷰의 최대 길이를 500자로 제한
    MAX_REVIEW_LENGTH = 500
    # 전체 결합된 리뷰의 최대 길이를 4000자로 제한
    MAX_TOTAL_LENGTH = 4000

    # 각 리뷰를 길이 제한하여 결합
    limited_reviews = []
    total_length = 0

    for review in reviews:
        if review.get("review"):
            review_text = review["review"][:MAX_REVIEW_LENGTH]
            if total_length + len(review_text) <= MAX_TOTAL_LENGTH:
                limited_reviews.append(review_text)
                total_length += len(review_text)
            else:
                break

    all_reviews_text = " ".join(limited_reviews)
    prompt = (
        "다음 리뷰들을 읽고, 장점과 단점을 명확하게 구분하여 요약해 주세요. "
        "반드시 아래 형식으로만 작성해 주세요. 다른 문구는 포함하지 말아주세요:\n"
        "장점:\n- 장점1\n- 장점2\n\n"
        "단점:\n- 단점1\n- 단점2"
    )
    input_text = prompt + " " + all_reviews_text

    summary = invoke_bedrock_model(input_text)
    return summary or ""

def process_reviews_with_comprehend(reviews):
    """
    각 리뷰에 대해 Amazon Comprehend를 호출하여 감성 분석을 수행하고,
    분석 결과(감성, 감성 점수)를 해당 리뷰 딕셔너리에 추가합니다.
    """
    for review in reviews:
        review_text = review.get("review")
        if review_text:
            try:
                # 리뷰 텍스트를 5000 바이트로 제한
                encoded_text = review_text.encode('utf-8')
                if len(encoded_text) > 5000:
                    # 5000 바이트까지만 자르고 디코딩
                    review_text = encoded_text[:5000].decode('utf-8', errors='ignore')

                sentiment_response = comprehend_client.detect_sentiment(
                    Text=review_text,
                    LanguageCode="ko"
                )
                # 분석 결과를 리뷰 딕셔너리에 추가
                review["sentiment"] = sentiment_response.get("Sentiment")
                review["sentimentScore"] = sentiment_response.get("SentimentScore")
                logging.info("Processed review sentiment: %s", sentiment_response.get("Sentiment"))
            except Exception as e:
                logging.error("Amazon Comprehend error for review %s: %s", review.get("reviewuid"), e)
    return reviews

def calculate_review_sentiment_percentages(reviews):
    """
    리뷰 목록에서 각 감성(긍정, 부정, 중립, 혼합)의 비율(%)을 계산합니다.
    """
    total = len(reviews)
    if total == 0:
        return {
            "positive": 0.0,
            "negative": 0.0,
            "neutral": 0.0,
            "mixed": 0.0
        }
    count_positive = sum(1 for r in reviews if r.get("sentiment") == "POSITIVE")
    count_negative = sum(1 for r in reviews if r.get("sentiment") == "NEGATIVE")
    count_neutral  = sum(1 for r in reviews if r.get("sentiment") == "NEUTRAL")
    count_mixed    = sum(1 for r in reviews if r.get("sentiment") == "MIXED")

    return {
        "positive": (count_positive / total) * 100,
        "negative": (count_negative / total) * 100,
        "neutral":  (count_neutral  / total) * 100,
        "mixed":    (count_mixed    / total) * 100
    }

def kafka_review_worker(worker_id):
    """Comprehend 감성 분석과 Bedrock 리뷰 요약을 위한 통합 Kafka Consumer"""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="review_analysis_group",  # 같은 group_id를 사용하여 부하 분산
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    logging.info(f"Worker {worker_id} started")

    while consumer_running:
        try:
            for message in consumer:
                logging.info(f"Worker {worker_id} processing message")
                data = message.value
                product_uid = data.get("uid") or data.get("product_uid")
                product_name = data.get("name") or data.get("product_name")
                if not product_uid:
                    continue

                if "reviews" in data:
                    reviews = data["reviews"]
                    # 가져온 리뷰 텍스트 출력 (리스트 형태)
                    review_texts = [review.get("review") for review in reviews if review.get("review")]
                    logging.info(f"Product {product_uid} reviews: {review_texts}")

                    # 리뷰가 5개 이상일 때만 분석 및 요약 수행
                    if len(reviews) >= 5:
                        processed_reviews = process_reviews_with_comprehend(reviews)
                        sentiment_percentages = calculate_review_sentiment_percentages(processed_reviews)
                        review_text_summary = generate_review_summary_nova_pro(reviews)
                    else:
                        # 리뷰가 5개 미만이면 분석/요약 결과를 빈 값으로 업데이트
                        sentiment_percentages = {}
                        review_text_summary = ""

                    # 감성 분석과 요약 결과를 함께 업데이트
                    update_data = {
                        "$set": {
                            "uid": product_uid,
                            "name": product_name,
                            "review_percent": sentiment_percentages,
                            "review_summary": review_text_summary
                        }
                    }

                    review_analyze_collection.update_one(
                        {"uid": product_uid},
                        update_data,
                        upsert=True
                    )

                consumer.commit()
        except Exception as e:
            logging.error("Review analysis worker error: %s", e)
            time.sleep(1)

# --------------------------------------------------
# Flask 엔드포인트
# --------------------------------------------------

@app.route("/")
def index():
    return "Kafka Consumer 백엔드가 실행 중입니다."

@app.route("/status")
def status():
    return jsonify({
        "messages_processed": messages_processed,
        "kafka_topic": KAFKA_TOPIC
    })

@app.route("/api/review-analysis/<uid>")
def get_review_analysis(uid):
    """특정 제품의 리뷰 분석 결과를 조회합니다."""
    try:
        # MongoDB에서 해당 uid의 분석 결과 조회
        analysis = review_analyze_collection.find_one({"uid": uid})

        if analysis:
            # MongoDB ObjectId는 JSON으로 직렬화할 수 없으므로 문자열로 변환
            analysis['_id'] = str(analysis['_id'])
            return jsonify(analysis)
        else:
            return jsonify({"error": "분석 결과를 찾을 수 없습니다."}), 404

    except Exception as e:
        logging.error("리뷰 분석 조회 오류: %s", e)
        return jsonify({"error": "서버 오류가 발생했습니다."}), 500

# --------------------------------------------------
# 애플리케이션 실행
# --------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 두 개의 워커 스레드 생성
    review_threads = []
    for i in range(2):  # 2개의 워커 생성
        thread = threading.Thread(
            target=kafka_review_worker,
            args=(i,),  # worker_id를 인자로 전달
            daemon=True
        )
        review_threads.append(thread)
        thread.start()

    try:
        app.run(host="0.0.0.0", port=5000)
    except KeyboardInterrupt:
        logging.info("서버 종료 시그널 수신...")
    finally:
        consumer_running = False
        for thread in review_threads:
            thread.join()
        client.close()
        logging.info("애플리케이션 종료됨.")

class ReviewConsumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.consumer = KafkaConsumer(
            'review',  # review 토픽 구독
            bootstrap_servers=['192.168.56.125:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group'
        )

class SummaryConsumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.consumer = KafkaConsumer(
            'summary',  # summary 토픽 구독
            bootstrap_servers=['192.168.56.125:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group'
        )

