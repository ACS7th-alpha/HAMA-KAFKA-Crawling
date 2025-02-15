import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError  # 중복 방지

# ✅ Kafka 설정
KAFKA_BROKER = "192.168.56.125:9092"
KAFKA_TOPIC = "product-topic"

# ✅ MongoDB 설정
MONGO_URI = "mongodb://192.168.1.245:27017"
client = MongoClient(MONGO_URI, replicaset="alpha-mongo")
db = client["product_db"]       
collection = db["products"] 

# ✅ 유니크 인덱스 설정 (중복 방지: link 필드)
try:
    collection.create_index("link", unique=True)
    print("[✓] MongoDB 인덱스 설정 완료: link(unique)")
except Exception as e:
    print(f"[!] 인덱스 생성 오류: {e}")

# ✅ Kafka Consumer 생성
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",  # 처음부터 모든 메시지를 가져옴
    enable_auto_commit=False,      # 메시지 처리 후에만 커밋
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))  # JSON 변환
)

print(f"[✓] Kafka Consumer 시작... (토픽: {KAFKA_TOPIC})")

# ✅ Kafka에서 메시지를 가져와서 MongoDB에 저장 (중복 방지: link 기준)
for message in consumer:
    data = message.value  # Kafka 메시지 데이터
    link = data.get("link")  # 중복 체크를 위한 link 필드

    print(f"[✓] Kafka 메시지 수신: {data}")

    # 미들웨어나 기타 설정에서 group_id 필드를 요구한다면,
    # 문서에 group_id가 없을 경우 기본값을 "HAMA"로 추가합니다.
    if "group_id" not in data:
        data["group_id"] = "HAMA"
        print("[i] group_id 필드가 없어서 기본값 'HAMA'를 추가했습니다.")

    try:
        # MongoDB에 저장 (bypass_document_validation 옵션을 사용하여 유효성 검사 우회)
        collection.insert_one(data, bypass_document_validation=True)
        print(f"[✓] MongoDB에 저장 완료: {data.get('name', '이름 미지정')}")
        consumer.commit()  # 메시지 처리 후 수동 커밋

    except DuplicateKeyError:
        print(f"[!] 중복 데이터 스킵 (MongoDB): {link}")
    except Exception as e:
        print(f"[X] MongoDB 저장 실패: {e}")

