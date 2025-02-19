# Node.js 공식 이미지 (경량 Alpine 기반)
FROM node:22-alpine AS builder
#  환경 변수 설정 (Docker 내부에서 사용)
ENV APP_HOME=/app
ENV GIT_REPO="https://github.com/ACS7th-alpha/HAMA-KAFKA-Crawling.git"
ENV BRANCH="master"
#  작업 디렉토리 설정
WORKDIR ${APP_HOME}
#  Git 및 필수 패키지 설치
RUN apk add --no-cache git
#  GitHub에서 프로젝트 클론 (BRANCH 설정 가능)
RUN git clone -b ${BRANCH} ${GIT_REPO} ${APP_HOME}
# 패키지 설치 (캐싱 활용)
RUN npm install
#  애플리케이션 빌드 (NestJS 프로젝트가 TypeScript일 경우)
RUN npm run build
#  멀티스테이지 빌드 (최적화) → 실행 환경 최소화
FROM node:22-alpine
# 실행 환경 변수 설정 (필요하면 여기에 추가)
ENV PORT=3002
# 작업 디렉토리 생성
WORKDIR /app
#  빌드된 파일만 복사 (최적화)
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./
#  포트 설정
EXPOSE ${PORT}
#  앱 실행
CMD ["node", "dist/main"]
