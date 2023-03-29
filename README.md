# 사전설치 (해당 단계를 수행하지 않아도 됨)
### 설치
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.5.2/docker-compose.yaml'

### 컨테이너와 마운팅 될 디렉토리 생성
mkdir -p ./dags ./logs ./plugins

### .env 수정
BASE_DIR의 값을 로컬의 git repository 폴더로 경로 변경

### 호스트 파일/폴더 소유자 권한 설정 
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# 설정
### airflow 초기화 
docker-compose -f docker-compose.yaml up airflow-init

### airflow web ui id/pwd
airflow / airflow 

### airflow 백그라운드 실행
docker-compose -f docker-compose.yaml up -d

### 실행된 커넽이너 확인
docker ps

### process log 확인
docker logs -f demo-airflow-docker-airflow-webserver_1

### process console 진입
docker exec -u root -it demo-airflow-docker-airflow-webserver-1 bash


# 초기화
### docker-compose로 실행 했던 서비스 초기화 (컨테이너/볼륨/DBdata/이미지)
docker-compose -f docker-compose.yaml down --volumes --rmi all


# Airflow 아키텍처
![스크린샷 2022-07-15 오전 8 49 17](https://user-images.githubusercontent.com/20849970/179121374-b69bffc7-ef84-476d-8024-ad9603040849.png)


# 참고
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html