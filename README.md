## 컨테이너와 마운팅 될 디렉토리 생성
mkdir -p ./dags ./logs ./plugins

## 호스트 파일/폴더 소유자 권한 설정 
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

## airflow 초기화 
docker-compose up airflow-init

## airflow web ui id/pwd
airflow / airflow 

## airflow 실행 
docker-compose up -d # -d: 백그라운드 실행

## 전체( 컨테이너 / 볼륨 / DB data / 이미지) 초기화
docker-compose down --volumes --rmi all

## process log 확인
docker logs -f demo-airflow-docker-airflow-webserver_1

## process console 진입
docker exec -u root -it demo-airflow-docker-airflow-webserver-1 bash