cd ./airflow-om

if ! docker network ls | grep -q openmetadata_network; then
    echo "Creating openmetadata_network..."
    docker network create openmetadata_network
fi

echo "Building custom Airflow image with Oracle dependencies..."
docker build -t custom-airflow-openmetadata:latest -f Dockerfile .

sed -i 's|image: apache/airflow:2.6.0|image: custom-airflow-openmetadata:latest|g' docker-compose.yaml

echo "Starting Airflow containers..."
docker-compose up -d

echo "===================================================="
echo "Airflow được khởi động tại http://localhost:8080"
echo "Username: airflow"
echo "Password: airflow"
echo "===================================================="
echo "Hãy đảm bảo OpenMetadata server đang chạy và có thể kết nối được từ Airflow"
echo "===================================================="
