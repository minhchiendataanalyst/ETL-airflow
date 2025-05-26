#!/bin/bash

# Script để khởi động lại môi trường Airflow

echo "Dừng và xóa các container hiện tại..."
docker-compose down

echo "Xóa các volumes không sử dụng..."
docker volume prune -f

echo "Xây dựng lại image Docker..."
docker build -t custom-airflow-openmetadata:latest -f Dockerfile .

echo "Khởi động lại các container..."
docker-compose up -d

echo "Đợi các container khởi động..."
sleep 30

echo "Kiểm tra trạng thái các container..."
docker-compose ps

echo "Hoàn tất khởi động lại môi trường!"
