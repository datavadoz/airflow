#!/bin/bash

docker build ./docker -t "airflow:v1"
mkdir -p ./dags ./logs ./plugins ./config

echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init
docker compose up -d
