#!/bin/bash
# Script used to run the bash commands for the build in a simpler way

if [ $# -eq 0 ]; then
    echo "No arguments provided. Please provide a command to run."
    exit 1
fi

arg=$1

case $arg in
    up)
        echo "Starting the containers..."
        docker compose -f docker-compose.airflow.yml -f docker-compose.yml up -d
        echo "Containers started."
        ;;
    down)
        echo "Stopping the containers..."
        docker compose -f docker-compose.airflow.yml -f docker-compose.yml down
        echo "Containers stopped."
        ;;
    restart)
        echo "Restarting the containers..."
        docker compose -f docker-compose.airflow.yml -f docker-compose.yml down
        docker compose -f docker-compose.airflow.yml -f docker-compose.yml up -d
        echo "Containers restarted."
        ;;
    status)
        echo "Checking the status of the containers..."
        docker compose -f docker-compose.airflow.yml -f docker-compose.yml ps
        ;;
    full_restart)
        echo "Performing a full restart of the containers..."
        docker compose -f docker-compose.airflow.yml -f docker-compose.yml down --volumes --remove-orphans
        rm -rf data
        rm -rf logs
        rm -rf metadata
        docker-compose -f docker-compose.airflow.yml build --no-cache
        docker compose -f docker-compose.airflow.yml -f docker-compose.yml up -d
        echo "Full restart completed."
        ;;
esac