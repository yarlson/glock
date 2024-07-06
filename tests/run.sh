#!/bin/bash

echo "Starting E2E test..."

# Start PostgreSQL using Docker Compose
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 10

# Build the test application
go build -o test-app main.go

# Run two instances of the test application
APP_NAME=app1 ./test-app &
APP_NAME=app2 ./test-app &

# Wait for user input to stop the test
echo "Press Enter to stop the test..."
read

# Kill the test applications
pkill -f test-app

# Stop and remove the Docker containers
docker-compose down

echo "E2E test completed."
