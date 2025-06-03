#!/bin/bash
set -euo pipefail

# Default to including Flink profile
INCLUDE_FLINK=true
BUILD_FLAG="--build"

# Parse arguments
for arg in "$@"
do
    case $arg in
        --no-flink)
        INCLUDE_FLINK=false
        shift
        ;;
        --no-build)
        BUILD_FLAG=""
        shift
        ;;
        *)
        ;;
    esac
done

echo "🔍 Checking for .env file in the current directory..."
if [ ! -f ./.env ]; then
    echo "📝 .env file not found. Copying .env.example to .env"
    cp .env.example .env
    echo "✅ .env file created from .env.example"
else
    echo "✅ .env file already exists."
fi

echo "🔄 Updating git submodules..."
git submodule update --init --recursive
echo "✅ Git submodules are up-to-date."

ECOMMERCE_ENV="./apps/ecommerce/.env"
echo "🔍 Checking for ${ECOMMERCE_ENV}..."
if [ ! -f "$ECOMMERCE_ENV" ]; then
    echo "📝 ${ECOMMERCE_ENV} file not found. Creating the file..."
    cat <<EOL > "$ECOMMERCE_ENV"
NEXT_PUBLIC_SNOWPLOW_COLLECTOR_URL=http://localhost:9090
SNOWPLOW_CONSOLE_API_KEY=
EOL
    echo "✅ ${ECOMMERCE_ENV} file created."
else
    echo "✅ ${ECOMMERCE_ENV} file already exists."
fi

echo "🚀 Starting Docker Compose in detached mode..."
if [ "$INCLUDE_FLINK" = true ] ; then
    echo -e "\t🐿️ including Flink profile..."
    docker compose --profile flink up -d ${BUILD_FLAG}
else
    echo -e "\t ❌🐿️ not including Flink profile..."
    docker compose up -d ${BUILD_FLAG}
fi
echo "✅ Docker Compose command executed."


echo "---------------------------------------------"
echo "📢 Service Status:"
echo "🛒 Ecommerce store is running on http://localhost:3000"
echo "👁️ AKHQ (Kafka visualization tool) is running on http://localhost:8085"
echo "🛠️ Kafka is running on port 9092 -> localhost"
echo "📡 Snowplow collector is running on http://localhost:9090"
echo "📊 Redis Insights is running on http://localhost:5540"
if [ "$INCLUDE_FLINK" = true ] ; then
    echo "🚀 Flink UI is running on http://localhost:8081"
    echo "📊 Grafana is running on http://localhost:3001 use admin:admin to login"
fi
echo "---------------------------------------------"
