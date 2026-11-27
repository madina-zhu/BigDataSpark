#!/usr/bin/env bash
set -euo pipefail

CONTAINER="hs_spark"
WORKDIR="/opt/spark/work"

# Пакеты, которые Spark скачает сам (включая транзитивные зависимости:
# httpclient5, httpcore5 и т.д. для ClickHouse JDBC)
PACKAGES="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0"

echo "==============================================="
echo " BigDataSpark ETL — полный запуск пайплайна"
echo "==============================================="

step () { echo; echo ">>> $1"; }

run_job () {
  local job="$1"
  step "Запуск $job"
  docker exec -i "$CONTAINER" spark-submit \
    --master local[*] \
    --packages "$PACKAGES" \
    "$WORKDIR/$job"
}

step "Ожидание готовности PostgreSQL..."
until docker exec hs_postgres pg_isready -U hs_user -d hs_db >/dev/null 2>&1; do
  sleep 2
done

step "Ожидание готовности ClickHouse..."
until docker exec hs_clickhouse wget -qO- http://localhost:8123/ping >/dev/null 2>&1; do
  sleep 2
done

step "Ожидание готовности Spark..."
until docker exec "$CONTAINER" test -f "$WORKDIR/etl_source_to_star.py"; do
  sleep 2
done

# --- обязательные этапы ---
run_job etl_source_to_star.py
run_job etl_star_to_clickhouse.py

# --- опциональные этапы (если контейнеры подняты) ---
if docker ps --format '{{.Names}}' | grep -q '^hs_cassandra$'; then
  run_job etl_star_to_cassandra.py
fi
if docker ps --format '{{.Names}}' | grep -q '^hs_neo4j$'; then
  run_job etl_star_to_neo4j.py
fi
if docker ps --format '{{.Names}}' | grep -q '^hs_mongo$'; then
  run_job etl_star_to_mongodb.py
fi
if docker ps --format '{{.Names}}' | grep -q '^hs_valkey$'; then
  run_job etl_star_to_valkey.py
fi

echo
echo "==============================================="
echo " Готово. Проверьте отчёты (см. README)."
echo "==============================================="