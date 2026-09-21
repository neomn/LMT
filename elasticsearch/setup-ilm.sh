#!/bin/sh
set -eu

ES_URL="http://elasticsearch:9200"
POLICY_NAME="banking-logs-retention"
RETENTION_DAYS="${LOG_RETENTION_DAYS:-7}"

until curl -fsS "$ES_URL/_cluster/health?wait_for_status=yellow" >/dev/null; do
  sleep 5
done

curl -fsS -X PUT "$ES_URL/_ilm/policy/$POLICY_NAME" \
  -H 'Content-Type: application/json' \
  -d '{
    "policy": {
      "phases": {
        "hot": {
          "min_age": "0ms",
          "actions": {}
        },
        "delete": {
          "min_age": "'"${RETENTION_DAYS}d"'",
          "actions": {
            "delete": {}
          }
        }
      }
    }
  }'

curl -fsS -X PUT "$ES_URL/_index_template/banking-logs-template" \
  -H 'Content-Type: application/json' \
  -d '{
    "index_patterns": ["banking-logs-*"],
    "priority": 100,
    "template": {
      "settings": {
        "index.lifecycle.name": "'"$POLICY_NAME"'"
      }
    }
  }'

curl -fsS -X PUT "$ES_URL/banking-logs-*/_settings" \
  -H 'Content-Type: application/json' \
  -d '{"index.lifecycle.name":"'"$POLICY_NAME"'"}' || true

echo "Elasticsearch ILM policy and banking log template configured (${RETENTION_DAYS} day retention)"
