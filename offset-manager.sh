#!/bin/bash

set -e

BOOTSTRAP_SERVER="lxp-qa-saas-namespace.servicebus.windows.net:9093"
KAFKA_CONFIG="../kafka.properties"

function usage() {
  echo "Usage: $0 {backup|reset|restore} <topic> <group> [--dry-run]"
  exit 1
}

ACTION=$1
TOPIC=$2
GROUP=$3
DRY_RUN=$4

if [[ -z "$ACTION" || -z "$TOPIC" || -z "$GROUP" ]]; then
  usage
fi

OFFSET_FILE="./${GROUP}-offsets.csv"

function check_group_exists() {
  ./kafka-consumer-groups.sh --bootstrap-server=$BOOTSTRAP_SERVER --command-config $KAFKA_CONFIG --describe --group "$GROUP" 2>&1 | grep -q "$GROUP"
}

function create_dummy_consumer() {
  echo "[INFO] Creating dummy consumer to initialize group..."
  timeout 10 ./kafka-console-consumer.sh \
    --bootstrap-server=$BOOTSTRAP_SERVER \
    --topic "$TOPIC" \
    --group "$GROUP" \
    --consumer.config $KAFKA_CONFIG \
    --max-messages 1 > /dev/null 2>&1 || true
}

function backup_offsets() {
  echo "[INFO] Backing up offsets for group $GROUP and topic $TOPIC..."

  if ! check_group_exists; then
    echo "[WARNING] Consumer group $GROUP does not exist. Attempting to create dummy consumer..."
    create_dummy_consumer

    if ! check_group_exists; then
      echo "[ERROR] Failed to initialize consumer group $GROUP"
      exit 1
    fi
  fi

  ./kafka-consumer-groups.sh \
    --bootstrap-server=$BOOTSTRAP_SERVER \
    --describe \
    --group "$GROUP" \
    --command-config $KAFKA_CONFIG \
    | awk -v topic="$TOPIC" 'BEGIN { OFS="," } $2 == topic { print $2, $3, $4 }' \
    | tee "$OFFSET_FILE"

  echo "[DONE] Backup completed and saved to $OFFSET_FILE"
}

function reset_offsets_to_latest() {
  echo "[INFO] Resetting offsets for group $GROUP on topic $TOPIC to latest..."

  if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "[INFO] Performing dry run of reset to latest with comparison to backed up offsets..."

    if [[ ! -f "$OFFSET_FILE" ]]; then
      echo "[ERROR] Offset backup file not found: $OFFSET_FILE"
      exit 1
    fi

    declare -A saved_offsets
    while IFS=, read -r topic partition offset; do
      saved_offsets["$partition"]=$offset
    done < "$OFFSET_FILE"

    ./kafka-consumer-groups.sh \
      --bootstrap-server=$BOOTSTRAP_SERVER \
      --describe \
      --group "$GROUP" \
      --command-config $KAFKA_CONFIG \
      | awk -v topic="$TOPIC" '$2 == topic { print $2, $3 }' \
      | while read -r topic partition; do
        latest=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
          --bootstrap-server=$BOOTSTRAP_SERVER \
          --topic "$topic" \
          --partition "$partition" \
          --time -1 \
          --command-config $KAFKA_CONFIG 2>/dev/null | awk -F: '{ print $3 }')

        saved=${saved_offsets["$partition"]}
        if [[ -n "$saved" && -n "$latest" ]]; then
          delta=$((latest - saved))
          if (( delta > 0 )); then
            echo "[INFO] $topic:$partition saved=$saved, latest=$latest → will skip $delta messages"
          elif (( delta < 0 )); then
            echo "[WARNING] $topic:$partition saved=$saved, latest=$latest → would rewind $((-delta)) messages (unusual!)"
          else
            echo "[INFO] $topic:$partition saved=$saved, latest=$latest → no change"
          fi
        else
          echo "[WARNING] $topic:$partition offset missing or unreadable"
        fi
      done
  else
    ./kafka-consumer-groups.sh \
      --bootstrap-server=$BOOTSTRAP_SERVER \
      --group "$GROUP" \
      --topic "$TOPIC" \
      --reset-offsets \
      --to-latest \
      --execute \
      --command-config $KAFKA_CONFIG
    echo "[DONE] Offsets have been reset to latest."
  fi
}

function restore_offsets() {
  echo "[INFO] Restoring offsets from $OFFSET_FILE..."

  if [[ ! -f "$OFFSET_FILE" ]]; then
    echo "[ERROR] Offset backup file not found: $OFFSET_FILE"
    exit 1
  fi

  if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "[INFO] Performing dry run of offset restore..."

    while IFS=, read -r topic partition offset; do
      latest=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
        --bootstrap-server=$BOOTSTRAP_SERVER \
        --topic "$topic" \
        --partition "$partition" \
        --time -1 \
        --command-config $KAFKA_CONFIG 2>/dev/null | awk -F: '{ print $3 }')
      if [[ -n "$latest" ]]; then
        delta=$((latest - offset))
        if (( delta > 0 )); then
          echo "[INFO] $topic:$partition saved=$offset, current=$latest → will rewind $delta messages"
        elif (( delta < 0 )); then
          echo "[INFO] $topic:$partition saved=$offset, current=$latest → will advance $((-delta)) messages"
        else
          echo "[INFO] $topic:$partition saved=$offset, current=$latest → no change"
        fi
      else
        echo "[WARNING] Could not fetch current offset for $topic:$partition"
      fi
    done < "$OFFSET_FILE"
  else
    ./kafka-consumer-groups.sh \
      --bootstrap-server=$BOOTSTRAP_SERVER \
      --group "$GROUP" \
      --reset-offsets \
      --from-file "$OFFSET_FILE" \
      --execute \
      --command-config $KAFKA_CONFIG
    echo "[DONE] Offsets restored from backup."
  fi
}

case $ACTION in
  backup)
    backup_offsets
    ;;
  reset)
    reset_offsets_to_latest
    ;;
  restore)
    restore_offsets
    ;;
  *)
    usage
    ;;
esac