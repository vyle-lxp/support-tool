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
  echo "Creating dummy consumer to initialize group..."
  timeout 10 ./kafka-console-consumer.sh \
    --bootstrap-server=$BOOTSTRAP_SERVER \
    --topic "$TOPIC" \
    --group "$GROUP" \
    --consumer.config $KAFKA_CONFIG \
    --max-messages 1 > /dev/null 2>&1 || true
}

function backup_offsets() {
  echo "Backing up offsets for group $GROUP and topic $TOPIC..."

  if ! check_group_exists; then
    echo "Consumer group $GROUP does not exist. Attempting to create dummy consumer..."
    create_dummy_consumer

    if ! check_group_exists; then
      echo "Failed to initialize consumer group $GROUP"
      exit 1
    fi
  fi

  ./kafka-consumer-groups.sh \
    --bootstrap-server=$BOOTSTRAP_SERVER \
    --describe \
    --group "$GROUP" \
    --command-config $KAFKA_CONFIG \
    | grep "$TOPIC" | awk '{ print $2 "," $3 "," $4 }' \
    > "$OFFSET_FILE"

  cat "$OFFSET_FILE"
}

function reset_offsets_to_latest() {
  echo "Resetting offsets for group $GROUP on topic $TOPIC to latest..."

  if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "Performing dry run of reset to latest with comparison to backed up offsets..."

    if [[ ! -f "$OFFSET_FILE" ]]; then
      echo "Offset backup file not found: $OFFSET_FILE"
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
      | grep "$TOPIC" | while read -r line; do
        partition=$(echo "$line" | awk '{print $3}')
        topic=$(echo "$line" | awk '{print $2}')
        latest=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
          --bootstrap-server=$BOOTSTRAP_SERVER \
          --topic "$topic" \
          --partition "$partition" \
          --time -1 \
          --command-config $KAFKA_CONFIG 2>/dev/null | awk -F: '{ print $3 }')
        saved=${saved_offsets["$partition"]}
        if [[ -n "$saved" ]]; then
          delta=$((latest - saved))
          if (( delta > 0 )); then
            echo "$topic:$partition saved=$saved, latest=$latest → will skip $delta messages"
          elif (( delta < 0 )); then
            echo "$topic:$partition saved=$saved, latest=$latest → would rewind $((-delta)) messages (unusual!)"
          else
            echo "$topic:$partition saved=$saved, latest=$latest → no change"
          fi
        else
          echo "$topic:$partition no saved offset found, latest=$latest"
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
  fi
}

function restore_offsets() {
  echo "Restoring offsets from $OFFSET_FILE..."

  if [[ ! -f "$OFFSET_FILE" ]]; then
    echo "Offset backup file not found: $OFFSET_FILE"
    exit 1
  fi

  if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "Performing dry run of offset restore..."

    while IFS=, read -r topic partition offset; do
      latest=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
        --bootstrap-server=$BOOTSTRAP_SERVER \
        --topic "$topic" \
        --partition "$partition" \
        --time -1 \
        --command-config $KAFKA_CONFIG 2>/dev/null | awk -F: '{ print $3 }')
      delta=$((latest - offset))
      if (( delta > 0 )); then
        echo "$topic:$partition saved=$offset, current=$latest → will rewind $delta messages"
      elif (( delta < 0 )); then
        echo "$topic:$partition saved=$offset, current=$latest → will advance $((-delta)) messages"
      else
        echo "$topic:$partition saved=$offset, current=$latest → no change"
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