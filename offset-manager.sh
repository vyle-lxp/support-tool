#!/bin/bash

set -euo pipefail

# Default options
BOOTSTRAP_SERVER=""
CONFIG=""

print_usage() {
  echo "Usage: $0 <backup|reset|restore> <topic> <group> [--dry-run] [--bootstrap-server <server>] [--config <file>]"
  echo
  echo "Examples:"
  echo "  $0 backup audit-native audit"
  echo "  $0 reset audit-native audit --dry-run --bootstrap-server kafka:9092"
  echo "  $0 restore audit-native audit --bootstrap-server kafka:9092 --config ../kafka.properties"
  exit 1
}

if [ $# -lt 3 ]; then
  print_usage
fi

MODE="$1"
TOPIC="$2"
GROUP="$3"
shift 3

DRY_RUN=false

# Parse optional args
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --bootstrap-server)
      BOOTSTRAP_SERVER="$2"
      shift 2
      ;;
    --config)
      CONFIG="--command-config $2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      print_usage
      ;;
  esac
done

OFFSET_FILE="./${GROUP}-offsets.csv"

do_backup() {
  echo "[INFO] Backing up offsets for group $GROUP and topic $TOPIC..."
  ./kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --group "$GROUP" \
    --describe $CONFIG | \
    awk -v topic="$TOPIC" '$2 == topic { print $2 "," $3 "," $4 }' > "$OFFSET_FILE"
  echo "[DONE] Backed up offsets to $OFFSET_FILE"
}

do_reset() {
  echo "[INFO] Resetting offsets for group $GROUP on topic $TOPIC to latest..."
  if $DRY_RUN; then
    echo "[INFO] Performing dry run of reset to latest with comparison to backed up offsets..."
    if [ ! -f "$OFFSET_FILE" ]; then
      echo "[WARNING] Offset backup file not found: $OFFSET_FILE"
      return
    fi
    while IFS=',' read -r topic partition saved_offset; do
      latest_offset=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list "$BOOTSTRAP_SERVER" \
        --topic "$topic" \
        --partitions "$partition" \
        --time -1 | grep ":$partition:" | awk -F: '{ print \$3 }')
      if [ -z "$latest_offset" ]; then
        echo "[WARNING] Could not fetch latest offset for $topic:$partition"
        continue
      fi
      delta=$((latest_offset - saved_offset))
      echo "[WARNING] $topic:$partition saved=$saved_offset, latest=$latest_offset → will skip $delta messages"
    done < "$OFFSET_FILE"
  else
    ./kafka-consumer-groups.sh \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --group "$GROUP" \
      --reset-offsets \
      --to-latest \
      --execute \
      --topic "$TOPIC" $CONFIG
    echo "[DONE] Offsets reset for $TOPIC"
  fi
}

do_restore() {
  echo "[INFO] Restoring offsets from $OFFSET_FILE..."
  if [ ! -f "$OFFSET_FILE" ]; then
    echo "[ERROR] Offset backup file not found: $OFFSET_FILE"
    exit 1
  fi

  if $DRY_RUN; then
    echo "[INFO] Performing dry run of offset restore..."
    while IFS=',' read -r topic partition saved_offset; do
      current_offset=$(./kafka-consumer-groups.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --group "$GROUP" \
        --describe $CONFIG | \
        awk -v t="$topic" -v p="$partition" '$2 == t && $3 == p { print $4 }')
      if [ -z "$current_offset" ]; then
        echo "[WARNING] Could not fetch current offset for $topic:$partition"
        continue
      fi
      delta=$((current_offset - saved_offset))
      direction="rewind"
      [ "$delta" -lt 0 ] && delta=$(( -1 * delta )) && direction="advance"
      echo "[WARNING] $topic:$partition saved=$saved_offset, current=$current_offset → will $direction $delta messages"
    done < "$OFFSET_FILE"
  else
    ./kafka-consumer-groups.sh \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --group "$GROUP" \
      --reset-offsets \
      --from-file "$OFFSET_FILE" \
      --execute $CONFIG
    echo "[DONE] Offsets restored for $TOPIC"
  fi
}

case "$MODE" in
  backup)
    do_backup
    ;;
  reset)
    do_reset
    ;;
  restore)
    do_restore
    ;;
  *)
    echo "Unknown mode: $MODE"
    print_usage
    ;;
esac
