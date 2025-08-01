#!/bin/bash

set -e

print_usage() {
  echo "Usage:"
  echo "  $0 backup <topic> <consumer-group> [--bootstrap-server <server>] [--config <file>]"
  echo "  $0 restore <topic> <consumer-group> [--dry-run] [--bootstrap-server <server>] [--config <file>]"
  echo "  $0 reset   <topic> <consumer-group> [--dry-run] [--bootstrap-server <server>] [--config <file>]"
}

log() {
  level="$1"
  shift
  echo "[$level] $*"
}

BOOTSTRAP_SERVER="localhost:9092"
KAFKA_CONFIG=""
DRY_RUN=false

COMMAND="$1"
TOPIC="$2"
CONSUMER_GROUP="$3"
shift 3 || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server)
      BOOTSTRAP_SERVER="$2"
      shift 2
      ;;
    --config)
      KAFKA_CONFIG="--command-config $2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      print_usage
      exit 1
      ;;
  esac
done

OFFSET_FILE="./${CONSUMER_GROUP}-offsets.csv"

backup_offsets() {
  log INFO "Backing up current offsets for topic '$TOPIC', group '$CONSUMER_GROUP' to $OFFSET_FILE..."
  ./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --group "$CONSUMER_GROUP" \
    --describe $KAFKA_CONFIG \
    | awk -v topic="$TOPIC" '$2 == topic { print $2 "," $3 "," $4 }' > "$OFFSET_FILE"
  log INFO "Backup complete."
}

restore_offsets() {
  if [[ ! -f "$OFFSET_FILE" ]]; then
    log ERROR "Offset backup file not found: $OFFSET_FILE"
    exit 1
  fi
  log INFO "Restoring offsets from $OFFSET_FILE..."
  if $DRY_RUN; then
    log INFO "Performing dry run of offset restore..."
    while IFS=, read -r topic partition offset; do
      CURRENT_OFFSET=$(./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
        --group "$CONSUMER_GROUP" --describe $KAFKA_CONFIG \
        | awk -v t="$topic" -v p="$partition" '$2 == t && $3 == p { print $4 }')
      if [[ -z "$CURRENT_OFFSET" ]]; then
        log WARNING "Could not fetch current offset for $topic:$partition"
      else
        DIFF=$((offset - CURRENT_OFFSET))
        if [[ $DIFF -gt 0 ]]; then
          log WARNING "$topic:$partition saved=$offset, current=$CURRENT_OFFSET → will advance $DIFF messages"
        elif [[ $DIFF -lt 0 ]]; then
          log WARNING "$topic:$partition saved=$offset, current=$CURRENT_OFFSET → will rewind $((-DIFF)) messages"
        else
          log INFO "$topic:$partition already at saved offset $offset"
        fi
      fi
    done < "$OFFSET_FILE"
  else
    ./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
      --group "$CONSUMER_GROUP" \
      --reset-offsets --from-file "$OFFSET_FILE" --execute $KAFKA_CONFIG
    log INFO "Offsets restored successfully."
  fi
}

reset_offsets() {
  log INFO "Resetting offsets for group '$CONSUMER_GROUP' on topic '$TOPIC' to latest..."
  if $DRY_RUN; then
    log INFO "(dry-run) showing comparison vs backup"
    if [[ ! -f "$OFFSET_FILE" ]]; then
      log WARNING "Offset backup file not found: $OFFSET_FILE"
      return
    fi
    while IFS=, read -r topic partition saved_offset; do
      latest_offset=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list "$BOOTSTRAP_SERVER" \
        --topic "$topic" --partitions "$partition" --time -1 $KAFKA_CONFIG \
        2>/dev/null | grep "$topic:$partition:" | cut -d: -f3)
      if [[ -z "$latest_offset" ]]; then
        log WARNING "Could not fetch latest offset for $topic:$partition"
      else
        diff=$((latest_offset - saved_offset))
        if [[ $diff -gt 0 ]]; then
          log WARNING "$topic:$partition saved=$saved_offset, latest=$latest_offset → would advance $diff messages"
        elif [[ $diff -lt 0 ]]; then
          log WARNING "$topic:$partition saved=$saved_offset, latest=$latest_offset → would rewind $((-diff)) messages (unusual!)"
        else
          log INFO "$topic:$partition saved=$saved_offset, latest=$latest_offset → no change"
        fi
      fi
    done < "$OFFSET_FILE"
  else
    ./kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
      --group "$CONSUMER_GROUP" \
      --topic "$TOPIC" \
      --reset-offsets --to-latest --execute $KAFKA_CONFIG
    log INFO "Offsets reset to latest successfully."
  fi
}

case "$COMMAND" in
  backup)
    backup_offsets
    ;;
  restore)
    restore_offsets
    ;;
  reset)
    reset_offsets
    ;;
  *)
    echo "Unknown command: $COMMAND"
    print_usage
    exit 1
    ;;
esac