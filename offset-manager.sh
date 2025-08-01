#!/bin/bash

set -e

# Defaults
BOOTSTRAP_SERVER="localhost:9092"
COMMAND_CONFIG=""

# Logger
log() {
  local level="$1"; shift
  echo "[$level] $*"
}

# Args
ACTION=$1
TOPIC=$2
GROUP=$3
shift 3 || true

DRY_RUN=false

# Parse optional flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --config)
      COMMAND_CONFIG="$2"
      shift 2
      ;;
    --bootstrap-server)
      BOOTSTRAP_SERVER="$2"
      shift 2
      ;;
    *)
      log "ERROR" "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Script location
DIR="$(cd "$(dirname "$0")" && pwd)"
OFFSET_FILE="$DIR/${GROUP}-offsets.csv"

# Kafka tool
KAFKA_GROUP_TOOL="./kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_SERVER"
if [[ -n "$COMMAND_CONFIG" ]]; then
  KAFKA_GROUP_TOOL+=" --command-config $COMMAND_CONFIG"
fi

# Backup
backup_offsets() {
  log "INFO" "Backing up offsets for group $GROUP and topic $TOPIC..."
  echo -n > "$OFFSET_FILE"
  $KAFKA_GROUP_TOOL --group "$GROUP" --describe | grep "$TOPIC" | awk -v topic="$TOPIC" -v group="$GROUP" '
    $2 == topic {
      print $2 "," $3 "," $4
    }
  ' >> "$OFFSET_FILE"

  if [[ ! -s "$OFFSET_FILE" ]]; then
    log "WARNING" "No offsets found to back up!"
  else
    cat "$OFFSET_FILE"
    log "DONE" "Offsets backed up to $OFFSET_FILE"
  fi
}

# Restore
restore_offsets() {
  log "INFO" "Restoring offsets from $OFFSET_FILE..."

  if [[ ! -f "$OFFSET_FILE" ]]; then
    log "ERROR" "Backup file not found: $OFFSET_FILE"
    exit 1
  fi

  log "INFO" "Performing dry run of offset restore..."

  while IFS=, read -r topic partition offset; do
    CURRENT=$($KAFKA_GROUP_TOOL --group "$GROUP" --describe | awk -v t="$topic" -v p="$partition" '$2 == t && $3 == p {print $4}')
    if [[ -z "$CURRENT" || "$CURRENT" == "-" ]]; then
      log "WARNING" "Could not fetch current offset for $topic:$partition"
    else
      DIFF=$((offset - CURRENT))
      if [[ "$DIFF" -gt 0 ]]; then
        log "INFO" "$topic:$partition saved=$offset, current=$CURRENT → will advance $DIFF messages"
      elif [[ "$DIFF" -lt 0 ]]; then
        log "WARNING" "$topic:$partition saved=$offset, current=$CURRENT → will rewind $((-DIFF)) messages"
      else
        log "INFO" "$topic:$partition is already at correct offset"
      fi
    fi
  done < "$OFFSET_FILE"

  if ! $DRY_RUN; then
    $KAFKA_GROUP_TOOL --group "$GROUP" --reset-offsets --from-file "$OFFSET_FILE" --execute
    log "DONE" "Offsets restored."
  fi
}

# Reset
reset_offsets() {
  log "INFO" "Resetting offsets for group $GROUP on topic $TOPIC to latest..."

  if $DRY_RUN; then
    log "INFO" "Performing dry run of reset to latest with comparison to backed up offsets..."

    if [[ ! -f "$OFFSET_FILE" ]]; then
      log "WARNING" "Offset backup file not found: $OFFSET_FILE"
    fi

    $KAFKA_GROUP_TOOL --group "$GROUP" --reset-offsets --topic "$TOPIC" --to-latest --dry-run | while read -r line; do
      if echo "$line" | grep -q "$TOPIC"; then
        PART=$(echo "$line" | awk '{print $3}')
        OFFSET=$(echo "$line" | awk '{print $4}')
        BACKUP=$(grep "$TOPIC,$PART" "$OFFSET_FILE" 2>/dev/null | cut -d, -f3)

        if [[ -n "$BACKUP" ]]; then
          DIFF=$((OFFSET - BACKUP))
          if [[ "$DIFF" -gt 0 ]]; then
            log "INFO" "$TOPIC:$PART saved=$BACKUP, latest=$OFFSET → would skip $DIFF messages"
          elif [[ "$DIFF" -lt 0 ]]; then
            log "WARNING" "$TOPIC:$PART saved=$BACKUP, latest=$OFFSET → would rewind $((-DIFF)) messages (unusual!)"
          else
            log "INFO" "$TOPIC:$PART no change in offset"
          fi
        else
          log "WARNING" "$TOPIC:$PART latest=$OFFSET → no backup available"
        fi
      fi
    done
  else
    $KAFKA_GROUP_TOOL --group "$GROUP" --reset-offsets --topic "$TOPIC" --to-latest --execute
    log "DONE" "Offsets reset to latest."
  fi
}

# Run selected action
case "$ACTION" in
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
    log "ERROR" "Usage: $0 {backup|restore|reset} <topic> <group> [--dry-run] [--bootstrap-server <server>] [--config <file>]"
    exit 1
    ;;
esac