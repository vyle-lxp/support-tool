#!/bin/bash

set -e

# Helper functions
log_info() { echo -e "[INFO] $1"; }
log_warn() { echo -e "[WARNING] $1"; }
log_error() { echo -e "[ERROR] $1"; }

usage() {
  echo "Usage:"
  echo "  $0 backup <topic> <group> [--bootstrap-server <broker>] [--config <file>]"
  echo "  $0 restore <topic> <group> [--dry-run] [--bootstrap-server <broker>] [--config <file>]"
  echo "  $0 reset <topic> <group> [--dry-run] [--bootstrap-server <broker>] [--config <file>]"
  exit 1
}

# Parse positional args
COMMAND=$1
TOPIC=$2
GROUP=$3
shift 3

# Default options
BOOTSTRAP_SERVER="localhost:9092"
COMMAND_CONFIG=""
DRY_RUN=false

# Parse flags
while [[ $# -gt 0 ]]; do
  case $1 in
    --bootstrap-server)
      BOOTSTRAP_SERVER="$2"
      shift 2
      ;;
    --config)
      COMMAND_CONFIG="--command-config $2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      log_error "Unknown option: $1"
      usage
      ;;
  esac
done

OFFSET_FILE="./${GROUP}-offsets.csv"

backup_offsets() {
  log_info "Backing up offsets for group $GROUP and topic $TOPIC..."
  ./kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    $COMMAND_CONFIG \
    --group "$GROUP" \
    --describe \
    | awk -v topic="$TOPIC" '$2 == topic { print $2 "," $3 "," $4 }' > "$OFFSET_FILE"
  log_info "Offsets saved to $OFFSET_FILE"
}

restore_offsets() {
  if [ ! -f "$OFFSET_FILE" ]; then
    log_error "Offset backup file not found: $OFFSET_FILE"
    exit 1
  fi

  log_info "Restoring offsets from $OFFSET_FILE..."

  if $DRY_RUN; then
    log_info "Performing dry run of offset restore..."
    while IFS=, read -r topic partition saved_offset; do
      current_offset=$(./kafka-consumer-groups.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        $COMMAND_CONFIG \
        --group "$GROUP" \
        --describe \
        | awk -v t="$topic" -v p="$partition" '$2==t && $3==p { print $4 }')

      if [[ -z "$current_offset" ]]; then
        log_warn "$topic:$partition saved=$saved_offset, current=? → current offset not found"
      else
        delta=$((saved_offset - current_offset))
        if (( delta > 0 )); then
          log_warn "$topic:$partition saved=$saved_offset, current=$current_offset → will advance $delta messages"
        elif (( delta < 0 )); then
          log_warn "$topic:$partition saved=$saved_offset, current=$current_offset → will rewind $((-delta)) messages"
        else
          log_info "$topic:$partition saved=$saved_offset, current=$current_offset → no change"
        fi
      fi
    done < "$OFFSET_FILE"
  else
    ./kafka-consumer-groups.sh \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      $COMMAND_CONFIG \
      --group "$GROUP" \
      --reset-offsets \
      --from-file "$OFFSET_FILE" \
      --execute
    log_info "Offsets restored from $OFFSET_FILE"
  fi
}

reset_offsets() {
  log_info "Resetting offsets for group $GROUP on topic $TOPIC to latest..."

  if $DRY_RUN; then
    log_info "Performing dry run of reset to latest with comparison to backed up offsets..."

    if [ ! -f "$OFFSET_FILE" ]; then
      log_warn "Offset backup file not found: $OFFSET_FILE"
      return
    fi

    while IFS=, read -r topic partition saved_offset; do
      latest_offset=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list "$BOOTSTRAP_SERVER" \
        --topic "$topic" \
        --partitions "$partition" \
        --time -1 \
        | grep "$topic:$partition" | cut -d: -f3)

      if [[ -z "$latest_offset" ]]; then
        log_warn "$topic:$partition → could not fetch latest offset"
      else
        delta=$((latest_offset - saved_offset))
        if (( delta > 0 )); then
          log_warn "$topic:$partition saved=$saved_offset, latest=$latest_offset → would advance $delta messages"
        elif (( delta < 0 )); then
          log_warn "$topic:$partition saved=$saved_offset, latest=$latest_offset → would rewind $((-delta)) messages"
        else
          log_info "$topic:$partition saved=$saved_offset, latest=$latest_offset → no change"
        fi
      fi
    done < "$OFFSET_FILE"
  else
    ./kafka-consumer-groups.sh \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      $COMMAND_CONFIG \
      --group "$GROUP" \
      --topic "$TOPIC" \
      --reset-offsets \
      --to-latest \
      --execute
    log_info "Offsets reset to latest"
  fi
}

# Command dispatch
case $COMMAND in
  backup)  backup_offsets ;;
  restore) restore_offsets ;;
  reset)   reset_offsets ;;
  *)       usage ;;
esac