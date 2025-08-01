#!/bin/bash
set -euo pipefail

log_info() { echo "[INFO] $1"; }
log_warn() { echo "[WARNING] $1"; }
log_error() { echo "[ERROR] $1"; }

usage() {
  echo "Usage: $0 {backup|reset|restore} <topic> <group> [--dry-run] [--bootstrap-server <addr>] [--config <file>]"
  exit 1
}

cmd=$1; topic=$2; group=$3
shift 3

dry_run=false; bootstrap="localhost:9092"; config_flag=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run) dry_run=true; shift ;;
    --bootstrap-server) bootstrap="$2"; shift 2 ;;
    --config) config_flag="--command-config $2"; shift 2 ;;
    *) log_error "Unknown option: $1"; usage ;;
  esac
done

offset_file="./${group}-offsets.csv"

backup() {
  log_info "Backing up offsets for group '$group' on topic '$topic'..."
  ./kafka-consumer-groups.sh --bootstrap-server "$bootstrap" $config_flag --group "$group" --describe \
   | awk -v t="$topic" '$2 == t { print $2 "," $3 "," $4 }' > "$offset_file"
  log_info "Backup written to $offset_file"
}

reset() {
  log_info "Resetting offsets for group '$group' on topic '$topic' to latest..."
  if $dry_run; then
    log_info "(dry-run) showing comparison vs backup"
    [[ -f "$offset_file" ]] || { log_warn "Backup file not found: $offset_file"; return; }
    while IFS=, read -r t p saved; do
      latest=$(./kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list "$bootstrap" \
        --topic "$t" --partitions "$p" --time -1 | awk -F: -v part="$p" '$2 == part { print $3 }')
      if [[ -z "$latest" ]]; then
        log_warn "$t:$p → cannot fetch latest offset"
      else
        delta=$((latest - saved))
        if (( delta > 0 )); then
          log_warn "$t:$p saved=$saved, latest=$latest → skip $delta"
        elif (( delta < 0 )); then
          log_warn "$t:$p saved=$saved, latest=$latest → rewind $((-delta))"
        else
          log_info "$t:$p saved=$saved, latest=$latest → no change"
        fi
      fi
    done < "$offset_file"
  else
    ./kafka-consumer-groups.sh --bootstrap-server "$bootstrap" $config_flag --group "$group" --topic "$topic" --reset-offsets --to-latest --execute
    log_info "Offsets reset to latest"
  fi
}

restore() {
  log_info "Restoring offsets from $offset_file..."
  [[ -f "$offset_file" ]] || { log_error "Backup file missing: $offset_file"; exit 1; }
  if $dry_run; then
    log_info "(dry-run) comparing current vs backup"
    while IFS=, read -r t p saved; do
      current=$(./kafka-consumer-groups.sh --bootstrap-server "$bootstrap" $config_flag --group "$group" --describe \
        | awk -v ti="$t" -v pa="$p" '$2 == ti && $3 == pa { print $4 }')
      if [[ -z "$current" ]]; then
        log_warn "$t:$p saved=$saved, current=? → cannot fetch current"
      else
        delta=$((saved - current))
        action="advance"
        (( delta < 0 )) && delta=$((-delta)) && action="rewind"
        log_warn "$t:$p saved=$saved, current=$current → $action $delta"
      fi
    done < "$offset_file"
  else
    ./kafka-consumer-groups.sh --bootstrap-server "$bootstrap" $config_flag --group "$group" --reset-offsets --from-file "$offset_file" --execute
    log_info "Offsets restored"
  fi
}

case "$cmd" in
  backup)  backup ;;
  reset)   reset ;;
  restore) restore ;;
  *)       usage ;;
esac