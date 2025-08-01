# Kafka Offset Manager

A utility script for managing Kafka consumer group offsets with backup, restore, and reset capabilities.

## Prerequisites

- Kafka CLI tools (`kafka-consumer-groups.sh`, `kafka-run-class.sh`) must be available in your PATH or in the current directory
- Access to a Kafka cluster
- Optional: Kafka configuration file for authentication/SSL settings

## Installation

### Option 1: Download directly to Kafka bin directory (Recommended)

Navigate to your Kafka installation's bin directory and download the script:

```bash
cd kafka_2.13-3.5.1/bin/
wget --no-cache -O offset-manager.sh https://raw.githubusercontent.com/vyle-lxp/support-tool/main/offset-manager.sh && chmod +x offset-manager.sh
```

This places the script alongside your Kafka CLI tools, making it easy to use without path issues.

### Option 2: Download to any directory

```bash
wget --no-cache -O offset-manager.sh https://raw.githubusercontent.com/vyle-lxp/support-tool/main/offset-manager.sh && chmod +x offset-manager.sh
```

Note: If you choose this option, ensure Kafka CLI tools are in your PATH or adjust the script paths accordingly.

## Usage

```bash
./offset-manager.sh <command> <topic> <consumer-group> [options]
```

### Commands

- **backup**: Save current consumer group offsets to a CSV file
- **restore**: Restore offsets from a previously saved CSV file
- **reset**: Reset offsets to latest available messages

### Options

- `--bootstrap-server <server>`: Kafka bootstrap server (default: localhost:9092)
- `--config <file>`: Path to Kafka configuration file
- `--dry-run`: Show what would be done without making changes (available for restore and reset)

## Examples

### Backup Current Offsets

```bash
./offset-manager.sh backup audit-native audit \
  --bootstrap-server localhost:9092 \
  --config kafka.properties
```

This will:
- Connect to local Kafka cluster
- Save current offsets for the `audit` consumer group on `audit-native` topic
- Create a backup file named `audit-offsets.csv`

### Reset Offsets to Latest (Dry Run)

```bash
./offset-manager.sh reset audit-native audit --dry-run \
  --bootstrap-server localhost:9092 \
  --config kafka.properties
```

This will:
- Show what offsets would be changed without actually changing them
- Compare current offsets with latest available offsets
- Display the number of messages that would be skipped

### Restore Previous Offsets (Dry Run)

```bash
./offset-manager.sh restore audit-native audit --dry-run \
  --bootstrap-server localhost:9092 \
  --config kafka.properties
```

This will:
- Show what offsets would be restored from the backup file
- Compare saved offsets with current offsets
- Display the difference without making changes

### Restore Previous Offsets (Execute)

```bash
./offset-manager.sh restore audit-native audit \
  --bootstrap-server localhost:9092 \
  --config kafka.properties
```

This will:
- Actually restore the offsets from the backup file
- Apply the changes to the consumer group

## Workflow Example

1. **Backup current state** before making changes:
   ```bash
   ./offset-manager.sh backup my-topic my-consumer-group
   ```

2. **Reset to latest** (if you want to skip all pending messages):
   ```bash
   # First, see what would change
   ./offset-manager.sh reset my-topic my-consumer-group --dry-run
   
   # Then execute if satisfied
   ./offset-manager.sh reset my-topic my-consumer-group
   ```

3. **Restore previous state** if needed:
   ```bash
   # Check what would be restored
   ./offset-manager.sh restore my-topic my-consumer-group --dry-run
   
   # Restore if needed
   ./offset-manager.sh restore my-topic my-consumer-group
   ```

## Files

- Backup files are saved as `<consumer-group>-offsets.csv` in the current directory
- Format: `topic,partition,offset` (one line per partition)

## Notes

- Always use `--dry-run` first to understand the impact of restore/reset operations
- The backup file contains offsets for all partitions of the specified topic
- Make sure the consumer group is not actively consuming when modifying offsets
- For clusters requiring authentication, use the `--config` option with appropriate properties file
