#!/bin/bash

backup_generated_configs() {
  CONFIG_BACKUP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/td_hotstuff_config_backup.XXXXXX")
  for file in "${CONFIG_FILES_TO_RESTORE[@]}"; do
    if [ -f "$file" ]; then
      mkdir -p "$CONFIG_BACKUP_DIR/$(dirname "$file")"
      cp -p "$file" "$CONFIG_BACKUP_DIR/$file"
    fi
  done
}

restore_generated_configs() {
  if [ -z "${CONFIG_BACKUP_DIR:-}" ]; then
    return
  fi
  for file in "${CONFIG_FILES_TO_RESTORE[@]}"; do
    if [ -f "$CONFIG_BACKUP_DIR/$file" ]; then
      mkdir -p "$(dirname "$file")"
      cp -p "$CONFIG_BACKUP_DIR/$file" "$file"
    fi
  done
  rm -rf "$CONFIG_BACKUP_DIR"
}
