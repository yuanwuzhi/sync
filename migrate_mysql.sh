#!/bin/bash

### ========== Global Variables ==========

# Source MySQL
SRC_MYSQL_HOST="127.0.0.1"
SRC_MYSQL_PORT="3306"
SRC_MYSQL_USER="root"
SRC_MYSQL_PASS="root"
SRC_MYSQL_DB="D2VM"

# Destination MySQL
DST_MYSQL_HOST="43.138.201.159"
DST_MYSQL_PORT="3306"
DST_MYSQL_USER="root"
DST_MYSQL_PASS="mysql_pQPFdS"
DST_MYSQL_DB="gangkeguang_backup"  # Added this line for target DB name

# Cloud Server SSH
CLOUD_SSH_USER="ubuntu"
CLOUD_SSH_HOST="43.138.201.159"
CLOUD_SSH_PORT="22"
CLOUD_SSH_PASS="flectrag@2024"

# Files
DUMP_FILE="db_backup_$(date +%Y%m%d_%H%M%S).sql"
REMOTE_TMP_PATH="/tmp/$DUMP_FILE"

### ========== Functions ==========

exit_on_error() {
  echo "Error: $1"
  exit 1
}

step() {
  echo "$1"
}

dump_database() {
  step "Dumping local database [$SRC_MYSQL_DB]..."
  mysqldump -h"$SRC_MYSQL_HOST" -P"$SRC_MYSQL_PORT" -u"$SRC_MYSQL_USER" -p"$SRC_MYSQL_PASS" \
    --add-drop-table --default-character-set=utf8mb4 --single-transaction --quick "$SRC_MYSQL_DB" > "$DUMP_FILE" \
    || exit_on_error "Dump failed. Check source DB connection or permissions."
  echo "Dump successful. File: $DUMP_FILE"
}

upload_to_cloud() {
  step "Uploading SQL file to cloud server [$CLOUD_SSH_HOST]..."
  sshpass -p "$CLOUD_SSH_PASS" scp -o StrictHostKeyChecking=no -P "$CLOUD_SSH_PORT" "$DUMP_FILE" "$CLOUD_SSH_USER@$CLOUD_SSH_HOST:$REMOTE_TMP_PATH" \
    || exit_on_error "Upload failed. Check SSH config or network connection."
  echo "Upload successful: $REMOTE_TMP_PATH"
}

import_to_cloud_mysql() {
  step "Preparing to rebuild database [$DST_MYSQL_DB] on cloud server and import data..."

  sshpass -p "$CLOUD_SSH_PASS" ssh -o StrictHostKeyChecking=no -p "$CLOUD_SSH_PORT" "$CLOUD_SSH_USER@$CLOUD_SSH_HOST" bash <<EOF
mysql -h"$DST_MYSQL_HOST" -P"$DST_MYSQL_PORT" -u"$DST_MYSQL_USER" -p"$DST_MYSQL_PASS" -e "
DROP DATABASE IF EXISTS \\\`$DST_MYSQL_DB\\\`;
CREATE DATABASE \\\`$DST_MYSQL_DB\\\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
if [ \$? -ne 0 ]; then
  echo "Failed to rebuild remote MySQL database. Check target DB permissions"
  exit 1
fi

mysql -h"$DST_MYSQL_HOST" -P"$DST_MYSQL_PORT" -u"$DST_MYSQL_USER" -p"$DST_MYSQL_PASS" "$DST_MYSQL_DB" < "$REMOTE_TMP_PATH"
if [ \$? -ne 0 ]; then
  echo "Failed to import SQL file. Check SQL content or remote permissions"
  exit 2
fi
echo "Data successfully imported to target MySQL"
EOF
}

cleanup_local() {
  step "Cleaning up local temp files..."
  rm -f "$DUMP_FILE"
  echo "Cleanup complete"
}

### ========== Main Process ==========

echo "MySQL Data Migration Tool: Full Export and Remote Import"
echo "----------------------------------------------"

dump_database
upload_to_cloud
import_to_cloud_mysql
cleanup_local

echo "All operations completed! Database [$SRC_MYSQL_DB] successfully migrated to [$DST_MYSQL_DB] on remote server [$CLOUD_SSH_HOST]"
