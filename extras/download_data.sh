#!/bin/bash

# Configuration
FILE_EXTENSION="csv"
USE_HEADERS="false"
DATA_DIR="../data"
TIMEOUT=2400                            # 2400 seconds = 40 minutes
RETRY_COUNT=3
LOG_FILE="./logs/download_log.log"

# Colors for terminal output (optional)
RESET="\033[0m"
GREEN="\033[32m"
YELLOW="\033[33m"
RED="\033[31m"
BLUE="\033[34m"

function log() {
    local log_level="$1"
    shift
    local message="$*"
    
    case "$log_level" in
        INFO)
            decorator="${GREEN}==>[INFO]${RESET}"
            ;;
        WARNING)
            decorator="${YELLOW}==>[WARNING]${RESET}"
            ;;
        ERROR)
            decorator="${RED}==>[ERROR]${RESET}"
            ;;
        DEBUG)
            decorator="${BLUE}==>[DEBUG]${RESET}"
            ;;
        *)
            decorator="==>[LOG]"
            ;;
    esac

    echo -e "$(date '+%Y-%m-%d %H:%M:%S') ${decorator} $message" >> "$LOG_FILE"
}

log "INFO" "Starting download process"

declare -A FILES_TO_DOWNLOAD=(
    ["reseau-hta.csv"]="https://data.enedis.fr/explore/dataset/reseau-hta/download/?format=${FILE_EXTENSION}&use_labels_for_header=${USE_HEADERS}&epsg=4326"
    ["reseau-bt.csv"]="https://data.enedis.fr/explore/dataset/reseau-bt/download/?format=${FILE_EXTENSION}&use_labels_for_header=${USE_HEADERS}&epsg=4326"
    ["reseau-souterrain-bt.csv"]="https://data.enedis.fr/explore/dataset/reseau-souterrain-bt/download/?format=${FILE_EXTENSION}&use_labels_for_header=${USE_HEADERS}&epsg=4326"
    ["reseau-souterrain-hta.csv"]="https://data.enedis.fr/explore/dataset/reseau-souterrain-hta/download/?format=${FILE_EXTENSION}&use_labels_for_header=${USE_HEADERS}&epsg=4326"
    ["poste-source.csv"]="https://data.enedis.fr/explore/dataset/poste-source/download/?format=${FILE_EXTENSION}&use_labels_for_header=${USE_HEADERS}&epsg=4326"
    ["poste-electrique.csv"]="https://data.enedis.fr/explore/dataset/poste-electrique/download/?format=${FILE_EXTENSION}&use_labels_for_header=${USE_HEADERS}&epsg=4326"
    ["position-geographique-des-poteaux-hta-et-bt.csv"]="https://data.enedis.fr/explore/dataset/position-geographique-des-poteaux-hta-et-bt/download/?format=${FILE_EXTENSION}&use_labels_for_header=${USE_HEADERS}&epsg=4326"
)

log "INFO" "Creating data directory: $DATA_DIR"
mkdir -p "$DATA_DIR"

function download_file() {
    local url=$1
    local output_file=$2
    local attempt=1

    while (( attempt <= RETRY_COUNT )); do
        curl -o "$output_file" --connect-timeout 60 --max-time $TIMEOUT --retry 3 "$url"

        if [[ $? -eq 0 ]]; then
            log "INFO" "Downloaded $url to $output_file"
            return 0
        else
            log "WARNING" "Attempt $attempt failed for $url"
            ((attempt++))
            sleep 5  
        fi
    done
    
    log "ERROR" "Failed to download $url"
    return 1
}

for filename in "${!FILES_TO_DOWNLOAD[@]}"; do
    url="${FILES_TO_DOWNLOAD[$filename]}"
    output_path="$DATA_DIR/$filename"

    if ! download_file "$url" "$output_path"; then
        log "ERROR" "Failed to download $filename"
    fi
done

log "INFO" "Download process completed"
