#!/usr/bin/env bash
set -euo pipefail

# Prevent Git Bash / MSYS from rewriting API-style paths like /v1/responses
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

INPUT_FILE="${1:-pilot_outputs/gdc_batch_input_test2.jsonl}"
OUT_DIR="${2:-pilot_outputs}"
COMPLETION_WINDOW="${3:-24h}"
POLL_SECONDS="${4:-30}"

if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "ERROR: OPENAI_API_KEY is not set." >&2
  exit 1
fi

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "ERROR: Input file not found: $INPUT_FILE" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required but not installed or not on PATH." >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BASENAME="$(basename "$INPUT_FILE" .jsonl)"

UPLOAD_JSON="${OUT_DIR}/${BASENAME}_upload_${STAMP}.json"
BATCH_CREATE_JSON="${OUT_DIR}/${BASENAME}_batch_create_${STAMP}.json"
BATCH_STATUS_JSON="${OUT_DIR}/${BASENAME}_batch_status_${STAMP}.json"
OUTPUT_JSONL="${OUT_DIR}/${BASENAME}_output_${STAMP}.jsonl"
ERROR_JSONL="${OUT_DIR}/${BASENAME}_error_${STAMP}.jsonl"
MANIFEST_JSON="${OUT_DIR}/${BASENAME}_manifest_${STAMP}.json"

echo "Uploading batch input file: $INPUT_FILE"
curl -sS https://api.openai.com/v1/files \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -F "purpose=batch" \
  -F "file=@${INPUT_FILE}" \
  > "$UPLOAD_JSON"

INPUT_FILE_ID="$(jq -r '.id // empty' "$UPLOAD_JSON")"

if [[ -z "$INPUT_FILE_ID" ]]; then
  echo "ERROR: Failed to get input_file_id from upload response." >&2
  cat "$UPLOAD_JSON" >&2
  exit 1
fi

echo "Uploaded file id: $INPUT_FILE_ID"

echo "Creating batch..."
curl -sS https://api.openai.com/v1/batches \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"input_file_id\": \"$INPUT_FILE_ID\",
    \"endpoint\": \"/v1/responses\",
    \"completion_window\": \"$COMPLETION_WINDOW\"
  }" \
  > "$BATCH_CREATE_JSON"

BATCH_ID="$(jq -r '.id // empty' "$BATCH_CREATE_JSON")"

if [[ -z "$BATCH_ID" ]]; then
  echo "ERROR: Failed to create batch." >&2
  cat "$BATCH_CREATE_JSON" >&2
  exit 1
fi

echo "Batch id: $BATCH_ID"

STATUS=""
OUTPUT_FILE_ID=""
ERROR_FILE_ID=""

echo "Polling batch status every ${POLL_SECONDS}s..."
while true; do
  curl -sS "https://api.openai.com/v1/batches/${BATCH_ID}" \
    -H "Authorization: Bearer $OPENAI_API_KEY" \
    > "$BATCH_STATUS_JSON"

  STATUS="$(jq -r '.status // empty' "$BATCH_STATUS_JSON")"
  OUTPUT_FILE_ID="$(jq -r '.output_file_id // empty' "$BATCH_STATUS_JSON")"
  ERROR_FILE_ID="$(jq -r '.error_file_id // empty' "$BATCH_STATUS_JSON")"

  echo "Current status: ${STATUS:-unknown}"

  case "$STATUS" in
    completed)
      break
      ;;
    failed|expired|cancelled)
      echo "ERROR: Batch ended with terminal status: $STATUS" >&2
      cat "$BATCH_STATUS_JSON" >&2
      ;;
    validating|in_progress|finalizing)
      sleep "$POLL_SECONDS"
      ;;
    *)
      echo "ERROR: Unexpected batch status: ${STATUS:-<empty>}" >&2
      cat "$BATCH_STATUS_JSON" >&2
      exit 1
      ;;
  esac
done

if [[ -n "$OUTPUT_FILE_ID" ]]; then
  echo "Downloading output file: $OUTPUT_FILE_ID"
  curl -sS "https://api.openai.com/v1/files/${OUTPUT_FILE_ID}/content" \
    -H "Authorization: Bearer $OPENAI_API_KEY" \
    -o "$OUTPUT_JSONL"
  echo "Saved output to: $OUTPUT_JSONL"
else
  echo "No output_file_id found."
fi

if [[ -n "$ERROR_FILE_ID" ]]; then
  echo "Downloading error file: $ERROR_FILE_ID"
  curl -sS "https://api.openai.com/v1/files/${ERROR_FILE_ID}/content" \
    -H "Authorization: Bearer $OPENAI_API_KEY" \
    -o "$ERROR_JSONL"
  echo "Saved error file to: $ERROR_JSONL"
fi

jq -n \
  --arg timestamp_utc "$STAMP" \
  --arg input_file "$INPUT_FILE" \
  --arg input_file_id "$INPUT_FILE_ID" \
  --arg batch_id "$BATCH_ID" \
  --arg status "$STATUS" \
  --arg output_file_id "$OUTPUT_FILE_ID" \
  --arg error_file_id "$ERROR_FILE_ID" \
  --arg output_jsonl "$OUTPUT_JSONL" \
  --arg error_jsonl "$ERROR_JSONL" \
  '{
    timestamp_utc: $timestamp_utc,
    input_file: $input_file,
    input_file_id: $input_file_id,
    batch_id: $batch_id,
    status: $status,
    output_file_id: $output_file_id,
    error_file_id: $error_file_id,
    output_jsonl: $output_jsonl,
    error_jsonl: $error_jsonl
  }' > "$MANIFEST_JSON"

echo
echo "Done."
echo "Manifest: $MANIFEST_JSON"
echo "Batch create response: $BATCH_CREATE_JSON"
echo "Final batch status: $BATCH_STATUS_JSON"