#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   GITHUB_TOKEN=... ./scripts/create_github_release.sh v0.1.0 /absolute/path/to/release.zip "Release notes"
#
# Creates a GitHub release in Yuchen971/Kodashboard and uploads the provided asset.

if [[ $# -lt 2 ]]; then
  echo "Usage: GITHUB_TOKEN=... $0 <tag> <asset_zip> [release_notes]" >&2
  exit 1
fi

TAG="$1"
ASSET="$2"
NOTES="${3:-UI refresh release}"
OWNER="Yuchen971"
REPO="Kodashboard"
API="https://api.github.com/repos/${OWNER}/${REPO}"

if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "GITHUB_TOKEN is required" >&2
  exit 1
fi

if [[ ! -f "$ASSET" ]]; then
  echo "Asset not found: $ASSET" >&2
  exit 1
fi

NAME="${TAG}"
ASSET_NAME="$(basename "$ASSET")"

RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$RESPONSE_FILE"' EXIT

curl -sS -X POST \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "Accept: application/vnd.github+json" \
  "$API/releases" \
  -d "$(printf '{"tag_name":"%s","name":"%s","body":"%s","draft":false,"prerelease":false}' "$TAG" "$NAME" "${NOTES//\"/\\\"}")" \
  > "$RESPONSE_FILE"

UPLOAD_URL="$(sed -n 's/.*"upload_url": *"\([^"]*\){?name,label}.*/\1/p' "$RESPONSE_FILE" | head -1)"
RELEASE_URL="$(sed -n 's/.*"html_url": *"\([^"]*\)".*/\1/p' "$RESPONSE_FILE" | head -1)"

if [[ -z "$UPLOAD_URL" ]]; then
  echo "Failed to create release. Response:" >&2
  cat "$RESPONSE_FILE" >&2
  exit 1
fi

curl -sS -X POST \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "Content-Type: application/zip" \
  -H "Accept: application/vnd.github+json" \
  "${UPLOAD_URL}?name=${ASSET_NAME}" \
  --data-binary @"$ASSET" > /dev/null

echo "Release created: ${RELEASE_URL:-https://github.com/${OWNER}/${REPO}/releases}"
