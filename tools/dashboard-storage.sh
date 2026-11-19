#!/usr/bin/env bash
set -euo pipefail

for name in BENCHMARK_STORAGE_ACCOUNT_NAME BENCHMARK_STORAGE_ACCOUNT_KEY; do
  if [[ -z "${!name:-}" ]]; then
    echo "::error::Missing $name" >&2
    exit 1
  fi
done
[[ "$BENCHMARK_STORAGE_ACCOUNT_NAME" == ivmbench ]] || { echo "Unexpected dashboard account" >&2; exit 1; }
export AZURE_STORAGE_ACCOUNT="$BENCHMARK_STORAGE_ACCOUNT_NAME"
export AZURE_STORAGE_KEY="$BENCHMARK_STORAGE_ACCOUNT_KEY"

case "${1:-}" in
  cors)
    origins=(https://ivmbench.z9.web.core.windows.net http://localhost:4175 http://127.0.0.1:4175)
    methods=(GET HEAD OPTIONS)
    allowed=(Range If-Match If-None-Match)
    exposed=(ETag Content-Range Content-Length Accept-Ranges Content-Type)
    join_csv() { local IFS=,; echo "$*"; }
    if az storage cors list --services b --only-show-errors -o json | jq -e \
      --arg origins "$(join_csv "${origins[@]}")" \
      --arg methods "$(join_csv "${methods[@]}")" \
      --arg allowed "$(join_csv "${allowed[@]}")" \
      --arg exposed "$(join_csv "${exposed[@]}")" \
      'def normalized: if type == "string" then split(",") else . end | map(ascii_downcase) | sort;
       any(.[]; (.allowedOrigins|normalized) == ($origins|normalized)
         and (.allowedMethods|normalized) == ($methods|normalized)
         and (.allowedHeaders|normalized) == ($allowed|normalized)
         and (.exposedHeaders|normalized) == ($exposed|normalized)
         and (.maxAgeInSeconds == 3600))' >/dev/null; then
      exit 0
    fi
    az storage cors add --services b --origins "${origins[@]}" --methods "${methods[@]}" \
      --allowed-headers "${allowed[@]}" --exposed-headers "${exposed[@]}" --max-age 3600 --only-show-errors -o none
    ;;
  deploy)
    dist="${2:?Expected website dist directory}"
    [[ -s "$dist/index.html" && -d "$dist/assets" && ! -e "$dist/data" ]] || { echo "Invalid production build" >&2; exit 1; }
    az storage blob delete-batch --source '$web' --only-show-errors -o none
    az storage blob upload-batch --source "$dist" --destination '$web' --overwrite true --only-show-errors -o none
    az storage blob upload --file "$dist/index.html" --container-name '$web' --name 404.html \
      --content-type text/html --overwrite true --only-show-errors -o none
    ;;
  *) echo "Usage: dashboard-storage.sh {cors|deploy DIST}" >&2; exit 2 ;;
esac
