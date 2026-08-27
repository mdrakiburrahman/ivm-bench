#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=pins.env
source "$SCRIPT_DIR/pins.env"

if [[ "${1:-}" == "--print-image" ]]; then
  printf '%s\n' "$RISINGWAVE_PINNED_IMAGE"
  exit 0
fi

if docker image inspect "$RISINGWAVE_PINNED_IMAGE" >/dev/null 2>&1; then
  cached_sha="$(docker image inspect \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
    "$RISINGWAVE_PINNED_IMAGE")"
  if [[ "$cached_sha" != "$RISINGWAVE_GIT_SHA" ]]; then
    echo "ERROR: cached $RISINGWAVE_PINNED_IMAGE labels revision $cached_sha, expected $RISINGWAVE_GIT_SHA" >&2
    exit 1
  fi
  echo "Using cached pinned RisingWave image: $RISINGWAVE_PINNED_IMAGE"
  exit 0
fi

BUILD_ROOT="${RUNNER_TEMP:-/tmp}/ivm-bench-risingwave-$RISINGWAVE_GIT_SHA"
BUILDER_IMAGE="ivm-bench/risingwave-builder:$RISINGWAVE_GIT_SHA"

cleanup_source() {
  rm -rf "$BUILD_ROOT"
}
trap cleanup_source EXIT
cleanup_source

echo "Cloning $RISINGWAVE_GIT_REPOSITORY at $RISINGWAVE_GIT_SHA"
mkdir -p "$BUILD_ROOT"
git -C "$BUILD_ROOT" init
git -C "$BUILD_ROOT" remote add origin "$RISINGWAVE_GIT_REPOSITORY"
git -C "$BUILD_ROOT" fetch --depth 1 --filter=blob:none origin "$RISINGWAVE_GIT_SHA"
git -C "$BUILD_ROOT" checkout --detach FETCH_HEAD

resolved_sha="$(git -C "$BUILD_ROOT" rev-parse HEAD)"
if [[ "$resolved_sha" != "$RISINGWAVE_GIT_SHA" ]]; then
  echo "ERROR: requested $RISINGWAVE_GIT_SHA but checked out $resolved_sha" >&2
  exit 1
fi

# The upstream target builds only the Rust binary and avoids rebuilding the
# unchanged Java connector layer. Its final RUN performs cargo clean, so the
# builder image does not retain the large target directory.
docker build \
  --file "$BUILD_ROOT/docker/Dockerfile" \
  --target rust-builder \
  --build-arg "GIT_SHA=$RISINGWAVE_GIT_SHA" \
  --build-arg "CARGO_PROFILE=production" \
  --tag "$BUILDER_IMAGE" \
  "$BUILD_ROOT"

docker build \
  --file "$SCRIPT_DIR/Dockerfile.pinned" \
  --build-arg "RISINGWAVE_BUILDER_IMAGE=$BUILDER_IMAGE" \
  --build-arg "RISINGWAVE_BASE_IMAGE=$RISINGWAVE_BASE_IMAGE" \
  --build-arg "RISINGWAVE_GIT_SHA=$RISINGWAVE_GIT_SHA" \
  --tag "$RISINGWAVE_PINNED_IMAGE" \
  "$SCRIPT_DIR"

docker image rm "$BUILDER_IMAGE" >/dev/null
echo "Built pinned RisingWave image: $RISINGWAVE_PINNED_IMAGE"
