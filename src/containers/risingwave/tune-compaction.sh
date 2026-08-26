#!/usr/bin/env bash

# Keep the write-heavy Hummock levels on a fast codec. RisingWave creates
# compaction groups while materialized views are backfilled, so a one-shot
# startup command misses most of this workload. This loop only updates groups
# whose first three levels are not already configured as requested.

set -uo pipefail

RW_CTL_BIN=${RW_CTL_BIN:-/risingwave/bin/risingwave}
RW_TUNER_READY_FILE=${RW_TUNER_READY_FILE:-/tmp/risingwave-tuner-ready}
export RW_META_ADDR=${RW_META_ADDR:-http://127.0.0.1:5690}
COMPRESSION_ALGORITHM=Lz4
TUNER_INTERVAL_SECONDS=${RW_TUNER_INTERVAL_SECONDS:-15}
TUNER_RETRY_SECONDS=2

groups_needing_tuning() {
	"${RW_CTL_BIN}" ctl hummock list-compaction-group 2>/dev/null | awk \
		-v expected="\"${COMPRESSION_ALGORITHM}\"," '
		/^[[:space:]]+id:/ {
			id = $2
			sub(/,$/, "", id)
		}
		/compression_algorithm: \[/ {
			needs_update = 0
			for (level = 0; level < 3; level++) {
				if (getline <= 0 || $1 != expected) {
					needs_update = 1
				}
			}
			if (needs_update && id != "") {
				if (ids != "") {
					ids = ids ","
				}
				ids = ids id
			}
		}
		END { print ids }
	'
}

while true; do
	if ! group_ids=$(groups_needing_tuning); then
		echo "RisingWave compaction tuner: ctl query failed; retrying" >&2
		sleep "${TUNER_RETRY_SECONDS}"
		continue
	fi

	updated=1
	if [[ -n "${group_ids}" ]]; then
		for level in 0 1 2; do
			if ! "${RW_CTL_BIN}" ctl hummock \
				update-compaction-config \
				--compaction-group-ids "${group_ids}" \
				--compression-level "${level}" \
				--compression-algorithm "${COMPRESSION_ALGORITHM}"; then
				updated=0
				break
			fi
		done
		if [[ "${updated}" -eq 1 ]]; then
			echo "RisingWave compaction tuner: set levels 0-2 to ${COMPRESSION_ALGORITHM} for groups ${group_ids}"
		fi
	fi

	if [[ "${updated}" -eq 1 ]]; then
		touch "${RW_TUNER_READY_FILE}"
		if [[ "${RW_TUNER_ONCE:-0}" == "1" ]]; then
			exit 0
		fi
		sleep "${TUNER_INTERVAL_SECONDS}"
	else
		sleep "${TUNER_RETRY_SECONDS}"
	fi
done
