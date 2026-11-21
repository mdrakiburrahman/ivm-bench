"""Count temporal intermediates from already eligible engagement observations.

Candidate rows preserve event multiplicity. Ranking retains one row per
engagement registry key and report date. Enrichment and final aggregation
require additional statistics and are deliberately outside this calculation.
"""

import argparse
import csv
import json
from collections import defaultdict
from datetime import date


def count_windows(events, report_start, report_end, windows):
    if report_end < report_start:
        raise ValueError("report end must be on or after report start")
    widths = list(windows)
    if not widths or any(type(width) is not int or width < 1 for width in widths):
        raise ValueError("window widths must be positive integers")
    widths = sorted(set(widths))

    observations = []
    for entity, effective_date in events:
        if not isinstance(entity, str) or not entity.strip():
            raise ValueError("engagement registry keys must be nonempty strings")
        observations.append((entity, effective_date.toordinal()))

    first, last = report_start.toordinal(), report_end.toordinal()
    report_days = last - first + 1
    counts = {}
    for width in widths:
        intervals = defaultdict(list)
        candidates = 0
        for entity, day in observations:
            start, end = max(first, day), min(last, day + width - 1)
            if start <= end:
                candidates += end - start + 1
                intervals[entity].append((start, end))

        selected = 0
        for spans in intervals.values():
            spans.sort()
            start, end = spans[0]
            for next_start, next_end in spans[1:]:
                if next_start > end + 1:
                    selected += end - start + 1
                    start, end = next_start, next_end
                else:
                    end = max(end, next_end)
            selected += end - start + 1

        counts[str(width)] = {
            "candidate_rows": candidates,
            "selected_entity_dates": selected,
            "unpivot_rows_without_enrichment_fanout": 3 * selected,
            "candidate_rows_per_input_event": (
                candidates / len(observations) if observations else 0.0
            ),
            "candidate_rows_per_selected_entity_date": (
                candidates / selected if selected else 0.0
            ),
        }

    return {
        "eligible_input_event_rows": len(observations),
        "report_date_rows": report_days,
        "report_window_date_map_rows": report_days * max(widths),
        "windows": counts,
    }


def read_events(path):
    with open(path, newline="", encoding="utf-8") as handle:
        rows = csv.DictReader(handle)
        if not {"engagement_registry_key", "effective_date"}.issubset(rows.fieldnames or []):
            raise ValueError("events CSV requires engagement_registry_key and effective_date columns")
        for row in rows:
            try:
                effective_date = date.fromisoformat(row["effective_date"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid effective date at CSV line {rows.line_num}") from exc
            yield row["engagement_registry_key"], effective_date


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True, help="CSV of already eligible events")
    parser.add_argument("--report-start", required=True, type=date.fromisoformat)
    parser.add_argument("--report-end", required=True, type=date.fromisoformat)
    parser.add_argument("--windows", nargs="+", type=int, default=[1, 7, 28])
    args = parser.parse_args()
    try:
        counts = count_windows(
            read_events(args.events), args.report_start, args.report_end, args.windows
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
