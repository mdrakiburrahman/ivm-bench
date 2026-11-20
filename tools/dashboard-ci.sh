#!/usr/bin/env bash
set -euo pipefail

if ! jq -e '.passed == true' ci-results/engine-coverage.json >/dev/null; then
  echo 'Dashboard publication skipped: engine coverage did not pass.'
  exit 0
fi

: "${GH_TOKEN:?GitHub token required}"
: "${RUNNER_TEMP:?Runner scratch directory required}"
umask 077
scratch="$(mktemp -d "$RUNNER_TEMP/ivm-dashboard.XXXXXXXX")"
trap 'rm -f -- "$scratch/commit.json" "$scratch/identity.json"; rmdir -- "$scratch"' EXIT

case "$GITHUB_EVENT_NAME" in
  pull_request)
    provenance="$(jq -ce --arg repo "$GITHUB_REPOSITORY" '
      .pull_request | select(.head.repo.full_name == $repo) |
      {commit_sha: .head.sha, branch: .head.ref, pr_number: .number, pr_title: .title}
    ' "$GITHUB_EVENT_PATH")" || { echo 'Dashboard publication requires a same-repository PR.' >&2; exit 1; }
    ;;
  workflow_dispatch)
    executed_sha="$(git rev-parse --verify HEAD)"
    if [[ "$executed_sha" != "$GITHUB_SHA" || "$GITHUB_REF" != refs/heads/* ]]; then
      echo 'Dashboard publication requires the dispatched branch checkout.' >&2
      exit 1
    fi
    provenance="$(jq -cn --arg sha "$executed_sha" --arg branch "${GITHUB_REF#refs/heads/}" \
      '{commit_sha: $sha, branch: $branch}')"
    ;;
  *)
    echo 'Dashboard publication does not support this event.' >&2
    exit 1
    ;;
esac
head="$(jq -er '.commit_sha | select(type == "string" and test("^[a-f0-9]{40}$"))' <<< "$provenance")"
curl --silent --show-error --fail \
  -H "Authorization: Bearer $GH_TOKEN" -H 'Accept: application/vnd.github+json' \
  "$GITHUB_API_URL/repos/$GITHUB_REPOSITORY/commits/$head" > "$scratch/commit.json"
jq --argjson provenance "$provenance" '
  $provenance + { committed_at: .commit.committer.date,
    subject: (.commit.message | split("\n")[0]), author: .author.login, run_id: env.GITHUB_RUN_ID,
    run_attempt: (env.GITHUB_RUN_ATTEMPT | tonumber), runner: env.RUNNER_NAME,
    run_url: (env.GITHUB_SERVER_URL + "/" + env.GITHUB_REPOSITORY + "/actions/runs/" + env.GITHUB_RUN_ID) }
' "$scratch/commit.json" > "$scratch/identity.json"

image=node:22-bookworm
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD/website:/workspace" -w /workspace "$image" npm ci --no-audit --no-fund
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD:/repo:ro" -v "$scratch:/run-meta:ro" -w /repo "$image" \
  node --env-file=/repo/.env --disable-warning=ExperimentalWarning \
  tools/dashboard-publish.mjs /repo /run-meta/identity.json
