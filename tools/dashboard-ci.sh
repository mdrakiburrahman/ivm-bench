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

head="$(jq -er '.pull_request.head.sha | select(test("^[a-f0-9]{40}$"))' "$GITHUB_EVENT_PATH")"
curl --silent --show-error --fail \
  -H "Authorization: Bearer $GH_TOKEN" -H 'Accept: application/vnd.github+json' \
  "$GITHUB_API_URL/repos/$GITHUB_REPOSITORY/commits/$head" > "$scratch/commit.json"
jq -n --slurpfile event "$GITHUB_EVENT_PATH" --slurpfile commit "$scratch/commit.json" '
  $event[0].pull_request as $pr | $commit[0] as $c |
  { commit_sha: $pr.head.sha, committed_at: $c.commit.committer.date,
    subject: ($c.commit.message | split("\n")[0]), author: $c.author.login, branch: $pr.head.ref,
    pr_number: $pr.number, pr_title: $pr.title, run_id: env.GITHUB_RUN_ID,
    run_attempt: (env.GITHUB_RUN_ATTEMPT | tonumber), runner: env.RUNNER_NAME,
    run_url: (env.GITHUB_SERVER_URL + "/" + env.GITHUB_REPOSITORY + "/actions/runs/" + env.GITHUB_RUN_ID) }
' > "$scratch/identity.json"

image=node:22-bookworm
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD/website:/workspace" -w /workspace "$image" npm ci --no-audit --no-fund
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD:/repo:ro" -v "$scratch:/run-meta:ro" -w /repo "$image" \
  node --env-file=/repo/.env --disable-warning=ExperimentalWarning \
  tools/dashboard-publish.mjs /repo /run-meta/identity.json
