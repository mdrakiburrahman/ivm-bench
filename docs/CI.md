# Manual cloud CI

Cloud CI is opt-in; GCI still gates PRs using local engines. In
[privy hub](http://127.0.0.1:7748/ui/relay-sas), mint an HTTPS sender URL
with enough TTL for the run (for example, six hours). Paste the full URL at
the secret prompt, never into command history. A repository admin creates the
`manual-run` environment once.

```bash
repo=mdrakiburrahman/ivm-bench
branch='<branch>'
gh api --method PUT "repos/$repo/environments/manual-run" --silent # admin, once
gh secret set IMDS_RELAY_TEMPORARY_SAS_URL --env manual-run --repo "$repo"
gh workflow run ci.yaml --repo "$repo" --ref "$branch" \
  -f engines=fabric-jvm-35 -f scale_factor=1 \
  -f batch_1_pct=1 -f batch_2_pct=0.001 -f batch_3_pct=0.002 \
  -f timeout_minutes=90
gh run list --repo "$repo" --workflow ci.yaml --branch "$branch" --event workflow_dispatch --limit 1
gh run watch <run-id> --repo "$repo" --exit-status
```

Alternatively, feed the URL file to `gh secret set` via stdin. Renew the secret
before it expires; the router cannot renew it. Other cloud credentials stay in
`BASE64_ENV`; the temporary URL is injected separately and takes precedence over
`.env`. See [CI inputs and experiment presets](CI_VS_GCI.md) for larger runs.
