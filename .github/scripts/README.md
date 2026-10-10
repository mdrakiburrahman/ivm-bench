# GCI review gate

GCI checks reviews on a hosted runner before allocating the benchmark runner and
checks again after queueing. Approval must be independent, from a collaborator
with write permission, and match the exact current PR head. Drafts, forks, closed
PRs, stale approvals and active change requests are rejected. Comments do not
erase review decisions; dismissed decisions no longer authorize work.

Review submission/edit/dismissal and PR head changes share a concurrency group,
so changes cancel older queued/running runs. Checkout uses the reviewed head SHA,
not a synthetic merge commit. Cancellation is best effort for already-running
external work; this gate is admission control, not a sandbox for approved code.

Manual dispatch retains the benchmark inputs but requires `reviewed_pr` and
`reviewed_sha`. The dispatch ref must resolve to that SHA, the actor must have
write permission, and the same current-review checks apply. This binds custom
Dockerfile pins to the reviewed commit rather than accepting an unreviewed ref.

Dispatch uses `batch_1_insert_pct`, `batch_2_insert_pct` and
`batch_3_insert_pct`. The three duplicate `batch_N_pct` dispatch aliases were
removed to fit [GitHub's 25-input limit](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onworkflow_dispatchinputs) with the two review inputs. Legacy batch
environment variables and experiment JSON aliases remain supported. Validation
defaults to disabled; set `openivm_validate=1` or explicitly enable it in the
selected experiment's feature flags.

Run deterministic tests: `node --test .github/scripts/gci-review-gate.test.cjs`.
Both checks load policy from the same immutable trusted commit: the PR base SHA,
or the default branch SHA resolved before manual dispatch admission. Candidate
scripts and tests cannot replace admission policy. Missing trusted policy fails
closed, including the bootstrap PR that introduces this gate. Deployment therefore
requires normal human-reviewed installation on the base branch; it cannot be
activated through its own PR without a merge. No merge is performed here.

Both jobs also reject fork PRs at workflow level. Same-repository workflow writers
remain trusted: `pull_request` workflows themselves are PR-controlled, so this is
not a defense against a collaborator rewriting the workflow to remove its guards.
Runner-group workflow restrictions should enforce a separately protected workflow
if adversarial repository writers are in scope. No benchmark has been run to test
this change.

GitHub references: [review events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request_review)
and [review API](https://docs.github.com/en/rest/pulls/reviews).
