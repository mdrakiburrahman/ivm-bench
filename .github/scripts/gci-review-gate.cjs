// Reviews bind to the actual pin/source commit, never GitHub's synthetic merge SHA.
function approved(pr, reviews, sha, trustedReviewers) {
  if (pr.state !== 'open' || pr.draft || pr.head.sha !== sha ||
      pr.head.repo?.full_name !== pr.base.repo?.full_name) return false;
  const latest = new Map();
  for (const review of [...reviews].sort((a, b) => a.id - b.id)) {
    if (['APPROVED', 'CHANGES_REQUESTED', 'DISMISSED'].includes(review.state)) {
      latest.set(review.user.login, review);
    }
  }
  const effective = [...latest.values()];
  if (effective.some(review => review.state === 'CHANGES_REQUESTED')) return false;
  return effective.some(review => review.state === 'APPROVED' && review.commit_id === sha &&
    review.user.login !== pr.user.login && trustedReviewers.has(review.user.login));
}

async function check({github, context}) {
  const manual = context.eventName === 'workflow_dispatch';
  const inputs = context.payload.inputs || {};
  const number = manual ? Number(inputs.reviewed_pr) : context.payload.pull_request?.number;
  const sha = manual ? inputs.reviewed_sha : context.payload.pull_request?.head.sha;
  const denied = reason => ({allowed: false, reason});
  if (!Number.isSafeInteger(number) || number < 1 || !/^[0-9a-f]{40}$/.test(sha || '')) {
    return denied('A PR number and exact reviewed SHA are required');
  }
  const repository = context.repo;
  if (manual) {
    if (context.sha !== sha) return denied('Dispatch ref does not match reviewed SHA');
    const actor = await github.rest.repos.getCollaboratorPermissionLevel({...repository, username: context.actor});
    if (!['admin', 'maintain', 'write'].includes(actor.data.permission)) {
      return denied('Manual dispatch requires repository write permission');
    }
  }
  const {data: pr} = await github.rest.pulls.get({...repository, pull_number: number});
  const reviews = await github.paginate(github.rest.pulls.listReviews,
    {...repository, pull_number: number, per_page: 100});
  const trusted = new Set();
  for (const login of new Set(reviews.filter(r => r.state === 'APPROVED' && r.commit_id === sha).map(r => r.user.login))) {
    const permission = await github.rest.repos.getCollaboratorPermissionLevel({...repository, username: login});
    if (['admin', 'maintain', 'write'].includes(permission.data.permission)) trusted.add(login);
  }
  if (!approved(pr, reviews, sha, trusted)) {
    return denied('Current same-repository PR head needs independent write-authorized approval and no active change requests');
  }
  return {allowed: true, sha, reason: `Reviewed PR #${number} at ${sha}`};
}

module.exports = {approved, check};
