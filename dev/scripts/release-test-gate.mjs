/**
 * Publication gate: an alpha is published only when the release test tier
 * passed on the exact tree being published.
 *
 * The tier runs on the Version Packages PR head (dispatched by
 * changesets-release-pr.yml, because GITHUB_TOKEN-created PRs do not trigger
 * pull_request workflows). The published commit is that PR's merge commit, so
 * the gate also requires the merge commit to carry the same tree.
 */

/**
 * `events` lists the triggers whose runs count. RN device acceptance only
 * runs its simulator/emulator jobs on dispatch (or a labelled PR), so an
 * ordinary pull_request run of it is scaffold-only and must not satisfy the
 * gate.
 */
export const RELEASE_TEST_WORKFLOWS = Object.freeze([
  Object.freeze({ file: "ci.yml", label: "CI", events: null }),
  Object.freeze({ file: "starters-e2e.yml", label: "Starters E2E", events: null }),
  Object.freeze({
    file: "rn-device-acceptance.yml",
    label: "React Native device acceptance",
    events: Object.freeze(["workflow_dispatch"]),
  }),
]);

/** True when a run is a completed success of the given workflow at `headSha`. */
export function isQualifyingRun(run, workflow, headSha) {
  return (
    run.head_sha === headSha &&
    run.status === "completed" &&
    run.conclusion === "success" &&
    (workflow.events === null || workflow.events.includes(run.event))
  );
}

/**
 * @param {object} options
 * @param {object} options.github Octokit client (actions/github-script `github`).
 * @param {string} options.owner
 * @param {string} options.repo
 * @param {string} options.sha The commit being published.
 * @param {string} options.headSha The Version Packages PR head the tier ran on.
 * @param {string} options.headRef The Version Packages PR head branch.
 * @returns {Promise<{ ok: boolean, lines: string[] }>}
 */
export async function evaluateReleaseTestGate({ github, owner, repo, sha, headSha, headRef }) {
  const lines = [];
  let ok = true;

  if (!headSha || !headRef) {
    return { ok: false, lines: ["release PR head is unknown; cannot locate release test runs"] };
  }

  const [published, tested] = await Promise.all([
    github.rest.git.getCommit({ owner, repo, commit_sha: sha }),
    github.rest.git.getCommit({ owner, repo, commit_sha: headSha }),
  ]);
  if (published.data.tree.sha !== tested.data.tree.sha) {
    ok = false;
    lines.push(
      `published tree ${published.data.tree.sha} (${sha}) differs from tested release PR head tree ${tested.data.tree.sha} (${headSha})`,
    );
  } else {
    lines.push(`published tree matches release PR head ${headRef}@${headSha}`);
  }

  for (const workflow of RELEASE_TEST_WORKFLOWS) {
    const runs = await github.paginate(github.rest.actions.listWorkflowRuns, {
      owner,
      repo,
      workflow_id: workflow.file,
      branch: headRef,
      head_sha: headSha,
      per_page: 100,
    });
    const passing = runs.find((run) => isQualifyingRun(run, workflow, headSha));
    if (passing) {
      lines.push(`${workflow.label}: passed in run ${passing.id} (${passing.event})`);
      continue;
    }
    ok = false;
    const seen = runs
      .filter((run) => run.head_sha === headSha)
      .map((run) => `${run.id} ${run.event} ${run.status}/${run.conclusion ?? "-"}`);
    lines.push(
      `${workflow.label}: no successful ${workflow.events ? workflow.events.join("/") + " " : ""}run of ${workflow.file} at ${headSha}` +
        (seen.length > 0 ? ` (seen: ${seen.join(", ")})` : " (no runs)"),
    );
  }

  return { ok, lines };
}
