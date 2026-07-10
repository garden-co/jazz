# Stale-framing & stated-semantics sweep

Type: `wayfinder:task`
Status: open
Assignee: (unclaimed)
Blocked by: [B — protocol-plane decisions](B-protocol-plane.md),
[C — client-plane decisions](C-client-plane.md)

## Question

One AFK editing pass over the already-amended spec pair (runs last so it
sweeps once), applying the review's tier-3 items
([consolidated](../notes/review-consolidated.md), final section) — no new
decisions:

- Problem Statement: stop promising the FileStream-grade cross-device
  offline experience core no longer delivers; point expectations at the
  opt-in package.
- Kill remaining resume-flavored fragments B/C didn't already fix
  ("restarts the upload with a fresh id / rewrites the still-local
  descriptor"; US 16's "abandon a device" framing).
- Stated-semantics additions: delete crash-window privacy line (bytes
  believed withdrawn may persist; re-call `delete()`);
  copy-descriptor-equals-byte-access; TTL day-granularity variance
  (±24h effective lifetime); a louder first-write-wins-class callout;
  rate-limit absence as an operator cost note.
- Slice-1 header hygiene: mark the "Grant ledger" ticket and
  "files-persistence" map references as resolved-to-nothing so their
  names can't mislead an implementation agent.
- Final consistency read of PRD + slice-1 + explainer against every
  amendment from B and C (the explainer must keep matching).

Resolution records the checklist of edits made; closing this ticket
reaches the destination (modulo the fog's re-review question).
