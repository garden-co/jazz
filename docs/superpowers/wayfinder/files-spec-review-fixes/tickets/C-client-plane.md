# Client-plane decisions (lifecycle edges, slice boundary, constants)

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: (none)

## Question

One grilling over the client/SDK half of the review findings
([consolidated](../notes/review-consolidated.md)). Decide and amend the
spec pair:

- **Upload lifecycle edges:** state the upload trigger crisply (nothing
  can start before the first cell write finalizes the id); define the
  released-then-rejected orphan semantics (accepted leak with a stated
  semantic? best-effort SDK delete? — decide); define `url()` on a
  pre-first-write handle (throw / null / lazy); define the outbox hold's
  "causally dependent" precisely; qualify lease-expiry restart and
  part-URL refresh as in-session only (kill the last resume-flavored
  fragments).
- **Slice-1 deliverable boundary:** pin what of the TS surface
  (`fromBlob`, upload state machine, in-memory hold, `delete()`) ships in
  slice 1 vs may be stubbed — resolving the circularity between slice-1's
  stub permission and its own TS test seam; add the missing
  serving-hardening test scenarios to slice-1 (nosniff/disposition,
  public-bytes-despite-hidden-row).
- **Constants & config surfaces:** pin the lease default, the
  single-PUT/multipart threshold, the inline-safe type allowlist
  (enumerate it), the TTL class-set configuration surface, and name the
  canonical-JSON algorithm (JSON-column precedent — look up what the
  codebase already does before asking); decide `fromBlob`'s input type
  per platform (web Blob / Node / RN); state the class-set-evolution
  footgun (removing a class orphans its bodies).
