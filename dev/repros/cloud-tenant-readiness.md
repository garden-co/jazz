# New Cloud tenant schema deployment readiness (#3044)

The alpha.55 receipt observed an empty schema catalogue HTTP 404 about 5.4 seconds
after credentials were created. The later successful run used bounded retries
without per-attempt logging. Neither receipt establishes that 404 means startup
is transient. A public server also deliberately returns an empty 404 when the
requested app ID is invalid or differs from its configured app ID
(`app_id_gate` in `crates/jazz-server/src/server/routes/mod.rs`). An authenticated
status response identifying the desired version does not prove data-plane route
readiness in every region.

The CLI's catalogue error now explains both checks: verify the server URL/app ID;
for a newly created Cloud app, wait for healthy, synced regional status before
retrying deployment. It makes one request and preserves the original HTTP status.
There is no automatic 404 retry or authorization change. This is guidance, not a
Cloud convergence fix.

## Fresh-tenant acceptance still required

Use an authorized disposable ordinary tenant and public synthetic schema. Keep
credentials and real tenant identifiers in private runtime state, never a public
receipt. Record relative timings from the tenant creation response:

1. Immediately capture authenticated control-plane status: each region's health,
   sync state, desired/applied configuration versions, and whether runtime exists.
2. Attempt the installed published CLI schema deployment once, logging the start
   and finish offsets, command exit status, and the catalogue HTTP status. Keep
   response bodies private; an empty response can be recorded as empty.
3. Capture status again after that attempt. If failed, preserve the first failure
   even if a later explicit attempt succeeds. Log every subsequent attempt and
   bounded delay separately. Stop on authorization errors or permanent schema
   failures; do not classify an unstructured 404 as transient just by its status.
4. After both regions are healthy and synced at desired configuration, attempt
   deployment again. If 404 persists, investigate routing/app-ID selection with
   the private control-plane receipts. Do not hide it with more retries.
5. Delete the disposable tenant and confirm authenticated status no longer finds
   it. Publish only synthetic, name-blind relative timing/status summaries.

A successful local HTTP regression proves the diagnostic and one-attempt behavior
for 404/401/403/400/503. It does not reproduce managed Cloud propagation. Keep
#3044 open until the fresh-tenant per-attempt receipt determines the responsible
boundary and validates its eventual fix.
