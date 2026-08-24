# EpicDrop adopter learnings

## Metadata hydration and partial bytes have explicit public receipts

EpicDrop can create `files.contents` from a browser `File.stream()` using the documented typed
`Db.insertStreaming` API. Its folder browser uses the public query DSL to filter by indexed
`folder_id` and select only `id`, name, content type, and size; it deliberately does not select
`contents` merely to render metadata.

## Controlled remote-value withholding (#1862)

EpicDrop has no skipped remote-hydration receipt today. #1862 provides the public test-harness
gap: a controlled way to withhold one selected remote large-value frontier. Its acceptance test is:
write one streamed `files` row, start a second app instance, hold only `contents` back, and verify
the indexed metadata-only folder subscription publishes its one row without requesting or
materializing `contents`. The example must not use private runtime hooks to manufacture that
topology.

Download, preview, resume, and bounded-range scenarios separately await #1833: the public
declarative query DSL should select byte ranges, and ordinary update descriptors should express
edits. The example must not bypass that design with imperative `Db` range, append, or splice helpers.

## Sharing needs an application ACL shape

EpicDrop currently models private folders and files only: every policy compares `owner_id` with
`session.user_id`. That gives the example an honest owner/non-owner authorization boundary, but it
cannot demonstrate grant and revocation behavior without first adding a shared-folder/member relation
and its corresponding policy graph. Keep that as a deliberate product-schema follow-up instead of
smuggling a synthetic authorization mechanism into the E2E fixtures.
