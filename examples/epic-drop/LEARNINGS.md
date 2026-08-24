# EpicDrop adopter learnings

## Typed range and edit access is missing

EpicDrop can create `files.contents` from a browser `File.stream()` using the documented typed `Db.insertStreaming` API. It cannot implement a browser download, preview, resume, or chunked range-read correctness test without reaching into private runtime state.

The low-level `JazzClient` has `readValueRange(table, objectId, column, start, end)`, `appendValue`, and `spliceValue` in `packages/jazz-tools/src/runtime/client.ts`, but the public typed `Db` used by `useDb()` exposes only streaming create/update/upsert. `useJazzClient()` returns `{ db, session, shutdown }`, not the low-level client.

Desired public follow-up: add typed `Db.readValueRange(table, id, bytesColumn, start, end)` plus typed `appendValue` and `spliceValue`, validating that the selected column is `bytes` and preserving the current permission/session and durability handling. EpicDrop must not use casts or private-access workarounds while that API is absent.

## Sharing needs an application ACL shape

EpicDrop currently models private folders and files only: every policy compares `owner_id` with
`session.user_id`. That gives the example an honest owner/non-owner authorization boundary, but it
cannot demonstrate grant and revocation behavior without first adding a shared-folder/member relation
and its corresponding policy graph. Keep that as a deliberate product-schema follow-up instead of
smuggling a synthetic authorization mechanism into the E2E fixtures.
