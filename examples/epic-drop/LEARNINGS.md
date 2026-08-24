# EpicDrop adopter learnings

## Partial bytes await the query/update DSL

EpicDrop can create `files.contents` from a browser `File.stream()` using the documented typed
`Db.insertStreaming` API. Its download, preview, resume, and bounded-range scenarios await #1833:
the public declarative query DSL should select byte ranges, and ordinary update descriptors should
express edits. The example must not bypass that design with imperative `Db` range, append, or splice
helpers, or by reaching into a private `JazzClient` runtime.

## Sharing needs an application ACL shape

EpicDrop currently models private folders and files only: every policy compares `owner_id` with
`session.user_id`. That gives the example an honest owner/non-owner authorization boundary, but it
cannot demonstrate grant and revocation behavior without first adding a shared-folder/member relation
and its corresponding policy graph. Keep that as a deliberate product-schema follow-up instead of
smuggling a synthetic authorization mechanism into the E2E fixtures.
