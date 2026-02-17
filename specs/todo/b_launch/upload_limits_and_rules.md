# Upload Limits & Rules — TODO

Server-side validation to protect app developers from denial-of-wallet attacks by end users.

## Overview

The sync server should enforce configurable limits at the ingest boundary:

- **File size limits** — max bytes per binary upload, per-object payload size
- **MIME type restrictions** — allowlist of accepted content types per app/table
- **Rate limiting** — per-client upload throughput caps
- **Storage quotas** — per-app or per-user storage ceilings

These protect app developers from unexpected costs when malicious or buggy clients push large volumes of data.

## Open Questions

- Where are limits configured? (Per-app in dashboard, per-table in schema, or both?)
- How to enforce on a local-first system where writes happen offline first?
- Rejection semantics: reject at sync time, or accept-then-quarantine?
- Should limits apply to row data as well as binary data (file parts)?
