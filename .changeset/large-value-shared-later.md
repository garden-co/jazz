---
"jazz-tools": patch
---

Fix large values (`s.bytes()`, long text) never arriving on a local-first client for a row it may read only through a grant made after the row's last write, such as a share row in an `exists` policy. The server checked the chunks against the policy as of the row's version, when no grant existed yet. It now serves the row's current version to anyone who may read the row now. Earlier versions are still checked as of their own position, and losing access still takes effect at once.
