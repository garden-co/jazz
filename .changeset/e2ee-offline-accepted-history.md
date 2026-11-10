---
"jazz-tools": patch
---

Support retained encrypted content and provisional offline writes with keys
previously validated online, including persistent native reopening. Local pending
state remains distinct from current membership and Global acceptance; genuine
reconnect validation can reject it. Retained old keys can still expose content
to removed recipients, so offline operation does not promise unseen revocation
detection.

Bind optional private cache records to the actual account, device, namespace,
scope, exact content and validated origins. Fresh memory owners require their
own genuine online same-cut validation. Reject changed bindings and closed or
retired ownership; cache persistence failure cannot undo an accepted commit.

For supported direct local encrypted equality reads, decrypt every scoped
candidate before filtering or paging, refuse unknown epochs and overflow, and
keep authority-bearing query modes unchanged.
