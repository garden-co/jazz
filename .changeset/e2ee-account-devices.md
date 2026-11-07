---
"jazz-tools": patch
---

Add explicitly configured account and device E2EE lifecycle with durable device keys,
signed membership, approval ancestry and revocation. Account-scoped accepted reads
share the serializable transaction's authority cut and validate its read set at commit;
missing or unavailable coverage fails closed. Supported row origins retain their exact
state and content positions, including restored content and strict earlier-transaction
ordering. Public roots and initial identities are published atomically and wait for
Global acceptance. Session-owned key copies are cleared on shutdown and failed
initialisation. Recovery record validation is included; recovery authoring, groups,
spaces and encrypted operations belong to later layers.
