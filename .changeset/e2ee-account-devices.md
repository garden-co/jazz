---
"jazz-tools": minor
---

Add explicitly configured authenticated account/device E2EE lifecycle: durable device keys,
authority-settled account roots, key-free signed membership, approval ancestry, and revocation.
Malformed candidates cannot poison accepted history. Recovery-signed record validation is
included for forward safety, but recovery authoring, groups, spaces, encrypted operations,
and durable offline history are not part of this layer.

Expose the managed device-request schema and permissions for application schema composition.
Propagate operational signing-adapter failures rather than reporting them as unavailable
device delivery. Complete prepared policy claim domains and preserve recursive binding
carriers, and fix release-build hydration of shared recursive query graphs.
