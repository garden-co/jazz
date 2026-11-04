---
"jazz-tools": patch
---

Upgrade Better Auth to 1.7.16 across the adapter and examples. Preserve account uniqueness on `(providerId, accountId)` after Better Auth removed the composite index metadata, update the generated auth schemas for the JWT plugin, and run the Next.js cookie integration after other plugins.

Resolve duplicate `(providerId, accountId)` pairs before upgrading. Applications upgrading from 1.7.0–1.7.2 must make the old account `issuer` column optional or remove it, then regenerate and publish their auth schema migrations before serving traffic with 1.7.16.

The examples retain their exact issuer-era snapshots and ship an explicit connected migration instead of replacing the old structural root. Existing issuer cells remain stored. The backwards projection supplies an empty issuer only for new rows created without one; it does not infer an issuer from a provider, restore an authentication identity, or make package downgrades safe. Keep the coordinated migration/cutover precautions above.
