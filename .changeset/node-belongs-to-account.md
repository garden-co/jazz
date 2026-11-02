---
"jazz-tools": patch
---

A device keeps writing when its account signs in another way. A local-first guest who signs up through the app's backend, or an account that later signs in under another issuer or a linked identity, keeps writing from the same device without "commit names a node another identity writes as" refusals. The node moves to the account's newest signed-in principal; a local-first guest key never takes a node back, so a leaked guest key can't write as a signed-up device.
