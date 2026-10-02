---
"jazz-tools": patch
---

Reloading a page signed in with an external provider no longer logs in again and reopens the client before showing data. Browser account managers remember the signed-in account's assignment (account id, issuer and subject, never a token), so the next load opens that account's local data straight away and revalidates the provider session in the background. Logging in again as the same identity keeps the open client instead of shutting it down; a different identity switches accounts and a signed-out provider logs out as before. Until the provider's token arrives, the core admits nothing for that client and local permission checks see no provider claims. The first context opened after a login also reuses the token the registry just accepted instead of fetching another.
