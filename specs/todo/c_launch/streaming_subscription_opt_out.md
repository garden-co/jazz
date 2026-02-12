# Streaming Subscription (Opt-Out of Initial Settle) — TODO (Launch)

Allow subscriptions to stream results incrementally instead of waiting for the initial QuerySettled.

## Overview

Currently, subscriptions wait for settlement before delivering results. For large result sets (e.g., streaming a file's binary chunks after `built_in_file_storage.md`), the client may want to start processing data as it arrives rather than waiting for the full initial batch.

- Opt-out flag on subscription: "stream immediately, settle later"
- Useful for file downloads, large collection browsing, progressive loading
- Settlement still happens — it just doesn't gate first delivery

## Open Questions

- API shape — flag on the query, or a separate subscription mode?
- How does this interact with ORDER BY — can we stream in order, or is ordering only guaranteed at settlement?
- Does the client need to distinguish "streaming partial" from "settled complete"?

Related: `../b_mvp/built_in_file_storage.md`
