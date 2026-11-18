---
"jazz-tools": patch
---

After a deploy that changes the schema, a local-first tab of the new build now waits for a tab of the previous build that still has the store open, instead of stopping for good. Before, it showed `{ state: "offline", reason: "stopped", retrying: false }` with no error. It only retried on the next explicit call, so it stayed stopped even after the old tab closed (#6100). Now it shows `connecting` and retries on its own. After 30 seconds it reports `held-by-another-tab`, and waits fail with a `StoreHeldByAnotherTabError` saying that another build's tab holds the store. It joins as soon as that tab reloads or closes. This is the same handling a tab already gets while another sign-in holds the store. A store refused for any other configuration (another worker asset, protocol or storage format) still fails visibly.
