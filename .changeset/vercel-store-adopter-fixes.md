---
"jazz-tools": patch
---

vercel() store fixes:

- It now works when the app is built with webpack (Next 15). Before, every request failed with "Received an instance of URL".
- A core that can't start is reported with the reason, for example a failed download from `coreTarballUrl`. When retrying can't help, such as a 404, its Sandbox is stopped so retries don't keep it awake. A temporary failure is retried as before.
- A preview deployment without a Git branch, such as a CLI deploy outside a Git checkout or a deploy through the API, now fails to open and asks for `name`. Before, all such previews silently shared one core.
