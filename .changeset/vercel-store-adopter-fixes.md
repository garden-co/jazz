---
"jazz-tools": patch
---

vercel() store fixes:

- It now works when the app is built with webpack (Next 15). Before, every request failed with "Received an instance of URL".
- A core that can't start, for example when its download from `coreTarballUrl` fails, is reported with the reason. Its Sandbox is stopped, so it isn't kept awake by retries.
- A preview deployment without a Git branch, such as a CLI deploy outside a Git checkout or a deploy through the API, now throws and asks for `name`. Before, all such previews silently shared one core.
