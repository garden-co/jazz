---
"jazz-tools": patch
---

A browser session now tries again on its own when a network blip, a timeout or a 5xx fails its check with your backend, or the sign-in that follows the check. It backs off from 2 seconds up to a minute. Before, the session stayed on the failure, often the error screen, until the page regained focus, so background tabs stayed broken.
