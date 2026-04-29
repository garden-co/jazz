---
"jazz-tools": patch
---

The `jazz-tools` CLI now loads `.env` from the working directory at startup, so `deploy` and other commands pick up `JAZZ_ADMIN_SECRET`, `JAZZ_SERVER_URL` (and framework-prefixed equivalents) from the same dotenv file your app uses. Real environment variables still take precedence.
