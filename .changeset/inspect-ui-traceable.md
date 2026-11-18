---
"jazz-tools": patch
---

The Inspector UI now works on Vercel. The backend read the UI's files from disk next to its own code, and Next.js's output file tracing never copied them, so a deployed app answered "This jazz-tools build has no Inspector UI" at `/jazz/inspect/`, which also took out the agent approval page. jazz-tools now carries the UI as a module the backend imports, so it ships wherever the code does.
