---
"jazz-tools": patch
---

Add first-class inspector CLI support in `jazz-tools` and package the standalone inspector build in npm artifacts. Introduces `jazz-tools inspector` (default port `8625`) and `jazz-tools server --inspector` for serving the UI on the server port at `/_inspector`.
