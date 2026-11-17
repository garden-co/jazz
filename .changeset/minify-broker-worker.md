---
"jazz-tools": patch
---

Ship the browser broker worker minified. Bundlers that serve `dist/worker/jazz-broker-worker.js` as it is, such as Next.js with Turbopack, now load about 114 KB gzip instead of 174 KB.
