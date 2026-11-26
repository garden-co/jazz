---
"jazz-tools": patch
---

Opening a store no longer scans every history table to find where its commit seq resumes. The seq lives in one `_jazz_meta` row (`commit_seq.v1`), kept current by SQLite triggers in the same transaction as every write that takes a seq. Older cores writing to the same store keep it current too. An existing store is scanned as before until its first write records the row. From then on, an open reads only the row. A large store on network storage (a Vercel Sandbox Drive) opens without reading its whole history.
