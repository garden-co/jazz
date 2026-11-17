---
"jazz-tools": patch
---

`compositeIndex` in TypeScript schemas now accepts the provenance timestamps `$createdAt` and `$updatedAt`, as the Rust schema builder already does, so a list's newest page (`where({ roomId }).orderBy("$createdAt", "desc").limit(n)`) is served from an index instead of sorting the whole list: `s.table({...}).compositeIndex(["roomId", "$createdAt"])`.
