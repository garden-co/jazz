# EpicDrop

EpicDrop is a deliberately small web file browser for Jazz large binary values.
It creates a file row from the browser's `File.stream()` through `db.insertStreaming`, so an upload is chunked into Jazz without constructing one complete `Uint8Array` in application JavaScript.

## Run

```bash
pnpm --filter epic-drop dev
```

Create a folder, choose a file, and see its metadata appear in the live file list.

## Shape shared with the benchmark

- `folders`: ownership boundary and browse root.
- `files`: a folder-scoped binary object with name, content type, byte length, owner, and `contents: bytes`.
- The eventual benchmark should use the same folder/file cardinalities, metadata list query, streamed create operation, and ranged download operation.

The current vertical slice intentionally stops at metadata listing after upload. Download and preview require typed `Db` range reads; see [LEARNINGS.md](./LEARNINGS.md).
