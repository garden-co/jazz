# EpicDrop

EpicDrop is a deliberately small web file browser for Jazz large binary values.
It creates a file row from the browser's `File.stream()` through `db.insertStreaming`, so an upload is chunked into Jazz without constructing one complete `Uint8Array` in application JavaScript.

## Run

```bash
pnpm --filter epic-drop dev
```

The configured Jazz Vite plugin starts the local Jazz development server as
well as Vite, and supplies `VITE_JAZZ_APP_ID` and `VITE_JAZZ_SERVER_URL` to the
browser automatically. No hand-written `.env` file is needed for local use.

Create a folder, choose a file, and see its metadata appear in the live file list.

Run the example's browser receipts with:

```bash
pnpm --filter epic-drop test
```

## Shape shared with the benchmark

- `folders`: ownership boundary and browse root.
- `files`: a folder-scoped binary object with name, content type, byte length, owner, and
  `contents: bytes`. `folder_id` is indexed because the browser's file-list query is scoped to one
  folder and selects metadata only.
- The eventual benchmark should use the same folder/file cardinalities, metadata list query, streamed create operation, and ranged download operation.

The current vertical slice intentionally stops at metadata listing after upload. Download and preview require typed `Db` range reads; see [LEARNINGS.md](./LEARNINGS.md).
