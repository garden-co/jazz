# EpicDrop

EpicDrop is a focused file-browser foundation for Jazz large binary values.

## Before and after

Before this foundation, the example catalogue had no file-browser receipt for a browser `File`
stream or an indexed metadata listing. EpicDrop now streams `File.stream()` directly through the
public typed `Db.insertStreaming` API and lists a selected folder without selecting file contents.

For example, uploading `set-list.wav` records its filename, content type, and byte length with its
streamed bytes. Browsing the folder renders only that metadata; it does not need a full file value
to draw the list.

## Run and receipts

```bash
pnpm --filter epic-drop dev
pnpm --filter epic-drop test
cargo test -p jazz-example-epic-drop-benchmark
```

The browser receipts cover multi-chunk upload plus metadata projection, and a cancelled upload
that publishes no file before a clean retry. The native fixture uses a deterministic 32 KiB
`PatternReader`, lists the indexed folder metadata, and validates a 64 KiB middle byte range.

## Non-goals

- No typed browser preview/download or edit API: #1833 owns typed page selections and updates.
- No remote chunk-withholding receipt: #1862 owns the public topology seam.
- No multi-browser large-value relay claim while #1978 remains open.
- No mounted folder/VFS, cache eviction, filesystem events, offline file conflict, or shared-folder
  membership design. This foundation deliberately has private owner-only folders.
