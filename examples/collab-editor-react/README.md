# Collaborative Editor React

This example demonstrates Jazz as the durable sync backbone for a collaborative Yjs document.
Monaco and Yjs handle the in-browser editing model, while Jazz stores the binary Yjs update log
and periodic full-document snapshots.

## Jazz stores Yjs

Each local edit updates `Y.Doc.getText("monaco")`. The Yjs `update` event is copied into a
`roomYjsUpdates` row and synced by Jazz. Other tabs and peers read those rows, apply the binary
updates to their own Yjs document, and Monaco reflects the text through `y-monaco`.

The app also writes debounced snapshots with `Y.encodeStateAsUpdate(doc)` so a new client can
bootstrap from the latest full state before replaying updates. It intentionally does not use
awareness or live cursors.

## Run

```sh
pnpm dev
```

The Jazz dev plugin (`jazzPlugin()` in `vite.config.ts`) starts a local Jazz server,
publishes this example's schema, and points the app at it automatically — so collaboration
works out of the box. Open http://localhost:5173 in two windows, create a room in one, and
open its **Copy link** URL in the other; edits sync both ways. Edits also persist locally
(OPFS) across reloads.

> In a headless/CI browser without OPFS support, set `VITE_JAZZ_DRIVER=memory` to keep state
> in the server only.

The permissions model is intentionally permissive, with open reads, because this is a
shareable-link demo.
