# Text editor

A minimal CodeMirror + Yjs editor persisted and synced through Jazz.

Create a document, then open the same URL in another browser. Documents are public:
anyone can read and contribute.

Check **Offline** to pause Jazz sync while continuing to edit locally. Uncheck it
to reconnect and merge with edits made in other browsers.

## Run it

```sh
pnpm install
pnpm --filter text-editor dev
```

## Under the hood

Each editing session appends to its own `documentLogs` row. The adapter subscribes to
the logs for one document and tracks an offset per log to apply only new Yjs edits.

Each record is appended with Jazz's byte-splice API. The sole writer keeps its
append offset locally.

Jazz handles chunk-level sync; JavaScript still receives complete byte values.
Opening a document replays all its logs.

Every mounted editor gets a fresh random log ID, even in another tab on the
same account. Its row is created only on the first edit. Concurrent and offline
writers therefore sync separate rows, and Yjs merges their updates. Reloading
starts a new session; existing logs remain readable.
