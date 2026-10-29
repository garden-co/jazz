# Jazz fixtures

Pinned byte payloads shared by the Rust and TypeScript tests. Each is
independent of any binding runtime; regenerate one only together with the
code that defines its format.

- `binding_codec_golden.json`: the binding row-batch codec (`jazz-db`
  `binding_codec.rs`, `binding-codec-golden.test.ts`).
- `native_query_codec.json`, `native_row_codec.json`: the TypeScript native
  query and row codecs.
- `relation_query_postcard.json`: relation IR postcard bytes
  (`jazz-model` `relation_codec.rs`, `ir.test.ts`).
- `relation_shape_id_preimage.json`: canonical shape-identity preimages
  (`jazz-query-v0` plus a retained relation's `jazz-relation-v1` tree) and the
  resulting shape ids. Receivers recompute shape ids on registration, so these
  bytes are a cross-release contract.
- `result-key.json`: result-key sidecar addresses (`jazz-types` `object.rs`).
- `snapshot_ref_v1.json`: snapshot reference codec.
- `sqlite_sync_wire_v1.json`: the sync wire corpus (`sync_wire_corpus.rs`,
  `wire-frame-artifact-corpus.test.ts`).
- `websocket_prelude_v1.json`: WebSocket v1 admission preludes (the one JSON
  message before binary wire frames).
- `wire_frame_artifact_corpus_v6.json`, `wire_hello_frames_v6.json`,
  `wire_message_frames_v6.json`: the wire v6 frame corpora.
- `sqlite-storage-v1/`: stores written by storage format 1 (see its README).
- `pre-sqlite/`: stores from before storage format 1, which must be refused
  (see its README).
