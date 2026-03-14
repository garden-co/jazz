# jazz-wasm

## 2.0.0-alpha.19

## 2.0.0-alpha.18

### Patch Changes

- 33bc53f: Fail indexed writes cleanly when an indexed value would exceed the storage key limit instead of panicking in native storage.

  Oversized indexed inserts and updates now return a normal mutation error to JS callers, and local updates can recover rows that were previously left in a partial index state by older panic-driven failures.

- 83f4f5d: Use xxHash-based checksums for `opfs-btree` pages and superblocks to reduce checksum overhead in persistent browser storage.

  Existing OPFS stores created by older builds are not checksum-compatible with this change and will need to be recreated after upgrading.

## 2.0.0-alpha.17

## 2.0.0-alpha.16

## 2.0.0-alpha.15

### Patch Changes

- bb39e15: Modify inserts to return the inserted row instead of just the id

## 2.0.0-alpha.14

### Patch Changes

- 2f5ccba: Add an in-memory storage driver across the Jazz JS, WASM, NAPI, and React Native runtimes.

  Backend contexts can now opt into memory-backed runtimes without local persistence, and runtime driver-mode coverage was expanded to exercise the new in-memory path.

## 2.0.0-alpha.13

## 2.0.0-alpha.12

## 2.0.0-alpha.11

## 2.0.0-alpha.10

## 2.0.0-alpha.9

## 2.0.0-alpha.8

## 2.0.0-alpha.7
