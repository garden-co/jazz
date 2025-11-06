# jazz-webhook

## 0.19.0

### Minor Changes

- 26386d9: Add explicit CoValue loading states:
  - Add `$isLoaded` field to discriminate between loaded and unloaded CoValues
  - Add `$jazz.loadingState` field to provide additional info about the loading state
  - All methods and functions that load CoValues now return a `MaybeLoaded<CoValue>` instead of `CoValue | null | undefined`
  - Rename `$onError: null` to `$onError: "catch"`
  - Split the `useAccount` hook into three separate hooks:
    - `useAccount`: now only returns an Account CoValue
    - `useLogOut`: returns a function for logging out of the current account
    - `useAgent`: returns the current agent
  - Add a `select` option (and an optional `equalityFn`) to `useAccount` and `useCoState`, and remove `useAccountWithSelector` and `useCoStateWithSelector`.
  - Allow specifying resolve queries at the schema level. Those queries will be used when loading CoValues, if no other resolve query is provided.

### Patch Changes

- Updated dependencies [26386d9]
  - jazz-tools@0.19.0
  - cojson@0.19.0
  - cojson-storage-sqlite@0.19.0
  - cojson-transport-ws@0.19.0

## 0.18.38

### Patch Changes

- Updated dependencies [349ca48]
- Updated dependencies [68781a0]
- Updated dependencies [349ca48]
  - cojson@0.18.38
  - jazz-tools@0.18.38
  - cojson-storage-sqlite@0.18.38
  - cojson-transport-ws@0.18.38

## 0.18.37

### Patch Changes

- Updated dependencies [0e923d1]
- Updated dependencies [feecdae]
- Updated dependencies [fd89225]
- Updated dependencies [a841071]
- Updated dependencies [68e0b26]
  - cojson@0.18.37
  - jazz-tools@0.18.37
  - cojson-storage-sqlite@0.18.37
  - cojson-transport-ws@0.18.37

## 0.18.36

### Patch Changes

- Updated dependencies [af3fe4c]
  - cojson@0.18.36
  - cojson-storage-sqlite@0.18.36
  - cojson-transport-ws@0.18.36
  - jazz-tools@0.18.36

## 0.18.35

### Patch Changes

- Updated dependencies [d47ac6d]
  - cojson@0.18.35
  - cojson-storage-sqlite@0.18.35
  - cojson-transport-ws@0.18.35
  - jazz-tools@0.18.35

## 0.18.34

### Patch Changes

- Updated dependencies [4a79953]
- Updated dependencies [7a64465]
- Updated dependencies [d7e5cc8]
  - cojson@0.18.34
  - jazz-tools@0.18.34
  - cojson-storage-sqlite@0.18.34
  - cojson-transport-ws@0.18.34

## 0.18.33

### Patch Changes

- Updated dependencies [df0045e]
- Updated dependencies [5ffe0a9]
  - jazz-tools@0.18.33
  - cojson@0.18.33
  - cojson-storage-sqlite@0.18.33
  - cojson-transport-ws@0.18.33

## 0.18.32

### Patch Changes

- Updated dependencies [8f47a9e]
- Updated dependencies [2c7013a]
- Updated dependencies [314c199]
  - cojson@0.18.32
  - cojson-transport-ws@0.18.32
  - jazz-tools@0.18.32
  - cojson-storage-sqlite@0.18.32

## 0.18.31

### Patch Changes

- Updated dependencies [7c2b7b8]
  - cojson@0.18.31
  - cojson-storage-sqlite@0.18.31
  - cojson-transport-ws@0.18.31
  - jazz-tools@0.18.31

## 0.18.30

### Patch Changes

- Updated dependencies [b3dbcaa]
- Updated dependencies [75d452e]
- Updated dependencies [ad83da2]
- Updated dependencies [346c5fb]
- Updated dependencies [354895b]
- Updated dependencies [162757c]
- Updated dependencies [d08b7e2]
- Updated dependencies [ad19280]
  - jazz-tools@0.18.30
  - cojson@0.18.30
  - cojson-storage-sqlite@0.18.30
  - cojson-transport-ws@0.18.30

## 0.18.29

### Patch Changes

- Updated dependencies [cc7efc8]
- Updated dependencies [f55d17f]
  - jazz-tools@0.18.29
  - cojson@0.18.29
  - cojson-storage-sqlite@0.18.29
  - cojson-transport-ws@0.18.29

## 0.18.28

### Patch Changes

- 52c8c89: Add webhook registries and the ability to run and create them with jazz-run
- Updated dependencies [8cbbe0e]
- Updated dependencies [14806c8]
- Updated dependencies [e8880dc]
- Updated dependencies [d83b5e3]
- Updated dependencies [5320349]
  - jazz-tools@0.18.28
  - cojson@0.18.28
  - cojson-storage-sqlite@0.18.28
  - cojson-transport-ws@0.18.28
