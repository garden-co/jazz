# jazz-webhook

## 0.19.17

### Patch Changes

- Updated dependencies [d46cffd]
  - jazz-tools@0.19.17
  - cojson@0.19.17
  - cojson-storage-sqlite@0.19.17
  - cojson-transport-ws@0.19.17

## 0.19.16

### Patch Changes

- Updated dependencies [25268bf]
- Updated dependencies [265d0e9]
  - jazz-tools@0.19.16
  - cojson@0.19.16
  - cojson-storage-sqlite@0.19.16
  - cojson-transport-ws@0.19.16

## 0.19.15

### Patch Changes

- Updated dependencies [94012a1]
- Updated dependencies [71ed9d6]
- Updated dependencies [86f9676]
- Updated dependencies [b27dbc2]
  - jazz-tools@0.19.15
  - cojson@0.19.15
  - cojson-storage-sqlite@0.19.15
  - cojson-transport-ws@0.19.15

## 0.19.14

### Patch Changes

- cojson@0.19.14
- cojson-storage-sqlite@0.19.14
- cojson-transport-ws@0.19.14
- jazz-tools@0.19.14

## 0.19.13

### Patch Changes

- Updated dependencies [bef1cc6]
- Updated dependencies [b839147]
  - jazz-tools@0.19.13
  - cojson@0.19.13
  - cojson-storage-sqlite@0.19.13
  - cojson-transport-ws@0.19.13

## 0.19.12

### Patch Changes

- Updated dependencies [9ca9e72]
- Updated dependencies [5b0bb7d]
- Updated dependencies [fa0759b]
- Updated dependencies [a2372db]
  - jazz-tools@0.19.12
  - cojson@0.19.12
  - cojson-storage-sqlite@0.19.12
  - cojson-transport-ws@0.19.12

## 0.19.11

### Patch Changes

- Updated dependencies [68acca4]
- Updated dependencies [c00a454]
  - jazz-tools@0.19.11
  - cojson@0.19.11
  - cojson-storage-sqlite@0.19.11
  - cojson-transport-ws@0.19.11

## 0.19.10

### Patch Changes

- cojson@0.19.10
- cojson-storage-sqlite@0.19.10
- cojson-transport-ws@0.19.10
- jazz-tools@0.19.10

## 0.19.9

### Patch Changes

- Updated dependencies [d901caa]
- Updated dependencies [a2bb9f0]
  - jazz-tools@0.19.9
  - cojson@0.19.9
  - cojson-storage-sqlite@0.19.9
  - cojson-transport-ws@0.19.9

## 0.19.8

### Patch Changes

- Updated dependencies [21f7d34]
- Updated dependencies [b22ad89]
- Updated dependencies [93e4a34]
- Updated dependencies [28b23dd]
  - jazz-tools@0.19.8
  - cojson@0.19.8
  - cojson-storage-sqlite@0.19.8
  - cojson-transport-ws@0.19.8

## 0.19.7

### Patch Changes

- Updated dependencies [e113a79]
  - jazz-tools@0.19.7
  - cojson@0.19.7
  - cojson-storage-sqlite@0.19.7
  - cojson-transport-ws@0.19.7

## 0.19.6

### Patch Changes

- Updated dependencies [23782f0]
- Updated dependencies [56d74e4]
- Updated dependencies [bc9120b]
  - jazz-tools@0.19.6
  - cojson@0.19.6
  - cojson-storage-sqlite@0.19.6
  - cojson-transport-ws@0.19.6

## 0.19.5

### Patch Changes

- Updated dependencies [343c2e4]
  - cojson@0.19.5
  - cojson-storage-sqlite@0.19.5
  - cojson-transport-ws@0.19.5
  - jazz-tools@0.19.5

## 0.19.4

### Patch Changes

- Updated dependencies [78dfffd]
- Updated dependencies [de2f8b5]
- Updated dependencies [763977a]
- Updated dependencies [e02e14c]
- Updated dependencies [3aaba61]
  - jazz-tools@0.19.4
  - cojson@0.19.4
  - cojson-storage-sqlite@0.19.4
  - cojson-transport-ws@0.19.4

## 0.19.3

### Patch Changes

- Updated dependencies [cddbfdb]
- Updated dependencies [114e4ce]
  - jazz-tools@0.19.3
  - cojson@0.19.3
  - cojson-storage-sqlite@0.19.3
  - cojson-transport-ws@0.19.3

## 0.19.2

### Patch Changes

- Updated dependencies [ef24afb]
- Updated dependencies [7e76313]
- Updated dependencies [5f2b34b]
  - jazz-tools@0.19.2
  - cojson@0.19.2
  - cojson-storage-sqlite@0.19.2
  - cojson-transport-ws@0.19.2

## 0.19.1

### Patch Changes

- Updated dependencies [f444bd9]
- Updated dependencies [afd2ded]
  - jazz-tools@0.19.1
  - cojson@0.19.1
  - cojson-storage-sqlite@0.19.1
  - cojson-transport-ws@0.19.1

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
