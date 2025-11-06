# jazz-sveltekit

## 0.0.20

### Patch Changes

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
- Updated dependencies [26386d9]
  - jazz-tools@0.19.0

## 0.0.19

### Patch Changes

- Updated dependencies [349ca48]
  - jazz-tools@0.18.38

## 0.0.18

### Patch Changes

- Updated dependencies [feecdae]
- Updated dependencies [a841071]
- Updated dependencies [68e0b26]
  - jazz-tools@0.18.37

## 0.0.17

### Patch Changes

- jazz-tools@0.18.36

## 0.0.16

### Patch Changes

- jazz-tools@0.18.35

## 0.0.15

### Patch Changes

- Updated dependencies [7a64465]
  - jazz-tools@0.18.34

## 0.0.14

### Patch Changes

- Updated dependencies [df0045e]
- Updated dependencies [5ffe0a9]
  - jazz-tools@0.18.33

## 0.0.13

### Patch Changes

- Updated dependencies [314c199]
  - jazz-tools@0.18.32

## 0.0.12

### Patch Changes

- jazz-tools@0.18.31

## 0.0.11

### Patch Changes

- Updated dependencies [b3dbcaa]
- Updated dependencies [75d452e]
- Updated dependencies [346c5fb]
- Updated dependencies [354895b]
- Updated dependencies [162757c]
- Updated dependencies [d08b7e2]
- Updated dependencies [ad19280]
  - jazz-tools@0.18.30

## 0.0.10

### Patch Changes

- Updated dependencies [cc7efc8]
- Updated dependencies [f55d17f]
  - jazz-tools@0.18.29

## 0.0.9

### Patch Changes

- Updated dependencies [8cbbe0e]
- Updated dependencies [14806c8]
  - jazz-tools@0.18.28

## 0.0.8

### Patch Changes

- Updated dependencies [6c6eb35]
- Updated dependencies [6ca0b59]
- Updated dependencies [88c5f1c]
  - jazz-tools@0.18.27

## 0.0.7

### Patch Changes

- Updated dependencies [4e0ea26]
  - jazz-tools@0.18.26

## 0.0.6

### Patch Changes

- Updated dependencies [4036737]
- Updated dependencies [8ae7d71]
- Updated dependencies [b1d0081]
- Updated dependencies [36a5c58]
- Updated dependencies [94e7d89]
  - jazz-tools@0.18.25

## 0.0.5

### Patch Changes

- Updated dependencies [f4c4ee9]
- Updated dependencies [a15e2ba]
  - jazz-tools@0.18.24

## 0.0.4

### Patch Changes

- Updated dependencies [a0c8a2d]
  - jazz-tools@0.18.23

## 0.0.3

### Patch Changes

- Updated dependencies [22200ac]
- Updated dependencies [1e20db6]
  - jazz-tools@0.18.22

## 0.0.2

### Patch Changes

- 6819f20: Implements SSR options for SvelteKit
- Updated dependencies [6819f20]
  - jazz-tools@0.18.21
