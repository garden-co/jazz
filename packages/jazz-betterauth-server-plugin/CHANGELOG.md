# jazz-betterauth-server-plugin

## 0.17.2

### Patch Changes

- Updated dependencies [794681a]
- Updated dependencies [5b2b16a]
- Updated dependencies [83fc22f]
  - jazz-tools@0.17.2
  - cojson@0.17.2

## 0.17.1

### Patch Changes

- Updated dependencies [0bcbf55]
- Updated dependencies [2fd88b9]
- Updated dependencies [d1bdbf5]
- Updated dependencies [4b73834]
  - jazz-tools@0.17.1
  - cojson@0.17.1

## 0.17.0

### Patch Changes

- Updated dependencies [fcaf4b9]
  - jazz-tools@0.17.0
  - cojson@0.17.0

## 0.16.6

### Patch Changes

- Updated dependencies [67e0968]
- Updated dependencies [ce9ca54]
- Updated dependencies [4b99ff1]
- Updated dependencies [ac5d20d]
- Updated dependencies [2c8120d]
- Updated dependencies [9bf7946]
  - jazz-tools@0.16.6
  - cojson@0.16.6

## 0.16.5

### Patch Changes

- Updated dependencies [3cd1586]
- Updated dependencies [267f689]
- Updated dependencies [33ebbf0]
  - jazz-tools@0.16.5
  - cojson@0.16.5

## 0.16.4

### Patch Changes

- Updated dependencies [f9d538f]
- Updated dependencies [16764f6]
- Updated dependencies [802b5a3]
  - cojson@0.16.4
  - jazz-tools@0.16.4

## 0.16.3

### Patch Changes

- Updated dependencies [43d3511]
  - jazz-tools@0.16.3
  - cojson@0.16.3

## 0.16.2

### Patch Changes

- cojson@0.16.2
- jazz-tools@0.16.2

## 0.16.1

### Patch Changes

- Updated dependencies [c62abef]
  - jazz-tools@0.16.1
  - cojson@0.16.1

## 0.16.0

### Patch Changes

- 2bbb07b: Introduce a cleaner separation between Zod and CoValue schemas:
  - Zod schemas and CoValue schemas are fully separated. Zod schemas can only be composed with other Zod schemas. CoValue schemas can be composed with either Zod or other CoValue schemas.
  - `z.optional()` and `z.discriminatedUnion()` no longer work with CoValue schemas. Use `co.optional()` and `co.discriminatedUnion()` instead.
  - Internal schema access is now simpler. You no longer need to use Zod’s `.def` to access internals. Use properties like `CoMapSchema.shape`, `CoListSchema.element`, and `CoOptionalSchema.innerType` directly.
  - CoValue schema types are now namespaced under `co.`. Non-namespaced exports have been removed
  - CoMap schemas no longer incorrectly inherit from Zod. Previously, methods like `.extend()` and `.partial()` appeared available but could cause unexpected behavior. These methods are now disabled. In their place, `.optional()` has been added, and more Zod-like methods will be introduced in future releases.
  - Upgraded Zod from `3.25.28` to `3.25.76`.
  - Removed deprecated `withHelpers` method from CoValue schemas
  - Removed deprecated `createCoValueObservable` function
- Updated dependencies [c09dcdf]
- Updated dependencies [2bbb07b]
  - jazz-tools@0.16.0
  - cojson@0.16.0

## 0.15.16

### Patch Changes

- Updated dependencies [9633d01]
- Updated dependencies [4beafb7]
  - jazz-tools@0.15.16
  - cojson@0.15.16

## 0.15.15

### Patch Changes

- Updated dependencies [3fe53a3]
  - jazz-tools@0.15.15
  - cojson@0.15.15

## 0.15.14

### Patch Changes

- Updated dependencies [70ce7c5]
- Updated dependencies [a584590]
- Updated dependencies [9acccb5]
  - cojson@0.15.14
  - jazz-tools@0.15.14

## 0.15.13

### Patch Changes

- Updated dependencies [6c76ff8]
  - jazz-tools@0.15.13
  - cojson@0.15.13

## 0.15.12

### Patch Changes

- Updated dependencies [d1c1b0c]
- Updated dependencies [cf4ad72]
  - jazz-tools@0.15.12
  - cojson@0.15.12

## 0.15.11

### Patch Changes

- Updated dependencies [bdc9aee]
  - jazz-tools@0.15.11
  - cojson@0.15.11

## 0.15.10

### Patch Changes

- Updated dependencies [9815ec6]
- Updated dependencies [b4fdab4]
  - jazz-tools@0.15.10
  - cojson@0.15.10

## 0.15.9

### Patch Changes

- Updated dependencies [27b4837]
- Updated dependencies [2776263]
  - jazz-tools@0.15.9
  - cojson@0.15.9

## 0.15.8

### Patch Changes

- Updated dependencies [3844666]
  - jazz-tools@0.15.8
  - cojson@0.15.8

## 0.15.7

### Patch Changes

- Updated dependencies [c09b636]
  - jazz-tools@0.15.7
  - cojson@0.15.7

## 0.15.6

### Patch Changes

- Updated dependencies [a5ceaff]
  - jazz-tools@0.15.6
  - cojson@0.15.6

## 0.15.5

### Patch Changes

- Updated dependencies [23bfea5]
- Updated dependencies [e4ba23c]
- Updated dependencies [4b89838]
  - jazz-tools@0.15.5
  - cojson@0.15.5

## 0.15.4

### Patch Changes

- Updated dependencies [277e4d4]
  - cojson@0.15.4
  - jazz-tools@0.15.4

## 0.15.3

### Patch Changes

- Updated dependencies [45f73a7]
  - jazz-tools@0.15.3
  - cojson@0.15.3

## 0.15.2

### Patch Changes

- Updated dependencies [4b964ed]
- Updated dependencies [0e7e532]
  - cojson@0.15.2
  - jazz-tools@0.15.2

## 0.15.1

### Patch Changes

- Updated dependencies [0e3a4d2]
- Updated dependencies [b110f00]
  - jazz-tools@0.15.1
  - cojson@0.15.1

## 0.15.0

### Patch Changes

- Updated dependencies [1378a1f]
- Updated dependencies [0fa051a]
  - jazz-tools@0.15.0
  - cojson@0.15.0

## 0.14.28

### Patch Changes

- Updated dependencies [06c5a1c]
  - jazz-tools@0.14.28
  - jazz-browser@0.14.28
  - cojson@0.14.28

## 0.14.27

### Patch Changes

- Updated dependencies [a026073]
  - jazz-tools@0.14.27
  - jazz-browser@0.14.27
  - cojson@0.14.27

## 0.14.26

### Patch Changes

- Updated dependencies [e74a077]
  - cojson@0.14.26
  - jazz-browser@0.14.26
  - jazz-tools@0.14.26

## 0.14.25

### Patch Changes

- Updated dependencies [99a2d9b]
- Updated dependencies [b3ff726]
  - jazz-tools@0.14.25
  - jazz-browser@0.14.25
  - cojson@0.14.25

## 0.14.24

### Patch Changes

- cojson@0.14.24
- jazz-browser@0.14.24
- jazz-tools@0.14.24

## 0.14.23

### Patch Changes

- Updated dependencies [1ca9299]
- Updated dependencies [9177579]
  - cojson@0.14.23
  - jazz-tools@0.14.23
  - jazz-browser@0.14.23

## 0.14.22

### Patch Changes

- Updated dependencies [57fb69f]
- Updated dependencies [048ac1d]
  - cojson@0.14.22
  - jazz-tools@0.14.22
  - jazz-browser@0.14.22

## 0.14.21

### Patch Changes

- Updated dependencies [e7e505e]
- Updated dependencies [c3d8779]
- Updated dependencies [13b57aa]
- Updated dependencies [5662faa]
- Updated dependencies [2116a59]
  - jazz-tools@0.14.21
  - cojson@0.14.21
  - jazz-browser@0.14.21

## 0.14.20

### Patch Changes

- Updated dependencies [6f72419]
- Updated dependencies [04b20c2]
  - jazz-tools@0.14.20
  - jazz-browser@0.14.20
  - cojson@0.14.20

## 0.14.19

### Patch Changes

- cojson@0.14.19
- jazz-browser@0.14.19
- jazz-tools@0.14.19

## 0.14.18

### Patch Changes

- Updated dependencies [4b950bc]
- Updated dependencies [0d5ee3e]
- Updated dependencies [d6d9c0a]
- Updated dependencies [c559054]
  - jazz-tools@0.14.18
  - cojson@0.14.18
  - jazz-browser@0.14.18

## 0.14.17

### Patch Changes

- e512df4: Move to latest stable version of Zod
- Updated dependencies [e512df4]
  - jazz-tools@0.14.17
  - jazz-browser@0.14.17

## 0.14.16

### Patch Changes

- Updated dependencies [5e253cc]
  - cojson@0.14.16
  - jazz-browser@0.14.16
  - jazz-tools@0.14.16

## 0.14.15

### Patch Changes

- Updated dependencies [23daa7c]
  - cojson@0.14.15
  - jazz-browser@0.14.15
  - jazz-tools@0.14.15

## 0.14.14

### Patch Changes

- Updated dependencies [e32a1f7]
  - jazz-tools@0.14.14
  - jazz-browser@0.14.14

## 0.14.10

### Patch Changes

- Updated dependencies [dc746a2]
- Updated dependencies [f869d9a]
- Updated dependencies [3fe6832]
  - jazz-tools@0.14.10
  - jazz-browser@0.14.10

## 0.14.9

### Patch Changes

- Updated dependencies [22c2600]
  - jazz-tools@0.14.9
  - jazz-browser@0.14.9

## 0.14.8

### Patch Changes

- Updated dependencies [637ae13]
  - jazz-tools@0.14.8
  - jazz-browser@0.14.8

## 0.14.7

### Patch Changes

- Updated dependencies [365b0ea]
  - jazz-tools@0.14.7
  - jazz-browser@0.14.7

## 0.14.6

### Patch Changes

- Updated dependencies [9d6d9fe]
- Updated dependencies [9d6d9fe]
  - jazz-tools@0.14.6
  - jazz-browser@0.14.6

## 0.14.5

### Patch Changes

- Updated dependencies [91cbb2f]
- Updated dependencies [20b3d88]
  - jazz-tools@0.14.5
  - jazz-browser@0.14.5

## 0.14.4

### Patch Changes

- Updated dependencies [011af55]
  - jazz-tools@0.14.4
  - jazz-browser@0.14.4

## 0.14.2

### Patch Changes

- Updated dependencies [3d1027f]
- Updated dependencies [c240eed]
  - jazz-tools@0.14.2
  - jazz-browser@0.14.2

## 0.14.1

### Patch Changes

- Updated dependencies [c8b33ad]
- Updated dependencies [cdfc105]
  - cojson@0.14.1
  - jazz-tools@0.14.1
  - jazz-browser@0.14.1

## 0.14.0

### Patch Changes

- Updated dependencies [5835ed1]
- Updated dependencies [5835ed1]
  - cojson@0.14.0
  - jazz-tools@0.14.0
  - jazz-browser@0.14.0

## 0.13.32

### Patch Changes

- jazz-browser@0.13.32

## 0.13.31

### Patch Changes

- Updated dependencies [e5b170f]
- Updated dependencies [d63716a]
- Updated dependencies [d5edad7]
  - jazz-tools@0.13.31
  - cojson@0.13.31
  - jazz-browser@0.13.31

## 0.13.30

### Patch Changes

- Updated dependencies [07dd2c5]
  - cojson@0.13.30
  - jazz-browser@0.13.30
  - jazz-tools@0.13.30

## 0.0.1

### Patch Changes

- Updated dependencies [eef1a5d]
- Updated dependencies [191ae38]
- Updated dependencies [daee7b9]
  - cojson@0.13.29
  - jazz-browser@0.13.29
  - jazz-tools@0.13.29
