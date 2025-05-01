# cojson

## 0.13.17

### Patch Changes

- 9fb98e2: Resolve CoValue load as soon as the core is available
- 0b89fad: Re-introduce incremental processing on RawCoList

## 0.13.16

### Patch Changes

- c6fb8dc: Handle null values in msg.id

## 0.13.15

### Patch Changes

- c712ef2: Revert the RawCoList incremental processing

## 0.13.14

### Patch Changes

- 5c2c7d4: Make the incoming messages handling in the sync manager syncronous

## 0.13.13

### Patch Changes

- ec9cb40: Remove .every() call on iterator to fix compat issues with React Native

## 0.13.12

### Patch Changes

- 65719f2: Simplified CoValue loading state management

## 0.13.11

### Patch Changes

- 17273a6: Adds locale support to Segmenter for CoPlainText
  Adds insertAfter to CoPlainText
- 3396ed4: Simplify unknown value management on handleNewContent
- 267ea4c: Fix max recursion error happening on large colists

## 0.13.10

### Patch Changes

- f837cfe: Fix fatal errors happening when loading a CoValueCore without having the dependencies

## 0.13.7

### Patch Changes

- bc3d7bb: Allow to assign the writeOnly role to everyone
- 4e9aae1: Remove risky awaits from handleSyncMessage code path
- 21c935c: Improved the management of peer reconnection and optimized the sync messages
- aa1c80e: feat: add transactions size histogram & loaded covalues gauges
- 13074be: Skip loading from closed peers as early as possible

## 0.13.5

### Patch Changes

- e090b39: Bump crypto version to remove WASM init warning

## 0.13.2

### Patch Changes

- c551839: Add jazz.messagequeue.pushed/pulled counters, remove jazz.messagequeue.size gauge

## 0.13.0

### Minor Changes

- bce3bcc: Version bump. Release focused on React Native.

### Patch Changes

- a013538: Correctly load CoValues after they are marked as unavailable and improve timeout management

## 0.12.2

### Patch Changes

- c2f4827: StreamingHash: Remove redundant clone and skip double hash generation when creating a local transaction

## 0.12.1

### Patch Changes

- 5a00fe0: Re-introducing linked lists on PriorityBasedMessageQueue

## 0.12.0

### Minor Changes

- 01523dc: Check CoValue access permissions when loading
- 01523dc: Return the EVERYONE role if the account is not direct a member of the group

## 0.11.8

### Patch Changes

- 6c86c4f: Fix "Expected header to be sent in first message error" on sync
- 9d0c9dc: Update @opentelemetry/api dependency

## 0.11.7

### Patch Changes

- 2b94bc8: Performance: optimize Group.roleOf getter and made the transactions validation incremental for CoMap and CoFeed
- 2957362: Throw an error when the user tries to load an invalid or undefined id

## 0.11.6

### Patch Changes

- 8ed144e: Fix transaction sorting when the timestamps are equal

## 0.11.5

### Patch Changes

- 60f5b3f: Downgrade the WasmCrypto initialization error logging to a warning

## 0.11.4

### Patch Changes

- 7f036c1: Use PureJSCrypto as fallback when WasmCrypto fails to initialize

## 0.11.3

### Patch Changes

- 68b0242: Improve the error logging to have more information on errors leveraging the pino err serializer

## 0.11.0

### Minor Changes

- e22de9f: Fix roleOf resolution for "everyone"
- 34cbdc3: Added revokeExtend method to Group

### Patch Changes

- b9d194a: Added getAllMemberKeysSet method on RawGroup
  Add everyone to the possible inputs of Group.roleOf
- a4713df: Moving to the d.ts files for the exported type definitions
- 0f67e0a: Allow optional fields in types passed to co.json

## 0.10.15

### Patch Changes

- f86e278: Downgrade the permissions error logs to debug

## 0.10.8

### Patch Changes

- 153dc99: Catch errors on CoValueCore subscribers to avoid effects on the sync

## 0.10.7

### Patch Changes

- 0f83320: Use jazz-crypto-rs isomorphic bundle
- 012022d: Improve error logging on sync errors

## 0.10.6

### Patch Changes

- 5c76e37: Ports Wasm crypto functions to use exported library `jazz-crypto-rs`

## 0.10.4

### Patch Changes

- 1af6072: Revert PriorityBasedMessageQueue to use an array-based queue

## 0.10.2

### Patch Changes

- cae3a9e: Add debug info to load failure end missing header errors

## 0.10.1

### Patch Changes

- 5a63cba: Crypto packages must now be imported from cojson/crypto/WasmCrypto or cojson/crypto/PureJSCrypto
  Removed the separated dists for React Native.

## 0.10.0

### Minor Changes

- 498954f: Introducing the new auth system!

### Patch Changes

- b426342: Export the coValue loading config to reduce the timeout on tests
- 8217981: Drop node 14 polyfill for globalThis.crypto
- ac3d9fa: Reduce the retries on coValue not found to two
- 610543c: Add role mapping to Group.extend

## 0.9.23

### Patch Changes

- 70c9a5d: Rotate keys before revoking access, so when admins remove themselves the keys are successfully rotated

## 0.9.19

### Patch Changes

- 6ad0a9f: Export Json types

## 0.9.18

### Patch Changes

- 8898b10: Export AccountRole type

## 0.9.13

### Patch Changes

- 8d29e50: Restore the logger wrapper and adapt the API to pino

## 0.9.12

### Patch Changes

- 15d4b2a: Revert the custom logger

## 0.9.11

### Patch Changes

- efbf3d8: Optimize queue management
- 5863bad: Wrap all the console logs with a logger class to make possible to customize the logger

## 0.9.10

### Patch Changes

- 4aa377d: Handle unkown coValue content type and optimize content access

## 0.9.9

### Patch Changes

- 8eb9247: Add CoPlainText and CoRichText covalues

## 0.9.0

### Patch Changes

- 8eda792: Add a crypto entry to optionally import the crypto modules
- 1ef3226: Add the assign method to RawCoMap to create bulk transactions and optimize RawCoMap init

## 0.8.50

### Patch Changes

- 43378ef: Handle circular references in group inheritance

## 0.8.49

### Patch Changes

- 25dfd90: Fixes the transactions collection on permissions to avoid RangeError issues

## 0.8.48

### Patch Changes

- 10ea733: Give the ability to extend a group to accounts with reader, writer and writeOnly access level to the parent group and not only admins. The account still needs to be an admin on the child group to be able to extend it.

## 0.8.45

### Patch Changes

- 6f0bd7f: Throw an error if an invite is created from an account owned coValue
- fca6a0b: skip verify step when creating a new local transaction
- 88d7d9a: Add Inbox a new experimental API to simplfy the initial handshake between accounts

## 0.8.44

### Patch Changes

- 5d20c81: Add an internal API to disable the permission errors logs

## 0.8.41

### Patch Changes

- 3252502: Optimize the transactions processing on CoMap and CoStream
- 6370348: Remove @opentelemetry/api as a peer dependency and add it as a dependency
- ac216b9: Add a new writeOnly role, to limit access only to their own changes. Useful to push objects into lists of moderated content.

## 0.8.39

### Patch Changes

- 249eecb: Added new APIs to wait for CoValue sync
- 3121551: Add connected peers gauge metric

## 0.8.38

### Patch Changes

- b00ee91: Apply time travel when checking the roles on a parent group
- f488c09: Add message queue size metrics

## 0.8.37

### Patch Changes

- 3d9f12e: Optimize the atTime processing on CoMap

## 0.8.36

### Patch Changes

- 441fe27: Optimise large record-like CoMaps for access of latest value

## 0.8.35

### Patch Changes

- 3f15a23: Resolve deadlock in cluster setup with multiple layers of sync & storage servers
- 46f2ab8: Add emptyKnownState and SessionNewContent to the package exports
- 8b87117: Implement Group Inheritance
- a6b6ccf: Upload new coValues when a peer is added

## 0.8.34

### Patch Changes

- e4f110f: Removed a no more valid error on the known state message sync

## 0.8.32

### Patch Changes

- df42b2b: Catch hard-to-debug errors when trying to get edits at a CoMap key called "constructor"

## 0.8.31

### Patch Changes

- e511d6d: Performance: make the isUploaded check on the SyncStateManager lazy

## 0.8.30

### Patch Changes

- 0a2fae3: More optimised way to get knownState
- 99cda2f: Reduce noise on peer close and increase the load timeout

## 0.8.29

### Patch Changes

- dcc9c2e: Clear out the queues when closing a Peer
- 699553f: Restore offline support and improve loading perfromance when values are cached

## 0.8.28

### Patch Changes

- 605734c: Send empty known state on all states except available

## 0.8.27

### Patch Changes

- 75fdff4: Improve the initial load by telling what's on storage in the first load request

## 0.8.23

### Patch Changes

- 6f745be: Retry unavailable coValues and improve the state management around them
- 124bf67: Add a flag to delete a peer when it is closed

## 0.8.21

### Patch Changes

- 0f30eea: Improved the known state tracking within the PeerState.knownState property

## 0.8.19

### Patch Changes

- 9c2aadb: Set a CoValue as errored per peer after first error

## 0.8.18

### Patch Changes

- d4319d8: Immediately ack new content before syncing it upstream

## 0.8.17

### Patch Changes

- d433cf4: Add a new API to wait for a CoValue to be uploaded in a peer

## 0.8.16

### Patch Changes

- b934fab: Temp fix for more graceful handling of accidental multiple agents in account

## 0.8.12

### Patch Changes

- 6ed75eb: Introduce "storage" peer role

## 0.8.11

### Patch Changes

- 1ed4ab5: Make the peer known states subscribable

## 0.8.5

### Patch Changes

- c3f4e6b: Fix order of exports fields in package.json
- d9152ed: Allow interface types as generic argument in co.json

## 0.8.3

### Patch Changes

- Experimental support for React Native

## 0.8.0

### Patch Changes

- 6a147c2: Move session generation to crypto provider
- ad40b88: First sketch of API for creating and finding unique CoValues

## 0.7.35

### Patch Changes

- 35bbcd9: Clean up binary stream ending logic
- f350e90: Added a priority system for the sync messages

## 0.7.34

### Patch Changes

- 5d91f9f: Stop using tryAddTransactionsAsync
- 5094e6d: Start introducing neverthrow, make tryAddNewTransactionsAsync and handleNewContent less throwy
- b09589b: Only one async transaction per CoValue at a time again
- 2c3a40c: Use fork of queueable
- 4e16575: Use queueable fork
- ea882ab: Better log message for failed transactions

## 0.7.34-neverthrow.8

### Patch Changes

- Use queueable fork

## 0.7.34-neverthrow.7

### Patch Changes

- Use fork of queueable

## 0.7.34-neverthrow.4

### Patch Changes

- Stop using tryAddTransactionsAsync

## 0.7.34-neverthrow.3

### Patch Changes

- Better log message for failed transactions

## 0.7.34-neverthrow.1

### Patch Changes

- Only one async transaction per CoValue at a time again

## 0.7.34-neverthrow.0

### Patch Changes

- Start introducing neverthrow, make tryAddNewTransactionsAsync and handleNewContent less throwy

## 0.7.33

### Patch Changes

- b297c93: Improve logging
- 3bf5127: Allow crashing whole node on peer errors
- a8b74ff: Throw properly on peer that should crash on close

## 0.7.33-hotfixes.5

### Patch Changes

- Make simulated errors even more likely

## 0.7.33-hotfixes.4

### Patch Changes

- Throw properly on peer that should crash on close

## 0.7.33-hotfixes.3

### Patch Changes

- Allow crashing whole node on peer errors

## 0.7.33-hotfixes.0

### Patch Changes

- Improve logging

## 0.7.31

### Patch Changes

- Close both ends of the peer on gracefulShutdown

## 0.7.29

### Patch Changes

- Remove noisy log

## 0.7.28

### Patch Changes

- Fix ignoring server peers

## 0.7.26

### Patch Changes

- Remove Effect from jazz/cojson internals

## 0.7.23

### Patch Changes

- Mostly complete OPFS implementation (single-tab only)

## 0.7.18

### Patch Changes

- Update to Effect 3.5.2

## 0.7.17

### Patch Changes

- Fix bugs in new storage interface

## 0.7.14

### Patch Changes

- Use Effect Queues and Streams instead of custom queue implementation

## 0.7.11

### Patch Changes

- Fix webpack import of node:crypto module

## 0.7.10

### Patch Changes

- Also cache agent ID in RawControlledAccount

## 0.7.9

### Patch Changes

- Cache currentAgentID in RawAccount

## 0.7.0

### Minor Changes

- e299c3e: New simplified API

### Patch Changes

- 1a35307: WIP working-ish version of LSM storage
- 96c494f: Implement profile visibility based on groups & new migration signature
- 19f52b7: Fixed bug with newRandomSessionID being called before crypto was ready
- d8fe2b1: Expose experimental OPFS storage
- 1200aae: CoJSON performance improvement
- 52675c9: Fix CoList.splice / RawCoList.append
- 1a35307: Optimizations for incoming sync messages
- bf0f8ec: Fix noble curves dependency
- c4151fc: Support stricter TS lint rules
- 8636319: Factor out implementation of crypto provider and provide pure JS implementation
- 952982e: Consistent proxy based API
- 21771c4: Reintroduce changes from main
- 69ac514: Use effect schema much less
- f0f6f1b: Clean up API more & re-add jazz-nodejs
- 1a44f87: Refactoring
- 63374cc: Make sure delete on CoMaps deletes keys

## 0.7.0-alpha.42

### Patch Changes

- Fixed bug with newRandomSessionID being called before crypto was ready

## 0.7.0-alpha.39

### Patch Changes

- Fix noble curves dependency

## 0.7.0-alpha.38

### Patch Changes

- Factor out implementation of crypto provider and provide pure JS implementation

## 0.7.0-alpha.37

### Patch Changes

- Expose experimental OPFS storage

## 0.7.0-alpha.36

### Patch Changes

- 1a35307: WIP working-ish version of LSM storage
- 1a35307: Optimizations for incoming sync messages

## 0.7.0-alpha.35

### Patch Changes

- CoJSON performance improvement

## 0.7.0-alpha.29

### Patch Changes

- Reintroduce changes from main

## 0.7.0-alpha.28

### Patch Changes

- Implement profile visibility based on groups & new migration signature

## 0.7.0-alpha.27

### Patch Changes

- Fix CoList.splice / RawCoList.append

## 0.7.0-alpha.24

### Patch Changes

- Make sure delete on CoMaps deletes keys

## 0.7.0-alpha.11

### Patch Changes

- Support stricter TS lint rules

## 0.7.0-alpha.10

### Patch Changes

- Clean up API more & re-add jazz-nodejs

## 0.7.0-alpha.7

### Patch Changes

- Consistent proxy based API

## 0.7.0-alpha.5

### Patch Changes

- Refactoring

## 0.7.0-alpha.1

### Patch Changes

- Use effect schema much less

## 0.7.0-alpha.0

### Minor Changes

- New simplified API

## 0.6.6

### Patch Changes

- Fix migration changes being lost on loaded account

## 0.6.5

### Patch Changes

- Fix loading of accounts

## 0.6.4

### Patch Changes

- IndexedDB & timer perf improvements

## 0.6.3

### Patch Changes

- Implement passphrase based auth

## 0.6.2

### Patch Changes

- Add peersToLoadFrom for node creation as well

## 0.6.1

### Patch Changes

- Provide localNode to AccountMigrations

## 0.6.0

### Minor Changes

- Make addMember and removeMember take loaded Accounts instead of just IDs

## 0.5.2

### Patch Changes

- Allow account migrations to be async

## 0.5.1

### Patch Changes

- Fix bug where accounts, profiles and data created in migrations isn't synced on account creation

## 0.5.0

### Minor Changes

- Adding a lot of performance improvements to cojson, add a stresstest for the twit example and make that run smoother in a lot of ways.
