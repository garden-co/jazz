# Implementation Tasks

## Tasks

### Crypto Layer

- [ ] 1. Add `SealedForGroup<T>` type to crypto types (`packages/cojson/src/crypto/crypto.ts`)
- [ ] 2. Add `groupSealerFromReadKey(readKeySecret: KeySecret)` method to `CryptoProvider` - derives sealer deterministically using BLAKE3 with "groupSealer" context
- [ ] 3. Add abstract `sealForGroup()` method signature to `CryptoProvider`
- [ ] 4. Add abstract `unsealForGroup()` method signature to `CryptoProvider`
- [ ] 5. Implement `sealForGroup()` in `PureJSCrypto` using anonymous box pattern (`packages/cojson/src/crypto/PureJSCrypto.ts`)
- [ ] 6. Implement `unsealForGroup()` in `PureJSCrypto`
- [ ] 7. Implement `sealForGroup()` in native crypto (Rust NAPI - `crates/cojson-core-napi/`)
- [ ] 8. Implement `unsealForGroup()` in native crypto (Rust NAPI)
- [ ] 9. Implement `sealForGroup()` in WASM crypto (`crates/cojson-core-wasm/`)
- [ ] 10. Implement `unsealForGroup()` in WASM crypto

### Group Schema

- [ ] 11. Update `GroupShape` type to include `groupSealer?: SealerID` field
- [ ] 12. Update `GroupShape` type to include `${KeyID}_for_${SealerID}` revelation pattern

### Group Creation & Rotation

- [ ] 13. Add `initializeGroupSealer()` private method to `RawGroup` (stores public key only)
- [ ] 14. Call `initializeGroupSealer()` in group creation flow
- [ ] 15. Add `getGroupSealerSecret()` method that derives sealer secret from read key
- [ ] 16. Update `rotateReadKey()` to also update the group sealer (derive new sealer from new read key)

### Permission Validation

- [ ] 17. Add `groupSealer` to the `MapOpPayload` type union in `determineValidTransactionsForGroup` (`packages/cojson/src/permissions.ts`)
- [ ] 18. Add validation case for `groupSealer` field - only admins/managers can set (same as `readKey`)

### Key Revelation (Core Change)

- [ ] 19. Add `storeKeyRevelationForGroupSealer()` private method to `RawGroup`
- [ ] 20. Update `revealReadKeyToParentGroup()` to use group sealer when available (instead of writeOnly key)
- [ ] 21. Add fallback logic for legacy groups without `groupSealer`

### Key Resolution

- [ ] 22. Add `getLastKeyEdit()` helper method if not existing (to retrieve value + tx info)
- [ ] 23. Update `getUncachedReadKey()` to check for keys revealed via parent group sealer (current sealer)
- [ ] 24. Update `getUncachedReadKey()` to also check historical group sealers (derived from historical read keys)
- [ ] 25. Ensure proper nOnceMaterial recovery from transaction info

### Tests

- [ ] 26. Add unit tests for `sealForGroup` / `unsealForGroup` crypto operations
- [ ] 27. Add integration test: groups created with `groupSealer`
- [ ] 28. Add integration test: new members can derive group sealer secret from read key
- [ ] 29. Add integration test: non-member extending child to parent via groupSealer
- [ ] 30. Add integration test: verify no writeOnly key created when parent has groupSealer
- [ ] 31. Add integration test: legacy fallback (parent without groupSealer uses writeOnly key)
- [ ] 32. Add integration test: key rotation in parent updates groupSealer
- [ ] 33. Add integration test: old revelations decryptable after parent key rotation (via historical sealer)
- [ ] 34. Add integration test: multiple non-member extensions to same parent
- [ ] 35. Add integration test: concurrent group sealer initialization by multiple admins produces same result
- [ ] 36. Add integration test: non-admin cannot set groupSealer (permission validation)

### Exports

- [ ] 37. Export new types (`SealedForGroup`) from `packages/cojson/src/exports.ts`
