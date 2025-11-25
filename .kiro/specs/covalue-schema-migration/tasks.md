# Implementation Plan

- [ ] 1. Update CoValue interface to support schema storage
  - Add optional `schema` property to the `$jazz` object in CoValue interface
  - Type the schema property as `HydratedCoValueSchema`
  - Update CoValue interface documentation to describe the schema property
  - _Requirements: 7.1, 7.4_

- [ ] 2. Update core type definitions
  - Replace `CoValueClassOrSchema` with `HydratedCoValueSchema` in type definitions
  - Update `ResolveQuery`, `ResolveQueryStrict`, `Loaded`, and `SchemaResolveQuery` types
  - Update `CoValueClassFromAnySchema` type to handle schema-only inputs
  - Ensure `HydratedCoValueSchema` is properly exported and imported where needed
  - _Requirements: 1.1, 1.3, 1.4_

- [ ] 3. Implement schema storage in CoValue creation
  - Update CoValue creation logic to store schema reference in `$jazz.schema`
  - Ensure schema is set when CoValues are created via `create()` methods
  - Ensure schema is set when CoValues are instantiated from schemas
  - _Requirements: 7.1_

- [ ] 4. Implement schema storage in loading and subscription
  - Update `loadCoValue()` to set schema reference when loading CoValues
  - Update `subscribeToCoValue()` to set schema reference when subscribing
  - Update `SubscriptionScope` to propagate schema references
  - Ensure schema is available on loaded CoValue instances
  - _Requirements: 7.1, 7.3_

- [ ] 5. Update function signatures in core interfaces
  - Update `unstable_loadUnique()` to accept `HydratedCoValueSchema`
  - Update `exportCoValue()` to accept `HydratedCoValueSchema`
  - Update `unstable_mergeBranchWithResolve()` to accept `HydratedCoValueSchema`
  - Identify and update any APIs using `CoValue` as generic parameter for schema operations
  - _Requirements: 1.1, 1.2, 1.5_

- [ ] 6. Replace function calls in core interfaces
  - Replace `coValueClassFromCoValueClassOrSchema()` calls with `schema.getCoValueClass()` in interfaces.ts
  - Replace `value.constructor as CoValueClass` patterns with `value.$jazz.schema`
  - Handle all variable name variations correctly
  - _Requirements: 2.1, 2.3, 7.2, 7.5_

- [ ] 7. Update React hooks signatures and implementations
  - Update `useCoValueSubscription()` to accept `HydratedCoValueSchema`
  - Update `useCoState()` to accept `HydratedCoValueSchema`
  - Update `useSubscriptionSelector()` to accept `HydratedCoValueSchema`
  - Replace all `coValueClassFromCoValueClassOrSchema()` calls with `schema.getCoValueClass()`
  - Replace `value.constructor` patterns with `value.$jazz.schema` where applicable
  - _Requirements: 1.1, 2.1, 2.3, 7.2, 7.5_

- [ ] 8. Update Svelte integration
  - Update `CoState` class to accept `HydratedCoValueSchema`
  - Replace all `coValueClassFromCoValueClassOrSchema()` calls with `schema.getCoValueClass()`
  - Replace `value.constructor` patterns with `value.$jazz.schema` where applicable
  - _Requirements: 1.1, 2.1, 2.3, 7.2, 7.5_

- [ ] 9. Update invite system
  - Update `consumeInviteLink()` to accept `HydratedCoValueSchema`
  - Update `consumeInviteLinkFromWindowLocation()` to accept `HydratedCoValueSchema`
  - Update `InviteListener` class to accept `HydratedCoValueSchema`
  - _Requirements: 1.1_

- [ ] 10. Update account methods
  - Update `acceptInvite()` method to accept `HydratedCoValueSchema`
  - _Requirements: 1.1_

- [ ] 11. Update test files
  - Replace all `coValueClassFromCoValueClassOrSchema()` calls in test files with `schema.getCoValueClass()`
  - Replace `value.constructor` patterns with `value.$jazz.schema` where applicable
  - Update test helper functions that use `CoValueClassOrSchema` to use `HydratedCoValueSchema`
  - _Requirements: 2.1, 2.3, 2.4, 7.2, 7.5_

- [ ] 12. Update utility functions in createContext
  - Replace `coValueClassFromCoValueClassOrSchema()` calls in createContext.ts
  - Update any type annotations as needed
  - _Requirements: 2.1, 2.3_

- [ ] 13. Update inbox and request modules
  - Replace `coValueClassFromCoValueClassOrSchema()` calls in inbox.ts
  - Replace `coValueClassFromCoValueClassOrSchema()` calls in request.ts
  - _Requirements: 2.1, 2.3_

- [ ] 14. Update CoValueBase module
  - Replace `coValueClassFromCoValueClassOrSchema()` calls in CoValueBase.ts
  - Replace `value.constructor` patterns with `value.$jazz.schema` where applicable
  - _Requirements: 2.1, 2.3, 7.2, 7.5_

- [ ] 15. Clean up imports
  - Remove unused imports of `coValueClassFromCoValueClassOrSchema` from all files
  - Remove empty import statements
  - Update re-exports to remove `coValueClassFromCoValueClassOrSchema`
  - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [ ] 16. Deprecate or remove coValueClassFromCoValueClassOrSchema function
  - Add deprecation notice to `coValueClassFromCoValueClassOrSchema()` function
  - Update function documentation to recommend using `schema.getCoValueClass()` instead
  - _Requirements: 2.1_

- [ ] 17. Update type exports
  - Remove or deprecate `CoValueClassOrSchema` from public exports
  - Ensure `HydratedCoValueSchema` is properly exported for public use
  - Update internal.ts exports as needed
  - _Requirements: 1.3_

- [ ] 18. Checkpoint - Verify type compilation
  - Run TypeScript compiler to check for type errors
  - Fix any type errors that arise from the migration
  - Ensure all tests pass, ask the user if questions arise
  - _Requirements: 4.3, 4.4_

- [ ] 19. Checkpoint - Run test suite
  - Run the complete test suite to verify functional equivalence
  - Fix any test failures
  - Ensure all tests pass, ask the user if questions arise
  - _Requirements: 4.1, 4.2_

- [ ] 20. Final verification and cleanup
  - Run full build process
  - Verify no remaining references to `CoValueClassOrSchema` in function parameters
  - Verify all schema-related APIs use `HydratedCoValueSchema` instead of `CoValue` generic parameters
  - Verify no remaining calls to `coValueClassFromCoValueClassOrSchema()`
  - Verify no remaining `value.constructor` patterns that should use `value.$jazz.schema`
  - Verify schema references are properly set on all CoValue instances
  - Run linters and fix any issues
  - _Requirements: 1.5, 2.4, 4.4, 7.2, 7.5_
