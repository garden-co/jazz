# Design Document

## Overview

This design document outlines the migration strategy for transitioning the Jazz Tools codebase from using `CoValueClassOrSchema` (a union type accepting both CoValue classes and schemas) to using `HydratedCoValueSchema` exclusively. The migration also involves replacing all calls to the `coValueClassFromCoValueClassOrSchema()` utility function with direct calls to the `schema.getCoValueClass()` method.

`HydratedCoValueSchema` is an interface that extends `CoreCoValueSchema` and includes the `getCoValueClass()` method, ensuring that all schemas passed to functions have the necessary method to retrieve their corresponding CoValue class.

The primary goal is to simplify the type system and API surface by standardizing on schema-based interfaces. This will make the codebase more maintainable and reduce cognitive overhead for developers working with the Jazz framework.

## Architecture

### Current Architecture

The current architecture supports two ways of specifying CoValue types:

1. **Class-based**: Direct use of CoValue classes (e.g., `CoMap`, `CoList`, `Account`)
2. **Schema-based**: Use of schema objects created with `co.map()`, `co.list()`, etc.

The `CoValueClassOrSchema` type union allows functions to accept either approach:

```typescript
type CoValueClassOrSchema = CoValueClass | CoreCoValueSchema;
```

When a function receives a `CoValueClassOrSchema` parameter, it uses the `coValueClassFromCoValueClassOrSchema()` utility to extract the actual class:

```typescript
const cls = coValueClassFromCoValueClassOrSchema(schema);
```

### Target Architecture

The target architecture will standardize on schema-based interfaces:

1. All public APIs will accept `HydratedCoValueSchema` instead of `CoValueClassOrSchema`
2. `HydratedCoValueSchema` extends `CoreCoValueSchema` and guarantees the presence of the `getCoValueClass()` method
3. Schema objects provide a `getCoValueClass()` method to retrieve the underlying class
4. The `coValueClassFromCoValueClassOrSchema()` function will be deprecated and removed

This simplification reduces the number of code paths and makes the type system more predictable. By using `HydratedCoValueSchema`, we ensure type safety at compile time that all schemas have the required `getCoValueClass()` method.

## Components and Interfaces

### Affected Components

1. **Type Definitions** (`packages/jazz-tools/src/tools/implementation/zodSchema/zodSchema.ts`)
   - `CoValueClassOrSchema` type (will be replaced with `HydratedCoValueSchema`)
   - `CoValueClassFromAnySchema` type (may need adjustment)
   - `ResolveQuery` type (uses `CoValueClassOrSchema` as constraint)
   - `ResolveQueryStrict` type (uses `CoValueClassOrSchema` as constraint)
   - `Loaded` type (uses `CoValueClassOrSchema` as constraint)
   - `SchemaResolveQuery` type (uses `CoValueClassOrSchema` as constraint)

2. **Utility Functions** (`packages/jazz-tools/src/tools/implementation/zodSchema/runtimeConverters/coValueSchemaTransformation.ts`)
   - `coValueClassFromCoValueClassOrSchema()` (will be deprecated)
   - `isCoValueSchema()` (may need adjustment)

3. **Core Interfaces** (`packages/jazz-tools/src/tools/coValues/interfaces.ts`)
   - `unstable_loadUnique()` - uses `CoValueClassOrSchema` parameter, needs migration to `HydratedCoValueSchema`
   - `exportCoValue()` - uses `CoValueClassOrSchema` parameter, needs migration to `HydratedCoValueSchema`
   - `unstable_mergeBranchWithResolve()` - uses `CoValueClassOrSchema` parameter, needs migration to `HydratedCoValueSchema`
   - Multiple internal calls to `coValueClassFromCoValueClassOrSchema()` that need replacement with `schema.getCoValueClass()`
   - APIs using `CoValue` as generic type parameter for schema operations need to be evaluated and potentially migrated to `HydratedCoValueSchema`

4. **React Hooks** (`packages/jazz-tools/src/react-core/hooks.ts`)
   - `useCoValueSubscription()` - uses `CoValueClassOrSchema` parameter
   - `useCoState()` - uses `CoValueClassOrSchema` parameter
   - `useSubscriptionSelector()` - uses `CoValueClassOrSchema` parameter
   - Multiple calls to `coValueClassFromCoValueClassOrSchema()`

5. **Svelte Integration** (`packages/jazz-tools/src/svelte/jazz.class.svelte.ts`)
   - `CoState` class - uses `CoValueClassOrSchema` parameter
   - Multiple calls to `coValueClassFromCoValueClassOrSchema()`

6. **Invite System** (`packages/jazz-tools/src/tools/implementation/invites.ts`, `packages/jazz-tools/src/browser/index.ts`)
   - `consumeInviteLink()` - uses `CoValueClassOrSchema` parameter
   - `consumeInviteLinkFromWindowLocation()` - uses `CoValueClassOrSchema` parameter

7. **Account Methods** (`packages/jazz-tools/src/tools/coValues/account.ts`)
   - `acceptInvite()` - uses `CoValueClassOrSchema` parameter

8. **Test Files** (multiple files in `packages/jazz-tools/src/tools/tests/`)
   - Extensive use of `coValueClassFromCoValueClassOrSchema()` in test code

### Schema Interface

The `HydratedCoValueSchema` interface extends `CoreCoValueSchema` and guarantees the presence of the `getCoValueClass()` method:

```typescript
interface HydratedCoValueSchema extends CoreCoValueSchema {
  getCoValueClass: () => CoValueClass<CoValue>;
}
```

All schema classes implement this interface:
- `CoMapSchema`
- `CoListSchema`
- `CoFeedSchema`
- `AccountSchema`
- `GroupSchema`
- `FileStreamSchema`
- `CoVectorSchema`
- `PlainTextSchema`
- `RichTextSchema`
- `CoOptionalSchema`
- `CoDiscriminatedUnionSchema`

### APIs Using CoValue Generic Parameters

Several APIs in `interfaces.ts` currently use `CoValue` as a generic type parameter when they should be using `HydratedCoValueSchema` for schema-based operations. These include:

1. **Type-level APIs**: Functions that accept schemas and need to extract type information
2. **Loading APIs**: Functions that load CoValues based on schema definitions
3. **Subscription APIs**: Functions that subscribe to CoValue changes using schemas

The migration will identify these cases and update them to use `HydratedCoValueSchema` where appropriate, ensuring type safety and consistency with the schema-based approach.

### Schema Storage in CoValue Instances

To enable migration away from `value.constructor` patterns, CoValue instances will store a reference to their schema in the `$jazz` object:

```typescript
interface CoValue {
  $jazz: {
    // ... existing properties
    schema?: HydratedCoValueSchema; // New property
  };
}
```

**Benefits:**
1. **Direct Schema Access**: Enables accessing the schema without relying on constructor casting
2. **Type Safety**: Provides compile-time guarantees about schema availability
3. **Cleaner API**: Eliminates the need for `value.constructor as CoValueClass` patterns
4. **Future-Proof**: Supports future schema-based features like introspection and validation

**Implementation Points:**
- Schema reference must be set when CoValue instances are created from schemas
- Schema reference must be set when CoValues are loaded or subscribed to
- Existing code using `value.constructor` should be migrated to use the stored schema reference
- The schema reference should be optional to maintain backward compatibility during migration

## Data Models

### Type Hierarchy

```
CoreCoValueSchema (interface)
├── CoreCoMapSchema
├── CoreAccountSchema
├── CoreGroupSchema
├── CoreCoRecordSchema
├── CoreCoListSchema
├── CoreCoFeedSchema
├── CoreCoDiscriminatedUnionSchema
├── CoreCoOptionalSchema
├── CorePlainTextSchema
├── CoreRichTextSchema
├── CoreFileStreamSchema
└── CoreCoVectorSchema

HydratedCoValueSchema (interface, extends CoreCoValueSchema)
└── Adds getCoValueClass() method requirement
```

Each schema type has a corresponding schema class that implements `HydratedCoValueSchema` by providing the `getCoValueClass()` method.

### Migration Mapping

| Current Pattern | Target Pattern |
|----------------|----------------|
| `S extends CoValueClassOrSchema` | `S extends HydratedCoValueSchema` |
| `coValueClassFromCoValueClassOrSchema(schema)` | `schema.getCoValueClass()` |
| `import { coValueClassFromCoValueClassOrSchema }` | (remove if unused) |
| `import { CoValueClassOrSchema }` | `import { HydratedCoValueSchema }` |

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Test suite preservation

*For any* migrated codebase, running the existing test suite should result in all tests passing with the same outcomes as before the migration.

**Validates: Requirements 4.1, 4.2**

### Property 2: Type safety preservation

*For any* migrated file, running the TypeScript compiler should produce no new type errors compared to the pre-migration state.

**Validates: Requirements 4.3**

## Error Handling

### Migration Errors

The migration process should handle the following error scenarios:

1. **Type Mismatch**: If a function truly needs to accept both classes and schemas (rare), document this as a special case requiring manual intervention
2. **Missing getCoValueClass Method**: If a schema doesn't implement `getCoValueClass()`, the migration should fail with a clear error message
3. **Complex Type Expressions**: If `CoValueClassOrSchema` appears in complex type expressions that can't be automatically transformed, flag for manual review
4. **Build Failures**: If the migration causes build failures, provide clear diagnostics about which changes caused the issue

### Runtime Considerations

Since this is primarily a type-level migration, runtime errors should be minimal. However:

1. **Schema Validation**: Ensure all schemas passed to migrated functions are valid `HydratedCoValueSchema` instances with the `getCoValueClass()` method
2. **Null/Undefined Handling**: Preserve existing null/undefined handling for schema parameters
3. **Type Guards**: Update or remove type guards that check for `CoValueClass` vs `CoreCoValueSchema`

## Testing Strategy

The migration is a refactoring operation that must preserve all existing functionality. The testing strategy is straightforward:

1. **Existing Test Suite**: Run the complete existing test suite after migration to verify functional equivalence
2. **Type Checking**: Run TypeScript compiler to ensure no new type errors are introduced
3. **Build Verification**: Run the full build process to ensure no compilation errors

No new tests need to be written for this migration. The existing test coverage is sufficient to validate that the refactoring preserves all functionality.

## Implementation Approach

### Phase 1: Type Definition Updates

1. Update the `CoValue` interface to include an optional `schema` property in the `$jazz` object
2. Update type definitions in `zodSchema.ts` to replace `CoValueClassOrSchema` with `HydratedCoValueSchema`
3. Update generic type constraints across the codebase
4. Verify type compilation

### Phase 2: Schema Storage Implementation

1. Update CoValue creation logic to store schema references in `$jazz.schema`
2. Update loading and subscription logic to set schema references
3. Verify schema references are properly propagated

### Phase 3: Function Call Replacements

1. Replace all calls to `coValueClassFromCoValueClassOrSchema(schema)` with `schema.getCoValueClass()`
2. Replace `value.constructor as CoValueClass` patterns with `value.$jazz.schema`
3. Handle variable name variations correctly
4. Verify each replacement maintains the same behavior

### Phase 4: Import Cleanup

1. Remove unused imports of `coValueClassFromCoValueClassOrSchema`
2. Clean up empty import statements
3. Update re-exports as needed

### Phase 5: Verification

1. Run TypeScript compiler to check for type errors
2. Run full test suite to verify functional equivalence
3. Run build process to ensure no build errors
4. Review all changes for correctness

### Phase 6: Documentation

1. Update API documentation to reflect schema-only interfaces
2. Document the new `$jazz.schema` property
3. Create migration guide for external users
4. Document any breaking changes
5. Update examples and tutorials

## Migration Considerations

### Breaking Changes

This migration introduces breaking changes for external users who:

1. Pass CoValue classes directly to functions that now require schemas
2. Use `CoValueClassOrSchema` type in their own code
3. Import and use `coValueClassFromCoValueClassOrSchema` utility

### Migration Path for Users

External users will need to:

1. Replace CoValue class usage with schema usage (e.g., use `co.map()` instead of `CoMap`)
2. Update type annotations from `CoValueClassOrSchema` to `HydratedCoValueSchema`
3. Replace `coValueClassFromCoValueClassOrSchema(x)` with `x.getCoValueClass()`
4. Update any APIs that were using `CoValue` as a generic parameter for schema operations to use `HydratedCoValueSchema`

### Backward Compatibility

To ease migration, we could:

1. Deprecate `CoValueClassOrSchema` and `coValueClassFromCoValueClassOrSchema` in one release
2. Remove them in a subsequent major version release
3. Provide clear deprecation warnings and migration instructions

However, for internal migration, we can proceed directly to removal since we control all usage sites.

## Performance Considerations

This migration should have minimal performance impact:

1. **Type-Level Changes**: Type changes have no runtime cost
2. **Method Call Changes**: `schema.getCoValueClass()` is a simple getter with the same performance as `coValueClassFromCoValueClassOrSchema()`
3. **No New Allocations**: The migration doesn't introduce new object allocations
4. **No Algorithm Changes**: The underlying algorithms remain unchanged

## Security Considerations

This migration has no direct security implications, as it's primarily a type-level refactoring. However:

1. **Type Safety**: Improved type safety reduces the risk of type-related bugs
2. **API Simplification**: Simpler APIs reduce the attack surface for misuse
3. **Validation**: Ensure schema validation remains intact after migration

## Future Enhancements

After this migration, future improvements could include:

1. **Schema Composition**: Better support for composing schemas
2. **Schema Validation**: Runtime schema validation for additional safety
3. **Schema Introspection**: Enhanced introspection capabilities for schemas
4. **Schema Versioning**: Support for schema evolution and versioning
