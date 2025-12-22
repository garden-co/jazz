# Requirements Document

## Introduction

This document specifies the requirements for migrating the Jazz Tools codebase from using `CoValueClassOrSchema` type to `CoreCoValueSchema` type, and replacing all `coValueClassFromCoValueClassOrSchema()` function calls with `schema.getCoValueClass()` method calls. This migration aims to simplify the type system by standardizing on schema-based APIs rather than accepting both classes and schemas.

## Glossary

- **CoValueClassOrSchema**: A union type that accepts either a CoValue class or a CoreCoValueSchema
- **CoreCoValueSchema**: An interface representing the core schema definition for collaborative values
- **HydratedCoValueSchema**: An interface that extends CoreCoValueSchema and includes the getCoValueClass() method
- **CoValue**: A collaborative value in the Jazz framework
- **coValueClassFromCoValueClassOrSchema**: A utility function that extracts a CoValue class from either a class or schema
- **getCoValueClass**: A method on schema objects that returns the corresponding CoValue class
- **Schema**: A declarative definition of a CoValue's structure and behavior
- **Migration**: The process of systematically updating code to use new APIs while maintaining functionality
- **Generic Type Parameter**: A type variable used in generic functions and types to provide type safety across different concrete types
- **$jazz object**: An internal object on CoValue instances that stores metadata including id, loading state, and other internal properties
- **Schema Reference**: A stored reference to the HydratedCoValueSchema used to create or load a CoValue instance

## Requirements

### Requirement 1

**User Story:** As a developer, I want all functions that currently accept `CoValueClassOrSchema` to accept only `HydratedCoValueSchema`, so that the API is more consistent and type-safe.

#### Acceptance Criteria

1. WHEN a function signature contains `CoValueClassOrSchema` as a parameter type, THEN the system SHALL replace it with `HydratedCoValueSchema`
2. WHEN a function receives a `HydratedCoValueSchema` parameter, THEN the system SHALL process it correctly without requiring type unions
3. WHEN type definitions reference `CoValueClassOrSchema`, THEN the system SHALL update them to use `HydratedCoValueSchema`
4. WHEN generic type parameters use `CoValueClassOrSchema` as a constraint, THEN the system SHALL replace the constraint with `HydratedCoValueSchema`
5. WHEN APIs in interfaces.ts use `CoValue` as a generic type parameter for schema-related operations, THEN the system SHALL migrate them to use `HydratedCoValueSchema`

### Requirement 2

**User Story:** As a developer, I want all calls to `coValueClassFromCoValueClassOrSchema()` replaced with `schema.getCoValueClass()`, so that the code uses the schema-based API consistently.

#### Acceptance Criteria

1. WHEN code calls `coValueClassFromCoValueClassOrSchema(schema)`, THEN the system SHALL replace it with `schema.getCoValueClass()`
2. WHEN the replacement is made, THEN the system SHALL preserve the same runtime behavior
3. WHEN the schema variable has a different name, THEN the system SHALL use that variable name in the replacement
4. WHEN multiple calls exist in a single file, THEN the system SHALL replace all occurrences

### Requirement 3

**User Story:** As a developer, I want unused imports of `coValueClassFromCoValueClassOrSchema` removed, so that the codebase remains clean and maintainable.

#### Acceptance Criteria

1. WHEN a file imports `coValueClassFromCoValueClassOrSchema` but no longer uses it, THEN the system SHALL remove the import
2. WHEN removing an import, THEN the system SHALL preserve other imports from the same module
3. WHEN an import statement becomes empty after removal, THEN the system SHALL remove the entire import statement
4. WHEN the import is part of a re-export, THEN the system SHALL remove it from the export statement

### Requirement 4

**User Story:** As a developer, I want the migration to preserve all existing functionality, so that no bugs are introduced during the refactoring.

#### Acceptance Criteria

1. WHEN the migration is complete, THEN all existing tests SHALL pass
2. WHEN a function is migrated, THEN its runtime behavior SHALL remain unchanged
3. WHEN type checking is performed, THEN no new type errors SHALL be introduced
4. WHEN the codebase is built, THEN the build SHALL succeed without errors

### Requirement 5

**User Story:** As a developer, I want comprehensive documentation of the migration changes, so that I can understand what was changed and why.

#### Acceptance Criteria

1. WHEN files are modified, THEN the system SHALL track which files were changed
2. WHEN function signatures are updated, THEN the system SHALL document the changes
3. WHEN the migration is complete, THEN a summary SHALL be provided listing all modifications
4. WHEN breaking changes occur, THEN they SHALL be clearly documented

### Requirement 6

**User Story:** As a developer, I want the migration to handle edge cases correctly, so that all code paths work after the migration.

#### Acceptance Criteria

1. WHEN a function has optional parameters of type `CoValueClassOrSchema`, THEN the system SHALL update them to `HydratedCoValueSchema`
2. WHEN a function has default values involving `CoValueClassOrSchema`, THEN the system SHALL preserve the default values with updated types
3. WHEN type assertions or casts involve `CoValueClassOrSchema`, THEN the system SHALL update them appropriately
4. WHEN conditional logic checks the type of a `CoValueClassOrSchema` parameter, THEN the system SHALL update or remove the checks as needed

### Requirement 7

**User Story:** As a developer, I want CoValue instances to store a reference to their schema in the `$jazz` object, so that I can access the schema without relying on `value.constructor`.

#### Acceptance Criteria

1. WHEN a CoValue instance is created from a schema, THEN the system SHALL store a reference to the schema in the `$jazz` object
2. WHEN code accesses `value.constructor` to get schema information, THEN the system SHALL migrate it to use the stored schema reference
3. WHEN a CoValue is loaded or subscribed to, THEN the system SHALL ensure the schema reference is properly set
4. WHEN the schema reference is accessed, THEN it SHALL return the `HydratedCoValueSchema` that was used to create or load the CoValue
5. WHEN existing APIs use `value.constructor as CoValueClass`, THEN the system SHALL migrate them to use the stored schema reference
