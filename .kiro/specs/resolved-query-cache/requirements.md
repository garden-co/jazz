# Requirements Document

## Introduction

This document specifies the requirements for implementing a cache system for resolved queries in the Jazz framework. The cache system enables efficient storage and retrieval of CoValue data by tracking cache keys through the subscription and storage layers. The StorageAPI manages the cache when the underlying driver supports caching capabilities, allowing for improved performance when resolving queries.

## Glossary

- **StorageAPI**: The interface responsible for managing data persistence and cache operations
- **SubscriptionScope**: A scope object that defines the boundaries of a subscription and can be marked as cacheable
- **CoValue**: A collaborative value object that represents data in the Jazz framework
- **CoValueCore**: The core implementation of a CoValue that manages dependencies and state
- **CoValueCoreSubscription**: A subscription object that connects SubscriptionScope to LocalNode
- **LocalNode**: The node that manages CoValue loading and storage operations
- **Cache Key**: A unique identifier generated from the resolve query, root CoValue ID, and account ID
- **Resolve Query**: A query specification used to resolve CoValue data
- **Cache Bucket**: A collection of CoValues associated with a specific cache key
- **Driver**: The underlying storage implementation that may or may not support caching
- **Account**: A user account that represents an identity in the Jazz framework and determines access permissions. Guest accounts have temporary access and caching is disabled for them

## Requirements

### Requirement 1

**User Story:** As a developer, I want the StorageAPI to manage caching when the driver supports it, so that I can leverage caching capabilities without changing application code.

#### Acceptance Criteria

1. WHEN the StorageAPI initializes with a driver THEN the StorageAPI SHALL determine whether the driver supports caching capabilities
2. WHERE the driver supports caching, WHEN cache operations are requested THEN the StorageAPI SHALL delegate cache operations to the driver
3. WHERE the driver does not support caching, WHEN cache operations are requested THEN the StorageAPI SHALL handle requests without caching behavior

### Requirement 2

**User Story:** As a developer, I want SubscriptionScope to generate cache keys from resolve queries, root CoValue IDs, and account IDs, so that queries can be uniquely identified for caching with proper account isolation.

#### Acceptance Criteria

1. WHEN a SubscriptionScope is marked as cacheable AND the current account is not a guest THEN the SubscriptionScope SHALL generate a cache key from the resolve query, the root CoValue ID, and the account ID
2. WHEN generating a cache key THEN the SubscriptionScope SHALL combine the resolve query, root CoValue ID, and account ID into a unique identifier
3. WHEN a SubscriptionScope is not marked as cacheable OR the current account is a guest THEN the SubscriptionScope SHALL not generate a cache key

### Requirement 3

**User Story:** As a developer, I want cache keys to propagate from root SubscriptionScope to child scopes, so that all related subscriptions share the same cache bucket.

#### Acceptance Criteria

1. WHEN a root SubscriptionScope has a cache key THEN the SubscriptionScope SHALL propagate the cache key to every child SubscriptionScope
2. WHEN a child SubscriptionScope is created from a parent with a cache key THEN the child SubscriptionScope SHALL inherit the parent's cache key
3. WHEN a SubscriptionScope has no parent cache key THEN the SubscriptionScope SHALL not propagate a cache key to its children

### Requirement 4

**User Story:** As a developer, I want SubscriptionScope to pass cache keys to CoValueCoreSubscription, so that cache tracking can flow through the subscription chain.

#### Acceptance Criteria

1. WHEN a SubscriptionScope with a cache key creates a CoValueCoreSubscription THEN the SubscriptionScope SHALL pass the cache key to the CoValueCoreSubscription
2. WHEN a CoValueCoreSubscription receives a cache key THEN the CoValueCoreSubscription SHALL pass the cache key to LocalNode
3. WHEN a SubscriptionScope has no cache key THEN the SubscriptionScope SHALL not pass a cache key to CoValueCoreSubscription

### Requirement 5

**User Story:** As a developer, I want LocalNode to use cache keys when loading CoValues, so that the storage layer can warm up cached data.

#### Acceptance Criteria

1. WHEN LocalNode receives a cache key from CoValueCoreSubscription THEN LocalNode SHALL pass the cache key to the storage layer when loading the CoValue
2. WHEN LocalNode loads a CoValue with a cache key THEN LocalNode SHALL track the cache key in CoValueCore
3. WHEN the storage layer receives a cache key during CoValue loading THEN the storage layer SHALL call loadCache with the cache key to warm up cached values

### Requirement 6

**User Story:** As a developer, I want the storage layer to warm up cache buckets, so that related CoValues are preloaded efficiently.

#### Acceptance Criteria

1. WHEN the storage layer calls loadCache with a cache key THEN the storage layer SHALL warm up all CoValues associated with the cache bucket
2. WHEN loadCache warms up cached CoValues THEN the storage layer SHALL make the cached data available for subsequent operations with improved performance
3. WHEN a cache key has no associated cache bucket THEN loadCache SHALL complete without warming up any data

### Requirement 7

**User Story:** As a developer, I want CoValueCore to propagate cache keys to dependencies, so that dependent CoValues are included in the same cache bucket.

#### Acceptance Criteria

1. WHEN CoValueCore addDependency is called and the dependency is available THEN CoValueCore SHALL add the cache keys of the parent CoValue to the dependency CoValue
2. WHEN a dependency CoValue receives cache keys from its parent THEN the dependency CoValue SHALL track all received cache keys
3. WHEN a dependency is not available THEN CoValueCore SHALL not propagate cache keys to the dependency

### Requirement 8

**User Story:** As a developer, I want CoValues with cache keys to update cache storage, so that cache buckets remain synchronized with current data.

#### Acceptance Criteria

1. WHEN a CoValue is stored and has cache keys THEN the storage layer SHALL embed the cache keys in the NewContentMessage.cacheKeys field and call addToCacheBucket for each cache key
2. WHEN store is called with a NewContentMessage containing cache keys THEN the storage layer SHALL update the cache bucket content with the CoValue data for each cache key
3. WHEN a CoValue has no cache keys THEN the storage layer SHALL call store with a NewContentMessage that does not contain cache keys

### Requirement 9

**User Story:** As a developer, I want cache keys to include account IDs, so that cached data is properly isolated between different accounts and respects permission boundaries.

#### Acceptance Criteria

1. WHEN generating a cache key THEN the SubscriptionScope SHALL include the account ID as part of the cache key components
2. WHEN two queries are identical but from different accounts THEN the SubscriptionScope SHALL generate different cache keys
3. WHEN accessing cached data THEN the system SHALL only return data cached under the current account's cache keys
