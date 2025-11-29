# Implementation Plan

- [ ] 1. Add cache key support to CoValueCore
  - Add cacheKeys Set to CoValueCore to track cache keys
  - Implement addCacheKey method to add cache keys to a CoValue
  - Implement getCacheKeys method to retrieve all cache keys
  - _Requirements: 5.2, 7.1_

- [ ] 2. Implement cache key propagation in CoValueCore dependencies
  - Modify addDependency to propagate cache keys to available dependencies
  - Ensure cache keys are only propagated when dependency is available
  - _Requirements: 7.1, 7.3_

- [ ] 3. Add cache key generation to SubscriptionScope
  - Implement cache key generation from resolve query and root CoValue ID
  - Create stable serialization for resolve queries (canonical form with sorted keys)
  - Use SHA-256 hash to create fixed-length cache keys
  - Add markAsCacheable method to enable caching for a scope
  - _Requirements: 2.1, 2.2, 2.3_

- [ ] 4. Implement cache key propagation in SubscriptionScope hierarchy
  - Modify SubscriptionScope constructor to accept optional parent cache key
  - Propagate cache key to child scopes when creating children
  - Ensure cache key is only propagated when parent has a cache key
  - _Requirements: 3.1, 3.2, 3.3_

- [ ] 5. Update CoValueCoreSubscription to handle cache keys
  - Add cacheKey parameter to CoValueCoreSubscription constructor
  - Pass cache key from SubscriptionScope to CoValueCoreSubscription
  - Pass cache key to LocalNode when loading CoValues
  - _Requirements: 4.1, 4.2, 4.3_

- [ ] 6. Update LocalNode to support cache keys
  - Modify loadCoValueCore to accept optional cache key parameter
  - Pass cache key to CoValueCore for tracking when loading
  - Pass cache key to storage layer during load operations
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 7. Add cache operation interfaces to StorageAPI
  - Add supportsCaching method to check driver capabilities
  - Add loadCache method to warm up cache buckets
  - Add storeCache method with same signature as store plus cacheKey parameter
  - _Requirements: 1.1, 1.2, 1.3_

- [ ] 8. Implement cache operations in StorageApiAsync
  - Implement supportsCaching to check if driver supports caching
  - Implement loadCache to delegate to driver when supported
  - Implement storeCache to delegate to driver when supported
  - Handle graceful degradation when driver doesn't support caching
  - _Requirements: 1.1, 1.2, 1.3, 6.1, 6.2, 6.3_

- [ ] 9. Update StorageApiAsync store method to call storeCache
  - Check if CoValue has cache keys after storing
  - Call storeCache for each cache key when present
  - Handle errors gracefully without blocking store operation
  - _Requirements: 8.1, 8.2, 8.3_

- [ ] 10. Add cache support interfaces to DBClientInterfaceAsync
  - Add supportsCaching method to interface
  - Add loadCacheBucket method returning Map<string, NewContentMessage[]>
  - Add addToCacheBucket method to store CoValue in cache bucket
  - _Requirements: 1.1, 6.1, 8.2_

- [ ] 11. Implement IndexedDB cache store schema
  - Define CacheEntry interface with coValueId, messages array, and lastUpdated
  - Implement cache key sanitization for IndexedDB store names
  - Create helper to generate store name from cache key
  - _Requirements: 8.2_

- [ ] 12. Implement IndexedDB cache store creation
  - Implement createCacheStore to create new object stores for cache keys
  - Handle IndexedDB version upgrades when adding new stores
  - Ensure store creation is idempotent
  - _Requirements: 8.2_

- [ ] 13. Implement IndexedDB loadCacheBucket
  - Check if cache store exists for the cache key
  - Return empty Map if store doesn't exist
  - Load all entries from cache store
  - Convert entries to Map<string, NewContentMessage[]>
  - _Requirements: 6.1, 6.2, 6.3_

- [ ] 14. Implement IndexedDB addToCacheBucket
  - Create cache store if it doesn't exist
  - Load existing entry for coValueId if present
  - Merge new message with existing messages (append strategy)
  - Update lastUpdated timestamp
  - Store updated entry back to cache store
  - _Requirements: 8.1, 8.2_

- [ ] 15. Implement message merging and deduplication
  - Create messagesAreEqual helper to compare messages
  - Implement mergeMessages to append new messages while avoiding duplicates
  - Maintain chronological order of messages
  - _Requirements: 8.2_

- [ ] 16. Add feature flag for IndexedDB cache
  - Add configuration option to enable/disable IndexedDB caching
  - Default to disabled for initial rollout
  - Check feature flag before performing cache operations
  - _Requirements: 1.1_

- [ ] 17. Add error handling and logging
  - Log cache operation failures without blocking main operations
  - Implement graceful degradation when cache operations fail
  - Add monitoring for cache hit/miss rates
  - _Requirements: 1.3_

- [ ] 18. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
