# Storage Limits and Eviction

## What

Bounded storage with LRU eviction of cold data on clients and edge servers, with lazy re-fetch from upstream.

## Notes

- Browser storage and edge servers have limited capacity. Without eviction, storage grows unbounded and eventually hits platform limits or degrades performance.
- Main consumers are browser clients, mobile clients, and edge server operators.
- Objects are the eviction unit, with index entries removed alongside them.
- Per-tier budgets should be tight for browsers, larger for edge, and absent for core servers.
- Open questions: interaction with reactive subscriptions, budget units, pinning policies, and OPFS quota limits.
