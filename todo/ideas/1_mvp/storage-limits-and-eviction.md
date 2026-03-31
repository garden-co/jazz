# Storage Limits and Eviction

## What

Bounded storage with LRU eviction of cold data on clients and edge servers, with lazy re-fetch from upstream.

## Why

Browser (OPFS) and edge servers have limited storage. Without eviction, storage grows unbounded and eventually hits platform limits or degrades performance.

## Who

All client-side users (browsers, mobile) and edge server operators.

## Rough appetite

big

## Notes

Objects are the eviction unit (index entries removed with them). Per-tier budgets: tight for browser, larger for edge, no eviction on core servers. Open questions around interaction with reactive subscriptions, budget units, pinning policies, and OPFS quota limits. Critical to design early — retrofitting eviction onto indices is much harder than building it in.
