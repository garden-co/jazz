# Ordered Index Top-K Query Path

## What

Make paginated ordered queries fast by scanning in order and stopping early, instead of loading everything then sorting.

## Why

`ORDER BY ... LIMIT` currently materializes the full result set before paginating — wasteful for top-k patterns that most apps use.

## Who

Any app running paginated or sorted queries.

## Rough appetite

big

## Notes

POC exists on branch `codex/index-first-topk-exact-match` but needs a clean rewrite.
