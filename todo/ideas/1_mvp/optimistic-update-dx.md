# Optimistic Update DX

## What

Developer-facing API for mutation settlement state — show pending/confirmed/rejected status on rows and filter queries by settlement tier.

## Why

Settlement tracking internals exist (PersistenceAck) but there's no way for app developers to surface pending/rejected states in their UI or handle rollbacks explicitly.

## Who

App developers building UIs that need to show sync status or handle permission rejections gracefully.

## Rough appetite

medium

## Notes

Already done: ReBAC policies, sync settlement tracking internals, scoped backend clients. What remains: exposing settlement state in the API, query filters for "only confirmed" / "include pending", rejection reason communication, and offline-duration handling patterns.
