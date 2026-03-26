# Policy resolution integration tests

## What

Missing integration tests for end-to-end policy resolution through the full query/write pipeline.

## Where

RuntimeCore / QueryManager integration tests

## Steps to reproduce

N/A — missing test coverage.

## Expected

Tests covering: ReBAC INHERITS chains granting/denying access, policy evaluation on reads and writes, scoped client queries respecting JWT-derived sessions, policy changes propagating to active subscriptions.

## Actual

Unit-level rebac_tests exist but no full RuntimeCore-level integration tests exercising policies through the complete pipeline.

## Priority

high

## Notes

Should test both the happy path (access granted through inheritance) and denial paths (permission rejected, rollback).
