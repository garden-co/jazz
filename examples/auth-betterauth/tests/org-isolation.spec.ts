/**
 * Org-Scoped Document Isolation Tests
 *
 * NOTE: Owner-based document isolation is now tested in sync.spec.ts.
 * The tests there verify that users only see their own documents using
 * the `owner_id = @viewer` policy.
 *
 * For org-based isolation (@viewer.claims.orgId), additional setup is needed:
 * 1. BetterAuth server must include orgId in JWT claims
 * 2. Documents schema needs org_id column
 * 3. Policy: WHERE org_id = @viewer.claims.orgId
 *
 * The underlying policy filtering is tested in:
 * - crates/groove/tests/sync_policy.rs (Rust E2E tests)
 *   - test_org_scoped_broadcast_e2e
 *   - test_multiple_orgs_isolation_e2e
 *   - test_combined_org_and_role_e2e
 *
 * These Rust tests verify org-based filtering works correctly with claims.
 * The browser E2E tests in sync.spec.ts verify the full flow with owner-based
 * policies, which uses the same underlying mechanism.
 */

export {};
