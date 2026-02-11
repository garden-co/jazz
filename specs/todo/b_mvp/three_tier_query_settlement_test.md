# Three-Tier Query Settlement Test — TODO (MVP)

The infrastructure exists (PersistenceAck + QuerySettled with tier). 2-tier tests pass (direct, tier-constraint, multiple-servers, one-shot). Missing: a 3-tier test where QuerySettled cascades through an intermediate node back to the originating client.

> `crates/groove/src/schema_manager/integration_tests.rs:2651-3045` (2-tier tests)
