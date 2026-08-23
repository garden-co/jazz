# BandChat TypeScript benchmarks

This self-contained package duplicates BandChat's domain schema and deterministic seed shape so benchmark results remain recognizable without loading React or Better Auth.

Run `pnpm bench` for local Vitest measurements. Once the repository adopts a shared CodSpeed project/workflow, run the same command through CodSpeed's supported action or `codspeed run -- pnpm bench`. No global workflow is added here.

Stable workload names cover the room list, a 40-message window with sender materialization, and rollback-isolated message insert churn.
