# Durability Guarantees and Rate Limits

## What

Document and enforce a clear durability contract: `await db.insertDurable(...)` resolving guarantees server persistence. Everything else (`db.insert`, etc.) is best-effort — the server may safely drop these requests for rate-limiting or resource reclamation without violating any contract.

## Notes

- Users need a clear mental model of what's durable and what's not.
- The durable/non-durable split already exists in the API surface — this is about documenting it as a guarantee and leveraging it operationally
- Rate-limit enforcement can safely drop non-durable incoming requests without breaking correctness
- Resource reclamation (memory pressure, queue depth) can also shed non-durable writes
