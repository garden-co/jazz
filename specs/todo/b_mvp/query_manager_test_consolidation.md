# QueryManager Test Consolidation — TODO (MVP)

~91 QueryManager tests remain at a level below RuntimeCore. Some are valuable for internal logic verification, but candidates for consolidation into E2E RuntimeCore scenarios include:

- `insert_and_query()` patterns that could be E2E'd with actual message pumping
- Basic CRUD tests that duplicate what RuntimeCore tests already cover
