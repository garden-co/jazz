# Scope-Bypass Regression Test — TODO (MVP)

No explicit RuntimeCore test that:

1. Creates a schema
2. Inserts a row through the client API (auto-generates realistic metadata)
3. Verifies unauthorized clients can't access it

The bug is fixed via role-based auth, but a regression test at RuntimeCore level would prevent re-introduction.
