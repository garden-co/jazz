---
"jazz-tools": patch
---

A session transition (sign in, sign out, user switch, `reauthenticate()`, close) no longer waits forever for a `<JazzProvider>` view that never acknowledges it, such as one suspended behind React Suspense. After 5 seconds the session moves on without that view, warns naming it, and doesn't wait for it again until it renders the session again.
