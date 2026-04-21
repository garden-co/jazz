# Move auth-mode gating from AuthConfig flags into the permissions DSL

## What

Replace `AuthConfig.allow_local_first_auth: bool` with declarative calls on the
permissions DSL:

```ts
export default definePermissions(app, ({ policy, session }) => {
  policy.disableAnonymousAuth();
  policy.disableLocalFirstAuth();
  // ...
});
```

Auth-mode gating becomes colocated with the rest of each app's policy — no
separate server config flag. The middleware reads the compiled permissions for
the app_id and rejects disallowed auth modes there, before session
construction.

## Notes

- Requires the middleware to ingest permissions output (today `AuthConfig` is
  passed in; permissions are consumed at the query_manager layer).
- Multi-tenant servers: each `app_id` has its own permissions, so the middleware
  lookup is app-scoped.
- Default: both modes enabled; apps opt out.
- Interim: the current `allow_local_first_auth` flag keeps working; anonymous
  stays always-on at transport and gated by normal policy rules.
- Related: the flag name `allow_local_first_auth` would go away entirely.
