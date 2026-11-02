# Authentication and bootstrap

Identify whether the app uses local-first identity or an external auth provider before changing
bootstrap, session, or logout behavior. Keep external authentication and trusted server-side
provisioning separate from client-side row policies. Never expose backend or admin credentials to
the browser.

- [Authentication](https://jazz.tools/docs/setup/auth/backend)
- [Sessions](https://jazz.tools/docs/setup/auth/sessions)
- [Lifecycle](https://jazz.tools/docs/setup/auth/lifecycle)
- [Local-first auth](https://jazz.tools/docs/setup/auth/local-first)
- [Auth provider integration](https://jazz.tools/docs/setup/auth/custom)
- [Better Auth adapter](https://jazz.tools/docs/recipes/auth/better-auth-adapter)
- [Server setup](https://jazz.tools/docs/setup/backend)
