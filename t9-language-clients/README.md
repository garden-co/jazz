t9-language-clients — .65 language gate: StagePlan served by a stock TS `createJazz` backend, used by
backend clients (Rust/Go, trusted + scoped + forUser) and end-user clients (Swift/Kotlin CLIs).

Run (jazz-tools must resolve from this dir: the rc tarballs, or a branch build symlinked into node_modules):
  OIDC_PORT=8790 npx tsx oidc.ts &      # workload issuer (backendClients: sub gate-trusted-job = trusted, gate-scoped-agent = scoped as user "scoped-agent"), aud jazz:stageplan-gate
  OIDC_PORT=8791 npx tsx oidc.ts &      # user issuer, aud jazz:stageplan-users (a SEPARATE issuer: tokens from a backendClients issuer never reach authenticate)
  APP_DIR=<stage-plan app dir with jazz.generated.ts + jazz.permissions.json> [DEVICE=1] npx tsx server.ts &   # http://127.0.0.1:8780/jazz
  APP_DIR=... npx tsx ts-backend-client.ts         # reference: trusted, forUser(A/B), scoped, unlisted workload refused  -> BACKEND-CLIENT PASS
  APP_BINDINGS=<...>/jazz.generated.ts SERVER=http://127.0.0.1:8780/jazz npx tsx ts-end-users.ts   # reference end users -> STAGEPLAN PASS
Tokens: curl "127.0.0.1:8790/token?sub=gate-trusted-job" (workloads), curl "127.0.0.1:8791/token?sub=alice&aud=jazz:stageplan-users" (users).
Users A/B for forUser: POST http://127.0.0.1:8780/account/user-a (and user-b) creates their accounts.
Language clients (Rust/Go backend clients, Swift/Kotlin CLIs) are run against the same backend with the same expectations.
