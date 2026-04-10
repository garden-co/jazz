# Better Auth Signup Continuity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve a user's self-signed Jazz identity when they sign up via Better Auth in the example app.

**Architecture:** The `Db` class gets a new `getSelfSignedToken()` method that mints a short-lived proof JWT. The Better Auth example's sign-up submit sends this token alongside the form fields. A Better Auth `hooks.before` handler validates the token server-side and injects the proved userId into the context. A `databaseHooks.user.create.before` hook reads that userId and sets it as the new user's id. A NAPI `verifySelfSignedToken` binding is added so the Next.js API route can validate tokens.

**Tech Stack:** TypeScript (jazz-tools, Next.js example), Rust (jazz-napi), Better Auth hooks, Vitest, Playwright

**Spec:** `docs/superpowers/specs/2026-04-10-betterauth-signup-continuity-design.md`

---

### Task 1: Add `verifySelfSignedToken` to NAPI bindings

The Better Auth server runs in Node.js and needs to verify proof tokens. The Rust `identity::verify_self_signed_token` exists but isn't exposed to JS. Add it as a NAPI binding.

**Files:**

- Modify: `crates/jazz-napi/src/lib.rs` (add `verify_self_signed_token` function)
- Modify: `crates/jazz-napi/index.d.ts` (add TypeScript declaration)
- Modify: `crates/jazz-napi/index.js` (add export)

- [ ] **Step 1: Add the NAPI binding in Rust**

In `crates/jazz-napi/src/lib.rs`, add after the standalone `mint_self_signed_token` function (around line 1572):

```rust
#[napi(js_name = "verifySelfSignedToken")]
pub fn verify_self_signed_token_napi(
    token: String,
    expected_audience: String,
) -> napi::Result<String> {
    let verified = identity::verify_self_signed_token(&token, &expected_audience)
        .map_err(napi::Error::from_reason)?;
    Ok(verified.user_id)
}
```

This returns just the `user_id` string — the example only needs the proved subject, not the full `VerifiedSelfSigned` struct.

- [ ] **Step 2: Add the TypeScript declaration**

In `crates/jazz-napi/index.d.ts`, add after the `mintSelfSignedToken` declaration:

```ts
export declare function verifySelfSignedToken(token: string, expectedAudience: string): string;
```

- [ ] **Step 3: Add the JS export**

In `crates/jazz-napi/index.js`, add after the `mintSelfSignedToken` export line:

```js
module.exports.verifySelfSignedToken = nativeBinding.verifySelfSignedToken;
```

- [ ] **Step 4: Build and verify**

Run: `cd crates/jazz-napi && cargo build`
Expected: compiles without errors

- [ ] **Step 5: Commit**

```
feat: expose verifySelfSignedToken via NAPI bindings
```

---

### Task 2: Add `getSelfSignedToken()` to the `Db` class

The spec defines `db.getSelfSignedToken({ ttlSeconds?, audience? })` as the client-side API for minting proof tokens. The `Db` class already has `_selfSignedSeed` and calls `WasmRuntime.mintSelfSignedToken` internally for token refresh — this task exposes it as a public method.

**Files:**

- Test: `packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts`
- Modify: `packages/jazz-tools/src/runtime/db.ts`

- [ ] **Step 1: Write the failing tests**

Append to `packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts`:

```ts
describe("getSelfSignedToken", () => {
  it("returns a token for a self-signed session", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      auth: { seed: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
    });

    const token = await db.getSelfSignedToken({ audience: "test-audience" });
    expect(token).toBeTypeOf("string");
    expect(token!.split(".")).toHaveLength(3); // JWT format: header.payload.signature
    await db.close();
  });

  it("returns null for a non-self-signed session", async () => {
    const { createDb } = await import("./db.js");
    // A db with jwtToken (not self-signed) — use a dummy token that will
    // cause an unauthenticated state, but getSelfSignedToken should still return null.
    const db = await createDb({
      appId: "test-app",
      jwtToken: "dummy-jwt",
    });

    const token = await db.getSelfSignedToken({ audience: "test-audience" });
    expect(token).toBeNull();
    await db.close();
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/jazz-tools && npx vitest run src/runtime/db.self-signed-auth.test.ts`
Expected: FAIL — `getSelfSignedToken` is not a function

- [ ] **Step 3: Implement `getSelfSignedToken` on the `Db` class**

In `packages/jazz-tools/src/runtime/db.ts`, add this public method to the `Db` class, after the `getAuthState()` method (around line 1082):

```ts
  /**
   * Mint a short-lived self-signed JWT proving possession of the current identity.
   * Returns `null` if the current session is not self-signed.
   */
  async getSelfSignedToken(options?: {
    ttlSeconds?: number;
    audience?: string;
  }): Promise<string | null> {
    if (!this._selfSignedSeed) {
      return null;
    }

    const wasmModule = this.wasmModule;
    if (!wasmModule) {
      return null;
    }

    const ttl = options?.ttlSeconds ?? 60;
    const audience = options?.audience ?? this.config.appId;
    const nowSeconds = BigInt(Math.floor(Date.now() / 1000));

    return wasmModule.WasmRuntime.mintSelfSignedToken(
      this._selfSignedSeed,
      audience,
      BigInt(ttl),
      nowSeconds,
    );
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/jazz-tools && npx vitest run src/runtime/db.self-signed-auth.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: add db.getSelfSignedToken() for identity proof minting
```

---

### Task 3: Add proof token to the sign-up submit path (client)

The example's sign-up handler currently calls `authClient.signUp.email(...)` with just email, name, and password. Add `proofToken` by minting a self-signed token before calling Better Auth.

**Files:**

- Modify: `examples/auth-betterauth-chat/app/page.tsx`

- [ ] **Step 1: Update the sign-up handler**

In `examples/auth-betterauth-chat/app/page.tsx`, replace the `handleSignUp` function:

```tsx
async function handleSignUp(email: string, password: string) {
  const proofToken = await db.getSelfSignedToken({
    ttlSeconds: 60,
    audience: "betterauth-signup",
  });

  if (!proofToken) {
    throw new Error("Sign up requires an active Jazz session");
  }

  const res = await authClient.signUp.email({
    email,
    name: email,
    password,
    proofToken,
  });

  if (res.error) {
    throw new Error(res.error.message);
  }
}
```

- [ ] **Step 2: Verify the app compiles**

Run: `cd examples/auth-betterauth-chat && npx next build`
Expected: builds without type errors (Better Auth's `signUp.email` accepts extra fields via `fetchOptions` or as additional body fields depending on the plugin config — if it rejects unknown fields, pass via `fetchOptions.body` instead)

- [ ] **Step 3: Commit**

```
feat: send proof token on Better Auth sign-up
```

---

### Task 4: Add server-side proof validation and user id override

The Better Auth server needs to: (1) validate the `proofToken` in a `hooks.before` handler, (2) inject the proved userId into the context, and (3) override the user id in `databaseHooks.user.create.before`.

**Files:**

- Modify: `examples/auth-betterauth-chat/src/lib/auth.ts`

- [ ] **Step 1: Add the imports**

At the top of `examples/auth-betterauth-chat/src/lib/auth.ts`, add:

```ts
import { verifySelfSignedToken } from "jazz-napi";
import { createAuthMiddleware } from "better-auth/api";
import { APIError } from "better-auth/api";
```

Note: `createAuthMiddleware` may be exported from `better-auth/api` or `better-auth`. Check the Better Auth docs or autocomplete if the import fails — the function name is correct per their docs.

- [ ] **Step 2: Add the before-hook for proof validation**

In the `betterAuth({...})` config object, add a `hooks` property (after `trustedOrigins`):

```ts
    hooks: {
      before: createAuthMiddleware(async (ctx) => {
        if (ctx.path !== "/sign-up/email") return;

        const proofToken = ctx.body?.proofToken;
        if (!proofToken) {
          throw new APIError("BAD_REQUEST", {
            message: "proofToken is required for sign-up",
          });
        }

        let provedUserId: string;
        try {
          provedUserId = verifySelfSignedToken(proofToken, "betterauth-signup");
        } catch {
          throw new APIError("UNAUTHORIZED", {
            message: "Invalid proof token",
          });
        }

        return {
          context: {
            ...ctx,
            body: { ...ctx.body, provedUserId },
          },
        };
      }),
    },
```

- [ ] **Step 3: Add the database hook for user id override**

In the `betterAuth({...})` config object, add a `databaseHooks` property:

```ts
    databaseHooks: {
      user: {
        create: {
          before: async (user, ctx) => {
            const provedUserId = (ctx as any).context?.body?.provedUserId;
            if (!provedUserId) {
              throw new APIError("BAD_REQUEST", {
                message: "Missing proved identity — refusing to create user",
              });
            }
            return { data: { ...user, id: provedUserId } };
          },
        },
      },
    },
```

- [ ] **Step 4: Update the pre-seeded admin creation**

The admin user is created via `auth.api.createUser(...)` which will also hit the `databaseHooks`. Since the admin has no proof token, we need to handle this. The simplest approach: make the database hook only enforce `provedUserId` when it's present in the context (meaning proof validation ran), and skip enforcement for programmatic user creation where no hooks.before ran:

```ts
    databaseHooks: {
      user: {
        create: {
          before: async (user, ctx) => {
            const provedUserId = (ctx as any).context?.body?.provedUserId;
            if (provedUserId) {
              return { data: { ...user, id: provedUserId } };
            }
            // No proof token — allow programmatic user creation (e.g. admin seeding)
          },
        },
      },
    },
```

- [ ] **Step 5: Verify the server compiles**

Run: `cd examples/auth-betterauth-chat && npx next build`
Expected: builds without errors

- [ ] **Step 6: Commit**

```
feat: validate proof token and set user id on Better Auth sign-up
```

---

### Task 5: Add E2E test for signup continuity

Add a Playwright test that verifies the Jazz userId is preserved across sign-up. The test starts with a self-signed session, reads the userId, signs up, and verifies the Better Auth user id matches.

**Files:**

- Modify: `examples/auth-betterauth-chat/e2e/chat-auth.spec.ts`

- [ ] **Step 1: Add the continuity test**

Add a new test to the existing `test.describe` block in `examples/auth-betterauth-chat/e2e/chat-auth.spec.ts`:

```ts
test("preserves Jazz userId across Better Auth sign-up", async ({ page }) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => {
    pageErrors.push(error.message);
  });

  await page.goto("/");

  // Wait for self-signed session to be active
  await expect(page.getByTestId("auth-status")).toContainText("Anonymous", { timeout: 20_000 });

  // Read the self-signed userId from the UI
  const preSignupUserId = await page.getByTestId("user-id").textContent();
  expect(preSignupUserId).toBeTruthy();

  // Sign up as a new user
  const runId = Date.now();
  const email = `continuity-${runId}@example.com`;
  await signUp(page, { email, password: "test123" });

  // Wait for authenticated session
  await expect(page.getByTestId("auth-status")).toContainText("member", { timeout: 20_000 });

  // Verify the userId is preserved
  const postSignupUserId = await page.getByTestId("user-id").textContent();
  expect(postSignupUserId).toBe(preSignupUserId);

  expect(pageErrors).toEqual([]);
});
```

- [ ] **Step 2: Add a `user-id` test ID to the UI**

In `examples/auth-betterauth-chat/app/page.tsx`, add a `data-testid="user-id"` element in the `ChatShell` component that displays the current userId. Add it inside the `<main>` element, before the `<section>`:

```tsx
    <main className="app-shell">
      <span data-testid="user-id" style={{ display: "none" }}>
        {session?.user_id ?? ""}
      </span>
      <section className="content-grid">
```

- [ ] **Step 3: Add the rejection test**

Add another test to the describe block that verifies sign-up fails without a valid proof token. This test directly calls the Better Auth API endpoint instead of going through the UI:

```ts
test("rejects sign-up with missing proof token", async ({ request }) => {
  const response = await request.post("/api/auth/sign-up/email", {
    data: {
      email: "no-proof@example.com",
      name: "no-proof",
      password: "test123",
    },
  });

  expect(response.ok()).toBe(false);
});
```

- [ ] **Step 4: Run the E2E tests**

Run: `cd examples/auth-betterauth-chat && npx playwright test`
Expected: all tests pass, including the new continuity and rejection tests

- [ ] **Step 5: Commit**

```
test: add E2E tests for Better Auth signup continuity
```
