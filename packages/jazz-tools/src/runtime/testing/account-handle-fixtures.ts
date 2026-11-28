import { createAccountManagerWithRuntime } from "../../accounts/enrollment.js";
import { accountRegistryUrl } from "../../accounts/context.js";

/** Unit tests substitute only the backend sign-in response, obtaining a
 * real opaque handle without loading a platform crypto or database runtime. */
export async function enrolledAccountConfig(appId: string, subject = "unit-user") {
  const issuer = "https://issuer.example";
  const registry = accountRegistryUrl("https://core.example", appId);
  const manager = createAccountManagerWithRuntime({
    registry,
    localFirst: {
      create() {
        throw new Error("Unit external enrollment has no local key");
      },
    },
    fetch: (async () =>
      new Response(
        JSON.stringify({
          account: crypto.randomUUID(),
          identity: { issuer, subject },
        }),
        { status: 200 },
      )) as typeof fetch,
  });
  return { appId, account: await manager.loginWithBackend() };
}
