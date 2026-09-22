import { createAccountManagerWithRuntime } from "../../accounts/enrollment.js";
import { accountRegistryUrl, type AccountDbConfig } from "../../accounts/context.js";

/** Unit tests substitute only the external enrollment response, obtaining a
 * real opaque handle without loading a platform crypto or database runtime. */
export async function enrolledAccountConfig(
  appId: string,
  subject = "unit-user",
): Promise<AccountDbConfig> {
  const issuer = "https://issuer.example";
  const registry = accountRegistryUrl("https://core.example", appId);
  const token = `header.${btoa(JSON.stringify({ iss: issuer, sub: subject }))}.signature`;
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
  return { appId, account: await manager.registerJWT(token) };
}
