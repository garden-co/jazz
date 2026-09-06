import { createAccountManager } from "jazz-tools";
import { createJazzClient } from "jazz-tools/client";
import { authClient, getJwtFromBetterAuth } from "../lib/auth-client";

// #region local-first-config-resolution
const config = {
  appId: "my-app",
  serverUrl: "https://your-jazz-server.example.com",
};
export async function openLocalFirstApp() {
  const accounts = await createAccountManager(config);
  let client: Awaited<ReturnType<typeof createJazzClient>> | undefined = await createJazzClient({
    ...config,
    account: accounts.getLoggedIn() ?? accounts.createLocalFirst(),
  });
  // #endregion local-first-config-resolution

  // #region local-first-proof-signup
  async function signUp(email: string, password: string) {
    const result = await authClient.signUp.email({ email, name: email, password });
    if (result.error) throw new Error(result.error.message);
    await client?.shutdown({ waitForSync: true });
    client = undefined;
    try {
      await accounts.linkJWT({
        getToken: async () => {
          const token = await getJwtFromBetterAuth();
          if (!token) throw new Error("Missing provider token");
          return token;
        },
      });
    } finally {
      const account = accounts.getLoggedIn();
      if (account) client = await createJazzClient({ ...config, account });
    }
  }
  // #endregion local-first-proof-signup
  return { accounts, signUp, getClient: () => client };
}
