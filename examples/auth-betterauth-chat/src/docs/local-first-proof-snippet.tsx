import { createJazzSession } from "jazz-tools/client";
import { authClient, getJwtFromBetterAuth } from "../lib/auth-client";

// #region local-first-config-resolution
export async function openLocalFirstApp() {
  const session = await createJazzSession({
    appId: "my-app",
    serverUrl: "https://your-jazz-server.example.com",
    initial: "local-first",
  });
  // #endregion local-first-config-resolution

  // #region local-first-proof-signup
  async function signUp(email: string, password: string) {
    const result = await authClient.signUp.email({ email, name: email, password });
    if (result.error) throw new Error(result.error.message);
    await session.linkJWT({
      getToken: async () => {
        const token = await getJwtFromBetterAuth();
        if (!token) throw new Error("Missing provider token");
        return token;
      },
    });
  }
  // #endregion local-first-proof-signup
  return { session, signUp };
}
