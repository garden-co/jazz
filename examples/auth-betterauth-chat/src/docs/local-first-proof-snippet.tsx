import { useEffect, useState } from "react";
import { type DbConfig, BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider, useDb } from "jazz-tools/react";
import { authClient, getJwtFromBetterAuth } from "../lib/auth-client";

function YourApp() {
  return null;
}

// #region local-first-proof-signup
function SignUpButton() {
  const db = useDb();

  async function handleSignUp(email: string, password: string) {
    // Generate proof of ownership of the current Jazz identity
    const proofToken = await db.getLocalFirstIdentityProof({
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
    } as Parameters<typeof authClient.signUp.email>[0]);

    if (res.error) {
      throw new Error(res.error.message);
    }
  }

  return <button onClick={() => handleSignUp("user@example.com", "password")}>Sign Up</button>;
}
// #endregion local-first-proof-signup

// #region local-first-config-resolution
function App() {
  const { data: authSession, isPending } = authClient.useSession();
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    if (isPending) return;

    async function resolveConfig() {
      const sharedConfig = {
        appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
      };

      if (authSession?.session) {
        // User is signed in with BetterAuth — use their JWT
        const jwtToken = await getJwtFromBetterAuth();
        setConfig({ ...sharedConfig, jwtToken: jwtToken! });
      } else {
        // No external session — use local-first auth
        const secret = await BrowserAuthSecretStore.getOrCreateSecret();
        setConfig({ ...sharedConfig, auth: { localFirstSecret: secret } });
      }
    }

    void resolveConfig();
  }, [isPending, authSession?.session]);

  if (isPending || !config) return null;

  return (
    <JazzProvider config={config}>
      <BetterAuthJazzSync hasBetterAuthSession={!!authSession?.session}>
        <YourApp />
      </BetterAuthJazzSync>
    </JazzProvider>
  );
}
// #endregion local-first-config-resolution

// #region local-first-token-refresh
function BetterAuthJazzSync({
  hasBetterAuthSession,
  children,
}: React.PropsWithChildren<{ hasBetterAuthSession: boolean }>) {
  const db = useDb();

  useEffect(() => {
    if (!hasBetterAuthSession) return;

    return db.onAuthChanged((state) => {
      if (state.status !== "unauthenticated") return;

      // JWT expired — fetch a fresh one from BetterAuth
      getJwtFromBetterAuth().then((jwt) => {
        if (jwt) db.updateAuthToken(jwt);
      });
    });
  }, [db, hasBetterAuthSession]);

  return children;
}
// #endregion local-first-token-refresh

export { App, SignUpButton, BetterAuthJazzSync };
