import { useEffect, useMemo, useState } from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, useDb, useLocalFirstAuth } from "jazz-tools/react";
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
function useBetterAuthJWT() {
  const { data, isPending } = authClient.useSession();
  const [jwt, setJwt] = useState<string | null>(null);
  const [isFetching, setIsFetching] = useState(false);

  useEffect(() => {
    if (isPending) return;
    if (!data?.session) {
      setJwt(null);
      return;
    }
    setIsFetching(true);
    void getJwtFromBetterAuth().then((token) => {
      setJwt(token ?? null);
      setIsFetching(false);
    });
  }, [isPending, data?.session?.id]);

  return {
    isLoading: isPending || isFetching,
    jwt,
    getRefreshedJWT: () => getJwtFromBetterAuth(),
  };
}

function App() {
  const betterAuth = useBetterAuthJWT();
  const { secret: localFirstSecret, isLoading: localFirstLoading } = useLocalFirstAuth();

  // Only mint a local-first secret when there's no BetterAuth session.
  const secret = !betterAuth.jwt ? (localFirstSecret ?? undefined) : undefined;

  const config = useMemo<DbConfig>(
    () => ({
      appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
      serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
      jwtToken: betterAuth.jwt ?? undefined,
      secret,
    }),
    [betterAuth.jwt, secret],
  );

  if (betterAuth.isLoading || (!betterAuth.jwt && localFirstLoading)) return <p>Loading auth…</p>;

  return (
    <JazzProvider
      config={config}
      onJWTExpired={() => betterAuth.getRefreshedJWT()}
      fallback={<p>Loading Jazz DB…</p>}
    >
      <YourApp />
    </JazzProvider>
  );
}
// #endregion local-first-config-resolution

export { App, SignUpButton };
