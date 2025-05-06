"use client";

import { createAuthClient } from "better-auth/client";
import { emailOTPClient, magicLinkClient } from "better-auth/client/plugins";
import { jazzClientPlugin } from "jazz-betterauth-client-plugin";
import { useBetterAuth } from "jazz-react-auth-betterauth";
import type { AuthCredentials } from "jazz-tools";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";

const authClient = () => {
  const auth = useBetterAuth(
    createAuthClient({
      baseURL: process.env.NEXT_PUBLIC_AUTH_BASE_URL,
      plugins: [jazzClientPlugin(), magicLinkClient(), emailOTPClient()],
    }),
  );
  const [user, setUser] = useState<AuthCredentials | undefined>(undefined);
  const [account, setAccount] = useState<
    typeof auth.authClient.$Infer.Session.user | undefined
  >(undefined);
  const updateUser = useCallback(() => {
    auth.authClient.jazzPlugin
      .decryptCredentials()
      .then((x) => {
        setUser(x.data === null ? undefined : x.data);
      })
      .catch((error) => {
        console.error("Error decrypting credentials:", error);
      });
  }, [auth.authClient.jazzPlugin]);
  useEffect(() => {
    auth.authClient.useSession.subscribe(({ data }) => {
      if (data?.user) setAccount(data.user);
      if (data?.user.encryptedCredentials) {
        updateUser();
      } else if (data && !data.user.encryptedCredentials) {
        auth.signIn().then(() => {
          updateUser();
        });
      }
    });
  }, [user, account]);
  return {
    auth: auth,
    user: user,
    account: account,
  };
};

const AuthContext = createContext<ReturnType<typeof authClient> | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  return (
    <AuthContext.Provider value={authClient()}>{children}</AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
