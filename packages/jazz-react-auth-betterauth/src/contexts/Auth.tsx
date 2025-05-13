"use client";

import type { ClientOptions } from "better-auth/client";
import type { AuthCredentials } from "jazz-tools";
import { createContext, useContext } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useBetterAuth } from "../index";

const equalCredentials = (a?: AuthCredentials, b?: AuthCredentials) => {
  if (a && b) {
    return (
      a.accountID === b.accountID &&
      JSON.stringify(a.secretSeed) === JSON.stringify(b.secretSeed) &&
      a.accountSecret === b.accountSecret &&
      a.provider === b.provider
    );
  } else {
    return a === undefined && b === undefined;
  }
};

const authClient = <T extends ClientOptions>(options?: T) => {
  const auth = useBetterAuth(options);
  return {
    auth: auth,
    hasCredentials: auth.state !== "anonymous",
    account: auth.account,
  };
};

const AuthContext = createContext<ReturnType<typeof authClient> | null>(null);

export function AuthProvider({
  children,
  options,
}: {
  children: React.ReactNode;
  options?: Parameters<typeof useBetterAuth>[0];
}) {
  return (
    <AuthContext.Provider
      value={{
        ...authClient(options),
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
