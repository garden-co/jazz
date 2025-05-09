"use client";

import type { ClientOptions } from "better-auth/client";
import { useAccount, useIsAuthenticated } from "jazz-react";
import type { AuthCredentials } from "jazz-tools";
import { createContext, useContext, useEffect, useState } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useBetterAuth } from "../index";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { DefaultImage, type Image as ImageType } from "../types/image";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { DefaultLink, type Link as LinkType } from "../types/link";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { defaultNavigate, defaultReplace } from "../types/router";

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

const authClient = <T extends ClientOptions>(
  onSessionChange?: () => void | Promise<void>,
  options?: T,
) => {
  const { me } = useAccount();
  const isAuthenticated = useIsAuthenticated();
  const auth = useBetterAuth(options);
  type Data = Awaited<
    ReturnType<typeof auth.authClient.getSession<{}>>
  >["data"];
  type User = NonNullable<Data>["user"];
  const [user, setUser] = useState<AuthCredentials | undefined>(undefined);
  const [account, setAccount] = useState<User | undefined>(undefined);
  function useUpdateUser() {
    auth.authClient.jazzPlugin
      .decryptCredentials()
      .then((x) => {
        if (x.error) console.error("Error decrypting credentials:", x.error);
        const data = x.data ?? undefined;
        if (!equalCredentials(user, data)) {
          setUser(data);
        }
      })
      .catch((error) => {
        console.error("Error decrypting credentials:", error);
      });
  }
  useEffect(() => {
    auth.authClient.useSession.subscribe(({ data }: { data: Data }) => {
      if (data?.user) setAccount(data.user);
      if (data?.user.encryptedCredentials) {
        useUpdateUser();
      } else if (data && !data.user.encryptedCredentials) {
        auth.signIn().then(() => {
          useUpdateUser();
        });
      }
      if (onSessionChange) onSessionChange();
    });
  }, [user, account, auth.state, isAuthenticated]);
  return {
    auth: auth,
    user: user,
    account: account,
  };
};

const AuthContext = createContext<
  | (ReturnType<typeof authClient> & {
      Link: LinkType;
      Image: ImageType;
      navigate: typeof defaultNavigate;
      replace: typeof defaultReplace;
    })
  | null
>(null);

export function AuthProvider({
  children,
  Image = DefaultImage,
  Link = DefaultLink,
  navigate = defaultNavigate,
  replace = defaultReplace,
  onSessionChange,
  options,
}: {
  children: React.ReactNode;
  Image?: ImageType;
  Link?: LinkType;
  navigate?: typeof defaultNavigate;
  replace?: typeof defaultReplace;
  onSessionChange?: () => void | Promise<void>;
  options: Parameters<typeof useBetterAuth>[0];
}) {
  return (
    <AuthContext.Provider
      value={{
        ...authClient(onSessionChange, options),
        Image,
        Link,
        navigate,
        replace,
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
