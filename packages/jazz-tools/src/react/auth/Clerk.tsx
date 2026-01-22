import { JazzClerkAuth, type MinimalClerkClient } from "jazz-tools";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  InMemoryKVStore,
  KvStoreContext,
} from "jazz-tools";
import { LocalStorageKVStore } from "jazz-tools/browser";
import {
  useAuthSecretStorage,
  useJazzContextValue,
} from "jazz-tools/react-core";
import { ReactNode, useEffect, useMemo, useState } from "react";
import { JazzProviderProps, JazzReactProvider } from "../provider.js";

function useJazzClerkAuth(clerk: MinimalClerkClient) {
  const context = useJazzContextValue();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Clerk auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new JazzClerkAuth(
      context.authenticate,
      context.logOut,
      authSecretStorage,
    );
  }, []);

  useEffect(() => {
    return authMethod.registerListener(clerk);
  }, []);
}

function RegisterClerkAuth(props: {
  clerk: MinimalClerkClient;
  children: React.ReactNode;
}) {
  useJazzClerkAuth(props.clerk);

  return props.children;
}

export const JazzReactProviderWithClerk = <
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(
  props: {
    clerk: MinimalClerkClient;
  } & JazzProviderProps<S>,
) => {
  const [isLoaded, setIsLoaded] = useState(false);

  /**
   * This effect ensures that a logged-in Clerk user is authenticated before the JazzReactProvider is mounted.
   *
   * This is done to optimize the initial load.
   */
  useEffect(() => {
    setupKvStore();

    JazzClerkAuth.initializeAuth(props.clerk).then(() => {
      setIsLoaded(true);
    });
  }, []);

  if (!isLoaded) {
    return props.fallback ?? null;
  }

  return (
    <JazzReactProvider {...props} onLogOut={props.clerk.signOut}>
      <RegisterClerkAuth clerk={props.clerk}>
        {props.children}
      </RegisterClerkAuth>
    </JazzReactProvider>
  );
};

function setupKvStore() {
  KvStoreContext.getInstance().initialize(
    typeof window === "undefined"
      ? new InMemoryKVStore()
      : new LocalStorageKVStore(),
  );
}
