import { JazzClerkAuth, type MinimalClerkClient } from "jazz-auth-clerk";
import { LocalStorageKVStore } from "jazz-browser";
import {
  JazzProvider,
  JazzProviderProps,
  useAuthSecretStorage,
  useJazzContext,
} from "jazz-react";
import { InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { useEffect, useMemo, useState } from "react";

function useJazzClerkAuth(clerk: MinimalClerkClient) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Clerk auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new JazzClerkAuth(context.authenticate, authSecretStorage);
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

export const JazzProviderWithClerk = (
  props: { clerk: MinimalClerkClient } & JazzProviderProps,
) => {
  const [isLoaded, setIsLoaded] = useState(false);

  /**
   * This effect ensures that a logged-in Clerk user is authenticated before the JazzProvider is mounted.
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
    return null;
  }

  return (
    <JazzProvider {...props} logOutReplacement={props.clerk.signOut}>
      <RegisterClerkAuth clerk={props.clerk}>
        {props.children}
      </RegisterClerkAuth>
    </JazzProvider>
  );
};

function setupKvStore() {
  KvStoreContext.getInstance().initialize(
    typeof window === "undefined"
      ? new InMemoryKVStore()
      : new LocalStorageKVStore(),
  );
}
