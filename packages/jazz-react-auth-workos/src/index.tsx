import { useEffect, useMemo, useState } from 'react';
import { JazzWorkOSAuth, type MinimalWorkOSClient } from 'jazz-auth-workos';
import { LocalStorageKVStore } from "jazz-browser";
import {
  JazzProvider,
  JazzProviderProps,
  useAuthSecretStorage,
  useJazzContext,
} from "jazz-react";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  InMemoryKVStore,
  KvStoreContext,
} from "jazz-tools";

const useJazzWorkOSAuth = (workos: MinimalWorkOSClient) => {
    const context = useJazzContext();
    const authSecretStorage = useAuthSecretStorage();

    if ("guest" in context) {
        throw new Error("WorkOS auth is not supported in guest mode");
    }

    const authMethod = useMemo(() => {
        return new JazzWorkOSAuth(context.authenticate, authSecretStorage);
    }, []);

    useEffect(() => {
        return authMethod.registerListener(workos);
    }, []);
}

const RegisterWorkOSAuth = (props: { workos: MinimalWorkOSClient, children: React.ReactNode }) => {
    useJazzWorkOSAuth(props.workos);
    return props.children;
}

export const JazzProviderWithWorkOS = <
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(
  props: { workos: MinimalWorkOSClient } & JazzProviderProps<S>,
) => {
  const [isLoaded, setIsLoaded] = useState(false);

  /**
   * This effect ensures that a logged-in WorkOS user is authenticated before the JazzProvider is mounted.
   *
   * This is done to optimize the initial load.
   */
  useEffect(() => {
    setupKvStore();

    JazzWorkOSAuth.initializeAuth(props.workos).then(() => {
      setIsLoaded(true);
    });
  }, []);

  if (!isLoaded) {
    return null;
  }

  return (
    <JazzProvider {...props} logOutReplacement={props.workos.signOut}>
      <RegisterWorkOSAuth workos={props.workos}>
        {props.children}
      </RegisterWorkOSAuth>
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