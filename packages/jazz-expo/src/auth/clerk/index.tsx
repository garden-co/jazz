import { JazzClerkAuth, type MinimalClerkClient } from "jazz-auth-clerk";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  KvStoreContext,
} from "jazz-tools";
import { useEffect, useMemo, useState } from "react";
import {
  ExpoSecureStoreAdapter,
  JazzProvider,
  JazzProviderProps,
  useAuthSecretStorage,
  useJazzContext,
} from "../../index.js";

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

export const JazzProviderWithClerk = <
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(
  props: { clerk: MinimalClerkClient } & JazzProviderProps<S>,
) => {
  const [isLoaded, setIsLoaded] = useState(false);

  /**
   * This effect ensures that a logged-in Clerk user is authenticated before the JazzProvider is mounted.
   *
   * This is done to optimize the initial load.
   */
  useEffect(() => {
    KvStoreContext.getInstance().initialize(
      props.kvStore ?? new ExpoSecureStoreAdapter(),
    );

    JazzClerkAuth.initializeAuth(props.clerk)
      .then(() => {
        setIsLoaded(true);
      })
      .catch((error) => {
        console.error("error initializing auth", error);
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
