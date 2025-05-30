import { JazzWorkOSAuth, type MinimalWorkOSClient } from "jazz-auth-workos";
import { KvStoreContext } from "jazz-tools";
import { useEffect, useMemo, useState } from "react";
import {
  ExpoSecureStoreAdapter,
  JazzProvider,
  JazzProviderProps,
  useAuthSecretStorage,
  useJazzContext,
} from "../../index.js";

function useJazzWorkOSAuth(workos: MinimalWorkOSClient) {
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

function RegisterWorkOSAuth(props: {
  workos: MinimalWorkOSClient;
  children: React.ReactNode;
}) {
  useJazzWorkOSAuth(props.workos);
  return props.children;
}

export const JazzProviderWithWorkOS = (
  props: { workos: MinimalWorkOSClient } & JazzProviderProps,
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

    JazzWorkOSAuth.initializeAuth(props.workos)
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
    <JazzProvider {...props} logOutReplacement={props.workos.signOut}>
      <RegisterWorkOSAuth workos={props.workos}>
        {props.children}
      </RegisterWorkOSAuth>
    </JazzProvider>
  );
};
