import { JazzClerkAuth, type MinimalClerkClient } from "jazz-auth-clerk";
import {
  JazzProvider,
  JazzProviderProps,
  useAuthSecretStorage,
  useJazzContext,
} from "jazz-expo";
import { useEffect, useMemo } from "react";

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
    // Need to use addListener because the clerk user object is not updated when the user logs in
    return clerk.addListener((event) => {
      authMethod.onClerkUserChange(event as Pick<MinimalClerkClient, "user">);
    });
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
): JSX.Element => {
  return (
    <JazzProvider {...props} onLogOut={props.clerk.signOut}>
      <RegisterClerkAuth clerk={props.clerk}>
        {props.children}
      </RegisterClerkAuth>
    </JazzProvider>
  );
};
