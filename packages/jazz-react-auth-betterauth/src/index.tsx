import type { ClientOptions } from "better-auth";
import { BetterAuth } from "jazz-auth-betterauth";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-react";
import { useEffect, useMemo } from "react";

// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./components/AccountProviders";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./components/SSOButton";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./components/DeleteAccountButton";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * as SignInForm from "./components/forms/SignIn";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * as SignUpForm from "./components/forms/SignUp";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * as ForgotForm from "./components/forms/Forgot";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * as ResetForm from "./components/forms/Reset";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * as SettingsForm from "./components/forms/Settings";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./types/auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * as MagicLinkSignIn from "./components/routes/magic-link/logIn/page";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * as MagicLinkSignUp from "./components/routes/magic-link/signIn/page";

/**
 * @category Auth Providers
 */
export function useBetterAuth<T extends ClientOptions>(options?: T) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Better Auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new BetterAuth(context.authenticate, authSecretStorage, options);
  }, [context.authenticate, authSecretStorage, options]);

  const isAuthenticated = useIsAuthenticated();

  useEffect(() => {
    authMethod.authClient.useSession.subscribe((value) => {
      authMethod.onUserChange(value.data ?? undefined);
    });
  }, [isAuthenticated]);

  return {
    state: isAuthenticated
      ? "signedIn"
      : ("anonymous" as "signedIn" | "anonymous"),
    logIn: authMethod.logIn as () => Promise<void>,
    signIn: authMethod.signIn as () => Promise<void>,
    authClient: authMethod.authClient,
  } as const;
}
