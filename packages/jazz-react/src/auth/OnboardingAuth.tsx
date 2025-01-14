"use client";

import { BrowserOnboardingAuth } from "jazz-browser";
import { useMemo, useState } from "react";

type OnboardingAuthState = (
  | {
      state: "uninitialized";
    }
  | {
      state: "loading";
    }
  | {
      state: "ready";
    }
  | {
      state: "signedIn";
      logOut: () => void;
    }
) & {
  errors: string[];
};

/**
 * `useOnboardingAuth` is a hook that provides a `JazzAuth` object for zero-interaction authentication.
 *
 *
 * ```ts
 * const [auth, state] = useOnboardingAuth();
 * ```
 *
 * @category Auth Providers
 */
export function useOnboardingAuth(
  {
    defaultUserName,
  }: {
    defaultUserName: string;
  } = {
    defaultUserName: "Anonymous user",
  },
) {
  const [state, setState] = useState<OnboardingAuthState>({
    state: "loading",
    errors: [],
  });

  const authMethod = useMemo(() => {
    return new BrowserOnboardingAuth(defaultUserName, {
      onSignedIn: ({ logOut }) => {
        setState({ state: "signedIn", logOut, errors: [] });
      },
      onError: (error) => {
        setState((current) => ({
          ...current,
          errors: [...current.errors, error.toString()],
        }));
      },
    });
  }, [defaultUserName]);

  return [authMethod, state] as const;
}
