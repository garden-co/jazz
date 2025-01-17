import { AuthSecretStorage, BrowserAnonymousAuth } from "jazz-browser";
import { useEffect, useMemo, useState } from "react";

type AnonymousAuthState = (
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
  const [state, setState] = useState<AnonymousAuthState>({
    state: "loading",
    errors: [],
  });

  const authMethod = useMemo(() => {
    return new BrowserAnonymousAuth(defaultUserName, {
      onSignedIn: ({ logOut }) => {
        setState({ state: "signedIn", logOut, errors: [] });
      },
      onError: (error) => {
        setState((current) => ({
          ...current,
          errors: [error.toString()],
        }));
      },
    });
  }, [defaultUserName]);

  return [authMethod, state] as const;
}

export function useIsAnonymousUser() {
  const [isAnonymous, setIsAnonymous] = useState(() =>
    AuthSecretStorage.isAnonymous(),
  );

  useEffect(() => {
    function handleUpdate() {
      setIsAnonymous(AuthSecretStorage.isAnonymous());
    }

    return AuthSecretStorage.onUpdate(handleUpdate);
  }, []);

  return isAnonymous;
}
