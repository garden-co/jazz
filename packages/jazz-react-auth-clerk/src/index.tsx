import { BrowserClerkAuth, MinimalClerkClient } from "jazz-browser-auth-clerk";
import { useMemo, useState } from "react";

export function useJazzClerkAuth(clerk: MinimalClerkClient) {
  const [state, setState] = useState<{ errors: string[] }>({ errors: [] });

  const authMethod = useMemo(() => {
    return new BrowserClerkAuth(
      {
        onError: (error) => {
          void clerk.signOut();
          setState((state) => ({
            ...state,
            errors: [...state.errors, error.toString()],
          }));
        },
      },
      clerk,
    );
  }, [clerk.user]);

  return [authMethod, state] as const;
}
