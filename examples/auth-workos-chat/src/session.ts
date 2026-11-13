import * as React from "react";

export interface WorkOSUser {
  firstName: string | null;
  lastName: string | null;
  email: string;
  role: string | null;
}

/** The signed-in WorkOS user, as the backend's AuthKit cookie says. */
export function useWorkOSSession() {
  const [state, setState] = React.useState<{ user: WorkOSUser | null; isLoading: boolean }>({
    user: null,
    isLoading: true,
  });
  React.useEffect(() => {
    let live = true;
    void fetch("/api/auth/session")
      .then((response) => (response.ok ? (response.json() as Promise<WorkOSUser | null>) : null))
      .catch(() => null)
      .then((user) => live && setState({ user, isLoading: false }));
    return () => {
      live = false;
    };
  }, []);
  return {
    ...state,
    signIn: () => window.location.assign("/auth/sign-in"),
    /** Ends the session at WorkOS and clears the cookie, without leaving the page. */
    async signOut() {
      await fetch("/auth/sign-out", { method: "POST" });
      setState({ user: null, isLoading: false });
    },
  };
}
