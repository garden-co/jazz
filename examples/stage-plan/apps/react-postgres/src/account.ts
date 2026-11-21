import { createContext, useContext, useEffect, useState } from "react";
import { get } from "./data/api.js";
import { connectLive, queryClient } from "./data/live.js";
import type { Crew } from "../shared/types.js";

/**
 * An anonymous account, like the Jazz device account: no sign-up. The app's
 * server keeps it in a signed cookie, issued on the first visit.
 */
export type Session = { account: string };

const SessionContext = createContext<Session | null>(null);
export const SessionProvider = SessionContext.Provider;

export function useSession(): Session {
  const session = useContext(SessionContext);
  if (!session) throw new Error("useSession must be used inside SessionProvider");
  return session;
}

/** Signs in anonymously (or resumes the cookie's account), then opens the live socket. */
export function useSignIn(): Session | undefined {
  const [session, setSession] = useState<Session>();
  useEffect(() => {
    void get<{ account: string; profile: Crew | null }>("/session").then(({ account, profile }) => {
      queryClient.setQueryData(["me"], profile);
      connectLive();
      setSession({ account });
    });
  }, []);
  return session;
}
