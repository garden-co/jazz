import { createContext, useContext, useEffect, useState } from "react";
import type { Db } from "./model/actions.js";

/**
 * An anonymous account, like the Jazz device account: no sign-up. The app's
 * server issues it as an HTTP-only JWT cookie on first visit and reads it on
 * every sealing and write request.
 */
export function useAccount(): string | undefined {
  const [account, setAccount] = useState<string>();
  useEffect(() => {
    fetch("/api/login", { credentials: "include" })
      .then((response) => response.json())
      .then(({ userID }: { userID: string }) => setAccount(userID));
  }, []);
  return account;
}

const DbContext = createContext<Db | null>(null);
export const DbProvider = DbContext.Provider;

/** The signed-in account, for the actions. */
export function useDb(): Db {
  const db = useContext(DbContext);
  if (!db) throw new Error("useDb must be used inside DbProvider");
  return db;
}
