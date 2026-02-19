import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { createDb, Db, type DbConfig } from "../index.js";
import type { Session } from "../runtime/context.js";
import { resolveLocalAuthDefaults } from "../runtime/local-auth.js";
import { resolveClientSession } from "../runtime/client-session.js";

export interface JazzProviderProps {
  config: DbConfig;
  children: ReactNode;
  fallback?: ReactNode;
}

interface JazzContextValue {
  db: Db;
  session: Session | null;
}

const JazzContext = createContext<JazzContextValue | null>(null);

export function JazzProvider({ config, children, fallback }: JazzProviderProps) {
  const [value, setValue] = useState<JazzContextValue | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let cancelled = false;
    let instance: Db | null = null;

    const resolvedConfig = resolveLocalAuthDefaults(config);
    Promise.all([createDb(resolvedConfig), resolveClientSession(resolvedConfig)])
      .then(([created, session]) => {
        if (cancelled) {
          void created.shutdown();
          return;
        }
        instance = created;
        setValue({ db: created, session });
      })
      .catch((reason) => {
        const nextError = reason instanceof Error ? reason : new Error(String(reason));
        setError(nextError);
      });

    return () => {
      cancelled = true;
      if (instance) {
        void instance.shutdown();
      }
    };
  }, []); // config is treated as stable (mount-only)

  if (error) {
    throw error;
  }

  if (!value) return <>{fallback ?? null}</>;
  return <JazzContext.Provider value={value}>{children}</JazzContext.Provider>;
}

export function useDb(): Db {
  const ctx = useContext(JazzContext);
  if (!ctx) throw new Error("useDb must be used within <JazzProvider>");
  return ctx.db;
}

export function useSession(): Session | null {
  const ctx = useContext(JazzContext);
  if (!ctx) throw new Error("useSession must be used within <JazzProvider>");
  return ctx.session;
}
