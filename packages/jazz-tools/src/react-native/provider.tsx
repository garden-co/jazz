import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { createDb, Db, type DbConfig } from "./db.js";

export interface JazzProviderProps {
  config: DbConfig;
  children: ReactNode;
  fallback?: ReactNode;
}

const JazzContext = createContext<Db | null>(null);

export function JazzProvider({ config, children, fallback }: JazzProviderProps) {
  const [db, setDb] = useState<Db | null>(null);

  useEffect(() => {
    let cancelled = false;
    let instance: Db | null = null;

    createDb(config).then((created) => {
      if (cancelled) {
        void created.shutdown();
        return;
      }
      instance = created;
      setDb(created);
    });

    return () => {
      cancelled = true;
      if (instance) {
        void instance.shutdown();
      }
    };
  }, []); // config is treated as stable (mount-only)

  if (!db) return <>{fallback ?? null}</>;
  return <JazzContext.Provider value={db}>{children}</JazzContext.Provider>;
}

export function useDb(): Db {
  const db = useContext(JazzContext);
  if (!db) throw new Error("useDb must be used within <JazzProvider>");
  return db;
}
