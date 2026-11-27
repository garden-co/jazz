"use client";
import { createContext, useContext } from "react";
import type { Db } from "./live";

export const DbContext = createContext<Db | null>(null);

/** The signed-in user's writes: `db.mutate(mutators.x.y(args))`. */
export function useDb(): Db {
  const db = useContext(DbContext);
  if (!db) throw new Error("useDb must be used inside NeonProvider");
  return db;
}
