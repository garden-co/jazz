import { createContext, useContext } from "react";
import type { Me } from "./actions.js";

const MeContext = createContext<Me | null>(null);

export const MeProvider = MeContext.Provider;

/** The signed-in person. Only used below the bootstrap in StagePlan. */
export function useMe(): Me {
  const me = useContext(MeContext);
  if (!me) throw new Error("useMe must be used inside MeProvider");
  return me;
}
