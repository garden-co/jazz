import { Account, JazzContextManager, JazzContextType } from "jazz-tools";
import React from "react";

export const JazzContext = React.createContext<
  JazzContextType<Account> | undefined
>(undefined);

export const JazzContextManagerContext = React.createContext<
  JazzContextManager<Account, {}> | undefined
>(undefined);
