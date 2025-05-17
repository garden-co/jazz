import React from "react";

import { Account, JazzContextManager, JazzContextType } from "jazz-tools";

export const JazzContext = React.createContext<
  JazzContextType<Account> | undefined
>(undefined);

export const JazzContextManagerContext = React.createContext<
  JazzContextManager<Account, {}> | undefined
>(undefined);
