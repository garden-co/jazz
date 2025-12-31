import React from "react";

import { Account, JazzContextManager } from "jazz-tools";

export const JazzContext = React.createContext<
  JazzContextManager<Account, {}> | undefined
>(undefined);
