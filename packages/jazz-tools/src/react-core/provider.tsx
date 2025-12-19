import React from "react";

import {
  Account,
  AnyAccountSchema,
  JazzContextManager,
  JazzContextType,
} from "jazz-tools";

export const JazzContext = React.createContext<
  JazzContextType<AnyAccountSchema> | undefined
>(undefined);

export const JazzContextManagerContext = React.createContext<
  JazzContextManager<AnyAccountSchema, {}> | undefined
>(undefined);
