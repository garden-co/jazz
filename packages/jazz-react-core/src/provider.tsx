import React from "react";

import { Account, JazzContextManager, JazzContextType } from "jazz-tools";

export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc }
  ? Acc
  : Account;

export const JazzContext = React.createContext<
  JazzContextType<Account> | undefined
>(undefined);

export const JazzContextManagerContext = React.createContext<
  JazzContextManager<Account, {}> | undefined
>(undefined);

declare module "jazz-tools" {
  export interface Register {
    Account: RegisteredAccount;
  }
}
