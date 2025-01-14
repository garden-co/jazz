"use client";

import React from "react";

import { Account, AccountClass, AnonymousJazzAgent } from "jazz-tools";

/** @category Context Creation */
export type JazzAuthContext<Acc extends Account> = {
  me: Acc;
  logOut: () => void;
  done: () => void;
};

export type JazzGuestContext = {
  guest: AnonymousJazzAgent;
  logOut: () => void;
  done: () => void;
};

export type JazzContextType<Acc extends Account> = (
  | JazzAuthContext<Acc>
  | JazzGuestContext
) & {
  AccountSchema: AccountClass<Acc>;
};

export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc }
  ? Acc
  : Account;

export const JazzContext = React.createContext<
  JazzContextType<Account> | undefined
>(undefined);
