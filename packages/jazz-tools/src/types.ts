import type { AgentSecret, LocalNode } from "cojson";
import type { Account } from "./exports.js";
import type { AnonymousJazzAgent, ID } from "./internal.js";

export type AuthCredentials = {
  accountID: ID<Account>;
  secretSeed?: Uint8Array;
  accountSecret: AgentSecret;
  provider?: "anonymous" | "clerk" | "demo" | "passkey" | "passphrase" | string;
};

export type AuthenticateAccountFunction = (
  credentials: AuthCredentials,
) => Promise<void>;
export type RegisterAccountFunction = (
  accountSecret: AgentSecret,
  creationProps: { name: string },
) => Promise<ID<Account>>;

/** @category Context Creation */
export type JazzAuthContext<Acc extends Account> = {
  me: Acc;
  node: LocalNode;
  authenticate: AuthenticateAccountFunction;
  logOut: () => Promise<void>;
  done: () => void;
};

export type JazzGuestContext = {
  guest: AnonymousJazzAgent;
  node: LocalNode;
  authenticate: AuthenticateAccountFunction;
  logOut: () => void;
  done: () => void;
};

export type JazzContextType<Acc extends Account> =
  | JazzAuthContext<Acc>
  | JazzGuestContext;

export type NewAccountProps = {
  secret?: AgentSecret;
  creationProps?: { name: string };
};

export type SyncConfig =
  | {
      peer: `wss://${string}` | `ws://${string}`;
      when?: "always" | "signedUp";
    }
  | {
      peer?: `wss://${string}` | `ws://${string}`;
      when: "never";
    };
