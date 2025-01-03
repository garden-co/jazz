import { AgentSecret } from "cojson";
import { cojsonInternals } from "cojson";
import { PureJSCrypto } from "cojson/crypto";
import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  CoValueClass,
  CryptoProvider,
  Peer,
  createAnonymousJazzContext,
} from "jazz-tools";
import { useMemo } from "react";
import { JazzContext } from "./provider.js";

type TestAccountSchema<Acc extends Account> = CoValueClass<Acc> & {
  fromNode: (typeof Account)["fromNode"];
  create: (options: {
    creationProps: { name: string };
    initialAgentSecret?: AgentSecret;
    peersToLoadFrom?: Peer[];
    crypto: CryptoProvider;
  }) => Promise<Acc>;
};

export async function createJazzTestAccount<Acc extends Account>(options?: {
  AccountSchema?: TestAccountSchema<Acc>;
}): Promise<Acc> {
  const AccountSchema =
    options?.AccountSchema ?? (Account as unknown as TestAccountSchema<Acc>);
  const account = await AccountSchema.create({
    creationProps: {
      name: "Test Account",
    },
    crypto: await PureJSCrypto.create(),
  });

  return account;
}

export async function createJazzTestGuest() {
  const ctx = await createAnonymousJazzContext({
    crypto: await PureJSCrypto.create(),
    peersToLoadFrom: [],
  });

  return {
    guest: ctx.agent,
  };
}

export function JazzTestProvider<Acc extends Account>({
  children,
  account,
}: {
  children: React.ReactNode;
  account: Acc | { guest: AnonymousJazzAgent };
}) {
  const value = useMemo(() => {
    if ("guest" in account) {
      return {
        guest: account.guest,
        AccountSchema: Account,
        logOut: () => account.guest.node.gracefulShutdown(),
        done: () => account.guest.node.gracefulShutdown(),
      };
    }

    return {
      me: account,
      AccountSchema: account.constructor as AccountClass<Acc>,
      logOut: () => account._raw.core.node.gracefulShutdown(),
      done: () => account._raw.core.node.gracefulShutdown(),
    };
  }, [account]);

  return <JazzContext.Provider value={value}>{children}</JazzContext.Provider>;
}

export function linkAccounts(a: Account, b: Account) {
  const [aPeer, bPeer] = cojsonInternals.connectedPeers("a", "b", {
    peer1role: "server",
    peer2role: "server",
  });

  a._raw.core.node.syncManager.addPeer(aPeer);
  b._raw.core.node.syncManager.addPeer(bPeer);
}
