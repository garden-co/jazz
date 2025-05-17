import { AccountClass, isControlledAccount } from "../coValues/account";

import { CoID, LocalNode, RawCoValue } from "cojson";
import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  Account,
  createJazzContextFromExistingCredentials,
  randomSessionProvider,
} from "../index";
import { CoValueFromRaw } from "../internal";

const Crypto = await WasmCrypto.create();

export async function setupAccount() {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const [initialAsPeer, secondPeer] = cojsonInternals.connectedPeers(
    "initial",
    "second",
    {
      peer1role: "server",
      peer2role: "client",
    },
  );

  if (!isControlledAccount(me)) {
    throw "me is not a controlled account";
  }
  me._raw.core.node.syncManager.addPeer(secondPeer);
  const { account: meOnSecondPeer } =
    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.id,
        secret: me._raw.core.node.getCurrentAgent().agentSecret,
      },
      sessionProvider: randomSessionProvider,
      peersToLoadFrom: [initialAsPeer],
      crypto: Crypto,
    });

  return { me, meOnSecondPeer };
}

export async function setupTwoNodes(options?: {
  ServerAccountSchema?: CoValueFromRaw<Account> & AccountClass<Account>;
}) {
  const ServerAccountSchema = options?.ServerAccountSchema ?? Account;

  const [serverAsPeer, clientAsPeer] = cojsonInternals.connectedPeers(
    "clientToServer",
    "serverToClient",
    {
      peer1role: "server",
      peer2role: "client",
    },
  );

  const client = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [serverAsPeer],
    crypto: Crypto,
    creationProps: { name: "Client" },
    migration: async (rawAccount, _node, creationProps) => {
      const account = new Account({
        fromRaw: rawAccount,
      });

      await account.applyMigration(creationProps);
    },
  });

  const server = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [clientAsPeer],
    crypto: Crypto,
    creationProps: { name: "Server" },
    migration: async (rawAccount, _node, creationProps) => {
      const account = new ServerAccountSchema({
        fromRaw: rawAccount,
      });

      await account.applyMigration(creationProps);
    },
  });

  return {
    clientNode: client.node,
    serverNode: server.node,
    clientAccount: Account.fromRaw(
      await loadCoValueOrFail(client.node, client.accountID),
    ),
    serverAccount: ServerAccountSchema.fromRaw(
      await loadCoValueOrFail(server.node, server.accountID),
    ),
  };
}

export function waitFor(
  callback: () => boolean | void | Promise<boolean | void>,
) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = async () => {
      try {
        return { ok: await callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(async () => {
      const { ok, error } = await checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}

export async function loadCoValueOrFail<V extends RawCoValue>(
  node: LocalNode,
  id: CoID<V>,
): Promise<V> {
  const value = await node.load(id);
  if (value === "unavailable") {
    throw new Error("CoValue not found");
  }
  return value;
}
