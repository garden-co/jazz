import { assert, expect } from "vitest";
import { AccountClass, isControlledAccount } from "../coValues/account";

import { CoID, LocalNode, RawCoValue } from "cojson";
import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  Account,
  createJazzContextFromExistingCredentials,
  MockSessionProvider,
} from "../index";
import {
  CoValue,
  CoValueFromRaw,
  CoValueLoadingState,
  MaybeLoaded,
  LoadedAndRequired,
  AccountSchema,
} from "../internal";

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
  me.$jazz.localNode.syncManager.addPeer(secondPeer);
  const { account: meOnSecondPeer } =
    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.$jazz.id,
        secret: me.$jazz.localNode.getCurrentAgent().agentSecret,
      },
      sessionProvider: new MockSessionProvider(),
      peers: [initialAsPeer],
      crypto: Crypto,
      asActiveAccount: true,
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
    peers: [serverAsPeer],
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
    peers: [clientAsPeer],
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
  if (value === CoValueLoadingState.UNAVAILABLE) {
    throw new Error("CoValue not found");
  }
  return value;
}

export function assertLoaded<T extends MaybeLoaded<CoValue>>(
  coValue: T,
): asserts coValue is LoadedAndRequired<T> {
  assert(coValue.$isLoaded, "CoValue is not loaded");
}

export async function createAccountAs<S extends AccountSchema<any, any>>(
  schema: S,
  as: Account,
  options: {
    creationProps: { name: string };
  },
) {
  const connectedPeers = cojsonInternals.connectedPeers(
    "creatingAccount",
    "createdAccount",
    { peer1role: "server", peer2role: "client" },
  );

  as.$jazz.localNode.syncManager.addPeer(connectedPeers[1]);

  const account = await schema.create({
    creationProps: options.creationProps,
    crypto: as.$jazz.localNode.crypto,
    peers: [connectedPeers[0]],
  });

  await account.$jazz.waitForAllCoValuesSync();

  return account;
}

function verifyValidationError(e: any, expectedIssues?: any) {
  if (e?.name !== "ZodError") {
    throw e;
  }

  if (expectedIssues) {
    expect(e.issues).toEqual(expectedIssues);
  }
}

export function expectValidationError(
  fn: () => Promise<any>,
  expectedIssues?: any,
): Promise<void>;
export function expectValidationError(
  fn: () => any,
  expectedIssues?: any,
): void;
export function expectValidationError(
  fn: () => any,
  expectedIssues?: any,
): void | Promise<void> {
  try {
    const result = fn();

    if (result instanceof Promise) {
      return result
        .then(() => {
          throw new Error("Expected validation error, but no error was thrown");
        })
        .catch((e: any) => {
          verifyValidationError(e, expectedIssues);
        });
    } else {
      throw new Error("Expected validation error, but no error was thrown");
    }
  } catch (e: any) {
    verifyValidationError(e, expectedIssues);
  }
}
