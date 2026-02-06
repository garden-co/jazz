import {
  AgentSecret,
  CoID,
  CryptoProvider,
  LocalNode,
  Peer,
  RawAccount,
  RawAccountID,
  SessionID,
  StorageAPI,
} from "cojson";
import { AuthSecretStorage } from "../auth/AuthSecretStorage.js";
import { type Account, type AccountClass } from "../coValues/account.js";
import { RegisteredSchemas } from "../coValues/registeredSchemas.js";
import {
  CoValueFromRaw,
  type CoreAccountSchema,
  type ID,
  type InstanceOfSchema,
  coValueClassFromCoValueClassOrSchema,
  type SyncWhen,
} from "../internal.js";
import { AuthCredentials, NewAccountProps } from "../types.js";
import { activeAccountContext } from "./activeAccountContext.js";
import { AnonymousJazzAgent } from "./anonymousJazzAgent.js";

export type Credentials = {
  accountID: ID<Account>;
  secret: AgentSecret;
};

export interface SessionProvider {
  acquireSession: (
    accountID: ID<Account>,
    crypto: CryptoProvider,
  ) => Promise<{ sessionID: SessionID; sessionDone: () => void }>;
  persistSession: (
    accountID: ID<Account>,
    sessionID: SessionID,
  ) => Promise<{ sessionDone: () => void }>;
}

export class MockSessionProvider implements SessionProvider {
  async acquireSession(
    accountID: ID<Account>,
    crypto: CryptoProvider,
  ): Promise<{ sessionID: SessionID; sessionDone: () => void }> {
    return {
      sessionID: crypto.newRandomSessionID(
        accountID as unknown as RawAccountID,
      ),
      sessionDone: () => {},
    };
  }

  async persistSession(
    _accountID: ID<Account>,
    _sessionID: SessionID,
  ): Promise<{ sessionDone: () => void }> {
    return {
      sessionDone: () => {},
    };
  }
}

export type AuthResult =
  | {
      type: "existing";
      username?: string;
      credentials: Credentials;
      saveCredentials?: (credentials: Credentials) => Promise<void>;
      onSuccess: () => void;
      onError: (error: string | Error) => void;
      logOut: () => Promise<void>;
    }
  | {
      type: "new";
      creationProps: {
        name: string;
        anonymous?: boolean;
        other?: Record<string, unknown>;
      };
      initialSecret?: AgentSecret;
      saveCredentials: (credentials: Credentials) => Promise<void>;
      onSuccess: () => void;
      onError: (error: string | Error) => void;
      logOut: () => Promise<void>;
    };

export type JazzContextWithAccount<Acc extends Account> = {
  node: LocalNode;
  account: Acc;
  done: () => void;
  logOut: () => Promise<void>;
};

export type JazzContextWithAgent = {
  agent: AnonymousJazzAgent;
  done: () => void;
  logOut: () => Promise<void>;
};

export type JazzContext<Acc extends Account> =
  | JazzContextWithAccount<Acc>
  | JazzContextWithAgent;

export async function createJazzContextFromExistingCredentials<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | CoreAccountSchema,
>({
  credentials,
  peers,
  syncWhen,
  crypto,
  storage,
  AccountSchema: PropsAccountSchema,
  sessionProvider,
  onLogOut,
  asActiveAccount,
}: {
  credentials: Credentials;
  peers: Peer[];
  syncWhen?: SyncWhen;
  crypto: CryptoProvider;
  AccountSchema?: S;
  sessionProvider: SessionProvider;
  onLogOut?: () => void;
  storage?: StorageAPI;
  asActiveAccount: boolean;
}): Promise<JazzContextWithAccount<InstanceOfSchema<S>>> {
  const { sessionID, sessionDone } = await sessionProvider.acquireSession(
    credentials.accountID,
    crypto,
  );

  const CurrentAccountSchema =
    PropsAccountSchema ?? (RegisteredSchemas["Account"] as unknown as S);

  const AccountClass =
    coValueClassFromCoValueClassOrSchema(CurrentAccountSchema);

  const node = await LocalNode.withLoadedAccount({
    accountID: credentials.accountID as unknown as CoID<RawAccount>,
    accountSecret: credentials.secret,
    sessionID,
    peers,
    syncWhen,
    crypto,
    storage,
    enableFullStorageReconciliation: !!storage,
    migration: async (rawAccount, _node, creationProps) => {
      const account = AccountClass.fromRaw(rawAccount) as InstanceOfSchema<S>;
      if (asActiveAccount) {
        activeAccountContext.set(account);
      }

      await account.applyMigration(creationProps);
    },
  });

  const account = AccountClass.fromNode(node);
  if (asActiveAccount) {
    activeAccountContext.set(account);
  }

  return {
    node,
    account: account as InstanceOfSchema<S>,
    done: () => {
      node.gracefulShutdown();
      sessionDone();
    },
    logOut: async () => {
      await node.gracefulShutdown();
      sessionDone();
      await onLogOut?.();
    },
  };
}

export async function createJazzContextForNewAccount<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | CoreAccountSchema,
>({
  creationProps,
  initialAgentSecret,
  peers,
  syncWhen,
  crypto,
  AccountSchema: PropsAccountSchema,
  onLogOut,
  storage,
  sessionProvider,
}: {
  creationProps: { name: string };
  initialAgentSecret?: AgentSecret;
  peers: Peer[];
  syncWhen?: SyncWhen;
  crypto: CryptoProvider;
  AccountSchema?: S;
  onLogOut?: () => Promise<void>;
  storage?: StorageAPI;
  sessionProvider: SessionProvider;
}): Promise<JazzContextWithAccount<InstanceOfSchema<S>>> {
  const CurrentAccountSchema =
    PropsAccountSchema ?? (RegisteredSchemas["Account"] as unknown as S);

  const AccountClass =
    coValueClassFromCoValueClassOrSchema(CurrentAccountSchema);

  const { node } = await LocalNode.withNewlyCreatedAccount({
    creationProps,
    peers,
    syncWhen,
    crypto,
    initialAgentSecret,
    storage,
    enableFullStorageReconciliation: !!storage,
    migration: async (rawAccount, _node, creationProps) => {
      const account = AccountClass.fromRaw(rawAccount) as InstanceOfSchema<S>;
      activeAccountContext.set(account);

      await account.applyMigration(creationProps);
    },
  });

  const account = AccountClass.fromNode(node);
  activeAccountContext.set(account);

  const { sessionDone } = await sessionProvider.persistSession(
    account.$jazz.id,
    node.currentSessionID,
  );

  return {
    node,
    account: account as InstanceOfSchema<S>,
    done: () => {
      node.gracefulShutdown();
      sessionDone();
    },
    logOut: async () => {
      await node.gracefulShutdown();
      await onLogOut?.();
    },
  };
}

export async function createJazzContext<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | CoreAccountSchema,
>(options: {
  credentials?: AuthCredentials;
  newAccountProps?: NewAccountProps;
  peers: Peer[];
  syncWhen?: SyncWhen;
  crypto: CryptoProvider;
  defaultProfileName?: string;
  AccountSchema?: S;
  sessionProvider: SessionProvider;
  authSecretStorage: AuthSecretStorage;
  storage?: StorageAPI;
}) {
  const crypto = options.crypto;

  let context: JazzContextWithAccount<InstanceOfSchema<S>>;

  const authSecretStorage = options.authSecretStorage;

  await authSecretStorage.migrate();

  const credentials = options.credentials ?? (await authSecretStorage.get());

  if (options.storage) {
    options.storage.enableDeletedCoValuesErasure();
  }

  if (credentials && !options.newAccountProps) {
    context = await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: credentials.accountID,
        secret: credentials.accountSecret,
      },
      peers: options.peers,
      syncWhen: options.syncWhen,
      crypto,
      AccountSchema: options.AccountSchema,
      sessionProvider: options.sessionProvider,
      onLogOut: async () => {
        await authSecretStorage.clearWithoutNotify();
      },
      storage: options.storage,
      asActiveAccount: true,
    });
  } else {
    const secretSeed = options.crypto.newRandomSecretSeed();

    const initialAgentSecret =
      options.newAccountProps?.secret ??
      crypto.agentSecretFromSecretSeed(secretSeed);

    const creationProps = options.newAccountProps?.creationProps ?? {
      name: options.defaultProfileName ?? "Anonymous user",
    };

    context = await createJazzContextForNewAccount({
      creationProps,
      initialAgentSecret,
      peers: options.peers,
      syncWhen: options.syncWhen,
      crypto,
      AccountSchema: options.AccountSchema,
      sessionProvider: options.sessionProvider,
      onLogOut: async () => {
        await authSecretStorage.clearWithoutNotify();
      },
      storage: options.storage,
    });

    if (!options.newAccountProps) {
      await authSecretStorage.setWithoutNotify({
        accountID: context.account.$jazz.id,
        secretSeed,
        accountSecret: context.node.getCurrentAgent().agentSecret,
        provider: "anonymous",
      });
    }
  }

  return {
    ...context,
    authSecretStorage,
  };
}

export function createAnonymousJazzContext({
  peers,
  syncWhen,
  crypto,
  storage,
}: {
  peers: Peer[];
  syncWhen?: SyncWhen;
  crypto: CryptoProvider;
  storage?: StorageAPI;
}): JazzContextWithAgent {
  const agentSecret = crypto.newRandomAgentSecret();

  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(crypto.getAgentID(agentSecret)),
    crypto,
    syncWhen,
    !!storage,
  );

  for (const peer of peers) {
    node.syncManager.addPeer(peer);
  }

  if (storage) {
    node.setStorage(storage);
  }

  activeAccountContext.setGuestMode();

  return {
    agent: new AnonymousJazzAgent(node),
    done: () => {},
    logOut: async () => {},
  };
}
