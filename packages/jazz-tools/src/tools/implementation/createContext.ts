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
import { type Account } from "../coValues/account.js";
import { RegisteredSchemas } from "../coValues/registeredSchemas.js";
import {
  AccountSchema,
  CoValueFromRaw,
  type CoreAccountSchema,
  type ID,
  asConstructable,
  Loaded,
} from "../internal.js";
import { AuthCredentials, NewAccountProps } from "../types.js";
import { activeAccountContext } from "./activeAccountContext.js";
import { AnonymousJazzAgent } from "./anonymousJazzAgent.js";

export type Credentials = {
  accountID: ID<CoreAccountSchema>;
  secret: AgentSecret;
};

type SessionProvider = (
  accountID: ID<CoreAccountSchema>,
  crypto: CryptoProvider,
) => Promise<{ sessionID: SessionID; sessionDone: () => void }>;

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

export async function randomSessionProvider(
  accountID: ID<CoreAccountSchema>,
  crypto: CryptoProvider,
) {
  return {
    sessionID: crypto.newRandomSessionID(accountID as unknown as RawAccountID),
    sessionDone: () => {},
  };
}

export type JazzContextWithAccount<Acc extends CoreAccountSchema> = {
  node: LocalNode;
  account: Loaded<Acc>;
  done: () => void;
  logOut: () => Promise<void>;
};

export type JazzContextWithAgent = {
  agent: AnonymousJazzAgent;
  done: () => void;
  logOut: () => Promise<void>;
};

export type JazzContext<Acc extends CoreAccountSchema> =
  | JazzContextWithAccount<Acc>
  | JazzContextWithAgent;

export async function createJazzContextFromExistingCredentials<
  S extends CoreAccountSchema,
>({
  credentials,
  peers,
  crypto,
  storage,
  AccountSchema: PropsAccountSchema,
  sessionProvider,
  onLogOut,
  asActiveAccount,
}: {
  credentials: Credentials;
  peers: Peer[];
  crypto: CryptoProvider;
  AccountSchema?: S;
  sessionProvider: SessionProvider;
  onLogOut?: () => void;
  storage?: StorageAPI;
  asActiveAccount: boolean;
}): Promise<JazzContextWithAccount<S>> {
  const { sessionID, sessionDone } = await sessionProvider(
    credentials.accountID,
    crypto,
  );

  const CurrentAccountSchema =
    PropsAccountSchema ?? (RegisteredSchemas["Account"] as unknown as S);

  const node = await LocalNode.withLoadedAccount({
    accountID: credentials.accountID as unknown as CoID<RawAccount>,
    accountSecret: credentials.secret,
    sessionID: sessionID,
    peers: peers,
    crypto: crypto,
    storage,
    migration: async (rawAccount, _node, creationProps) => {
      const account = asConstructable(CurrentAccountSchema).fromRaw(rawAccount);
      if (asActiveAccount) {
        activeAccountContext.set(account);
      }

      await account.applyMigration(creationProps);
    },
  });

  const account = asConstructable(CurrentAccountSchema).fromNode(node);
  if (asActiveAccount) {
    activeAccountContext.set(account);
  }

  return {
    node,
    account: account as Loaded<S>,
    done: () => {
      node.gracefulShutdown();
      sessionDone();
    },
    logOut: async () => {
      node.gracefulShutdown();
      sessionDone();
      await onLogOut?.();
    },
  };
}

export async function createJazzContextForNewAccount<
  S extends CoreAccountSchema,
>({
  creationProps,
  initialAgentSecret,
  peers,
  crypto,
  AccountSchema: PropsAccountSchema,
  onLogOut,
  storage,
}: {
  creationProps: { name: string };
  initialAgentSecret?: AgentSecret;
  peers: Peer[];
  crypto: CryptoProvider;
  AccountSchema?: S;
  onLogOut?: () => Promise<void>;
  storage?: StorageAPI;
}): Promise<JazzContextWithAccount<S>> {
  const CurrentAccountSchema =
    PropsAccountSchema ?? (RegisteredSchemas["Account"] as unknown as S);

  const { node } = await LocalNode.withNewlyCreatedAccount({
    creationProps,
    peers,
    crypto,
    initialAgentSecret,
    storage,
    migration: async (rawAccount, _node, creationProps) => {
      const account = asConstructable(CurrentAccountSchema).fromRaw(rawAccount);
      activeAccountContext.set(account);

      await account.applyMigration(creationProps);
    },
  });

  const account = asConstructable(CurrentAccountSchema).fromNode(node);
  activeAccountContext.set(account);

  return {
    node,
    account: account as Loaded<S>,
    done: () => {
      node.gracefulShutdown();
    },
    logOut: async () => {
      node.gracefulShutdown();
      await onLogOut?.();
    },
  };
}

export async function createJazzContext<S extends CoreAccountSchema>(options: {
  credentials?: AuthCredentials;
  newAccountProps?: NewAccountProps;
  peers: Peer[];
  crypto: CryptoProvider;
  defaultProfileName?: string;
  AccountSchema?: S;
  sessionProvider: SessionProvider;
  authSecretStorage: AuthSecretStorage;
  storage?: StorageAPI;
}): Promise<
  JazzContextWithAccount<S> & { authSecretStorage: AuthSecretStorage }
> {
  const crypto = options.crypto;

  let context: JazzContextWithAccount<S>;

  const authSecretStorage = options.authSecretStorage;

  await authSecretStorage.migrate();

  const credentials = options.credentials ?? (await authSecretStorage.get());

  if (credentials && !options.newAccountProps) {
    context = await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: credentials.accountID,
        secret: credentials.accountSecret,
      },
      peers: options.peers,
      crypto,
      AccountSchema: options.AccountSchema,
      sessionProvider: options.sessionProvider,
      onLogOut: () => {
        authSecretStorage.clearWithoutNotify();
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
      crypto,
      AccountSchema: options.AccountSchema,
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
  crypto,
  storage,
}: {
  peers: Peer[];
  crypto: CryptoProvider;
  storage?: StorageAPI;
}): JazzContextWithAgent {
  const agentSecret = crypto.newRandomAgentSecret();

  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(crypto.getAgentID(agentSecret)),
    crypto,
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
