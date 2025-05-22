import {
  AgentSecret,
  CoID,
  ControlledAgent,
  CryptoProvider,
  LocalNode,
  Peer,
  RawAccount,
  RawAccountID,
  SessionID,
} from "cojson";
import { AuthSecretStorage } from "../auth/AuthSecretStorage.js";
import { type Account, type AccountClass } from "../coValues/account.js";
import { RegisteredSchemas } from "../coValues/registeredSchemas.js";
import {
  type AccountSchema,
  type AnyAccountSchema,
  CoValueFromRaw,
  type ID,
  type InstanceOfSchema,
  anySchemaToCoSchema,
} from "../internal.js";
import { AuthCredentials, NewAccountProps } from "../types.js";
import { activeAccountContext } from "./activeAccountContext.js";
import { AnonymousJazzAgent } from "./anonymousJazzAgent.js";

export type Credentials = {
  accountID: ID<Account>;
  secret: AgentSecret;
};

type SessionProvider = (
  accountID: ID<Account>,
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
  accountID: ID<Account>,
  crypto: CryptoProvider,
) {
  return {
    sessionID: crypto.newRandomSessionID(accountID as unknown as RawAccountID),
    sessionDone: () => {},
  };
}

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
    | AnyAccountSchema,
>({
  credentials,
  peersToLoadFrom,
  crypto,
  AccountSchema: PropsAccountSchema,
  sessionProvider,
  onLogOut,
}: {
  credentials: Credentials;
  peersToLoadFrom: Peer[];
  crypto: CryptoProvider;
  AccountSchema?: S;
  sessionProvider: SessionProvider;
  onLogOut?: () => void;
}): Promise<JazzContextWithAccount<InstanceOfSchema<S>>> {
  const { sessionID, sessionDone } = await sessionProvider(
    credentials.accountID,
    crypto,
  );

  const CurrentAccountSchema =
    PropsAccountSchema ?? (RegisteredSchemas["Account"] as unknown as S);

  const AccountClass = anySchemaToCoSchema(CurrentAccountSchema);

  const node = await LocalNode.withLoadedAccount({
    accountID: credentials.accountID as unknown as CoID<RawAccount>,
    accountSecret: credentials.secret,
    sessionID: sessionID,
    peersToLoadFrom: peersToLoadFrom,
    crypto: crypto,
    migration: async (rawAccount, _node, creationProps) => {
      const account = AccountClass.fromRaw(rawAccount) as InstanceOfSchema<S>;
      activeAccountContext.set(account);

      await account.applyMigration(creationProps);
    },
  });

  const account = AccountClass.fromNode(node);
  activeAccountContext.set(account);

  return {
    node,
    account: account as InstanceOfSchema<S>,
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
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>({
  creationProps,
  initialAgentSecret,
  peersToLoadFrom,
  crypto,
  AccountSchema: PropsAccountSchema,
  onLogOut,
}: {
  creationProps: { name: string };
  initialAgentSecret?: AgentSecret;
  peersToLoadFrom: Peer[];
  crypto: CryptoProvider;
  AccountSchema?: S;
  onLogOut?: () => Promise<void>;
}): Promise<JazzContextWithAccount<InstanceOfSchema<S>>> {
  const CurrentAccountSchema =
    PropsAccountSchema ?? (RegisteredSchemas["Account"] as unknown as S);

  const AccountClass = anySchemaToCoSchema(CurrentAccountSchema);

  const { node } = await LocalNode.withNewlyCreatedAccount({
    creationProps,
    peersToLoadFrom,
    crypto,
    initialAgentSecret,
    migration: async (rawAccount, _node, creationProps) => {
      const account = AccountClass.fromRaw(rawAccount) as InstanceOfSchema<S>;
      activeAccountContext.set(account);

      await account.applyMigration(creationProps);
    },
  });

  const account = AccountClass.fromNode(node);
  activeAccountContext.set(account);

  return {
    node,
    account: account as InstanceOfSchema<S>,
    done: () => {
      node.gracefulShutdown();
    },
    logOut: async () => {
      node.gracefulShutdown();
      await onLogOut?.();
    },
  };
}

export async function createJazzContext<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(options: {
  credentials?: AuthCredentials;
  newAccountProps?: NewAccountProps;
  peersToLoadFrom: Peer[];
  crypto: CryptoProvider;
  defaultProfileName?: string;
  AccountSchema?: S;
  sessionProvider: SessionProvider;
  authSecretStorage: AuthSecretStorage;
}) {
  const crypto = options.crypto;

  let context: JazzContextWithAccount<InstanceOfSchema<S>>;

  const authSecretStorage = options.authSecretStorage;

  await authSecretStorage.migrate();

  const credentials = options.credentials ?? (await authSecretStorage.get());

  if (credentials && !options.newAccountProps) {
    context = await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: credentials.accountID,
        secret: credentials.accountSecret,
      },
      peersToLoadFrom: options.peersToLoadFrom,
      crypto,
      AccountSchema: options.AccountSchema,
      sessionProvider: options.sessionProvider,
      onLogOut: () => {
        authSecretStorage.clearWithoutNotify();
      },
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
      peersToLoadFrom: options.peersToLoadFrom,
      crypto,
      AccountSchema: options.AccountSchema,
      onLogOut: async () => {
        await authSecretStorage.clearWithoutNotify();
      },
    });

    if (!options.newAccountProps) {
      await authSecretStorage.setWithoutNotify({
        accountID: context.account.id,
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
  peersToLoadFrom,
  crypto,
}: {
  peersToLoadFrom: Peer[];
  crypto: CryptoProvider;
}): JazzContextWithAgent {
  const agentSecret = crypto.newRandomAgentSecret();

  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(crypto.getAgentID(agentSecret)),
    crypto,
  );

  for (const peer of peersToLoadFrom) {
    node.syncManager.addPeer(peer);
  }

  activeAccountContext.setGuestMode();

  return {
    agent: new AnonymousJazzAgent(node),
    done: () => {},
    logOut: async () => {},
  };
}
