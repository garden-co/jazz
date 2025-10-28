import { AgentSecret, LocalNode, cojsonInternals } from "cojson";
import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto";
import { AuthSecretStorage } from "../auth/AuthSecretStorage.js";
import { InMemoryKVStore } from "../auth/InMemoryKVStore.js";
import { KvStore, KvStoreContext } from "../auth/KvStoreContext.js";
import { Account } from "../coValues/account.js";
import { AuthCredentials } from "../types.js";
import { JazzContextType } from "../types.js";
import { AnonymousJazzAgent } from "./anonymousJazzAgent.js";
import { createAnonymousJazzContext } from "./createContext.js";
import { InstanceOfSchema } from "./zodSchema/typeConverters/InstanceOfSchema.js";

export type JazzContextManagerAuthProps = {
  credentials?: AuthCredentials;
  newAccountProps?: { secret: AgentSecret; creationProps: { name: string } };
};

export type JazzContextManagerBaseProps<Acc extends Account> = {
  onAnonymousAccountDiscarded?: (anonymousAccount: Acc) => Promise<void>;
  onLogOut?: () => void | Promise<unknown>;
  logOutReplacement?: () => void | Promise<unknown>;
};

type PlatformSpecificAuthContext<Acc extends Account> = {
  me: Acc;
  node: LocalNode;
  logOut: () => Promise<void>;
  done: () => void;
  addConnectionListener: (listener: (connected: boolean) => void) => () => void;
  connected: () => boolean;
};

type PlatformSpecificGuestContext = {
  guest: AnonymousJazzAgent;
  node: LocalNode;
  logOut: () => Promise<void>;
  done: () => void;
  addConnectionListener: (listener: (connected: boolean) => void) => () => void;
  connected: () => boolean;
};

type PlatformSpecificContext<Acc extends Account> =
  | PlatformSpecificAuthContext<Acc>
  | PlatformSpecificGuestContext;

function getAnonymousFallback() {
  const context = createAnonymousJazzContext({
    peers: [],
    crypto: new PureJSCrypto(),
  });

  return {
    guest: context.agent,
    node: context.agent.node,
    done: () => {},
    logOut: async () => {},
    isAuthenticated: false,
    authenticate: async () => {},
    addConnectionListener: () => () => {},
    connected: () => false,
    register: async () => {
      throw new Error("Not implemented");
    },
  } satisfies JazzContextType<InstanceOfSchema<any>>;
}

export class JazzContextManager<
  Acc extends Account,
  P extends JazzContextManagerBaseProps<Acc>,
> {
  protected value: JazzContextType<Acc> | undefined;
  protected context: PlatformSpecificContext<Acc> | undefined;
  protected props: P | undefined;
  protected authSecretStorage;
  protected keepContextOpen = false;
  contextPromise: Promise<void> | undefined;
  protected authenticatingAccountID: string | null = null;

  constructor(opts?: {
    useAnonymousFallback?: boolean;
    authSecretStorageKey?: string;
  }) {
    KvStoreContext.getInstance().initialize(this.getKvStore());
    this.authSecretStorage = new AuthSecretStorage(opts?.authSecretStorageKey);

    if (opts?.useAnonymousFallback) {
      this.value = getAnonymousFallback();
    }
  }

  getKvStore(): KvStore {
    return new InMemoryKVStore();
  }

  async createContext(props: P, authProps?: JazzContextManagerAuthProps) {
    // We need to store the props here to block the double effect execution
    // on React. Otherwise when calling propsChanged this.props is undefined.
    this.props = props;

    // Avoid race condition between the previous context and the new one
    const { promise, resolve } = createResolvablePromise<void>();

    const prevPromise = this.contextPromise;
    this.contextPromise = promise;

    await prevPromise;

    try {
      const result = await this.getNewContext(props, authProps);
      await this.updateContext(props, result, authProps);

      resolve();
    } catch (error) {
      resolve();
      throw error;
    }
  }

  async getNewContext(
    props: P,
    authProps?: JazzContextManagerAuthProps,
  ): Promise<PlatformSpecificContext<Acc>> {
    props;
    authProps;
    throw new Error("Not implemented");
  }

  async updateContext(
    props: P,
    context: PlatformSpecificContext<Acc>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    // When keepContextOpen we don't want to close the previous context
    // because we might need to handle the onAnonymousAccountDiscarded callback
    if (!this.keepContextOpen) {
      this.context?.done();
    }

    this.context = context;
    this.props = props;
    this.value = {
      ...context,
      node: context.node,
      authenticate: this.authenticate,
      register: this.register,
      logOut: this.logOut,
      addConnectionListener: context.addConnectionListener,
      connected: context.connected,
    };

    if (authProps?.credentials) {
      this.authSecretStorage.emitUpdate(authProps.credentials);
    } else {
      this.authSecretStorage.emitUpdate(await this.authSecretStorage.get());
    }

    this.notify();
  }

  propsChanged(props: P) {
    props;
    throw new Error("Not implemented");
  }

  getCurrentValue() {
    return this.value;
  }

  setCurrentValue(value: JazzContextType<Acc>) {
    this.value = value;
  }

  getAuthSecretStorage() {
    return this.authSecretStorage;
  }

  getAuthenticatingAccountID() {
    return this.authenticatingAccountID;
  }

  logOut = async () => {
    if (!this.context || !this.props) {
      return;
    }

    this.authenticatingAccountID = null;

    await this.props.onLogOut?.();

    if (this.props.logOutReplacement) {
      await this.props.logOutReplacement();
    } else {
      await this.context.logOut();
      return this.createContext(this.props);
    }
  };

  done = () => {
    if (!this.context) {
      return;
    }

    this.context.done();
  };

  shouldMigrateAnonymousAccount = async () => {
    if (!this.props?.onAnonymousAccountDiscarded) {
      return false;
    }

    const prevCredentials = await this.authSecretStorage.get();
    const wasAnonymous =
      this.authSecretStorage.getIsAuthenticated(prevCredentials) === false;

    return wasAnonymous;
  };

  /**
   * Authenticates the user with the given credentials
   */
  authenticate = async (credentials: AuthCredentials) => {
    if (!this.props) {
      throw new Error("Props required");
    }

    if (
      this.authenticatingAccountID &&
      this.authenticatingAccountID === credentials.accountID
    ) {
      console.info(
        "Authentication already in progress for account",
        credentials.accountID,
        "skipping duplicate request",
      );
      return;
    }

    if (
      this.authenticatingAccountID &&
      this.authenticatingAccountID !== credentials.accountID
    ) {
      throw new Error(
        `Authentication already in progress for different account (${this.authenticatingAccountID}), cannot authenticate ${credentials.accountID}`,
      );
    }

    this.authenticatingAccountID = credentials.accountID;

    try {
      const prevContext = this.context;
      const migratingAnonymousAccount =
        await this.shouldMigrateAnonymousAccount();

      this.keepContextOpen = migratingAnonymousAccount;
      await this.createContext(this.props, { credentials }).finally(() => {
        this.keepContextOpen = false;
      });

      if (migratingAnonymousAccount) {
        await this.handleAnonymousAccountMigration(prevContext);
      }
    } finally {
      this.authenticatingAccountID = null;
    }
  };

  register = async (
    accountSecret: AgentSecret,
    creationProps: { name: string },
  ) => {
    if (!this.props) {
      throw new Error("Props required");
    }

    if (this.authenticatingAccountID) {
      throw new Error("Authentication already in progress");
    }

    // For registration, we don't know the account ID yet, so we'll set it to "register"
    this.authenticatingAccountID = "register";

    try {
      const prevContext = this.context;
      const migratingAnonymousAccount =
        await this.shouldMigrateAnonymousAccount();

      this.keepContextOpen = migratingAnonymousAccount;
      await this.createContext(this.props, {
        newAccountProps: {
          secret: accountSecret,
          creationProps,
        },
      }).finally(() => {
        this.keepContextOpen = false;
      });

      if (migratingAnonymousAccount) {
        await this.handleAnonymousAccountMigration(prevContext);
      }

      if (this.context && "me" in this.context) {
        return this.context.me.$jazz.id;
      }

      throw new Error("The registration hasn't created a new account");
    } finally {
      this.authenticatingAccountID = null;
    }
  };

  private async handleAnonymousAccountMigration(
    prevContext: PlatformSpecificContext<Acc> | undefined,
  ) {
    if (!this.props) {
      throw new Error("Props required");
    }

    const currentContext = this.context;

    if (
      prevContext &&
      currentContext &&
      "me" in prevContext &&
      "me" in currentContext
    ) {
      // Using a direct connection to make coValue transfer almost synchronous
      const [prevAccountAsPeer, currentAccountAsPeer] =
        cojsonInternals.connectedPeers(
          prevContext.me.$jazz.id,
          currentContext.me.$jazz.id,
          {
            peer1role: "client",
            peer2role: "server",
          },
        );

      // Closing storage on the prevContext to avoid conflicting transactions and getting stuck on waitForAllCoValuesSync
      // The storage is reachable through currentContext using the connectedPeers
      prevContext.node.removeStorage();

      currentContext.node.syncManager.addPeer(prevAccountAsPeer);
      prevContext.node.syncManager.addPeer(currentAccountAsPeer);

      try {
        await this.props.onAnonymousAccountDiscarded?.(prevContext.me);
        await prevContext.me.$jazz.waitForAllCoValuesSync();
      } catch (error) {
        console.error("Error onAnonymousAccountDiscarded", error);
      }

      prevAccountAsPeer.outgoing.close();
      currentAccountAsPeer.outgoing.close();
    }

    prevContext?.done();
  }

  listeners = new Set<() => void>();
  subscribe = (callback: () => void) => {
    this.listeners.add(callback);

    return () => {
      this.listeners.delete(callback);
    };
  };

  notify() {
    for (const listener of this.listeners) {
      listener();
    }
  }
}

function createResolvablePromise<T>() {
  let resolve!: (value: T) => void;

  const promise = new Promise<T>((res) => {
    resolve = res;
  });

  return { promise, resolve };
}
