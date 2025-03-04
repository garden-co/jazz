import { AgentSecret, LocalNode, cojsonInternals } from "cojson";
import { AuthSecretStorage } from "../auth/AuthSecretStorage.js";
import { InMemoryKVStore } from "../auth/InMemoryKVStore.js";
import { KvStore, KvStoreContext } from "../auth/KvStoreContext.js";
import { Account } from "../coValues/account.js";
import { AuthCredentials } from "../types.js";
import { JazzContextType } from "../types.js";
import { AnonymousJazzAgent } from "./anonymousJazzAgent.js";

export type JazzContextManagerAuthProps = {
  credentials?: AuthCredentials;
  newAccountProps?: { secret: AgentSecret; creationProps: { name: string } };
};

export type JazzContextManagerBaseProps<Acc extends Account> = {
  onAnonymousAccountDiscarded?: (anonymousAccount: Acc) => Promise<void>;
  onLogOut?: () => void | Promise<unknown>;
};

type PlatformSpecificAuthContext<Acc extends Account> = {
  me: Acc;
  node: LocalNode;
  logOut: () => Promise<void>;
  done: () => void;
};

type PlatformSpecificGuestContext = {
  guest: AnonymousJazzAgent;
  node: LocalNode;
  logOut: () => Promise<void>;
  done: () => void;
};

type PlatformSpecificContext<Acc extends Account> =
  | PlatformSpecificAuthContext<Acc>
  | PlatformSpecificGuestContext;

export class JazzContextManager<
  Acc extends Account,
  P extends JazzContextManagerBaseProps<Acc>,
> {
  protected value: JazzContextType<Acc> | undefined;
  protected context: PlatformSpecificContext<Acc> | undefined;
  protected props: P | undefined;
  protected authSecretStorage = new AuthSecretStorage();
  protected authSecretStorageWithNotify = Object.assign(
    Object.create(this.authSecretStorage),
    { notify: true },
  );
  protected authenticating = false;

  constructor() {
    KvStoreContext.getInstance().initialize(this.getKvStore());
  }

  getKvStore(): KvStore {
    return new InMemoryKVStore();
  }

  async createContext(props: P, authProps?: JazzContextManagerAuthProps) {
    props;
    authProps;
    throw new Error("Not implemented");
  }

  async updateContext(
    props: P,
    context: PlatformSpecificContext<Acc>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    // When authenticating we don't want to close the previous context
    // because we might need to handle the onAnonymousAccountDiscarded callback
    if (!this.authenticating) {
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
    };

    if (authProps?.credentials) {
      this.authSecretStorage.emitUpdate(authProps.credentials);
    } else {
      this.authSecretStorage.emitUpdate(await this.authSecretStorage.get());
    }

    this.notify();

    if (this.context && "me" in this.context) {
      await this.context.me.onAuthChange(
        this.authSecretStorage.isAuthenticated,
      );
    }
  }

  propsChanged(props: P) {
    props;
    throw new Error("Not implemented");
  }

  getCurrentValue() {
    return this.value;
  }

  getAuthSecretStorage() {
    // External updates of the auth secret storage are notified by default (e.g. when registering a new Auth provider)
    // We skip internal notify to ensure that the isAuthenticated changes are notified along with the context updates
    return this.authSecretStorageWithNotify;
  }

  logOut = async () => {
    if (!this.context || !this.props) {
      return;
    }

    await this.props.onLogOut?.();
    await this.context.logOut();
    return this.createContext(this.props);
  };

  done = () => {
    if (!this.context) {
      return;
    }

    this.context.done();
  };

  /**
   * Authenticates the user with the given credentials
   */
  authenticate = async (credentials: AuthCredentials) => {
    if (!this.props) {
      throw new Error("Props required");
    }

    const prevContext = this.context;
    const prevCredentials = await this.authSecretStorage.get();
    const wasAnonymous =
      this.authSecretStorage.getIsAuthenticated(prevCredentials) === false;

    this.authenticating = true;
    await this.createContext(this.props, { credentials }).finally(() => {
      this.authenticating = false;
    });

    if (wasAnonymous) {
      await this.handleAnonymousAccountMigration(prevContext);
    }
  };

  register = async (
    accountSecret: AgentSecret,
    creationProps: { name: string },
  ) => {
    if (!this.props) {
      throw new Error("Props required");
    }

    const prevContext = this.context;
    const prevCredentials = await this.authSecretStorage.get();
    const wasAnonymous =
      this.authSecretStorage.getIsAuthenticated(prevCredentials) === false;

    this.authenticating = true;
    await this.createContext(this.props, {
      newAccountProps: {
        secret: accountSecret,
        creationProps,
      },
    }).finally(() => {
      this.authenticating = false;
    });

    if (wasAnonymous) {
      await this.handleAnonymousAccountMigration(prevContext);
    }

    if (this.context && "me" in this.context) {
      return this.context.me.id;
    }

    throw new Error("The registration hasn't created a new account");
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
          prevContext.me.id,
          currentContext.me.id,
          {
            peer1role: "client",
            peer2role: "server",
          },
        );

      prevContext.node.syncManager.addPeer(currentAccountAsPeer);
      currentContext.node.syncManager.addPeer(prevAccountAsPeer);

      try {
        await this.props.onAnonymousAccountDiscarded?.(prevContext.me);
        await prevContext.me.waitForAllCoValuesSync();
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
