import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  createAnonymousJazzContext,
  InMemoryKVStore,
  InstanceOfSchema,
  JazzContextManager,
  JazzContextType,
  SyncConfig,
} from "jazz-tools";
import { JazzContextManagerAuthProps } from "jazz-tools";
import { LocalStorageKVStore } from "./auth/LocalStorageKVStore.js";
import {
  BaseBrowserContextOptions,
  createJazzBrowserContext,
  createJazzBrowserGuestContext,
} from "./createBrowserContext.js";
import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto";

export type JazzContextManagerProps<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  guestMode?: boolean;
  sync: SyncConfig;
  onLogOut?: () => void;
  logOutReplacement?: () => void;
  onAnonymousAccountDiscarded?: (
    anonymousAccount: InstanceOfSchema<S>,
  ) => Promise<void>;
  storage?: BaseBrowserContextOptions["storage"];
  AccountSchema?: S;
  defaultProfileName?: string;
};

function isSSR() {
  return typeof window === "undefined";
}

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

export class JazzBrowserContextManager<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> extends JazzContextManager<InstanceOfSchema<S>, JazzContextManagerProps<S>> {
  // TODO: When the storage changes, if the user is changed, update the context
  getKvStore() {
    if (isSSR()) {
      // To handle running in SSR
      return new InMemoryKVStore();
    } else {
      return new LocalStorageKVStore();
    }
  }

  async createContext(
    props: JazzContextManagerProps<S>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    if (isSSR()) {
      this.updateContext(props, getAnonymousFallback(), authProps);

      this.contextPromise = Promise.resolve(this.value);
      this.contextPromise.status = "fulfilled";
      this.contextPromise.value = this.value;

      this.notify();
      return;
    }

    return super.createContext(props, authProps);
  }

  getNewContext(
    props: JazzContextManagerProps<S>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    if (isSSR()) {
      return getAnonymousFallback();
    }

    if (props.guestMode) {
      return createJazzBrowserGuestContext({
        sync: props.sync,
        storage: props.storage,
        authSecretStorage: this.authSecretStorage,
      });
    } else {
      return createJazzBrowserContext<S>({
        sync: props.sync,
        storage: props.storage,
        AccountSchema: props.AccountSchema,
        credentials: authProps?.credentials,
        newAccountProps: authProps?.newAccountProps,
        defaultProfileName: props.defaultProfileName,
        authSecretStorage: this.authSecretStorage,
      });
    }
  }

  propsChanged(props: JazzContextManagerProps<S>) {
    if (!this.props) {
      return true;
    }

    return (
      this.props.sync.when !== props.sync.when ||
      this.props.sync.peer !== props.sync.peer ||
      this.props.guestMode !== props.guestMode
    );
  }
}
