import {
  Account,
  AccountClass,
  CoValueFromRaw,
  JazzContextManager,
  KvStore,
  SyncConfig,
} from "jazz-tools";
import type {
  AnyAccountSchema,
  InstanceOfSchema,
  JazzContextManagerAuthProps,
} from "jazz-tools";
import {
  BaseReactNativeContextOptions,
  createJazzReactNativeContext,
  createJazzReactNativeGuestContext,
} from "./platform.js";
import { KvStoreContext } from "./storage/kv-store-context.js";

export type JazzContextManagerProps<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  guestMode?: boolean;
  sync: SyncConfig;
  onLogOut?: () => void;
  logOutReplacement?: () => void;
  storage?: BaseReactNativeContextOptions["storage"];
  AccountSchema?: S;
  defaultProfileName?: string;
  onAnonymousAccountDiscarded?: (
    anonymousAccount: InstanceOfSchema<S>,
  ) => Promise<void>;
};

export class ReactNativeContextManager<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> extends JazzContextManager<InstanceOfSchema<S>, JazzContextManagerProps<S>> {
  async getNewContext(
    props: JazzContextManagerProps<S>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    if (props.guestMode) {
      return createJazzReactNativeGuestContext({
        sync: props.sync,
        storage: props.storage,
        authSecretStorage: this.authSecretStorage,
      });
    } else {
      return createJazzReactNativeContext<S>({
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

  getKvStore(): KvStore {
    return KvStoreContext.getInstance().getStorage();
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
