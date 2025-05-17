import {
  Account,
  AccountClass,
  CoValueFromRaw,
  JazzContextManager,
  KvStore,
  SyncConfig,
} from "jazz-tools";
import type { JazzContextManagerAuthProps } from "jazz-tools";
import {
  BaseReactNativeContextOptions,
  createJazzReactNativeContext,
  createJazzReactNativeGuestContext,
} from "./platform.js";
import { KvStoreContext } from "./storage/kv-store-context.js";

export type JazzContextManagerProps<Acc extends Account> = {
  guestMode?: boolean;
  sync: SyncConfig;
  onLogOut?: () => void;
  logOutReplacement?: () => void;
  storage?: BaseReactNativeContextOptions["storage"];
  AccountSchema?: AccountClass<Acc> & CoValueFromRaw<Acc>;
  defaultProfileName?: string;
  onAnonymousAccountDiscarded?: (anonymousAccount: Acc) => Promise<void>;
  CryptoProvider?: BaseReactNativeContextOptions["CryptoProvider"];
};

export class ReactNativeContextManager<
  Acc extends Account,
> extends JazzContextManager<Acc, JazzContextManagerProps<Acc>> {
  async getNewContext(
    props: JazzContextManagerProps<Acc>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    if (props.guestMode) {
      return createJazzReactNativeGuestContext({
        sync: props.sync,
        storage: props.storage,
        authSecretStorage: this.authSecretStorage,
        CryptoProvider: props.CryptoProvider,
      });
    } else {
      return createJazzReactNativeContext<
        AccountClass<Acc> & CoValueFromRaw<Acc>
      >({
        sync: props.sync,
        storage: props.storage,
        AccountSchema: props.AccountSchema,
        credentials: authProps?.credentials,
        newAccountProps: authProps?.newAccountProps,
        defaultProfileName: props.defaultProfileName,
        authSecretStorage: this.authSecretStorage,
        CryptoProvider: props.CryptoProvider,
      });
    }
  }

  getKvStore(): KvStore {
    return KvStoreContext.getInstance().getStorage();
  }

  propsChanged(props: JazzContextManagerProps<Acc>) {
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
