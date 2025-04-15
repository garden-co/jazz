import {
  Account,
  AccountClass,
  InMemoryKVStore,
  JazzContextManager,
  SyncConfig,
} from "jazz-tools";
import { JazzContextManagerAuthProps } from "jazz-tools";
import { LocalStorageKVStore } from "./auth/LocalStorageKVStore.js";
import {
  BaseBrowserContextOptions,
  createJazzBrowserContext,
  createJazzBrowserGuestContext,
} from "./createBrowserContext.js";

export type JazzContextManagerProps<Acc extends Account> = {
  guestMode?: boolean;
  sync: SyncConfig;
  onLogOut?: () => void;
  logOutReplacement?: () => void;
  onAnonymousAccountDiscarded?: (anonymousAccount: Acc) => Promise<void>;
  storage?: BaseBrowserContextOptions["storage"];
  AccountSchema?: AccountClass<Acc>;
  defaultProfileName?: string;
};

export class JazzBrowserContextManager<
  Acc extends Account,
> extends JazzContextManager<Acc, JazzContextManagerProps<Acc>> {
  // TODO: When the storage changes, if the user is changed, update the context
  getKvStore() {
    if (typeof window === "undefined") {
      // To handle running in SSR
      return new InMemoryKVStore();
    } else {
      return new LocalStorageKVStore();
    }
  }

  async getNewContext(
    props: JazzContextManagerProps<Acc>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    if (props.guestMode) {
      return createJazzBrowserGuestContext({
        sync: props.sync,
        storage: props.storage,
        authSecretStorage: this.authSecretStorage,
      });
    } else {
      return createJazzBrowserContext<Acc>({
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
