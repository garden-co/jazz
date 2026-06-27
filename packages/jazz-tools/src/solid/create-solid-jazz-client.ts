import { type Accessor } from "solid-js";
import type { AuthState } from "../runtime/auth-state.js";
import type { Session } from "../runtime/context.js";
import type { Db, DbConfig } from "../runtime/db.js";
import { createExtensionJazzClient, createJazzClient } from "../web/create-jazz-client.js";
import {
  JazzClientFactory,
  createSolidJazzClientInternal,
} from "./create-solid-jazz-client-internal.js";
import { createSolidJazzClientStore } from "./solid-jazz-client-store.js";
import { getSubscriptionStore, subscriptionStoreKey } from "../subscription-store-internal.js";

const makeCreateSolidJazzClient =
  ({ factory }: { factory: JazzClientFactory }) =>
  (config: Accessor<DbConfig>): PendingSolidJazzClient => {
    const internal = createSolidJazzClientInternal(config, factory);
    const stateStore = createSolidJazzClientStore(() => internal.client);

    const client: PendingSolidJazzClient = {
      get db() {
        return internal.client?.db;
      },
      get session() {
        return stateStore.session;
      },
      get authState() {
        return stateStore.authState;
      },
      shutdown: () => internal.client?.shutdown() ?? Promise.resolve(),

      get loading() {
        return internal.loading;
      },
      get error() {
        return internal.error;
      },
      get state() {
        return internal.state;
      },
    };

    Object.defineProperty(client, subscriptionStoreKey, {
      configurable: false,
      enumerable: false,
      get() {
        if (!internal.client) {
          throw new Error("Jazz client is not ready yet.");
        }
        return getSubscriptionStore(internal.client);
      },
    });

    return client;
  };

export const createSolidJazzClient = makeCreateSolidJazzClient({
  factory: createJazzClient,
});
export const createSolidExtensionJazzClient = makeCreateSolidJazzClient({
  factory: createExtensionJazzClient,
});

type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};

export type PendingSolidJazzClient = {
  readonly db: Db | undefined;
  readonly session: Session | null;
  readonly authState: AuthState | null;
  shutdown(): Promise<void>;
  readonly loading: boolean;
  readonly error: unknown;
  readonly state: unknown;
};

export type SolidJazzClient = Prettify<
  PendingSolidJazzClient & {
    db: Db;
  }
>;

export function isPendingSolidJazzClientReady(
  client: PendingSolidJazzClient,
): client is SolidJazzClient {
  return client.db !== undefined;
}
