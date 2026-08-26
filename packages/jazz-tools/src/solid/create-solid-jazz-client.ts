import { type Accessor } from "solid-js";
import type { AuthState } from "../runtime/auth-state.js";
import type { PublicSession } from "../runtime/context.js";
import type { Db, DbConfig } from "../runtime/db.js";
import { createJazzClient } from "../web/create-jazz-client.js";
import { createSolidJazzClientInternal } from "./create-solid-jazz-client-internal.js";
import { createSolidJazzClientStore } from "./solid-jazz-client-store.js";
import { getSubscriptionStore, subscriptionStoreKey } from "../subscription-store-internal.js";

export function createSolidJazzClient(config: Accessor<DbConfig>): PendingSolidJazzClient {
  const internal = createSolidJazzClientInternal(config, createJazzClient);
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
}

type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};

export type PendingSolidJazzClient = {
  readonly db: Db | undefined;
  readonly session: PublicSession | null;
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
