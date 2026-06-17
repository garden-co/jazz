import { type Accessor } from "solid-js";
import type { Db, DbConfig } from "../runtime/db.js";
import { createExtensionJazzClient, createJazzClient } from "../web/create-jazz-client.js";
import {
  JazzClientFactory,
  createSolidJazzClientInternal,
} from "./create-solid-jazz-client-internal.js";
import { createSolidJazzClientStore } from "./solid-jazz-client-store.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

const makeCreateSolidJazzClient =
  ({ factory }: { factory: JazzClientFactory }) =>
  (config: Accessor<DbConfig>) => {
    const internal = createSolidJazzClientInternal(config, factory);
    const stateStore = createSolidJazzClientStore(() => internal.client);

    return {
      get db() {
        return internal.client?.db;
      },
      get session() {
        return stateStore.session;
      },
      get authState() {
        return stateStore.authState;
      },
      get manager() {
        return internal.client?.manager;
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

export type PendingSolidJazzClient = ReturnType<typeof createSolidJazzClient>;
export type SolidJazzClient = Prettify<
  PendingSolidJazzClient & {
    db: Db;
    manager: SubscriptionsOrchestrator;
  }
>;

export function isPendingSolidJazzClientReady(
  client: PendingSolidJazzClient,
): client is SolidJazzClient {
  return client.db !== undefined && client.manager !== undefined;
}
