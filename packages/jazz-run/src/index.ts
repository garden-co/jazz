#!/usr/bin/env node
/* istanbul ignore file -- @preserve */
import { Command, Options } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect } from "effect";
import { createWorkerAccount } from "./createWorkerAccount.js";
import { startSyncServer } from "./startSyncServer.js";
import { serverDefaults } from "./config.js";
import { startWorker } from "jazz-tools/worker";
import { RegistryState, WebhookRegistry } from "jazz-webhook";
import { co } from "jazz-tools";

const jazzTools = Command.make("jazz-tools");

const nameOption = Options.text("name").pipe(Options.withAlias("n"));
const jsonOption = Options.boolean("json");
const peerOption = Options.text("peer")
  .pipe(Options.withAlias("p"))
  .pipe(Options.withDefault("wss://cloud.jazz.tools"));

const createAccountCommand = Command.make(
  "create",
  { name: nameOption, peer: peerOption, json: jsonOption },
  ({ name, peer, json }) => {
    return Effect.gen(function* () {
      const { accountID, agentSecret } = yield* Effect.promise(() =>
        createWorkerAccount({ name, peer }),
      );

      if (json) {
        yield* Console.log(JSON.stringify({ accountID, agentSecret }));
      } else {
        yield* Console.log(`
# Credentials for Jazz account "${name}":
JAZZ_WORKER_ACCOUNT=${accountID}
JAZZ_WORKER_SECRET=${agentSecret}
`);
      }
    });
  },
);

const accountCommand = Command.make("account").pipe(
  Command.withSubcommands([createAccountCommand]),
);

const hostOption = Options.text("host")
  .pipe(Options.withAlias("h"))
  .pipe(
    Options.withDescription(
      `The host to listen on. Default is ${serverDefaults.host}`,
    ),
  )
  .pipe(Options.withDefault(serverDefaults.host));

const portOption = Options.text("port")
  .pipe(Options.withAlias("p"))
  .pipe(
    Options.withDescription(
      `Select a different port for the WebSocket server. Default is ${serverDefaults.port}`,
    ),
  )
  .pipe(Options.withDefault(serverDefaults.port.toString()));

const inMemoryOption = Options.boolean("in-memory").pipe(
  Options.withDescription("Use an in-memory storage instead of file-based."),
);

const dbOption = Options.file("db")
  .pipe(
    Options.withDescription(
      `The path to the file where to store the data. Default is '${serverDefaults.db}'`,
    ),
  )
  .pipe(Options.withDefault(serverDefaults.db));

const enableFullStorageReconciliationOption = Options.boolean(
  "full-storage-reconciliation",
).pipe(
  Options.withDescription(
    "Enable full storage reconciliation on the server's node",
  ),
);

const startSyncServerCommand = Command.make(
  "sync",
  {
    host: hostOption,
    port: portOption,
    inMemory: inMemoryOption,
    db: dbOption,
    enableFullStorageReconciliation: enableFullStorageReconciliationOption,
  },
  ({ host, port, inMemory, db, enableFullStorageReconciliation }) => {
    return Effect.gen(function* () {
      const server = yield* Effect.promise(() =>
        startSyncServer({
          host,
          port,
          inMemory,
          db,
          enableFullStorageReconciliation,
        }),
      );

      const serverAddress = server.address();

      if (!serverAddress) {
        return yield* Effect.fail(new Error("Failed to start sync server."));
      }

      const socketAddress =
        typeof serverAddress === "object"
          ? `${serverAddress.address}:${serverAddress.port}`
          : serverAddress;

      yield* Console.log(
        `COJSON sync server listening on ws://${socketAddress}`,
      );

      // Keep the server up
      yield* Effect.never;
    });
  },
);

const webhookRunCommand = Command.make(
  "run",
  {
    peer: peerOption,
  },
  ({ peer }) => {
    return Effect.gen(function* () {
      const webhookRegistrySecret = process.env.JAZZ_WEBHOOK_REGISTRY_SECRET;

      if (!webhookRegistrySecret) {
        throw new Error("JAZZ_WEBHOOK_REGISTRY_SECRET is not set");
      }

      const [registryID, registryAccountID, registryAccountSecret] =
        webhookRegistrySecret.split("__");

      if (!registryID || !registryAccountID || !registryAccountSecret) {
        throw new Error("Invalid JAZZ_WEBHOOK_REGISTRY_SECRET");
      }

      const { worker, shutdownWorker } = yield* Effect.promise(() =>
        startWorker({
          syncServer: peer,
          accountID: registryAccountID,
          accountSecret: registryAccountSecret,
        }),
      );

      const webhook = yield* Effect.promise(() =>
        WebhookRegistry.loadAndStart(registryID),
      );

      yield* Effect.addFinalizer(() => Effect.sync(() => webhook.shutdown()));
      yield* Effect.addFinalizer(() => Effect.promise(() => shutdownWorker()));

      yield* Console.log(
        `Webhook registry ${registryID} running as ${registryAccountID}`,
      );

      // Wait until interrupt
      yield* Effect.never;
    }).pipe(Effect.scoped);
  },
);

const grantRegistrationRights = ({
  peer,
  accountID,
  webhookRegistrySecret,
}: {
  peer: string;
  accountID: string;
  webhookRegistrySecret?: string;
}) => {
  return Effect.gen(function* () {
    webhookRegistrySecret =
      webhookRegistrySecret || process.env.JAZZ_WEBHOOK_REGISTRY_SECRET;

    if (!webhookRegistrySecret) {
      throw new Error("JAZZ_WEBHOOK_REGISTRY_SECRET is not set");
    }

    const [registryID, registryAccountID, registryAccountSecret] =
      webhookRegistrySecret.split("__");

    if (!registryID || !registryAccountID || !registryAccountSecret) {
      throw new Error("Invalid JAZZ_WEBHOOK_REGISTRY_SECRET");
    }

    const { worker, shutdownWorker } = yield* Effect.promise(() =>
      startWorker({
        syncServer: peer,
        accountID: registryAccountID,
        accountSecret: registryAccountSecret,
      }),
    );

    const registry = yield* Effect.promise(() =>
      RegistryState.load(registryID),
    );

    const account = yield* Effect.promise(() => co.account().load(accountID));

    if (!account.$isLoaded) {
      throw new Error("Account not found");
    }

    if (!registry.$isLoaded) {
      throw new Error("Couldn't load registry with ID " + registryID);
    }

    registry.$jazz.owner.addMember(account, "writer");

    yield* Console.error(
      "Webhook registration rights granted to " +
        accountID +
        " for registry " +
        registryID +
        " ✅",
    );

    yield* Effect.promise(() => shutdownWorker());
  });
};

const accountIDOption = Options.text("accountID").pipe(Options.withAlias("a"));

const webhookGrantCommand = Command.make(
  "grant",
  {
    peer: peerOption,
    accountID: accountIDOption,
  },
  grantRegistrationRights,
);

const webhookRevokeCommand = Command.make(
  "revoke",
  {
    peer: peerOption,
    accountID: accountIDOption,
  },
  ({ peer, accountID }) => {
    return Effect.gen(function* () {
      yield* Console.log("NOT IMPLEMENTED");
    });
  },
);

const grantOption = Options.text("grant")
  .pipe(Options.withAlias("g"))
  .pipe(Options.optional);

const webhookCreateRegistryCommand = Command.make(
  "create-registry",
  {
    peer: peerOption,
    grant: grantOption,
  },
  ({ peer, grant }) => {
    return Effect.gen(function* () {
      const { accountID, agentSecret } = yield* Effect.promise(() =>
        createWorkerAccount({ name: "Webhook Worker", peer }),
      );

      const { worker, shutdownWorker } = yield* Effect.promise(() =>
        startWorker({
          syncServer: peer,
          accountID,
          accountSecret: agentSecret,
        }),
      );

      const registryGroup = co.group().create({ owner: worker });
      const registry = WebhookRegistry.createRegistry(registryGroup);
      const webhookRegistrySecret =
        registry.$jazz.id + "__" + accountID + "__" + agentSecret;

      yield* Console.log(
        "# Credentials for *running* the Jazz webhook registry:",
      );
      yield* Console.log(
        "JAZZ_WEBHOOK_REGISTRY_SECRET=" + webhookRegistrySecret,
      );
      yield* Console.log("# Registry ID for *registering* webhooks:");
      yield* Console.log("JAZZ_WEBHOOK_REGISTRY_ID=" + registry.$jazz.id);

      yield* Effect.promise(() => shutdownWorker());

      if (grant._tag === "Some") {
        yield* grantRegistrationRights({
          peer,
          accountID: grant.value,
          webhookRegistrySecret,
        });
      }
    });
  },
);

const webhookCommand = Command.make("webhook").pipe(
  Command.withSubcommands([
    webhookRunCommand,
    webhookCreateRegistryCommand,
    webhookGrantCommand,
    webhookRevokeCommand,
  ]),
);

const command = jazzTools.pipe(
  Command.withSubcommands([
    accountCommand,
    startSyncServerCommand,
    webhookCommand,
  ]),
);

const cli = Command.run(command, {
  name: "Jazz CLI Tools",
  version: "v0.8.11",
});

Effect.suspend(() => cli(process.argv)).pipe(
  Effect.provide(NodeContext.layer),
  NodeRuntime.runMain,
);
