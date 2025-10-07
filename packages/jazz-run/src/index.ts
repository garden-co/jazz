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

const startSyncServerCommand = Command.make(
  "sync",
  {
    host: hostOption,
    port: portOption,
    inMemory: inMemoryOption,
    db: dbOption,
  },
  ({ host, port, inMemory, db }) => {
    return Effect.gen(function* () {
      const server = yield* Effect.promise(() =>
        startSyncServer({ host, port, inMemory, db }),
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
      const { worker, shutdownWorker } = yield* Effect.promise(() =>
        startWorker({
          syncServer: peer,
          accountID: process.env.JAZZ_WEBHOOK_ACCOUNT,
          accountSecret: process.env.JAZZ_WEBHOOK_ACCOUNT_SECRET,
        }),
      );

      const webhook = yield* Effect.promise(() =>
        WebhookRegistry.loadAndStart(process.env.JAZZ_WEBHOOK_REGISTRY_ID!),
      );

      yield* Effect.addFinalizer(() => Effect.sync(() => webhook.shutdown()));
      yield* Effect.addFinalizer(() => Effect.promise(() => shutdownWorker()));

      yield* Console.log(
        `Webhook registry ${process.env.JAZZ_WEBHOOK_REGISTRY_ID} running as ${process.env.JAZZ_WEBHOOK_ACCOUNT}`,
      );

      // Wait until interrupt
      yield* Effect.never;
    }).pipe(Effect.scoped);
  },
);

const accountIDOption = Options.text("accountID").pipe(Options.withAlias("a"));

const webhookGrantCommand = Command.make(
  "grant",
  {
    peer: peerOption,
    accountID: accountIDOption,
  },
  ({ peer, accountID }) => {
    return Effect.gen(function* () {
      const webhookAccountID = process.env.JAZZ_WEBHOOK_ACCOUNT;

      if (!webhookAccountID) {
        throw new Error("JAZZ_WEBHOOK_ACCOUNT is not set");
      }

      const webhookAccountSecret = process.env.JAZZ_WEBHOOK_ACCOUNT_SECRET;

      if (!webhookAccountSecret) {
        throw new Error("JAZZ_WEBHOOK_ACCOUNT_SECRET is not set");
      }

      const { worker, shutdownWorker } = yield* Effect.promise(() =>
        startWorker({
          syncServer: peer,
          accountID: webhookAccountID,
          accountSecret: webhookAccountSecret,
        }),
      );

      const registryID = process.env.JAZZ_WEBHOOK_REGISTRY_ID;

      if (!registryID) {
        throw new Error("JAZZ_WEBHOOK_REGISTRY_ID is not set");
      }

      const registry = yield* Effect.promise(() =>
        RegistryState.load(registryID),
      );

      const account = yield* Effect.promise(() => co.account().load(accountID));

      if (!account) {
        throw new Error("Account not found");
      }

      if (!registry) {
        throw new Error("Couldn't load registry with ID " + registryID);
      }

      registry.$jazz.owner.addMember(account, "writer");

      yield* Console.log(
        "Webhook registration rights granted to " +
          accountID +
          " for registry " +
          registryID,
      );

      yield* Effect.promise(() => shutdownWorker());
    });
  },
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

const webhookCreateRegistryCommand = Command.make(
  "create-registry",
  {
    peer: peerOption,
  },
  ({ peer }) => {
    return Effect.gen(function* () {
      const { accountID, agentSecret } = yield* Effect.promise(() =>
        createWorkerAccount({ name: "Webhook Worker", peer }),
      );

      yield* Console.log("# Credentials for Jazz webhook registry:");
      yield* Console.log("JAZZ_WEBHOOK_ACCOUNT=" + accountID);
      yield* Console.log("JAZZ_WEBHOOK_ACCOUNT_SECRET=" + agentSecret);

      const { worker, shutdownWorker } = yield* Effect.promise(() =>
        startWorker({
          syncServer: peer,
          accountID,
          accountSecret: agentSecret,
        }),
      );

      const registryGroup = co.group().create({ owner: worker });
      const registry = WebhookRegistry.createRegistry(registryGroup);

      yield* Console.log("JAZZ_WEBHOOK_REGISTRY_ID=" + registry.$jazz.id);
      yield* Effect.promise(() => shutdownWorker());
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
