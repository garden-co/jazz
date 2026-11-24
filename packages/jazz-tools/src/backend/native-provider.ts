import type { JazzCoreSocket } from "jazz-napi";
import { randomBytes, randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { resolve } from "node:path";
import { files, type BlobStore } from "./blobs.js";
import { inProcessCore } from "./backend-native.js";
import { onVercelHost, vercelWebSocketUpgrade } from "./vercel-host.js";
import { newTokenSecret } from "./inspect.js";
import type { CoreSocket, JazzCore, JazzCoreOpenContext, JazzProvider } from "./core-provider.js";
import { logStoreOpened } from "./store-log.js";

class SqliteProvider implements JazzProvider {
  assertSupportedEnvironment() {
    inProcessCore(this.kind);
    const host = serverlessHost();
    if (host) {
      throw new Error(
        `sqlite({ path: ${JSON.stringify(this.path)} }) can't keep data on ${host}: its functions have no lasting disk. ` +
          (host === "Vercel"
            ? "Use vercel(), whose local store runs sqlite() in dev, or "
            : "Use ") +
          "remote({ url, appId, secret }) to a sync server or Jazz Cloud.",
      );
    }
  }
  readonly kind = "sqlite";
  constructor(
    readonly path: string,
    readonly blobs: BlobStore,
  ) {}
  get key() {
    return `sqlite:${resolve(this.path)}`;
  }
  reload(next: SqliteProvider) {
    if (!isDeepStrictEqual(this.blobs, next.blobs)) {
      throw new Error(
        `createJazz(): ${this.key} is already open in this process with other store settings; restart the process to change the blob store`,
      );
    }
  }
  open(context: JazzCoreOpenContext) {
    return openInProcessCore(this, context);
  }
}
class MemoryProvider implements JazzProvider {
  assertSupportedEnvironment() {
    inProcessCore(this.kind);
  }
  getWebSocketUpgrade(handler: "fetch" | "next") {
    return vercelWebSocketUpgrade(handler);
  }
  readonly kind = "memory";
  open(context: JazzCoreOpenContext) {
    return openInProcessCore(this, context);
  }
}

/**
 * A SQLite core in this process, kept in the directory `path`. It generates
 * its app id on first start and keeps it in its own file, `<path>/jazz.sqlite`,
 * with the rest of its identity, so a copy of that file is the same store
 * (alpha.65's `jazz-alpha59-alpha60-import` leaves an alpha.59 app's id in
 * `<path>/app-id`, which the first open takes over). To
 * open a copy beside the original, detach it first: `jazz-tools store detach`. Large
 * values' bytes go to `blobs`, by default `files()` in `<path>/blobs`.
 *
 * Only one process can open `path` at a time (it holds `<path>/jazz.lock`
 * while the core is open); a second one fails with "already open". It
 * refuses to start on serverless hosts with no lasting disk (Vercel, AWS
 * Lambda, Netlify): use `vercel()` or `remote()` there.
 */
export function sqlite(options: { path: string; blobs?: BlobStore }): SqliteProvider {
  refuseStoreAppId("sqlite", options);
  const blobs = options.blobs ?? files();
  if (
    blobs !== "sqlite" &&
    (typeof blobs !== "object" || (blobs.kind !== "files" && blobs.kind !== "s3"))
  ) {
    throw new Error('sqlite(): blobs must be files(), s3() or "sqlite"');
  }
  return new SqliteProvider(options.path, blobs);
}

/**
 * An in-memory core in this process: for tests and throwaway previews. Each
 * one is its own store, with a new app id.
 */
export function memory(options?: { appId?: never }): MemoryProvider {
  refuseStoreAppId("memory", options);
  return new MemoryProvider();
}

/** Stores keep their own app id; one passed from JavaScript is refused, not ignored. */
function refuseStoreAppId(store: string, options: unknown): void {
  if (typeof options === "object" && options !== null && "appId" in options) {
    throw new Error(
      `${store}(): appId is not an option: the store keeps its own app id. To keep an alpha.59 app's id, import its data with \`jazz-alpha59-alpha60-import import --app-id\` from the v2.0.0-alpha.65 release.`,
    );
  }
}

function blobOptions(blobs: BlobStore): {
  blobs: "files" | "s3" | "sqlite";
  blobsDir?: string;
  s3Bucket?: string;
  s3Region?: string;
  s3Endpoint?: string;
  s3Prefix?: string;
} {
  if (blobs === "sqlite") return { blobs: "sqlite" };
  if (blobs.kind === "s3") {
    return {
      blobs: "s3",
      ...(blobs.bucket === undefined ? {} : { s3Bucket: blobs.bucket }),
      ...(blobs.region === undefined ? {} : { s3Region: blobs.region }),
      ...(blobs.endpoint === undefined ? {} : { s3Endpoint: blobs.endpoint }),
      ...(blobs.prefix === undefined ? {} : { s3Prefix: blobs.prefix }),
    };
  }
  return { blobs: "files", ...(blobs.dir === undefined ? {} : { blobsDir: blobs.dir }) };
}

function nativeSocket(socket: JazzCoreSocket): CoreSocket {
  return {
    send: (data) => socket.sendWithRoom(data),
    close: () => socket.close(),
  };
}

async function openInProcessCore(
  store: SqliteProvider | MemoryProvider,
  context: JazzCoreOpenContext,
): Promise<JazzCore> {
  const backendSecret = randomBytes(32).toString("base64url");
  const adminSecret = randomBytes(32).toString("base64url");
  // A SQLite store keeps its app id in its own file: a new one on first
  // start, so the app's identities stay the same across restarts (and
  // restores of the file) without the app naming an id.
  const core = await inProcessCore(store.kind).open({
    ...(store.kind === "sqlite" ? {} : { appId: randomUUID() }),
    backendSecret,
    adminSecret,
    ...(store.kind === "sqlite"
      ? { dataDir: store.path, ...blobOptions(store.blobs) }
      : { inMemory: true }),
  });
  const connection = context.connect(
    {
      appId: core.appId,
      handle: (method, path, headers, body) =>
        core.handle(method, path, headers, Buffer.from(body)),
      openSocket: (_path, headers, onEvent) => nativeSocket(core.openSocket(headers, onEvent)),
      close: () => core.close(),
    },
    {
      adminSecret,
      backendSecret,
      openClientSocket: (onEvent) => nativeSocket(core.openInProcessSocket(onEvent)),
    },
  );
  logStoreOpened(
    store.kind === "sqlite"
      ? `jazz: store sqlite at ${store.path}`
      : "jazz: store memory (its data lasts as long as this process)",
  );
  const tokenSecret = store.kind === "memory" ? newTokenSecret() : core.tokenSecret;
  return { ...connection, appSecret: adminSecret, ...(tokenSecret == null ? {} : { tokenSecret }) };
}

/**
 * Serverless functions have no disk that outlives an instance (or none
 * writable at all), so a sqlite store there loses data silently.
 */
export function serverlessHost(): string | undefined {
  return onVercelHost()
    ? "Vercel"
    : process.env.AWS_LAMBDA_FUNCTION_NAME
      ? "AWS Lambda"
      : process.env.NETLIFY
        ? "Netlify"
        : undefined;
}
