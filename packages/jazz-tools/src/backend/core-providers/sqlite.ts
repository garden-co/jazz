import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { onVercelHost } from "./vercel-store.js";
import { files, type BlobStore } from "../blobs.js";
import type { JazzCoreProvider } from "./core-provider.js";
import { openInProcessCore } from "./in-process.js";
/**
 * A SQLite core in this process, kept in the directory `path`. It generates
 * its app id on first start and keeps it beside its data. Large values' bytes go to `blobs`,
 * by default `files()` in `<path>/blobs`.
 */
function sqliteConfig(options: { path: string; appId?: never; blobs?: BlobStore }): {
  path: string;
  appId?: never;
  blobs: BlobStore;
} {
  refuseStoreAppId("sqlite", options);
  const blobs = options.blobs ?? files();
  if (
    blobs !== "sqlite" &&
    (typeof blobs !== "object" || (blobs.kind !== "files" && blobs.kind !== "s3"))
  ) {
    throw new Error('sqlite(): blobs must be files(), s3() or "sqlite"');
  }
  return {
    path: options.path,
    blobs,
  };
}

/** Open a persistent SQLite core in this process. */
export function sqlite(options: Parameters<typeof sqliteConfig>[0]): JazzCoreProvider {
  const config = sqliteConfig(options);
  return {
    reuse: { key: `sqlite:${resolve(config.path)}`, settings: config },
    async open() {
      // The core holds `<path>/jazz.lock` with an OS lock while it's open.
      refuseServerlessDisk(config.path);
      const appId = await keptAppId(config.path);
      return openInProcessCore({
        appId,
        dataDir: config.path,
        ...blobOptions(config.blobs),
      });
    },
  };
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

/**
 * The app id an in-process SQLite core keeps beside its data, in
 * `<path>/app-id`: a new one on first start, so the app's
 * identities stay the same across restarts without the app naming an id.
 */
async function keptAppId(dir: string): Promise<string> {
  const file = join(dir, "app-id");
  const kept = async (): Promise<string | undefined> => {
    try {
      return (await readFile(file, "utf8")).trim() || undefined;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      return undefined;
    }
  };
  let appId = await kept();
  if (appId === undefined) {
    await mkdir(dir, { recursive: true });
    // `wx`: two processes starting the same store at once agree on one id.
    try {
      appId = randomUUID();
      await writeFile(file, `${appId}\n`, { flag: "wx" });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      appId = (await kept())!;
    }
  }
  return appId;
}

/**
 * Serverless functions have no disk that outlives an instance (or none
 * writable at all), so a sqlite store there loses data silently.
 */
function refuseServerlessDisk(dir: string): void {
  const host = onVercelHost()
    ? "Vercel"
    : process.env.AWS_LAMBDA_FUNCTION_NAME
      ? "AWS Lambda"
      : process.env.NETLIFY
        ? "Netlify"
        : undefined;
  if (!host) return;
  throw new Error(
    `sqlite({ path: ${JSON.stringify(dir)} }) can't keep data on ${host}: its functions have no lasting disk. ` +
      "Use remote({ url, appId, secret }) to a sync server or Jazz Cloud.",
  );
}

export function refuseStoreAppId(store: string, options: unknown): void {
  if (typeof options === "object" && options !== null && "appId" in options) {
    throw new Error(
      `${store}(): appId is not an option: the store keeps its own app id. To keep an alpha.59 app's id, import its data with \`jazz-alpha59-alpha60-import import --app-id\`.`,
    );
  }
}
