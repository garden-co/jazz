import { mkdir, readFile, readdir, rename, rm, unlink, writeFile } from "node:fs/promises";
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import {
  PrivateStoreConflictError,
  PrivateStoreWipedError,
  type PrivateStore,
} from "jazz-tools/backend";

/** `machineKey` is 32 bytes from your operating system's key store. */
export function directoryStore(dir: string, machineKey: Buffer): PrivateStore {
  const at = (scope: string) => {
    if (!scope || scope === "." || scope === "..") throw new Error("Invalid scope name");
    return `${dir}/${encodeURIComponent(scope)}`;
  };
  const docPath = (scope: string, doc: string) => {
    if (!doc || doc.startsWith(".")) throw new Error("Invalid document name");
    return `${at(scope)}/${encodeURIComponent(doc)}`;
  };
  const marker = (scope: string) => `${at(scope)}/.incarnation`;
  const isMissing = (error: unknown) => (error as NodeJS.ErrnoException).code === "ENOENT";
  const incarnationOf = async (scope: string) => {
    try {
      return BigInt(await readFile(marker(scope), "utf8"));
    } catch (error) {
      if (isMissing(error)) return null;
      throw error;
    }
  };
  const read = async (scope: string, doc: string) => {
    try {
      const text = await readFile(docPath(scope, doc), "utf8");
      const { revision, bytes } = JSON.parse(text);
      return { revision: BigInt(revision), bytes: new Uint8Array(Buffer.from(bytes, "base64")) };
    } catch (error) {
      // Only a missing file means "absent"; any other error must not read as a new device.
      if (isMissing(error)) return null;
      throw error;
    }
  };
  let queue: Promise<unknown> = Promise.resolve();
  // One operation at a time: a save checks and replaces atomically.
  const serial = <T>(work: () => Promise<T>) => (queue = queue.then(work, work)) as Promise<T>;
  return {
    load: (scope, doc) =>
      serial(async () => ((await incarnationOf(scope)) === null ? null : read(scope, doc))),
    openScope: (scope) =>
      serial(async () => {
        const current = await incarnationOf(scope);
        if (current !== null) return current;

        await rm(at(scope), { recursive: true, force: true });
        await mkdir(at(scope), { recursive: true, mode: 0o700 });
        const incarnation = randomBytes(8).readBigUInt64BE() || 1n;
        const pending = `${marker(scope)}.tmp`;
        await writeFile(pending, incarnation.toString(), { mode: 0o600 });
        await rename(pending, marker(scope));
        return incarnation;
      }),
    save: (scope, incarnation, doc, bytes, expected) =>
      serial(async () => {
        if ((await incarnationOf(scope)) !== incarnation) throw new PrivateStoreWipedError();
        const current = await read(scope, doc);
        if ((current?.revision ?? null) !== expected) throw new PrivateStoreConflictError();
        const revision = (current?.revision ?? 0n) + 1n;
        const body = JSON.stringify({
          revision: revision.toString(),
          bytes: Buffer.from(bytes).toString("base64"),
        });
        const path = docPath(scope, doc);
        const pending = `${at(scope)}/.document.tmp`;
        await writeFile(pending, body, { mode: 0o600 });
        await rename(pending, path);
        return revision;
      }),
    list: (scope) =>
      serial(async () => {
        if ((await incarnationOf(scope)) === null) return [];
        return (await readdir(at(scope)))
          .filter((name) => !name.startsWith("."))
          .map(decodeURIComponent);
      }),
    delete: (scope) =>
      serial(async () => {
        await unlink(marker(scope)).catch((error) => {
          if (!isMissing(error)) throw error;
        });
        await rm(at(scope), { recursive: true, force: true });
      }),
    async protect(secret) {
      const iv = randomBytes(12);
      const cipher = createCipheriv("aes-256-gcm", machineKey, iv);
      const sealed = Buffer.concat([cipher.update(secret), cipher.final()]);
      return new Uint8Array(Buffer.concat([iv, cipher.getAuthTag(), sealed]));
    },
    async unprotect(blob) {
      const bytes = Buffer.from(blob);
      const decipher = createDecipheriv("aes-256-gcm", machineKey, bytes.subarray(0, 12));
      decipher.setAuthTag(bytes.subarray(12, 28));
      return new Uint8Array(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]));
    },
  };
}
