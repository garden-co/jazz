import { waitForLock, unlock } from "fs-native-extensions";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import {
  catalogueCacheKey,
  decodeCatalogueCache,
  encodeCatalogueCache,
  type CatalogueCache,
  type CatalogueCacheScope,
  type CatalogueReplacementValidator,
} from "./catalogue-cache.js";

/** Atomic replacement plus file/directory fsync before reporting durable publication. */
export class NodeCatalogueCache implements CatalogueCache {
  constructor(private readonly directory: string) {}

  async load(scope: CatalogueCacheScope): Promise<Uint8Array | null> {
    const file = join(
      this.directory,
      `${createHash("sha256").update(catalogueCacheKey(scope)).digest("hex")}.json`,
    );
    let encoded: string;
    try {
      encoded = await readFile(file, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    }
    return decodeCatalogueCache(scope, encoded);
  }

  async publish(
    scope: CatalogueCacheScope,
    capture: Uint8Array,
    validateReplacement: CatalogueReplacementValidator,
  ): Promise<void> {
    const encoded = encodeCatalogueCache(scope, capture);
    const created = await mkdir(this.directory, { recursive: true, mode: 0o700 });
    if (created) {
      const parent = dirname(resolve(created));
      for (let path = resolve(this.directory); ; path = dirname(path)) {
        const handle = await open(path, "r");
        try {
          await handle.sync();
        } finally {
          await handle.close();
        }
        if (path === parent) break;
      }
    }
    // Stable inode: process death releases the kernel lock, never a stale lockfile heuristic.
    const lock = await open(join(this.directory, "catalogue.lock"), "a+", 0o600);
    let locked = false;
    const file = join(
      this.directory,
      `${createHash("sha256").update(catalogueCacheKey(scope)).digest("hex")}.json`,
    );
    const temporary = `${file}.${randomUUID()}.tmp`;
    try {
      await waitForLock(lock.fd);
      locked = true;
      const previous = await this.load(scope);
      if (previous) validateReplacement(previous, capture);
      const handle = await open(temporary, "wx", 0o600);
      try {
        await handle.writeFile(encoded, "utf8");
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(temporary, file);
      const directory = await open(this.directory, "r");
      try {
        await directory.sync();
      } finally {
        await directory.close();
      }
    } finally {
      try {
        await unlink(temporary).catch((error: NodeJS.ErrnoException) => {
          if (error.code !== "ENOENT") throw error;
        });
      } finally {
        try {
          if (locked) unlock(lock.fd);
        } finally {
          await lock.close();
        }
      }
    }
  }
}
