export interface CatalogueCacheScope {
  readonly registryAuthority: string;
  readonly appId: string;
  readonly environment: string;
}

export type CatalogueReplacementValidator = (previous: Uint8Array, next: Uint8Array) => void;

/** Account-independent host storage; the capture is an opaque, core-validated versioned encoding. */
export interface CatalogueCache {
  load(scope: CatalogueCacheScope): Promise<Uint8Array | null>;
  publish(
    scope: CatalogueCacheScope,
    authenticatedCapture: Uint8Array,
    validateReplacement: CatalogueReplacementValidator,
  ): Promise<void>;
}

export function catalogueCacheKey(scope: CatalogueCacheScope): string {
  if (!scope.registryAuthority || !scope.appId || !scope.environment)
    throw new Error("Catalogue cache requires an exact registry/application/environment scope");
  return JSON.stringify([scope.registryAuthority, scope.appId, scope.environment]);
}

export function encodeCatalogueCache(
  scope: CatalogueCacheScope,
  authenticatedCapture: Uint8Array,
): string {
  if (!authenticatedCapture.length) throw new Error("Empty authenticated catalogue capture");
  let binary = "";
  for (const byte of authenticatedCapture) binary += String.fromCharCode(byte);
  return JSON.stringify({
    format: "jazz-authenticated-catalogue",
    version: 1,
    scope: catalogueCacheKey(scope),
    capture: btoa(binary),
  });
}

export function decodeCatalogueCache(scope: CatalogueCacheScope, encoded: string): Uint8Array {
  const envelope = JSON.parse(encoded);
  if (
    envelope?.format !== "jazz-authenticated-catalogue" ||
    envelope.version !== 1 ||
    envelope.scope !== catalogueCacheKey(scope) ||
    typeof envelope.capture !== "string" ||
    !envelope.capture
  )
    throw new Error("Invalid or wrong-scope authenticated catalogue cache");
  const binary = atob(envelope.capture);
  if (btoa(binary) !== envelope.capture) throw new Error("Noncanonical catalogue cache encoding");
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

/** Explicitly ephemeral: no process-survival guarantee. */
export class EphemeralCatalogueCache implements CatalogueCache {
  private readonly entries = new Map<string, string>();
  async load(scope: CatalogueCacheScope): Promise<Uint8Array | null> {
    const value = this.entries.get(catalogueCacheKey(scope));
    return value === undefined ? null : decodeCatalogueCache(scope, value);
  }
  async publish(
    scope: CatalogueCacheScope,
    capture: Uint8Array,
    validateReplacement: CatalogueReplacementValidator,
  ): Promise<void> {
    const previous = this.entries.get(catalogueCacheKey(scope));
    if (previous !== undefined) validateReplacement(decodeCatalogueCache(scope, previous), capture);
    this.entries.set(catalogueCacheKey(scope), encodeCatalogueCache(scope, capture));
  }
}

/** One host lifetime, shared across account roots but never persisted. */
export const ephemeralCatalogueCache = new EphemeralCatalogueCache();

/** Separate IDB root, deliberately unaffected by account logout or account-row reset. */
export class BrowserCatalogueCache implements CatalogueCache {
  private async open(): Promise<IDBDatabase> {
    // Keep the package's ES2022 host support; Promise.withResolvers is ES2024.
    return new Promise((resolve, reject) => {
      const request = indexedDB.open("jazz-authenticated-catalogue-v1", 1);
      let blocked = false;
      request.onupgradeneeded = () => request.result.createObjectStore("catalogues");
      request.onerror = () => reject(request.error ?? new Error("Catalogue cache open failed"));
      request.onblocked = () => {
        blocked = true;
        reject(new Error("Catalogue cache open blocked"));
      };
      request.onsuccess = () => {
        if (blocked) request.result.close();
        else resolve(request.result);
      };
    });
  }
  async load(scope: CatalogueCacheScope): Promise<Uint8Array | null> {
    const db = await this.open();
    try {
      const value = await new Promise<unknown>((resolve, reject) => {
        const transaction = db.transaction("catalogues", "readonly");
        const request = transaction.objectStore("catalogues").get(catalogueCacheKey(scope));
        transaction.oncomplete = () => resolve(request.result);
        transaction.onabort = () =>
          reject(transaction.error ?? new Error("Catalogue cache read aborted"));
        transaction.onerror = () =>
          reject(transaction.error ?? new Error("Catalogue cache read failed"));
      });
      if (value === undefined) return null;
      if (typeof value !== "string") throw new Error("Malformed catalogue cache value");
      return decodeCatalogueCache(scope, value);
    } finally {
      db.close();
    }
  }
  async publish(
    scope: CatalogueCacheScope,
    capture: Uint8Array,
    validateReplacement: CatalogueReplacementValidator,
  ): Promise<void> {
    const encoded = encodeCatalogueCache(scope, capture);
    const db = await this.open();
    try {
      await new Promise<void>((resolve, reject) => {
        const transaction = db.transaction("catalogues", "readwrite", { durability: "strict" });
        transaction.oncomplete = () => resolve();
        transaction.onabort = () =>
          reject(transaction.error ?? new Error("Catalogue cache write aborted"));
        transaction.onerror = () =>
          reject(transaction.error ?? new Error("Catalogue cache write failed"));
        const store = transaction.objectStore("catalogues");
        const previous = store.get(catalogueCacheKey(scope));
        previous.onsuccess = () => {
          try {
            if (previous.result !== undefined) {
              if (typeof previous.result !== "string")
                throw new Error("Malformed catalogue cache value");
              validateReplacement(decodeCatalogueCache(scope, previous.result), capture);
            }
            store.put(encoded, catalogueCacheKey(scope));
          } catch (error) {
            transaction.abort();
            reject(error);
          }
        };
      });
    } finally {
      db.close();
    }
  }
}
