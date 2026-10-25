/** Stable `code` of a storage-format refusal on every platform and binding. */
export const UNSUPPORTED_STORAGE_CODECS_ERROR_CODE = "unsupported_storage_codecs";

/**
 * The local store was written in a storage format this build does not read,
 * so opening the database was refused.
 *
 * There is no migration. Delete the local store and resync from Core (in the
 * browser, `db.deleteClientStorage()`; in Node, delete the data directory).
 * Writes in the old store that never reached Core are lost.
 *
 * Codec IDs carry their format version (`name.vN`): `missing` names formats
 * this build requires that the store lacks, and `unknown` names formats the
 * store was written with that this build does not read. A native SQLite root
 * refusal supplies `format` instead: the named layout the opener requires.
 * Its codec lists are empty because that refusal does not inspect old codecs.
 *
 * Class identity does not survive native bindings or browser worker
 * messages, so detect the refusal with {@link isUnsupportedStorageCodecsError},
 * which checks the stable `code`.
 */
export class UnsupportedStorageCodecsError extends Error {
  override readonly name = "UnsupportedStorageCodecsError";
  readonly code = UNSUPPORTED_STORAGE_CODECS_ERROR_CODE;
  readonly missing: readonly string[];
  readonly unknown: readonly string[];
  readonly format?: string;

  constructor(
    missing: readonly string[],
    unknown: readonly string[],
    message = `Unsupported storage format: this store lacks codec families ${JSON.stringify(missing)} ` +
      `required by this build and declares ${JSON.stringify(unknown)} that this build does not read`,
    options?: { cause?: unknown; format?: string },
  ) {
    super(message, options);
    this.missing = missing;
    this.unknown = unknown;
    if (options?.format !== undefined) this.format = options.format;
  }
}

/**
 * Whether `error` is a storage-format refusal, by its stable `code`, from any
 * platform: an {@link UnsupportedStorageCodecsError}, or an error a native
 * binding or worker relay threw with code `"unsupported_storage_codecs"`.
 */
export function isUnsupportedStorageCodecsError(error: unknown): error is Error & {
  readonly code: typeof UNSUPPORTED_STORAGE_CODECS_ERROR_CODE;
  readonly missing?: readonly string[];
  readonly unknown?: readonly string[];
  readonly format?: string;
} {
  if (!error || typeof error !== "object") return false;
  return (error as { code?: unknown }).code === UNSUPPORTED_STORAGE_CODECS_ERROR_CODE;
}

/**
 * Rebuild a storage-format refusal that crossed a binding or relay as an
 * {@link UnsupportedStorageCodecsError}, keeping its message and codec lists
 * or native storage format. Refusals without either complete codec lists or a
 * nonempty format identifier are returned unchanged.
 */
export function asUnsupportedStorageCodecsError(error: unknown): unknown {
  if (error instanceof UnsupportedStorageCodecsError) return error;
  if (!isUnsupportedStorageCodecsError(error)) return error;
  const { missing, unknown } = error;
  const format =
    typeof error.format === "string" && error.format.length > 0 ? error.format : undefined;
  const lists = isStringArray(missing) && isStringArray(unknown);
  // A format-only refusal has no codec lists. Malformed or partial lists
  // must not be silently replaced with empty ones merely because format exists.
  if (!lists && !(format !== undefined && missing === undefined && unknown === undefined))
    return error;
  return new UnsupportedStorageCodecsError(
    lists ? [...missing] : [],
    lists ? [...unknown] : [],
    error.message,
    { cause: error, ...(format === undefined ? {} : { format }) },
  );
}

function isStringArray(value: unknown): value is readonly string[] {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}
