/**
 * The types a keyless backend reads an encrypted app with. The generated
 * types are the clients': an encrypted column has its plaintext type
 * (`title: string`). A backend holds no keys, so its reads never carry that
 * column and it may not write, filter or order by one; {@link keyless} gives
 * it the app typed that way, from the same generated bindings.
 *
 * ```ts
 * import { app } from "./jazz.generated.js";
 * const backendApp = keyless(app);
 * const tasks = await jazz.db.all(backendApp.tasks); // task.title: undefined
 * ```
 */
import type { App, SchemaLike, Simplify } from "../typed-app.js";

/** The columns of a generated table types entry marked `encrypted: true`. */
export type EncryptedColumnName<TTable> = TTable extends { readonly columns: infer C }
  ? Extract<
      { [K in keyof C]-?: C[K] extends { readonly encrypted: true } ? K : never }[keyof C],
      string
    >
  : never;

/** `TRow` as a keyless backend reads it: each column in `TEncrypted` is absent (`?: undefined`). */
export type KeylessRow<TRow, TEncrypted extends string> = [TEncrypted] extends [never]
  ? TRow
  : Simplify<Omit<TRow, TEncrypted> & { [K in TEncrypted & keyof TRow]?: undefined }>;

type KeylessTable<TTable> = [EncryptedColumnName<TTable>] extends [never]
  ? TTable
  : TTable extends {
        readonly row: infer TRow;
        readonly insert: infer TInsert;
        readonly columns: infer TColumns;
      }
    ? Simplify<
        Omit<TTable, "row" | "insert" | "columns"> & {
          row: KeylessRow<TRow, EncryptedColumnName<TTable>>;
          // Written, filtered and ordered by clients only: the backend refuses them.
          insert: Simplify<Omit<TInsert, EncryptedColumnName<TTable>>>;
          columns: Simplify<Omit<TColumns, EncryptedColumnName<TTable>>>;
        }
      >
    : TTable;

/** An app's generated `Types` as a keyless backend sees them. */
export type KeylessTypes<TTypes extends SchemaLike> = {
  [K in keyof TTypes]: KeylessTable<TTypes[K]>;
};

/** `TApp` typed for a keyless backend; an app without encrypted columns is unchanged. */
export type KeylessApp<TApp> = TApp extends { readonly "~types"?: infer TTypes }
  ? [Exclude<TTypes, undefined>] extends [infer T extends SchemaLike]
    ? [{ [K in keyof T]: EncryptedColumnName<T[K]> }[keyof T]] extends [never]
      ? TApp
      : App<KeylessTypes<T>>
    : TApp
  : TApp;

/**
 * `app` typed for a keyless backend: each encrypted column is absent from
 * its rows (`?: undefined`), its inserts and its filters. The same object at
 * runtime; clients keep using `app` itself.
 */
export function keyless<TApp>(app: TApp): KeylessApp<TApp> {
  return app as KeylessApp<TApp>;
}
