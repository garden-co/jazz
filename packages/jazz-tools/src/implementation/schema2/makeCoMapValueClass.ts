import { RawCoMap } from "cojson";
import { ID, ItemsSym, Schema } from "../../internal.js";
import { ValueClassBase } from "./ValueClassBase.js";
import { CoMapDef, sym } from "./schema2.js";

export function makeCoMapValueClass<S extends CoMapDef<any>>(schema: S) {
  return class CoMapValueClass extends ValueClassBase {
    declare id: ID<typeof schema>;
    declare _type: "CoMap";
    declare _raw: RawCoMap;
    /** @category Internals */
    declare _instanceID: string;

    /** @internal */
    [sym.Schema]: S = schema;

    /**
     * If property `prop` is a `co.ref(...)`, you can use `coMaps._refs.prop` to access
     * the `Ref` instead of the potentially loaded/null value.
     *
     * This allows you to always get the ID or load the value manually.
     *
     * @example
     * ```ts
     * person._refs.pet.id; // => ID<Animal>
     * person._refs.pet.value;
     * // => Animal | null
     * const pet = await person._refs.pet.load();
     * ```
     *
     * @category Content
     **/
    get _refs(): {
      [Key in CoKeys<this>]: IfCo<this[Key], RefIfCoValue<this[Key]>>;
    } {
      return makeRefs<CoKeys<this>>(
        (key) => this._raw.get(key as string) as unknown as ID<CoValue>,
        () => {
          const keys = this._raw.keys().filter((key) => {
            const schema =
              this._schema[key as keyof typeof this._schema] ||
              (this._schema[ItemsSym] as Schema | undefined);
            return schema && schema !== "json" && isRefEncoded(schema);
          }) as CoKeys<this>[];

          return keys;
        },
        this._loadedAs,
        (key) =>
          (this._schema[key] || this._schema[ItemsSym]) as RefEncoded<CoValue>,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ) as any;
    }

    /** @internal */
    private getEditFromRaw(
      target: CoMap,
      rawEdit: {
        by: RawAccountID | AgentID;
        tx: CojsonInternalTypes.TransactionID;
        at: Date;
        value?: JsonValue | undefined;
      },
      descriptor: Schema,
      key: string,
    ) {
      return {
        value:
          descriptor === "json"
            ? rawEdit.value
            : "encoded" in descriptor
              ? rawEdit.value === null || rawEdit.value === undefined
                ? rawEdit.value
                : descriptor.encoded.decode(rawEdit.value)
              : new Ref(
                  rawEdit.value as ID<CoValue>,
                  target._loadedAs,
                  descriptor,
                ).accessFrom(target, "_edits." + key + ".value"),
        ref:
          descriptor !== "json" && isRefEncoded(descriptor)
            ? new Ref(
                rawEdit.value as ID<CoValue>,
                target._loadedAs,
                descriptor,
              )
            : undefined,
        by:
          rawEdit.by &&
          new Ref<Account>(rawEdit.by as ID<Account>, target._loadedAs, {
            ref: RegisteredSchemas["Account"],
            optional: false,
          }).accessFrom(target, "_edits." + key + ".by"),
        madeAt: rawEdit.at,
        key,
      };
    }

    /** @category Collaboration */
    get _edits() {
      const map = this;
      return new Proxy(
        {},
        {
          get(_target, key) {
            const rawEdit = map._raw.lastEditAt(key as string);
            if (!rawEdit) return undefined;

            const descriptor = map._schema[
              key as keyof typeof map._schema
            ] as Schema;

            return {
              ...map.getEditFromRaw(map, rawEdit, descriptor, key as string),
              get all() {
                return [...map._raw.editsAt(key as string)].map((rawEdit) =>
                  map.getEditFromRaw(map, rawEdit, descriptor, key as string),
                );
              },
            };
          },
          ownKeys(_target) {
            return map._raw.keys();
          },
          getOwnPropertyDescriptor(target, key) {
            return {
              value: Reflect.get(target, key),
              writable: false,
              enumerable: true,
              configurable: true,
            };
          },
        },
      ) as {
        [Key in CoKeys<this>]: IfCo<this[Key], LastAndAllCoMapEdits<this[Key]>>;
      };
    }
  };
}
