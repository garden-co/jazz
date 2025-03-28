import { z } from "zod";
import { CoMapRecordDef, CoMapSchemaClass } from "./coMap/schema.js";
import { CoMapSchemaShape } from "./coMap/schema.js";
import { lazy } from "./coValue/lazy.js";
import { optional } from "./coValue/optional.js";

function map<S extends CoMapSchemaShape>(schema: S) {
  return new CoMapSchemaClass(schema, undefined, false);
}

function record<
  K extends CoMapRecordDef["key"],
  V extends CoMapRecordDef["value"],
>(key: K, value: V) {
  return new CoMapSchemaClass({}, { key, value }, false);
}

export const co = {
  map,
  optional,
  record,
  lazy,
};

export { z };
export type { LoadedCoMapJazzProps as CoMap } from "./coMap/instance.js";
export type { CoValueSchema } from "./coMap/schema.js";
export type {
  ResolveQuery,
  ResolveQueryStrict,
  Loaded,
} from "./coValue/types.js";
