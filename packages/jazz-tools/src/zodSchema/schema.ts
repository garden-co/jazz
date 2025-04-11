import { z } from "zod";
import { CoListSchemaClass } from "./coList/schema.js";
import { CoListItem } from "./coList/schema.js";
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

function list<I extends CoListItem>(items: I) {
  return new CoListSchemaClass(items, false);
}

export const co = {
  map,
  optional,
  record,
  lazy,
  list,
};

export { z };
export type { LoadedCoMapJazzProps as CoMap } from "./coMap/instance.js";
export type { CoValueSchema } from "./types.js";
export type {
  ResolveQuery,
  ResolveQueryStrict,
  Loaded,
  Unloaded,
  MaybeLoaded,
} from "./coValue/types.js";
export type { ID } from "./coValue/types.js";
