import { z } from "zod";
import { CoMapRecordDef, CoMapSchemaClass } from "./coMap/schema.js";
import { CoMapSchemaShape } from "./coMap/schema.js";
import { optional } from "./coValue/optional.js";
import { self } from "./coValue/self.js";

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
  self,
  optional,
  record,
};

export { z };
export type { CoMap } from "./coMap/instance.js";
export type { CoValueSchema } from "./coMap/schema.js";
export type {
  ResolveQuery,
  ResolveQueryStrict,
  Loaded,
} from "./coValue/types.js";
